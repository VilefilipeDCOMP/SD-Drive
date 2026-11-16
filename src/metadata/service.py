from dataclasses import dataclass, replace, asdict
from contextlib import contextmanager
from metadata.store import MetadataStore
from decimal import Decimal
from itertools import combinations
from pathlib import PurePosixPath
from threading import RLock
import time
import copy
import logging
import json
from datetime import datetime, timezone
from typing import Callable
from uuid import UUID, uuid4

import grpc
from google.protobuf.empty_pb2 import Empty
import dcomp_drive_pb2 as pb
import dcomp_drive_pb2_grpc as rpc
from common.ports import NodeRegistryPort

class AllocationError(Exception):
    def __init__(self, code: grpc.StatusCode, details: str):
        super().__init__(details)
        self.code = code

@dataclass(frozen=True)
class PendingUpload:
    fingerprint: bytes
    response: bytes
    expires_at_ms: int
    path: str

class MetadataService(rpc.MetadataServiceServicer):
    def __init__(self, registry: NodeRegistryPort, *, replication_factor: int = 3, write_quorum: int | str = "all", max_usage_fraction: float = 0.90, lease_seconds: int = 900, clock: Callable[[], float] = time.time, max_lifetime_seconds: int = 14400, db_path: str | None = None):
        if type(replication_factor) is not int or replication_factor < 1:
            raise ValueError("Fator de replicação deve ser inteiro positivo")

        if write_quorum == "all":
            quorum = replication_factor
        else:
            quorum = write_quorum

        if type(quorum) is not int or not 1 <= quorum <= replication_factor:
            raise ValueError("Quórum de escrita deve ser 'all' ou inteiro entre 1 e N")

        if not 0 < max_usage_fraction <= 1 or lease_seconds <= 0 or max_lifetime_seconds <= 0:
            raise ValueError("Ocupação e duração do lease inválidas")

        self._registry = registry
        self._n = replication_factor
        self._w = quorum
        self._max_usage = Decimal(str(max_usage_fraction))
        self._lease_seconds = lease_seconds
        self._clock = clock
        self._max_lifetime_seconds = max_lifetime_seconds
        self._upload_started = {}

        self._lock = RLock()
        self._operations: dict[str, PendingUpload] = {}
        self._paths: dict[str, PendingUpload] = {}
        self._reserved: dict[str, int] = {}
        self._upload_reservations: dict[str, dict[str, int]] = {}
        self._block_ids: set[str] = set()
        self._files: dict[str, bytes] = {}
        self._committed_receipts = {}
        self._upload_results = {}
        self._aborted = set()
        self._directories = {"/"}
        self._invalid_replicas = set()
        self._original_receipts = {}
        self._block_reservations = {}
        self._gc_confirmed = set()
        self._recovering_blocks = set()
        self._store = MetadataStore(db_path) if db_path is not None else None
        self._transaction_depth = 0
        if self._store is not None:
            stored = self._store.load()
            if stored is not None:
                self._restore(stored)

    def AllocateUpload(self, request, context):
        try:
            return self.allocate_upload(request)
        except AllocationError as exc:
            context.abort(exc.code, str(exc))

    def allocate_upload(self, request: pb.AllocateUploadRequest) -> pb.AllocateUploadResponse:
        with self._transaction("allocate_upload"):
            req = self._prepare_request(request)
            fingerprint = req.SerializeToString(deterministic=True)
            now_ms = int(self._clock() * 1000)
            self._release_expired_uploads(now_ms)

            previous_response = self._find_previous_upload(req, fingerprint, now_ms)
            if previous_response is not None:
                return previous_response

            self._check_upload_conflicts(req)
            placements, reservations = self._allocate_chunks(req)

            current = self._files.get(req.path)
            version = 1
            file_id = str(uuid4())
            if current is not None:
                previous = pb.FileMetadata.FromString(current)
                version = previous.version + 1
                file_id = previous.file_id

            response = pb.AllocateUploadResponse(upload_id=str(uuid4()), file_id=file_id, version=version, replication_factor=self._n, write_quorum=self._w, chunks=placements, lease_expires_at_unix_ms=now_ms + min(self._lease_seconds, self._max_lifetime_seconds) * 1000)

            self._register_upload(req, fingerprint, response, reservations)
            return response

    def CommitUpload(self, request, context):
        try:
            return self.commit_upload(request)
        except AllocationError as exc:
            context.abort(exc.code, str(exc))

    def commit_upload(self, request: pb.CommitUploadRequest) -> pb.FileMetadata:
        req = pb.CommitUploadRequest()
        req.CopyFrom(request)

        with self._transaction("commit_upload"):
            now_ms = int(self._clock() * 1000)
            self._release_expired_uploads(now_ms)
            pending, allocation = self._find_upload(req.upload_id)

            if req.upload_id not in self._committed_receipts and pending.expires_at_ms <= now_ms:
                raise AllocationError(grpc.StatusCode.ABORTED, "Lease expirado")

            if req.upload_id in self._aborted:
                raise AllocationError(grpc.StatusCode.ABORTED, "Upload cancelado")

            manifest = pb.AllocateUploadRequest.FromString(pending.fingerprint)
            confirmed = self._validate_receipts(manifest, allocation, req.receipts)
            previous = self._original_receipts.get(req.upload_id, self._committed_receipts.get(req.upload_id))

            if previous is not None:
                if confirmed != previous:
                    raise AllocationError(grpc.StatusCode.FAILED_PRECONDITION, "Upload já confirmado com recibos diferentes")
                return pb.FileMetadata.FromString(self._upload_results[req.upload_id])

            if self._paths.get(pending.path) is not pending:
                raise AllocationError(grpc.StatusCode.ABORTED, "Upload não possui mais a reserva do caminho")

            current = self._files.get(pending.path)
            current_version = 0
            created_at = now_ms
            if current is not None:
                previous_metadata = pb.FileMetadata.FromString(current)
                current_version = previous_metadata.version
                created_at = previous_metadata.created_at_unix_ms
            if current_version != allocation.version - 1:
                raise AllocationError(grpc.StatusCode.ABORTED, "Versão alterada durante o upload")

            replication_state = self._commit_replication_state(confirmed, allocation.replication_factor)
            now_ms = int(self._clock() * 1000)
            if pending.expires_at_ms <= now_ms:
                raise AllocationError(grpc.StatusCode.ABORTED, "Lease expirado")

            metadata = pb.FileMetadata(file_id=allocation.file_id, path=manifest.path, version=allocation.version, plaintext_size_bytes=manifest.plaintext_size_bytes, plaintext_sha256=manifest.plaintext_sha256, encryption=manifest.encryption, state=pb.FILE_COMMITTED, replication_state=replication_state, desired_replica_count=allocation.replication_factor, created_at_unix_ms=created_at, updated_at_unix_ms=now_ms)

            self._files[pending.path] = metadata.SerializeToString()
            self._upload_results[req.upload_id] = metadata.SerializeToString()
            self._committed_receipts[req.upload_id] = confirmed
            del self._paths[pending.path]
            # A capacidade continua contabilizada; o lease não deve liberar blocos confirmados.
            del self._upload_reservations[manifest.operation_id]
            return metadata

    def _find_upload(self, upload_id):
        for pending in self._operations.values():
            allocation = pb.AllocateUploadResponse.FromString(pending.response)
            if allocation.upload_id == upload_id:
                return pending, allocation

        raise AllocationError(grpc.StatusCode.FAILED_PRECONDITION, "Upload não encontrado")

    def _validate_receipts(self, manifest, allocation, receipts):
        chunks = {}
        targets = {}
        confirmed = {}

        for chunk in manifest.chunks:
            chunks[chunk.block_id] = chunk
            confirmed[chunk.block_id] = {}

        for placement in allocation.chunks:
            targets[placement.block_id] = set()
            for node in placement.targets:
                targets[placement.block_id].add(node.node_id)

        for receipt in receipts:
            chunk = chunks.get(receipt.block_id)
            if chunk is None:
                raise AllocationError(grpc.StatusCode.FAILED_PRECONDITION, "Recibo de bloco não pertencente ao upload")
            if receipt.node_id not in targets[receipt.block_id]:
                raise AllocationError(grpc.StatusCode.FAILED_PRECONDITION, "Recibo de nó não previsto na alocação")
            if receipt.stored_size_bytes != chunk.stored_size_bytes:
                raise AllocationError(grpc.StatusCode.FAILED_PRECONDITION, "Tamanho do recibo difere do manifesto")
            if receipt.verified_storage_sha256 != chunk.storage_sha256:
                raise AllocationError(grpc.StatusCode.FAILED_PRECONDITION, "SHA-256 do recibo difere do manifesto")

            confirmed[receipt.block_id][receipt.node_id] = receipt.SerializeToString()

        for replicas in confirmed.values():
            if len(replicas) < allocation.write_quorum:
                raise AllocationError(grpc.StatusCode.FAILED_PRECONDITION, "Quórum de escrita insuficiente em um dos chunks")

        return confirmed

    def _commit_replication_state(self, confirmed, replica_count):
        state = pb.REPLICATION_HEALTHY
        for replicas in confirmed.values():
            accessible = 0
            for node_id in replicas:
                try:
                    node_state = self._registry.get_node_state(node_id)
                except KeyError:
                    continue
                if node_state in (pb.NODE_ALIVE, pb.NODE_DRAINING):
                    accessible += 1

            if accessible == 0:
                return pb.REPLICATION_UNAVAILABLE
            if accessible < replica_count:
                state = pb.REPLICATION_UNDER_REPLICATED

        return state

    def ResolveDownload(self, request, context):
        try:
            return self.resolve_download(request)
        except AllocationError as exc:
            context.abort(exc.code, str(exc))

    def resolve_download(self, request: pb.ResolveDownloadRequest) -> pb.ResolveDownloadResponse:
        path = request.path
        with self._transaction("resolve_download"):
            self._validate_path(path)
            stored = self._files.get(path)
            if stored is None:
                raise AllocationError(grpc.StatusCode.NOT_FOUND, "Arquivo não encontrado")

            metadata = pb.FileMetadata.FromString(stored)
            if metadata.state != pb.FILE_COMMITTED:
                raise AllocationError(grpc.StatusCode.NOT_FOUND, "Arquivo não encontrado")

            manifest, allocation, confirmed = self._find_committed_upload(metadata)
            chunks, replication_state = self._build_download_chunks(manifest, allocation, confirmed)
            metadata.replication_state = replication_state
            return pb.ResolveDownloadResponse(file=metadata, chunks=chunks)

    def _find_committed_upload(self, metadata):
        for pending in self._operations.values():
            allocation = pb.AllocateUploadResponse.FromString(pending.response)
            if allocation.file_id != metadata.file_id or allocation.version != metadata.version:
                continue

            confirmed = self._committed_receipts.get(allocation.upload_id)
            if confirmed is not None:
                manifest = pb.AllocateUploadRequest.FromString(pending.fingerprint)
                return manifest, allocation, confirmed

        raise AllocationError(grpc.StatusCode.FAILED_PRECONDITION, "Arquivo confirmado sem manifesto correspondente")

    def _build_download_chunks(self, manifest, allocation, confirmed):
        placements = {}
        for placement in allocation.chunks:
            placements[placement.block_id] = placement

        chunks = []
        replication_state = pb.REPLICATION_HEALTHY
        recovering = False
        for chunk in sorted(manifest.chunks, key=lambda chunk: chunk.index):
            location = pb.ChunkLocation(manifest=chunk)
            for target in placements[chunk.block_id].targets:
                if target.node_id not in confirmed[chunk.block_id] or f"{chunk.block_id}:{target.node_id}" in self._invalid_replicas:
                    continue

                try:
                    node = self._registry.get_node_capacity(target.node_id)
                except KeyError:
                    continue
                if node.state not in (pb.NODE_ALIVE, pb.NODE_DRAINING):
                    continue

                address = pb.NodeAddress(node_id=node.node_id, machine_id=node.machine_id, disk_id=node.disk_id, grpc_target=node.grpc_target)
                location.replicas.append(pb.ReplicaLocation(node=address, healthy=True))

            if len(location.replicas) == 0:
                replication_state = pb.REPLICATION_UNAVAILABLE
            elif len(location.replicas) < allocation.replication_factor and replication_state != pb.REPLICATION_UNAVAILABLE:
                replication_state = pb.REPLICATION_UNDER_REPLICATED

            if 0 < len(location.replicas) < allocation.replication_factor and chunk.block_id in self._recovering_blocks:
                recovering = True
            chunks.append(location)

        if replication_state == pb.REPLICATION_UNDER_REPLICATED and recovering:
            replication_state = pb.REPLICATION_RECOVERING
        return chunks, replication_state

    def _prepare_request(self, request):
        req = pb.AllocateUploadRequest()
        req.CopyFrom(request)
        self._validate(req)

        exclusions = set()
        for value in req.excluded_node_ids:
            exclusions.add(str(UUID(value)))

        del req.excluded_node_ids[:]
        req.excluded_node_ids.extend(sorted(exclusions))
        return req

    def _find_previous_upload(self, req, fingerprint, now_ms):
        previous = self._operations.get(req.operation_id)
        if previous is None:
            return None

        if previous.fingerprint != fingerprint:
            raise AllocationError(grpc.StatusCode.INVALID_ARGUMENT, 'operation_id reutilizado com pedido diferente')

        allocation = pb.AllocateUploadResponse.FromString(previous.response)
        if allocation.upload_id in self._aborted:
            raise AllocationError(grpc.StatusCode.ABORTED, "Upload cancelado")

        if previous.expires_at_ms <= now_ms:
            raise AllocationError(grpc.StatusCode.ABORTED, "Lease expirado")

        return pb.AllocateUploadResponse.FromString(previous.response)

    def _check_upload_conflicts(self, req):
        if req.path in self._directories:
            raise AllocationError(grpc.StatusCode.ALREADY_EXISTS, "Caminho é um diretório")

        if req.path in self._paths:
            raise AllocationError(grpc.StatusCode.ABORTED, "Caminho com upload em andamento")

        current_version = 0
        if req.path in self._files:
            current = pb.FileMetadata.FromString(self._files[req.path])
            current_version = current.version
            if current.state == pb.FILE_COMMITTED and not req.overwrite:
                raise AllocationError(grpc.StatusCode.ALREADY_EXISTS, "Arquivo já existe")

        if req.HasField("expected_version") and req.expected_version != current_version:
            raise AllocationError(grpc.StatusCode.ABORTED, "Versão esperada não corresponde à versão atual")

        for chunk in req.chunks:
            if chunk.block_id in self._block_ids:
                raise AllocationError(grpc.StatusCode.INVALID_ARGUMENT, "block_id já utilizado")

    def _allocate_chunks(self, req):
        candidates = sorted(self._registry.list_eligible_nodes(), key=lambda node: node.node_id)
        reservations = self._reserved.copy()
        placements = []

        for chunk in req.chunks:
            eligible = self._filter_eligible_nodes(candidates, req.excluded_node_ids, chunk.stored_size_bytes, reservations)
            targets = self._select_targets(eligible)

            addresses = []
            for node in targets:
                address = pb.NodeAddress(node_id=node.node_id, machine_id=node.machine_id, disk_id=node.disk_id, grpc_target=node.grpc_target)
                addresses.append(address)

                reserved = reservations.get(node.node_id, 0)
                reservations[node.node_id] = reserved + chunk.stored_size_bytes

            placement = pb.ChunkPlacement(block_id=chunk.block_id, index=chunk.index, targets=addresses)
            placements.append(placement)

        return placements, reservations

    def _filter_eligible_nodes(self, candidates, exclusions, size, reservations):
        eligible = []
        for node in candidates:
            if node.state != pb.NODE_ALIVE:
                continue
            if node.node_id in exclusions:
                continue
            if not node.node_id or not node.machine_id or not node.disk_id or not node.grpc_target:
                continue
            if node.total_bytes <= 0 or node.used_bytes < 0:
                continue

            reserved = reservations.get(node.node_id, 0)
            if node.available_bytes < reserved + size:
                continue

            usage_after_write = node.used_bytes + reserved + size
            maximum_usage = self._max_usage * node.total_bytes
            if usage_after_write > maximum_usage:
                continue

            eligible.append(node)

        return eligible

    def _select_targets(self, eligible):
        for group in combinations(eligible, self._n):
            node_ids = set()
            machine_ids = set()
            disk_ids = set()

            for node in group:
                node_ids.add(node.node_id)
                machine_ids.add(node.machine_id)
                disk_ids.add(node.disk_id)

            if len(node_ids) != self._n:
                continue
            if len(machine_ids) != self._n:
                continue
            if len(disk_ids) != self._n:
                continue

            return group

        raise AllocationError(grpc.StatusCode.FAILED_PRECONDITION, 'Sem N destinos distintos com capacidade suficiente')

    def _register_upload(self, req, fingerprint, response, reservations):
        pending = PendingUpload(fingerprint=fingerprint, response=response.SerializeToString(), expires_at_ms=response.lease_expires_at_unix_ms, path=req.path)

        upload_reservations = {}
        for node_id, total in reservations.items():
            previous_total = self._reserved.get(node_id, 0)
            if total > previous_total:
                upload_reservations[node_id] = total - previous_total

        for chunk, placement in zip(req.chunks, response.chunks):
            self._block_reservations[chunk.block_id] = {}
            for node in placement.targets:
                self._block_reservations[chunk.block_id][node.node_id] = chunk.stored_size_bytes

        self._reserved = reservations
        self._upload_reservations[req.operation_id] = upload_reservations
        self._operations[req.operation_id] = pending
        self._upload_started[req.operation_id] = int(self._clock() * 1000)
        self._paths[req.path] = pending

        for chunk in req.chunks:
            self._block_ids.add(chunk.block_id)

    def _release_expired_uploads(self, now_ms):
        """Libera reservas lógicas; mantém os registros para retries e futura limpeza física."""
        for operation_id in list(self._upload_reservations):
            pending = self._operations[operation_id]
            if pending.expires_at_ms > now_ms:
                continue

            manifest = pb.AllocateUploadRequest.FromString(pending.fingerprint)
            for chunk in manifest.chunks:
                self._block_reservations.pop(chunk.block_id, None)
            reservations = self._upload_reservations.pop(operation_id)
            for node_id, size in reservations.items():
                self._reserved[node_id] -= size
                if self._reserved[node_id] == 0:
                    del self._reserved[node_id]

            if self._paths.get(pending.path) is pending:
                del self._paths[pending.path]

    def _validate(self, req):
        self._validate_path(req.path)
        self._validate_identifiers(req)

        if len(req.plaintext_sha256) != 32:
            self._invalid_argument("SHA-256 do arquivo deve ter 32 bytes")

        if req.encryption not in (pb.ENCRYPTION_NONE, pb.ENCRYPTION_AES_256_GCM):
            self._invalid_argument("Modo de criptografia inválido")

        self._validate_chunks(req)

    def _validate_path(self, path):
        if not path.startswith("/") or path == "/" or "\x00" in path or "\\" in path:
            self._invalid_argument("Caminho deve ser absoluto, canônico e identificar um arquivo")

        if str(PurePosixPath(path)) != path:
            self._invalid_argument("Caminho deve ser absoluto, canônico e identificar um arquivo")

        for part in path.split("/")[1:]:
            if part in (".", "..", ""):
                self._invalid_argument("Caminho deve ser absoluto, canônico e identificar um arquivo")

        if str(PurePosixPath(path).parent) not in self._directories:
            raise AllocationError(grpc.StatusCode.NOT_FOUND, "Diretório pai não existe")

    def _validate_identifiers(self, req):
        try:
            UUID(req.operation_id)

            for value in req.excluded_node_ids:
                UUID(value)

            for chunk in req.chunks:
                UUID(chunk.block_id)
        except ValueError:
            self._invalid_argument("Identificadores devem ser UUIDs textuais válidos")

    def _validate_chunks(self, req):
        block_ids = set()
        nonces = set()
        total = 0
        overhead = 0

        for index, chunk in enumerate(req.chunks):
            if chunk.index != index:
                self._invalid_argument("Índices devem ser contíguos e começar em zero")

            if chunk.block_id in block_ids:
                self._invalid_argument("block_id duplicado no manifesto")
            block_ids.add(chunk.block_id)

            self._validate_chunk(chunk, req.encryption)
            total += chunk.stored_size_bytes

            if req.encryption == pb.ENCRYPTION_AES_256_GCM:
                if chunk.nonce in nonces:
                    self._invalid_argument("Nonce deve ser único por chunk")
                nonces.add(chunk.nonce)
                overhead += 16

        if total - overhead != req.plaintext_size_bytes:
            self._invalid_argument("Tamanho do manifesto não corresponde ao tamanho original")

    def _validate_chunk(self, chunk, encryption):
        if len(chunk.storage_sha256) != 32 or chunk.stored_size_bytes == 0:
            self._invalid_argument("Chunk deve ter tamanho positivo e SHA-256 de 32 bytes")

        expected_nonce = 0
        if encryption == pb.ENCRYPTION_AES_256_GCM:
            expected_nonce = 12

        if len(chunk.nonce) != expected_nonce:
            self._invalid_argument("Nonce incompatível com o modo de criptografia")

        if encryption == pb.ENCRYPTION_AES_256_GCM and chunk.stored_size_bytes <= 16:
            self._invalid_argument("Chunk cifrado deve conter dados e tag GCM")

    @staticmethod
    def _invalid_argument(message):
        raise AllocationError(grpc.StatusCode.INVALID_ARGUMENT, message)

    def AbortUpload(self, request, context):
        try:
            with self._transaction("AbortUpload"):
                pending, allocation = self._find_upload(request.upload_id)
                if request.upload_id in self._committed_receipts:
                    raise AllocationError(grpc.StatusCode.FAILED_PRECONDITION, "Upload já confirmado")
                self._aborted.add(request.upload_id)
                manifest = pb.AllocateUploadRequest.FromString(pending.fingerprint)
                for chunk in manifest.chunks:
                    self._block_reservations.pop(chunk.block_id, None)
                reservations = self._upload_reservations.pop(manifest.operation_id, {})
                for node_id, size in reservations.items():
                    self._reserved[node_id] -= size
                    if self._reserved[node_id] == 0:
                        del self._reserved[node_id]
                if self._paths.get(pending.path) is pending:
                    del self._paths[pending.path]
                return Empty()
        except AllocationError as exc:
            context.abort(exc.code, str(exc))

    def RenewLease(self, request, context):
        try:
            with self._transaction("RenewLease"):
                pending, allocation = self._find_upload(request.upload_id)
                now_ms = int(self._clock() * 1000)
                if request.upload_id in self._aborted or request.upload_id in self._committed_receipts:
                    raise AllocationError(grpc.StatusCode.FAILED_PRECONDITION, "Upload encerrado")
                if pending.expires_at_ms <= now_ms:
                    raise AllocationError(grpc.StatusCode.ABORTED, "Lease expirado")
                manifest = pb.AllocateUploadRequest.FromString(pending.fingerprint)
                maximum = self._upload_started[manifest.operation_id] + self._max_lifetime_seconds * 1000
                allocation.lease_expires_at_unix_ms = min(now_ms + self._lease_seconds * 1000, maximum)
                updated = replace(pending, expires_at_ms=allocation.lease_expires_at_unix_ms, response=allocation.SerializeToString())
                self._operations[manifest.operation_id] = updated
                self._paths[pending.path] = updated
                return pb.RenewLeaseResponse(lease_expires_at_unix_ms=updated.expires_at_ms)
        except AllocationError as exc:
            context.abort(exc.code, str(exc))

    _state_fields = ("_reserved", "_upload_reservations", "_block_ids", "_files", "_committed_receipts", "_upload_results", "_aborted", "_upload_started", "_directories", "_invalid_replicas", "_original_receipts", "_block_reservations", "_gc_confirmed", "_recovering_blocks")

    def _snapshot(self):
        state = {}
        for name in self._state_fields:
            state[name] = getattr(self, name)
        state["operations"] = {}
        state["paths"] = {}
        for operation_id, pending in self._operations.items():
            state["operations"][operation_id] = asdict(pending)
            if self._paths.get(pending.path) is pending:
                state["paths"][pending.path] = operation_id
        return state

    def _restore(self, state):
        for name in self._state_fields:
            if name in state:
                setattr(self, name, state[name])
        self._operations = {}
        for operation_id, fields in state["operations"].items():
            self._operations[operation_id] = PendingUpload(**fields)
        self._paths = {}
        for path, operation_id in state["paths"].items():
            self._paths[path] = self._operations[operation_id]

    @contextmanager
    def _transaction(self, operation="metadata"):
        with self._lock:
            if self._store is None or self._transaction_depth:
                try:
                    yield
                    self._log_event("metadata.operation_completed", operation=operation)
                except Exception:
                    self._log_event("metadata.operation_failed", operation=operation)
                    raise
                return
            connection = self._store.connection
            connection.execute("BEGIN IMMEDIATE")
            self._transaction_depth = 1
            original = copy.deepcopy(self._snapshot())
            try:
                stored = self._store.load()
                if stored is not None:
                    self._restore(stored)
                    original = copy.deepcopy(self._snapshot())
                yield
                self._store.save(self._snapshot())
                connection.commit()
                self._log_event("metadata.operation_completed", operation=operation)
            except BaseException:
                connection.rollback()
                self._log_event("metadata.operation_failed", operation=operation)
                self._restore(original)
                raise
            finally:
                self._transaction_depth = 0

    def close(self):
        with self._lock:
            if self._store is not None:
                self._store.close()

    def Mkdir(self, request, context):
        try:
            with self._transaction("Mkdir"):
                if request.path in self._directories:
                    raise AllocationError(grpc.StatusCode.ALREADY_EXISTS, "Diretório já existe")
                self._validate_path(request.path)
                self._release_expired_uploads(int(self._clock() * 1000))
                if request.path in self._paths:
                    raise AllocationError(grpc.StatusCode.ABORTED, "Caminho reservado por upload")
                stored = self._files.get(request.path)
                if stored is not None and pb.FileMetadata.FromString(stored).state == pb.FILE_COMMITTED:
                    raise AllocationError(grpc.StatusCode.ALREADY_EXISTS, "Arquivo já existe")
                self._directories.add(request.path)
                return Empty()
        except AllocationError as exc:
            context.abort(exc.code, str(exc))

    def Stat(self, request, context):
        try:
            with self._transaction("Stat"):
                if request.path in self._directories:
                    return pb.StatResponse(is_directory=True)
                result = self.resolve_download(pb.ResolveDownloadRequest(path=request.path))
                return pb.StatResponse(is_directory=False, file=result.file, chunks=result.chunks)
        except AllocationError as exc:
            context.abort(exc.code, str(exc))

    def ListDir(self, request, context):
        try:
            with self._transaction("ListDir"):
                if request.path != "/":
                    self._validate_path(request.path)
                if request.path not in self._directories:
                    raise AllocationError(grpc.StatusCode.NOT_FOUND, "Diretório não encontrado")
                if request.page_size > 1000:
                    raise AllocationError(grpc.StatusCode.INVALID_ARGUMENT, "Página excede 1000 entradas")
                entries = []
                for path in self._directories:
                    if path != "/" and str(PurePosixPath(path).parent) == request.path:
                        entries.append(pb.DirectoryEntry(path=path, is_directory=True))
                for path, stored in self._files.items():
                    metadata = pb.FileMetadata.FromString(stored)
                    if metadata.state == pb.FILE_COMMITTED and str(PurePosixPath(path).parent) == request.path:
                        entries.append(pb.DirectoryEntry(path=path, size_bytes=metadata.plaintext_size_bytes))
                entries.sort(key=lambda entry: entry.path)
                start = 0
                if request.page_token:
                    paths = [entry.path for entry in entries]
                    if request.page_token not in paths:
                        raise AllocationError(grpc.StatusCode.INVALID_ARGUMENT, "Token de paginação inválido")
                    start = paths.index(request.page_token) + 1
                size = request.page_size or 100
                page = entries[start:start + size]
                token = ""
                if start + size < len(entries):
                    token = page[-1].path
                return pb.ListDirResponse(entries=page, next_page_token=token)
        except AllocationError as exc:
            context.abort(exc.code, str(exc))

    def Rename(self, request, context):
        try:
            with self._transaction("Rename"):
                source = request.source_path
                target = request.target_path
                self._validate_path(source)
                self._validate_path(target)
                self._release_expired_uploads(int(self._clock() * 1000))
                source_is_dir = source in self._directories
                stored = self._files.get(source)
                if not source_is_dir and (stored is None or pb.FileMetadata.FromString(stored).state != pb.FILE_COMMITTED):
                    raise AllocationError(grpc.StatusCode.NOT_FOUND, "Origem não encontrada")
                if source == target:
                    return Empty()
                if target.startswith(source + "/"):
                    raise AllocationError(grpc.StatusCode.INVALID_ARGUMENT, "Destino dentro da própria origem")
                for path in self._paths:
                    if path == source or path.startswith(source + "/") or path == target or path.startswith(target + "/"):
                        raise AllocationError(grpc.StatusCode.ABORTED, "Caminho reservado por upload")
                if target in self._directories:
                    raise AllocationError(grpc.StatusCode.ALREADY_EXISTS, "Diretório de destino já existe")
                destination = self._files.get(target)
                if destination is not None and pb.FileMetadata.FromString(destination).state == pb.FILE_COMMITTED:
                    if source_is_dir or not request.overwrite:
                        raise AllocationError(grpc.StatusCode.ALREADY_EXISTS, "Destino já existe")
                for path in list(self._files):
                    if path == source or (source_is_dir and path.startswith(source + "/")):
                        metadata = pb.FileMetadata.FromString(self._files.pop(path))
                        metadata.path = target + path[len(source):]
                        metadata.updated_at_unix_ms = int(self._clock() * 1000)
                        self._files[metadata.path] = metadata.SerializeToString()
                if source_is_dir:
                    for path in list(self._directories):
                        if path == source or path.startswith(source + "/"):
                            self._directories.remove(path)
                            self._directories.add(target + path[len(source):])
                return Empty()
        except AllocationError as exc:
            context.abort(exc.code, str(exc))

    def Delete(self, request, context):
        try:
            with self._transaction("Delete"):
                self._validate_path(request.path)
                self._release_expired_uploads(int(self._clock() * 1000))
                if request.path in self._paths:
                    raise AllocationError(grpc.StatusCode.ABORTED, "Caminho reservado por upload")
                stored = self._files.get(request.path)
                if stored is None:
                    raise AllocationError(grpc.StatusCode.NOT_FOUND, "Arquivo não encontrado")
                metadata = pb.FileMetadata.FromString(stored)
                if request.HasField("expected_version") and metadata.version != request.expected_version:
                    raise AllocationError(grpc.StatusCode.ABORTED, "Versão esperada divergente")
                metadata.state = pb.FILE_DELETED
                metadata.updated_at_unix_ms = int(self._clock() * 1000)
                self._files[request.path] = metadata.SerializeToString()
                return Empty()
        except AllocationError as exc:
            context.abort(exc.code, str(exc))

    def Rmdir(self, request, context):
        try:
            with self._transaction("Rmdir"):
                if request.path == "/":
                    raise AllocationError(grpc.StatusCode.FAILED_PRECONDITION, "Não é permitido remover a raiz")
                self._validate_path(request.path)
                if request.path not in self._directories:
                    raise AllocationError(grpc.StatusCode.NOT_FOUND, "Diretório não encontrado")
                self._release_expired_uploads(int(self._clock() * 1000))
                prefix = request.path + "/"
                for path in self._paths:
                    if path.startswith(prefix):
                        raise AllocationError(grpc.StatusCode.ABORTED, "Diretório contém upload em andamento")
                directories = []
                files = []
                for path in self._directories:
                    if path.startswith(prefix):
                        directories.append(path)
                for path, stored in self._files.items():
                    if path.startswith(prefix) and pb.FileMetadata.FromString(stored).state == pb.FILE_COMMITTED:
                        files.append(path)
                if (directories or files) and not request.recursive:
                    raise AllocationError(grpc.StatusCode.FAILED_PRECONDITION, "Diretório não está vazio")
                for path in files:
                    metadata = pb.FileMetadata.FromString(self._files[path])
                    metadata.state = pb.FILE_DELETED
                    metadata.updated_at_unix_ms = int(self._clock() * 1000)
                    self._files[path] = metadata.SerializeToString()
                for path in directories:
                    self._directories.remove(path)
                self._directories.remove(request.path)
                return Empty()
        except AllocationError as exc:
            context.abort(exc.code, str(exc))

    def _block_records(self):
        records = []
        for stored in self._files.values():
            metadata = pb.FileMetadata.FromString(stored)
            if metadata.state != pb.FILE_COMMITTED:
                continue
            manifest, allocation, confirmed = self._find_committed_upload(metadata)
            for chunk, placement in zip(manifest.chunks, allocation.chunks):
                replicas = []
                for address in placement.targets:
                    if address.node_id not in confirmed[chunk.block_id]:
                        continue
                    try:
                        node = self._registry.get_node_capacity(address.node_id)
                        state = node.state
                        address = pb.NodeAddress(node_id=node.node_id, machine_id=node.machine_id, disk_id=node.disk_id, grpc_target=node.grpc_target)
                    except KeyError:
                        state = pb.NODE_DEAD
                    replicas.append(pb.ReplicaLocation(node=address, healthy=state in (pb.NODE_ALIVE, pb.NODE_DRAINING) and f"{chunk.block_id}:{address.node_id}" not in self._invalid_replicas))
                records.append({"block_id": chunk.block_id, "manifest": chunk, "version": metadata.version, "desired_replica_count": metadata.desired_replica_count, "replicas": replicas})
        return records

    def list_blocks_on_node(self, node_id):
        with self._transaction("list_blocks_on_node"):
            result = []
            for record in self._block_records():
                for replica in record["replicas"]:
                    if replica.node.node_id == node_id:
                        result.append(record)
                        break
            return result

    def list_healthy_sources(self, block_id):
        with self._transaction("list_healthy_sources"):
            for record in self._block_records():
                if record["block_id"] == block_id:
                    return [replica.node for replica in record["replicas"] if replica.healthy]
            return []

    def list_under_replicated_blocks(self):
        with self._transaction("list_under_replicated_blocks"):
            result = []
            for record in self._block_records():
                count = 0
                for replica in record["replicas"]:
                    if replica.healthy:
                        count += 1
                if count < record["desired_replica_count"]:
                    record["live_replica_count"] = count
                    result.append(record)
            result.sort(key=lambda record: record["live_replica_count"])
            return result

    def select_destination(self, block_id, excluded_node_ids):
        with self._transaction("select_destination"):
            for record in self._block_records():
                if record["block_id"] != block_id:
                    continue
                excluded = set(excluded_node_ids)
                machines = set()
                disks = set()
                for replica in record["replicas"]:
                    excluded.add(replica.node.node_id)
                    machines.add(replica.node.machine_id)
                    disks.add(replica.node.disk_id)
                candidates = self._filter_eligible_nodes(self._registry.list_eligible_nodes(), excluded, record["manifest"].stored_size_bytes, self._reserved)
                for node in sorted(candidates, key=lambda node: node.node_id):
                    if node.machine_id not in machines and node.disk_id not in disks:
                        return pb.NodeAddress(node_id=node.node_id, machine_id=node.machine_id, disk_id=node.disk_id, grpc_target=node.grpc_target)
                raise AllocationError(grpc.StatusCode.FAILED_PRECONDITION, "Sem destino elegível para recuperação")
            raise AllocationError(grpc.StatusCode.NOT_FOUND, "Bloco não encontrado")

    def mark_replica_invalid(self, block_id, node_id):
        with self._transaction("mark_replica_invalid"):
            for record in self._block_records():
                if record["block_id"] == block_id:
                    for replica in record["replicas"]:
                        if replica.node.node_id == node_id:
                            self._invalid_replicas.add(f"{block_id}:{node_id}")
                            return
            raise AllocationError(grpc.StatusCode.NOT_FOUND, "Réplica não encontrada")

    def register_recovered_replica(self, block_id, receipt):
        with self._transaction("register_recovered_replica"):
            for stored in self._files.values():
                metadata = pb.FileMetadata.FromString(stored)
                if metadata.state != pb.FILE_COMMITTED:
                    continue
                manifest, allocation, confirmed = self._find_committed_upload(metadata)
                for chunk, placement in zip(manifest.chunks, allocation.chunks):
                    if chunk.block_id != block_id:
                        continue
                    if receipt.block_id != block_id or receipt.stored_size_bytes != chunk.stored_size_bytes or receipt.verified_storage_sha256 != chunk.storage_sha256:
                        raise AllocationError(grpc.StatusCode.FAILED_PRECONDITION, "Recibo recuperado difere do manifesto")
                    if receipt.node_id not in confirmed[block_id]:
                        try:
                            node = self._registry.get_node_capacity(receipt.node_id)
                        except KeyError:
                            raise AllocationError(grpc.StatusCode.FAILED_PRECONDITION, "Nó recuperado não registrado")
                        if node.state != pb.NODE_ALIVE:
                            raise AllocationError(grpc.StatusCode.FAILED_PRECONDITION, "Destino recuperado não está ALIVE")
                        if len(self.list_healthy_sources(block_id)) >= metadata.desired_replica_count:
                            raise AllocationError(grpc.StatusCode.FAILED_PRECONDITION, "Bloco já possui réplicas suficientes")
                        for address in placement.targets:
                            if address.node_id in confirmed[block_id] and (address.machine_id == node.machine_id or address.disk_id == node.disk_id):
                                raise AllocationError(grpc.StatusCode.FAILED_PRECONDITION, "Domínio de réplica repetido")
                        if receipt.node_id not in [address.node_id for address in placement.targets]:
                            placement.targets.append(pb.NodeAddress(node_id=node.node_id, machine_id=node.machine_id, disk_id=node.disk_id, grpc_target=node.grpc_target))
                    if allocation.upload_id not in self._original_receipts:
                        self._original_receipts[allocation.upload_id] = copy.deepcopy(confirmed)
                    confirmed[block_id][receipt.node_id] = receipt.SerializeToString()
                    self._invalid_replicas.discard(f"{block_id}:{receipt.node_id}")
                    pending = self._operations[manifest.operation_id]
                    self._operations[manifest.operation_id] = replace(pending, response=allocation.SerializeToString())
                    if len(self.list_healthy_sources(block_id)) >= metadata.desired_replica_count:
                        self._recovering_blocks.discard(block_id)
                    return
            raise AllocationError(grpc.StatusCode.NOT_FOUND, "Bloco não encontrado")

    def gc_candidates(self):
        with self._transaction("gc_candidates"):
            self._release_expired_uploads(int(self._clock() * 1000))
            protected = set()
            for record in self._block_records():
                protected.add(record["block_id"])
            for pending in self._paths.values():
                manifest = pb.AllocateUploadRequest.FromString(pending.fingerprint)
                for chunk in manifest.chunks:
                    protected.add(chunk.block_id)
            candidates = {}
            for pending in self._operations.values():
                manifest = pb.AllocateUploadRequest.FromString(pending.fingerprint)
                allocation = pb.AllocateUploadResponse.FromString(pending.response)
                for chunk, placement in zip(manifest.chunks, allocation.chunks):
                    if chunk.block_id in protected:
                        continue
                    for node in placement.targets:
                        key = f"{chunk.block_id}:{node.node_id}"
                        if key in self._gc_confirmed:
                            continue
                        candidates[key] = {"block_id": chunk.block_id, "node": node, "stored_size_bytes": chunk.stored_size_bytes, "version": allocation.version}
            return list(candidates.values())

    def _log_event(self, event, **fields):
        record = {"ts": datetime.now(timezone.utc).isoformat(), "component": "metadata", "event": event, "level": "INFO"}
        record.update(fields)
        logging.getLogger("dcomp_drive.metadata").info(json.dumps(record, ensure_ascii=False))

    def acknowledge_gc(self, block_id, node_id):
        """O controlador chama somente após DeleteBlock concluir com sucesso."""
        with self._transaction("acknowledge_gc"):
            key = f"{block_id}:{node_id}"
            if key in self._gc_confirmed:
                return
            for candidate in self.gc_candidates():
                if candidate["block_id"] == block_id and candidate["node"].node_id == node_id:
                    reservations = self._block_reservations.get(block_id, {})
                    size = reservations.pop(node_id, 0)
                    if size:
                        self._reserved[node_id] -= size
                        if self._reserved[node_id] == 0:
                            del self._reserved[node_id]
                    self._gc_confirmed.add(key)
                    return
            raise AllocationError(grpc.StatusCode.FAILED_PRECONDITION, "Bloco ainda referenciado ou sem ordem de limpeza")

    def set_recovery_state(self, block_id, recovering):
        """O controlador informa o início e o encerramento de seu job de cópia."""
        with self._transaction("set_recovery_state"):
            for record in self._block_records():
                if record["block_id"] == block_id:
                    if recovering:
                        self._recovering_blocks.add(block_id)
                    else:
                        self._recovering_blocks.discard(block_id)
                    return
            raise AllocationError(grpc.StatusCode.NOT_FOUND, "Bloco não encontrado")
