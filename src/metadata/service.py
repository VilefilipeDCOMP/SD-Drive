from dataclasses import dataclass
from decimal import Decimal
from itertools import combinations
from pathlib import PurePosixPath
from threading import RLock
import time
from typing import Callable
from uuid import UUID, uuid4

import grpc
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
    def __init__(self, registry: NodeRegistryPort, *, replication_factor: int = 3, write_quorum: int | str = "all", max_usage_fraction: float = 0.90, lease_seconds: int = 900, clock: Callable[[], float] = time.time):
        if type(replication_factor) is not int or replication_factor < 1:
            raise ValueError("Fator de replicação deve ser inteiro positivo")

        if write_quorum == "all":
            quorum = replication_factor
        else:
            quorum = write_quorum

        if type(quorum) is not int or not 1 <= quorum <= replication_factor:
            raise ValueError("Quórum de escrita deve ser 'all' ou inteiro entre 1 e N")

        if not 0 < max_usage_fraction <= 1 or lease_seconds <= 0:
            raise ValueError("Ocupação e duração do lease inválidas")

        self._registry = registry
        self._n = replication_factor
        self._w = quorum
        self._max_usage = Decimal(str(max_usage_fraction))
        self._lease_seconds = lease_seconds
        self._clock = clock

        self._lock = RLock()
        self._operations: dict[str, PendingUpload] = {}
        self._paths: dict[str, PendingUpload] = {}
        self._reserved: dict[str, int] = {}
        self._upload_reservations: dict[str, dict[str, int]] = {}
        self._block_ids: set[str] = set()
        self._files: dict[str, bytes] = {}
        self._committed_receipts = {}

    def AllocateUpload(self, request, context):
        try:
            return self.allocate_upload(request)
        except AllocationError as exc:
            context.abort(exc.code, str(exc))

    def allocate_upload(self, request: pb.AllocateUploadRequest) -> pb.AllocateUploadResponse:
        req = self._prepare_request(request)
        fingerprint = req.SerializeToString(deterministic=True)

        with self._lock:
            now_ms = int(self._clock() * 1000)
            self._release_expired_uploads(now_ms)

            previous_response = self._find_previous_upload(req, fingerprint, now_ms)
            if previous_response is not None:
                return previous_response

            self._check_upload_conflicts(req)
            placements, reservations = self._allocate_chunks(req)

            response = pb.AllocateUploadResponse(upload_id=str(uuid4()), file_id=str(uuid4()), version=1, replication_factor=self._n, write_quorum=self._w, chunks=placements, lease_expires_at_unix_ms=now_ms + self._lease_seconds * 1000)

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

        with self._lock:
            now_ms = int(self._clock() * 1000)
            self._release_expired_uploads(now_ms)
            pending, allocation = self._find_upload(req.upload_id)

            if req.upload_id not in self._committed_receipts and pending.expires_at_ms <= now_ms:
                raise AllocationError(grpc.StatusCode.ABORTED, "Lease expirado")

            manifest = pb.AllocateUploadRequest.FromString(pending.fingerprint)
            confirmed = self._validate_receipts(manifest, allocation, req.receipts)
            previous = self._committed_receipts.get(req.upload_id)

            if previous is not None:
                if confirmed != previous:
                    raise AllocationError(grpc.StatusCode.FAILED_PRECONDITION, "Upload já confirmado com recibos diferentes")
                return pb.FileMetadata.FromString(self._files[pending.path])

            if self._paths.get(pending.path) is not pending or pending.path in self._files:
                raise AllocationError(grpc.StatusCode.ABORTED, "Upload não possui mais a reserva do caminho")

            replication_state = self._commit_replication_state(confirmed, allocation.replication_factor)
            now_ms = int(self._clock() * 1000)
            if pending.expires_at_ms <= now_ms:
                raise AllocationError(grpc.StatusCode.ABORTED, "Lease expirado")

            metadata = pb.FileMetadata(file_id=allocation.file_id, path=manifest.path, version=allocation.version, plaintext_size_bytes=manifest.plaintext_size_bytes, plaintext_sha256=manifest.plaintext_sha256, encryption=manifest.encryption, state=pb.FILE_COMMITTED, replication_state=replication_state, desired_replica_count=allocation.replication_factor, created_at_unix_ms=now_ms, updated_at_unix_ms=now_ms)

            self._files[pending.path] = metadata.SerializeToString()
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

        if previous.expires_at_ms <= now_ms:
            raise AllocationError(grpc.StatusCode.ABORTED, "Lease expirado")

        return pb.AllocateUploadResponse.FromString(previous.response)

    def _check_upload_conflicts(self, req):
        if req.path in self._paths:
            raise AllocationError(grpc.StatusCode.ABORTED, "Caminho com upload em andamento")

        if req.path in self._files:
            if req.overwrite:
                raise AllocationError(grpc.StatusCode.FAILED_PRECONDITION, "Sobrescrita de arquivo confirmado ainda não implementada")
            raise AllocationError(grpc.StatusCode.ALREADY_EXISTS, "Arquivo já existe")

        if req.HasField("expected_version") and req.expected_version != 0:
            raise AllocationError(grpc.StatusCode.ABORTED, "Não existe versão confirmada")

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

        self._reserved = reservations
        self._upload_reservations[req.operation_id] = upload_reservations
        self._operations[req.operation_id] = pending
        self._paths[req.path] = pending

        for chunk in req.chunks:
            self._block_ids.add(chunk.block_id)

    def _release_expired_uploads(self, now_ms):
        """Libera reservas lógicas; mantém os registros para retries e futura limpeza física."""
        for operation_id in list(self._upload_reservations):
            pending = self._operations[operation_id]
            if pending.expires_at_ms > now_ms:
                continue

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

        if str(PurePosixPath(path).parent) != "/":
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
