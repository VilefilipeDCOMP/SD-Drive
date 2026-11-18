from __future__ import annotations

import errno
import threading
import time
from typing import Iterator, Optional

import grpc
from google.protobuf import empty_pb2

import dcomp_drive_pb2 as pb
import dcomp_drive_pb2_grpc as pb_grpc

from . import jsonlog
from .config import Identity, StorageConfig
from .metrics import Metrics
from .store import (BlockConflict, BlockStore, Existing, HashMismatch, InsufficientSpace, InvalidBlockId,
                    SizeMismatch, StoreError, VerifyResult, validate_block_id)

SHA256_LEN = 32


class StorageServicer(pb_grpc.StorageServiceServicer):
    def __init__(self, cfg: StorageConfig, identity: Identity, store: BlockStore, metrics: Metrics):
        self.cfg = cfg
        self.identity = identity
        self.store = store
        self.metrics = metrics
        self._channels: dict[str, grpc.Channel] = {}
        self._channels_lock = threading.Lock()

    def _receipt(self, block_id: str, size: int, sha: bytes) -> "pb.BlockReceipt":
        return pb.BlockReceipt(block_id=block_id, node_id=self.identity.node_id, stored_size_bytes=size,
                               verified_storage_sha256=sha)

    def _valid_id(self, context: grpc.ServicerContext, block_id: str) -> str:
        try:
            return validate_block_id(block_id)
        except InvalidBlockId as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))

    def _channel(self, target: str) -> grpc.Channel:
        with self._channels_lock:
            ch = self._channels.get(target)
            if ch is None:
                ch = grpc.insecure_channel(target)
                self._channels[target] = ch
            return ch

    def close(self) -> None:
        with self._channels_lock:
            for ch in self._channels.values():
                ch.close()
            self._channels.clear()

    def StoreBlock(self, request_iterator, context):
        started = time.monotonic()
        try:
            first = next(request_iterator)
        except StopIteration:
            self.metrics.inc_store("invalid")
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "stream vazio: o primeiro frame deve ser o header")
        if first.WhichOneof("payload") != "header":
            self.metrics.inc_store("invalid")
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "o primeiro frame deve ser o header")
        header = first.header
        block_id = self._valid_id(context, header.block_id)
        expected_sha = bytes(header.expected_storage_sha256)
        expected_size = int(header.expected_size_bytes)
        if len(expected_sha) != SHA256_LEN:
            self.metrics.inc_store("invalid")
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "expected_storage_sha256 deve ter 32 bytes")

        pending = None
        try:
            try:
                existing = self.store.classify_existing(block_id, expected_size, expected_sha)
            except BlockConflict as exc:
                self.metrics.inc_store("conflict")
                jsonlog.emit("storage.store_rejected", level="warning", block_id=block_id, reason="conflict")
                context.abort(grpc.StatusCode.ALREADY_EXISTS, str(exc))
            if existing == Existing.SAME:
                self._drain_and_check(request_iterator, context, block_id, expected_size, expected_sha)
                self.metrics.inc_store("idempotent", seconds=time.monotonic() - started)
                jsonlog.emit("storage.block_stored", block_id=block_id, upload_id=header.upload_id or None,
                             bytes=expected_size, duration_ms=int((time.monotonic() - started) * 1000),
                             idempotent=True)
                return self._receipt(block_id, expected_size, expected_sha)

            try:
                pending = self.store.begin_write(block_id, expected_size, expected_sha, int(header.version),
                                                 header.upload_id)
            except InsufficientSpace as exc:
                self.metrics.inc_store("no_space")
                usage = self.store.usage()
                jsonlog.emit("storage.store_rejected", level="warning", block_id=block_id, reason="no_space",
                             needed=exc.needed, available=exc.available, total_bytes=usage.total_bytes,
                             used_bytes=usage.used_bytes)
                context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, str(exc))

            for frame in request_iterator:
                if frame.WhichOneof("payload") != "data":
                    self.metrics.inc_store("invalid")
                    context.abort(grpc.StatusCode.INVALID_ARGUMENT, "frame inesperado: header só pode vir uma vez, no início")
                pending.write(frame.data)
            record = pending.commit()
        except SizeMismatch as exc:
            self.metrics.inc_store("invalid")
            jsonlog.emit("storage.store_rejected", level="warning", block_id=block_id, reason="size_mismatch",
                         error=str(exc))
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        except HashMismatch as exc:
            self.metrics.inc_store("hash_mismatch")
            jsonlog.emit("storage.store_rejected", level="error", block_id=block_id, reason="hash_mismatch",
                         expected_sha256=exc.expected, observed_sha256=exc.observed)
            context.abort(grpc.StatusCode.DATA_LOSS, str(exc))
        except BlockConflict as exc:
            self.metrics.inc_store("conflict")
            context.abort(grpc.StatusCode.ALREADY_EXISTS, str(exc))
        except OSError as exc:
            self.metrics.inc_store("io_error")
            if exc.errno in (errno.ENOSPC, errno.EDQUOT):
                jsonlog.emit("storage.store_rejected", level="error", block_id=block_id, reason="enospc")
                context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "sem espaço físico no volume do nó")
            jsonlog.emit("storage.io_error", level="error", op="store", block_id=block_id, error=str(exc))
            context.abort(grpc.StatusCode.INTERNAL, f"erro de E/S ao gravar o bloco: {exc}")
        finally:
            if pending is not None:
                pending.abort()

        elapsed = time.monotonic() - started
        self.metrics.inc_store("ok", nbytes=record.size, seconds=elapsed)
        jsonlog.emit("storage.block_stored", block_id=block_id, upload_id=header.upload_id or None,
                     bytes=record.size, duration_ms=int(elapsed * 1000), version=record.version)
        return self._receipt(block_id, record.size, record.sha256)

    def _drain_and_check(self, request_iterator, context, block_id: str, expected_size: int,
                         expected_sha: bytes) -> None:
        import hashlib
        h = hashlib.sha256()
        total = 0
        for frame in request_iterator:
            if frame.WhichOneof("payload") != "data":
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "frame inesperado: header só pode vir uma vez")
            total += len(frame.data)
            if total > expected_size:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "bloco excede o tamanho declarado")
            h.update(frame.data)
        if total != expected_size:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                          f"tamanho recebido {total} B difere do declarado {expected_size} B")
        if h.digest() != expected_sha:
            context.abort(grpc.StatusCode.DATA_LOSS, "SHA-256 dos dados recebidos difere do declarado")

    def GetBlock(self, request, context) -> Iterator["pb.GetBlockFrame"]:
        started = time.monotonic()
        block_id = self._valid_id(context, request.block_id)
        rec = self.store.get_record(block_id)
        if rec is None:
            self.metrics.inc_get("not_found")
            context.abort(grpc.StatusCode.NOT_FOUND, f"bloco {block_id} não existe neste nó")
        if rec.corrupt:
            self.metrics.inc_get("corrupt")
            context.abort(grpc.StatusCode.DATA_LOSS, f"réplica de {block_id} está corrompida neste nó")
        if self.cfg.verify_on_read:
            result = self.store.verify(block_id, "read")
            if result in (VerifyResult.CORRUPT, VerifyResult.MISSING):
                self.metrics.inc_get("corrupt")
                context.abort(grpc.StatusCode.DATA_LOSS if result == VerifyResult.CORRUPT else grpc.StatusCode.NOT_FOUND,
                              f"bloco {block_id}: integridade não confirmada ({result.value})")
        try:
            f = open(self.store.path_for(block_id), "rb")
        except FileNotFoundError:
            self.metrics.inc_get("not_found")
            context.abort(grpc.StatusCode.NOT_FOUND, f"bloco {block_id} não existe neste nó")
        sent = 0
        with f:
            yield pb.GetBlockFrame(header=pb.GetBlockHeader(block_id=block_id, stored_size_bytes=rec.size,
                                                            storage_sha256=rec.sha256))
            while True:
                if not context.is_active():
                    self.metrics.inc_get("cancelled")
                    return
                chunk = f.read(self.cfg.frame_size)
                if not chunk:
                    break
                sent += len(chunk)
                yield pb.GetBlockFrame(data=chunk)
        if sent != rec.size:
            self.store.mark_corrupt(rec, b"", "read", observed_size=sent)
            self.metrics.inc_get("corrupt")
            context.abort(grpc.StatusCode.DATA_LOSS, f"bloco {block_id}: {sent} B lidos, esperado {rec.size} B")
        elapsed = time.monotonic() - started
        self.metrics.inc_get("ok", nbytes=sent, seconds=elapsed)
        jsonlog.emit("storage.block_read", block_id=block_id, bytes=sent, duration_ms=int(elapsed * 1000))

    def DeleteBlock(self, request, context):
        block_id = self._valid_id(context, request.block_id)
        existed = self.store.delete(block_id)
        self.metrics.inc_delete()
        jsonlog.emit("storage.block_deleted", block_id=block_id, existed=existed)
        return empty_pb2.Empty()

    def CopyBlock(self, request, context):
        started = time.monotonic()
        block_id = self._valid_id(context, request.block_id)
        dest = request.destination
        if not dest.grpc_target:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "destination.grpc_target ausente")
        if dest.node_id and dest.node_id == self.identity.node_id:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "o destino da cópia é este próprio nó")
        expected_sha = bytes(request.expected_storage_sha256)
        if len(expected_sha) != SHA256_LEN:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "expected_storage_sha256 deve ter 32 bytes")
        rec = self.store.get_record(block_id)
        if rec is None:
            self.metrics.inc_copy("not_found")
            context.abort(grpc.StatusCode.NOT_FOUND, f"bloco {block_id} não existe neste nó")
        if rec.corrupt:
            self.metrics.inc_copy("corrupt")
            context.abort(grpc.StatusCode.DATA_LOSS, f"réplica de {block_id} corrompida: não pode ser origem")
        if rec.sha256 != expected_sha or (request.stored_size_bytes and request.stored_size_bytes != rec.size):
            self.metrics.inc_copy("mismatch")
            context.abort(grpc.StatusCode.FAILED_PRECONDITION,
                          f"bloco {block_id} local não corresponde ao hash/tamanho esperado pelo controlador")
        result = self.store.verify(block_id, "copy")
        if result != VerifyResult.OK:
            self.metrics.inc_copy("corrupt")
            context.abort(grpc.StatusCode.DATA_LOSS if result == VerifyResult.CORRUPT else grpc.StatusCode.NOT_FOUND,
                          f"bloco {block_id}: integridade local não confirmada ({result.value})")

        version = request.version or rec.version
        path = self.store.path_for(block_id)
        frame_size = self.cfg.frame_size

        def frames() -> Iterator["pb.StoreBlockFrame"]:
            yield pb.StoreBlockFrame(header=pb.StoreBlockHeader(
                block_id=block_id, upload_id="", version=version, expected_size_bytes=rec.size,
                expected_storage_sha256=rec.sha256))
            with open(path, "rb") as f:
                while True:
                    chunk = f.read(frame_size)
                    if not chunk:
                        break
                    yield pb.StoreBlockFrame(data=chunk)

        stub = pb_grpc.StorageServiceStub(self._channel(dest.grpc_target))
        try:
            receipt = stub.StoreBlock(frames(), timeout=self.cfg.copy_deadline)
        except grpc.RpcError as exc:
            self.metrics.inc_copy("failed")
            jsonlog.emit("storage.copy_failed", level="warning", block_id=block_id, destination=dest.grpc_target,
                         destination_node_id=dest.node_id or None, code=exc.code().name, detail=exc.details())
            context.abort(exc.code(), f"falha ao copiar {block_id} para {dest.grpc_target}: {exc.details()}")
        except OSError as exc:
            self.metrics.inc_copy("io_error")
            jsonlog.emit("storage.io_error", level="error", op="copy", block_id=block_id, error=str(exc))
            context.abort(grpc.StatusCode.INTERNAL, f"erro de E/S ao ler o bloco {block_id}: {exc}")
        if receipt.verified_storage_sha256 != rec.sha256 or receipt.stored_size_bytes != rec.size:
            self.metrics.inc_copy("bad_receipt")
            context.abort(grpc.StatusCode.DATA_LOSS, f"recibo do destino não confere com o bloco {block_id}")
        elapsed = time.monotonic() - started
        self.metrics.inc_copy("ok", nbytes=rec.size, seconds=elapsed)
        jsonlog.emit("storage.block_copied", block_id=block_id, destination=dest.grpc_target,
                     destination_node_id=dest.node_id or None, bytes=rec.size, duration_ms=int(elapsed * 1000))
        return receipt
