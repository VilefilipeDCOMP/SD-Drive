"""
Testes do Orquestrador de Upload do Cliente (Quórum W, Renovação de Lease e Retry com Exclusão de Nós).
"""
from __future__ import annotations

import hashlib
import os
import time
from pathlib import Path
from unittest.mock import MagicMock

import grpc
import pytest

import dcomp_drive_pb2 as pb
from client.config import ClientConfig
from client.controller_client import ControllerClient
from client.service import ClientService, InsufficientNodesError, _LeaseRenewer
from client.storage_client import StorageClient


def _node(node_id: str) -> pb.NodeAddress:
    return pb.NodeAddress(
        node_id=node_id,
        machine_id=f"m-{node_id}",
        disk_id=f"d-{node_id}",
        grpc_target=f"{node_id}:50052",
    )


def test_upload_happy_path_and_encryption(tmp_path: Path):
    """Valida upload multi-bloco com criptografia AES-256-GCM e quórum W=N."""
    src = tmp_path / "arquivo.bin"
    raw = os.urandom(2200)
    src.write_bytes(raw)
    key = os.urandom(32)

    cfg = ClientConfig(logical_chunk_size_bytes=1000, grpc_frame_size_bytes=256, encryption_enabled=True)
    controller = MagicMock(spec=ControllerClient)
    storage = MagicMock(spec=StorageClient)

    targets = [_node("n1"), _node("n2"), _node("n3")]

    def fake_alloc(**kw):
        return pb.AllocateUploadResponse(
            upload_id="up-100",
            file_id="f-100",
            version=1,
            replication_factor=3,
            write_quorum=3,
            chunks=[pb.ChunkPlacement(block_id=c.block_id, index=c.index, targets=targets) for c in kw["chunks"]],
            lease_expires_at_unix_ms=999999999,
        )

    controller.allocate_upload.side_effect = fake_alloc
    controller.commit_upload.return_value = pb.FileMetadata(
        file_id="f-100",
        path="/remoto.bin",
        version=1,
        plaintext_size_bytes=len(raw),
        plaintext_sha256=hashlib.sha256(raw).digest(),
        encryption=pb.ENCRYPTION_AES_256_GCM,
        state=pb.FILE_COMMITTED,
    )

    storage.store_block.side_effect = lambda **kw: pb.BlockReceipt(
        block_id=kw["header"].block_id,
        node_id=kw["grpc_target"].split(":")[0],
        stored_size_bytes=kw["header"].expected_size_bytes,
        verified_storage_sha256=kw["header"].expected_storage_sha256,
    )

    svc = ClientService(config=cfg, controller=controller, storage=storage)
    meta = svc.upload_file(src, "/remoto.bin", key=key)

    assert meta.version == 1
    assert meta.encryption == pb.ENCRYPTION_AES_256_GCM
    # 3 chunks * 3 réplicas = 9 chamadas a store_block
    assert storage.store_block.call_count == 9
    controller.commit_upload.assert_called_once()


def test_upload_retry_with_abort_and_excluded_nodes(tmp_path: Path):
    """
    Valida que quando um nó falha impedindo o quórum W, o cliente:
      1. Chama AbortUpload(upload_id)
      2. Acumula o nó falho em excluded_node_ids
      3. Regenera operation_id e block_ids
      4. Conclui o upload na segunda tentativa com o nó substituto.
    """
    src = tmp_path / "retry.bin"
    data = b"conteudo-para-retry" * 40
    src.write_bytes(data)

    cfg = ClientConfig(
        logical_chunk_size_bytes=1024,
        grpc_frame_size_bytes=256,
        upload_retry_max_attempts=3,
        upload_retry_backoff_initial_seconds=0.01,
        upload_retry_backoff_max_seconds=0.02,
    )

    controller = MagicMock(spec=ControllerClient)
    storage = MagicMock(spec=StorageClient)

    alloc_requests = []

    def fake_alloc(**kw):
        alloc_requests.append(kw)
        attempt = len(alloc_requests)
        chosen = [_node("node-morto"), _node("node-bom-1")] if attempt == 1 else [_node("node-bom-1"), _node("node-bom-2")]
        return pb.AllocateUploadResponse(
            upload_id=f"up-{attempt}",
            file_id="f-retry",
            version=1,
            replication_factor=2,
            write_quorum=2,
            chunks=[pb.ChunkPlacement(block_id=c.block_id, index=c.index, targets=chosen) for c in kw["chunks"]],
            lease_expires_at_unix_ms=999999999,
        )

    controller.allocate_upload.side_effect = fake_alloc
    controller.commit_upload.return_value = pb.FileMetadata(
        file_id="f-retry",
        path="/retry.bin",
        version=1,
        plaintext_size_bytes=len(data),
        plaintext_sha256=hashlib.sha256(data).digest(),
        encryption=pb.ENCRYPTION_NONE,
        state=pb.FILE_COMMITTED,
    )

    def fake_store(**kw):
        if "node-morto" in kw["grpc_target"]:
            raise RuntimeError("Conexao recusada pelo node-morto")
        hdr = kw["header"]
        return pb.BlockReceipt(
            block_id=hdr.block_id,
            node_id=kw["grpc_target"].split(":")[0],
            stored_size_bytes=hdr.expected_size_bytes,
            verified_storage_sha256=hdr.expected_storage_sha256,
        )

    storage.store_block.side_effect = fake_store

    svc = ClientService(config=cfg, controller=controller, storage=storage)
    res = svc.upload_file(src, "/retry.bin")

    assert res.version == 1
    assert len(alloc_requests) == 2
    assert alloc_requests[0]["excluded_node_ids"] == []
    assert alloc_requests[1]["excluded_node_ids"] == ["node-morto"]
    assert alloc_requests[0]["operation_id"] != alloc_requests[1]["operation_id"]
    controller.abort_upload.assert_called_once_with(upload_id="up-1")


def test_upload_insufficient_nodes_empty_file_and_lease_renewer(tmp_path: Path):
    """Valida erro InsufficientNodesError, upload de arquivo vazio e thread _LeaseRenewer."""
    empty_file = tmp_path / "empty.txt"
    empty_file.write_bytes(b"")

    controller = MagicMock(spec=ControllerClient)
    controller.allocate_upload.return_value = pb.AllocateUploadResponse(
        upload_id="up-empty", file_id="f-empty", version=1, replication_factor=3, write_quorum=3, chunks=[]
    )
    controller.commit_upload.return_value = pb.FileMetadata(
        file_id="f-empty", path="/empty.txt", version=1, plaintext_size_bytes=0, state=pb.FILE_COMMITTED
    )

    storage = MagicMock(spec=StorageClient)
    svc = ClientService(config=ClientConfig(), controller=controller, storage=storage)
    meta = svc.upload_file(empty_file, "/empty.txt")
    assert meta.plaintext_size_bytes == 0
    storage.store_block.assert_not_called()

    # Erro FAILED_PRECONDITION deve levantar InsufficientNodesError
    non_empty = tmp_path / "data.txt"
    non_empty.write_bytes(b"123")
    rpc_err = grpc.RpcError()
    rpc_err.code = lambda: grpc.StatusCode.FAILED_PRECONDITION
    rpc_err.details = lambda: "Destinos insuficientes"
    controller.allocate_upload.side_effect = rpc_err

    with pytest.raises(InsufficientNodesError):
        svc.upload_file(non_empty, "/data.txt")

    # Renovador periódico de lease
    renew_ctrl = MagicMock(spec=ControllerClient)
    renew_ctrl.renew_lease.return_value = pb.RenewLeaseResponse(lease_expires_at_unix_ms=777777)
    renewer = _LeaseRenewer(renew_ctrl, "up-lease", interval_seconds=0.02)
    renewer.start()
    time.sleep(0.06)
    renewer.stop()
    assert renew_ctrl.renew_lease.call_count >= 1
    assert renewer.last_renewal_ms == 777777

