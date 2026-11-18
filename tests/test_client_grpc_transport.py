"""
Testes da Camada de Transporte gRPC do Cliente (StorageClient em Streaming e ControllerClient).
"""
from __future__ import annotations

import hashlib
from typing import List
from unittest.mock import MagicMock

import grpc
import pytest
from google.protobuf import empty_pb2

import dcomp_drive_pb2 as pb
from client.controller_client import ControllerClient, format_rpc_error
from client.storage_client import StorageClient, StorageStreamError


def test_storage_client_store_get_delete_streaming():
    """Valida envio em frames de StoreBlock (header + fatias), leitura em GetBlock e DeleteBlock."""
    sent_frames: List[pb.StoreBlockFrame] = []

    class FakeStorageStub:
        def StoreBlock(self, frame_iter, timeout=None):
            for f in frame_iter:
                sent_frames.append(f)
            hdr = sent_frames[0].header
            return pb.BlockReceipt(
                block_id=hdr.block_id,
                node_id="node-1",
                stored_size_bytes=hdr.expected_size_bytes,
                verified_storage_sha256=hdr.expected_storage_sha256,
            )

        def GetBlock(self, request, timeout=None):
            data = b"bloco-em-streaming-" * 20
            yield pb.GetBlockFrame(
                header=pb.GetBlockHeader(
                    block_id=request.block_id,
                    stored_size_bytes=len(data),
                    storage_sha256=hashlib.sha256(data).digest(),
                )
            )
            yield pb.GetBlockFrame(data=data[:100])
            yield pb.GetBlockFrame(data=data[100:])

        def DeleteBlock(self, request, timeout=None):
            return empty_pb2.Empty()

    client = StorageClient(default_timeout=15.0)
    stub = FakeStorageStub()
    payload = b"D" * 1200
    header = pb.StoreBlockHeader(
        block_id="blk-10",
        upload_id="up-10",
        version=1,
        expected_size_bytes=len(payload),
        expected_storage_sha256=hashlib.sha256(payload).digest(),
    )

    receipt = client.store_block("storage-1:50052", header, payload, frame_size_bytes=500, stub=stub)
    assert receipt.block_id == "blk-10"
    assert len(sent_frames) == 4  # 1 header + 3 frames de dados (500, 500, 200)
    assert sent_frames[0].WhichOneof("payload") == "header"
    assert b"".join(f.data for f in sent_frames[1:]) == payload

    hdr, data = client.get_block("storage-1:50052", "blk-10", stub=stub)
    assert hdr.block_id == "blk-10"
    assert data == b"bloco-em-streaming-" * 20

    client.delete_block("storage-1:50052", "blk-10", stub=stub)


def test_storage_client_missing_header_raises():
    """Garante que stream GetBlock sem cabeçalho lança StorageStreamError."""

    class BrokenStorageStub:
        def GetBlock(self, request, timeout=None):
            yield pb.GetBlockFrame(data=b"dados-sem-header")

    client = StorageClient()
    with pytest.raises(StorageStreamError):
        client.get_block("storage-1:50052", "blk-x", stub=BrokenStorageStub())


def test_controller_client_all_rpcs_and_error_formatting():
    """Valida todos os 14 RPCs do ControllerClient e a tradução de erros em format_rpc_error."""
    meta_stub = MagicMock()
    ctrl_stub = MagicMock()
    ctrl = ControllerClient(metadata_stub=meta_stub, control_stub=ctrl_stub)

    ctrl.allocate_upload("/arq.bin", "op-1", 100, b"a" * 32, pb.ENCRYPTION_NONE, [])
    meta_stub.AllocateUpload.assert_called_once()

    ctrl.renew_lease("up-1")
    meta_stub.RenewLease.assert_called_once()

    ctrl.commit_upload("up-1", [])
    meta_stub.CommitUpload.assert_called_once()

    ctrl.abort_upload("up-1")
    meta_stub.AbortUpload.assert_called_once()

    ctrl.resolve_download("/arq.bin")
    meta_stub.ResolveDownload.assert_called_once()

    ctrl.delete("/arq.bin", expected_version=1)
    meta_stub.Delete.assert_called_once()

    ctrl.rename("/arq.bin", "/novo.bin", overwrite=True)
    meta_stub.Rename.assert_called_once()

    ctrl.list_dir("/", page_size=50, page_token="t1")
    meta_stub.ListDir.assert_called_once()

    ctrl.stat("/arq.bin")
    meta_stub.Stat.assert_called_once()

    ctrl.mkdir("/pasta")
    meta_stub.Mkdir.assert_called_once()

    ctrl.rmdir("/pasta", recursive=True)
    meta_stub.Rmdir.assert_called_once()

    ctrl.drain_node("node-1")
    ctrl_stub.DrainNode.assert_called_once()

    ctrl.get_status()
    ctrl_stub.GetStatus.assert_called_once()

    ctrl.report_corruption("blk-1", "node-1", b"b" * 32)
    ctrl_stub.ReportCorruption.assert_called_once()

    for code, snippet in [
        (grpc.StatusCode.NOT_FOUND, "Recurso não encontrado"),
        (grpc.StatusCode.ALREADY_EXISTS, "já existe"),
        (grpc.StatusCode.FAILED_PRECONDITION, "Condição prévia"),
        (grpc.StatusCode.ABORTED, "abortada"),
        (grpc.StatusCode.UNAVAILABLE, "inacessível"),
        (grpc.StatusCode.DATA_LOSS, "corrupção"),
    ]:
        rpc_err = grpc.RpcError()
        rpc_err.code = lambda c=code: c
        rpc_err.details = lambda: "detalhe"
        assert snippet in format_rpc_error(rpc_err)

