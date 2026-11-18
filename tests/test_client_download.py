"""
Testes do Orquestrador de Download do Cliente (Fallback de Réplicas, ReportCorruption, Decifragem e Escrita Atômica).
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
from unittest.mock import MagicMock

import grpc
import pytest

import dcomp_drive_pb2 as pb
from client.config import ClientConfig
from client.controller_client import ControllerClient
from client.crypto import encrypt_chunk, generate_nonce
from client.service import ClientService, DownloadError, IntegrityError
from client.storage_client import StorageClient


def _node(node_id: str) -> pb.NodeAddress:
    return pb.NodeAddress(
        node_id=node_id,
        machine_id=f"m-{node_id}",
        disk_id=f"d-{node_id}",
        grpc_target=f"{node_id}:50052",
    )


def test_download_happy_path_and_encrypted(tmp_path: Path):
    """Valida download de arquivo criptografado multi-bloco com ordenação por índice e rename atômico."""
    key = os.urandom(32)
    p0 = b"Primeiro bloco de dados " * 30
    p1 = b"Segundo bloco de dados " * 20
    full_plain = p0 + p1

    n0, n1 = generate_nonce(), generate_nonce()
    c0, c1 = encrypt_chunk(p0, key, n0), encrypt_chunk(p1, key, n1)

    cl0 = pb.ChunkLocation(
        manifest=pb.ChunkManifest(
            block_id="b-0", index=0, stored_size_bytes=len(c0), storage_sha256=hashlib.sha256(c0).digest(), nonce=n0
        ),
        replicas=[pb.ReplicaLocation(node=_node("s1"), healthy=True)],
    )
    cl1 = pb.ChunkLocation(
        manifest=pb.ChunkManifest(
            block_id="b-1", index=1, stored_size_bytes=len(c1), storage_sha256=hashlib.sha256(c1).digest(), nonce=n1
        ),
        replicas=[pb.ReplicaLocation(node=_node("s2"), healthy=True)],
    )

    meta = pb.FileMetadata(
        file_id="f-enc",
        path="/secreto.bin",
        version=3,
        plaintext_size_bytes=len(full_plain),
        plaintext_sha256=hashlib.sha256(full_plain).digest(),
        encryption=pb.ENCRYPTION_AES_256_GCM,
        state=pb.FILE_COMMITTED,
    )

    controller = MagicMock(spec=ControllerClient)
    # Devolve chunks fora de ordem (cl1, cl0) para validar a ordenação por index
    controller.resolve_download.return_value = pb.ResolveDownloadResponse(file=meta, chunks=[cl1, cl0])

    storage = MagicMock(spec=StorageClient)
    storage.get_block.side_effect = [
        (pb.GetBlockHeader(block_id="b-0"), c0),
        (pb.GetBlockHeader(block_id="b-1"), c1),
    ]

    svc = ClientService(config=ClientConfig(encryption_enabled=True), controller=controller, storage=storage)
    dest = tmp_path / "baixado.bin"
    res = svc.download_file("/secreto.bin", dest, key=key)

    assert dest.exists()
    assert dest.read_bytes() == full_plain
    assert res.version == 3
    assert [p for p in tmp_path.iterdir() if ".tmp." in p.name] == []


def test_download_corruption_and_network_fallback(tmp_path: Path):
    """
    Valida que:
      - 1ª réplica com hash corrompido aciona ReportCorruption e faz fallback;
      - 2ª réplica com falha de rede (UNAVAILABLE) faz fallback sem chamar ReportCorruption;
      - 3ª réplica saudável entrega o bloco com sucesso.
    """
    good_data = b"conteudo-integro-do-bloco" * 20
    corrupt_data = b"conteudo-adulterado-xxxx" * 20

    cl = pb.ChunkLocation(
        manifest=pb.ChunkManifest(
            block_id="b-fb",
            index=0,
            stored_size_bytes=len(good_data),
            storage_sha256=hashlib.sha256(good_data).digest(),
        ),
        replicas=[
            pb.ReplicaLocation(node=_node("node-corrupt"), healthy=True),
            pb.ReplicaLocation(node=_node("node-down"), healthy=True),
            pb.ReplicaLocation(node=_node("node-ok"), healthy=True),
        ],
    )
    meta = pb.FileMetadata(
        file_id="f-fb",
        path="/arq.bin",
        version=1,
        plaintext_size_bytes=len(good_data),
        plaintext_sha256=hashlib.sha256(good_data).digest(),
        encryption=pb.ENCRYPTION_NONE,
        state=pb.FILE_COMMITTED,
    )

    controller = MagicMock(spec=ControllerClient)
    controller.resolve_download.return_value = pb.ResolveDownloadResponse(file=meta, chunks=[cl])

    net_err = grpc.RpcError()
    net_err.code = lambda: grpc.StatusCode.UNAVAILABLE
    net_err.details = lambda: "Connection refused"

    storage = MagicMock(spec=StorageClient)
    storage.get_block.side_effect = [
        (pb.GetBlockHeader(block_id="b-fb"), corrupt_data),
        net_err,
        (pb.GetBlockHeader(block_id="b-fb"), good_data),
    ]

    svc = ClientService(config=ClientConfig(), controller=controller, storage=storage)
    dest = tmp_path / "recuperado.bin"
    svc.download_file("/arq.bin", dest)

    assert dest.read_bytes() == good_data
    # ReportCorruption chamado exatamente 1 vez (apenas para node-corrupt, não para node-down)
    controller.report_corruption.assert_called_once()
    assert controller.report_corruption.call_args.kwargs["suspected_node_id"] == "node-corrupt"


def test_download_failures_clean_temporary_files(tmp_path: Path):
    """Garante remoção do arquivo temporário quando todas as réplicas falham ou o hash final diverge."""
    data = b"dados-bloco"
    cl = pb.ChunkLocation(
        manifest=pb.ChunkManifest(
            block_id="b-fail",
            index=0,
            stored_size_bytes=len(data),
            storage_sha256=hashlib.sha256(data).digest(),
        ),
        replicas=[pb.ReplicaLocation(node=_node("n1"), healthy=True)],
    )

    # Caso 1: Todas as réplicas retornam dados corrompidos -> DownloadError
    meta_ok = pb.FileMetadata(
        file_id="f-1",
        path="/a.bin",
        version=1,
        plaintext_size_bytes=len(data),
        plaintext_sha256=hashlib.sha256(data).digest(),
        encryption=pb.ENCRYPTION_NONE,
        state=pb.FILE_COMMITTED,
    )
    controller = MagicMock(spec=ControllerClient)
    controller.resolve_download.return_value = pb.ResolveDownloadResponse(file=meta_ok, chunks=[cl])
    storage = MagicMock(spec=StorageClient)
    storage.get_block.return_value = (pb.GetBlockHeader(block_id="b-fail"), b"corrompido")

    svc = ClientService(config=ClientConfig(), controller=controller, storage=storage)
    dest1 = tmp_path / "falha1.bin"
    with pytest.raises(DownloadError):
        svc.download_file("/a.bin", dest1)
    assert not dest1.exists()

    # Caso 2: Bloco passa, mas hash final do arquivo completo não bate -> IntegrityError
    meta_bad_final = pb.FileMetadata(
        file_id="f-2",
        path="/b.bin",
        version=1,
        plaintext_size_bytes=len(data),
        plaintext_sha256=hashlib.sha256(b"outro-conteudo-global").digest(),
        encryption=pb.ENCRYPTION_NONE,
        state=pb.FILE_COMMITTED,
    )
    controller.resolve_download.return_value = pb.ResolveDownloadResponse(file=meta_bad_final, chunks=[cl])
    storage.get_block.return_value = (pb.GetBlockHeader(block_id="b-fail"), data)

    dest2 = tmp_path / "falha2.bin"
    with pytest.raises(IntegrityError):
        svc.download_file("/b.bin", dest2)
    assert not dest2.exists()
    assert [p for p in tmp_path.iterdir() if ".tmp." in p.name] == []

