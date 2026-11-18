"""
Teste de Integração Fim-a-Fim do Cliente (ClientService conectado ao MetadataService Real).
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Dict, List

import dcomp_drive_pb2 as pb
from client.config import ClientConfig
from client.service import ClientService
from common.ports import NodeSnapshot
from common.simulated_registry import SimulatedNodeRegistry
from metadata.service import MetadataService


class _InMemoryControllerAdapter:
    """Conecta o ClientService diretamente ao MetadataService real."""

    def __init__(self, metadata_service: MetadataService):
        self.meta = metadata_service
        self.corruptions_reported: List[tuple] = []

    def allocate_upload(self, **kwargs) -> pb.AllocateUploadResponse:
        return self.meta.allocate_upload(pb.AllocateUploadRequest(**kwargs))

    def renew_lease(self, upload_id: str, timeout=None) -> pb.RenewLeaseResponse:
        return self.meta.renew_lease(pb.RenewLeaseRequest(upload_id=upload_id))

    def commit_upload(self, upload_id: str, receipts: List[pb.BlockReceipt], timeout=None) -> pb.FileMetadata:
        return self.meta.commit_upload(pb.CommitUploadRequest(upload_id=upload_id, receipts=receipts))

    def abort_upload(self, upload_id: str, timeout=None) -> None:
        self.meta.abort_upload(pb.AbortUploadRequest(upload_id=upload_id))

    def resolve_download(self, path: str, timeout=None) -> pb.ResolveDownloadResponse:
        return self.meta.resolve_download(pb.ResolveDownloadRequest(path=path))

    def report_corruption(self, block_id: str, suspected_node_id: str, observed_sha256: bytes, **kw) -> None:
        self.corruptions_reported.append((block_id, suspected_node_id, observed_sha256))
        self.meta.mark_replica_invalid(block_id, suspected_node_id)

    def close(self) -> None:
        pass


class _InMemoryStorageCluster:
    """Simula os nós de armazenamento em memória validando tamanho e hash SHA-256."""

    def __init__(self):
        self.blocks: Dict[tuple[str, str], bytes] = {}
        self.corrupt_targets: set[tuple[str, str]] = set()

    def store_block(self, grpc_target: str, header: pb.StoreBlockHeader, data: bytes, frame_size_bytes: int = 262144):
        node_id = grpc_target.split(":")[0]
        actual_sha = hashlib.sha256(data).digest()
        assert len(data) == header.expected_size_bytes
        assert actual_sha == header.expected_storage_sha256
        self.blocks[(node_id, header.block_id)] = data
        return pb.BlockReceipt(
            block_id=header.block_id,
            node_id=node_id,
            stored_size_bytes=len(data),
            verified_storage_sha256=actual_sha,
        )

    def get_block(self, grpc_target: str, block_id: str):
        node_id = grpc_target.split(":")[0]
        raw = self.blocks[(node_id, block_id)]
        if (node_id, block_id) in self.corrupt_targets:
            raw = b"CORRUPTED!" + raw[10:]
        hdr = pb.GetBlockHeader(
            block_id=block_id,
            stored_size_bytes=len(raw),
            storage_sha256=hashlib.sha256(raw).digest(),
        )
        return hdr, raw

    def close(self) -> None:
        pass


def test_e2e_client_upload_and_download_with_real_metadata_service(tmp_path: Path):
    """
    Executa ciclo completo Fim-a-Fim:
      1. Cria diretório no MetadataService real;
      2. Faz upload criptografado (AES-256-GCM) de arquivo multi-bloco pelo ClientService;
      3. Injeta corrupção física em uma réplica do 1º bloco;
      4. Faz download pelo ClientService, que detecta a corrupção, reporta e invalida a réplica no MetadataService,
         recupera o bloco de outra réplica saudável, decifra e valida o SHA-256 final.
    """
    nodes = [
        NodeSnapshot(
            f"storage-{i}",
            f"machine-{i}",
            f"disk-{i}",
            f"storage-{i}:50052",
            pb.NODE_ALIVE,
            10_000_000,
            0,
            10_000_000,
        )
        for i in range(1, 4)
    ]
    real_metadata = MetadataService(SimulatedNodeRegistry(nodes), replication_factor=3, write_quorum="all")
    ctrl_adapter = _InMemoryControllerAdapter(real_metadata)
    storage_cluster = _InMemoryStorageCluster()

    cfg = ClientConfig(logical_chunk_size_bytes=1024, grpc_frame_size_bytes=256, encryption_enabled=True)
    key = os.urandom(32)
    client_svc = ClientService(config=cfg, controller=ctrl_adapter, storage=storage_cluster)

    real_metadata.Mkdir(pb.MkdirRequest(path="/relatorios"), None)
    original_bytes = os.urandom(2500)
    src_file = tmp_path / "relatorio.bin"
    src_file.write_bytes(original_bytes)

    committed = client_svc.upload_file(src_file, "/relatorios/secreto.bin", key=key)
    assert committed.state == pb.FILE_COMMITTED
    assert committed.version == 1
    assert committed.plaintext_size_bytes == 2500
    assert committed.replication_state == pb.REPLICATION_HEALTHY

    # Corrompe uma réplica de um dos blocos armazenados
    first_stored_key = next(iter(storage_cluster.blocks.keys()))
    storage_cluster.corrupt_targets.add(first_stored_key)

    downloaded = tmp_path / "relatorio_recuperado.bin"
    dl_meta = client_svc.download_file("/relatorios/secreto.bin", downloaded, key=key)

    assert dl_meta.file_id == committed.file_id
    assert downloaded.read_bytes() == original_bytes
    assert len(ctrl_adapter.corruptions_reported) == 1

