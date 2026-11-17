from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional
from uuid import uuid4

import dcomp_drive_pb2 as pb
from client.crypto import encrypt_chunk, generate_nonce


@dataclass
class PreparedChunk:
    index: int
    block_id: str
    raw_bytes: bytes
    stored_bytes: bytes
    nonce: bytes
    stored_size_bytes: int
    storage_sha256: bytes
    manifest: pb.ChunkManifest


@dataclass
class PreparedUpload:
    local_path: Path
    remote_path: str
    plaintext_size_bytes: int
    plaintext_sha256: bytes
    encryption_mode: int
    key: Optional[bytes] = None
    chunks: List[PreparedChunk] = field(default_factory=list)

    @property
    def manifests(self) -> List[pb.ChunkManifest]:
        return [c.manifest for c in self.chunks]

    def regenerate_attempt(self) -> None:
        """
        Regenera novos block_ids (e novos nonces se criptografado) para uma nova
        tentativa de upload após falha, conforme exigido pelo contrato.
        """
        new_chunks: List[PreparedChunk] = []
        for c in self.chunks:
            new_block_id = str(uuid4())
            if self.encryption_mode == pb.ENCRYPTION_AES_256_GCM and self.key:
                new_nonce = generate_nonce()
                stored_bytes = encrypt_chunk(c.raw_bytes, self.key, new_nonce)
            else:
                new_nonce = b""
                stored_bytes = c.raw_bytes

            storage_sha256 = hashlib.sha256(stored_bytes).digest()
            stored_size_bytes = len(stored_bytes)
            manifest = pb.ChunkManifest(
                block_id=new_block_id,
                index=c.index,
                stored_size_bytes=stored_size_bytes,
                storage_sha256=storage_sha256,
                nonce=new_nonce,
            )
            new_chunks.append(
                PreparedChunk(
                    index=c.index,
                    block_id=new_block_id,
                    raw_bytes=c.raw_bytes,
                    stored_bytes=stored_bytes,
                    nonce=new_nonce,
                    stored_size_bytes=stored_size_bytes,
                    storage_sha256=storage_sha256,
                    manifest=manifest,
                )
            )
        self.chunks = new_chunks

    def get_chunk_by_index(self, index: int) -> PreparedChunk:
        return self.chunks[index]

    def get_chunk_by_block_id(self, block_id: str) -> PreparedChunk:
        for c in self.chunks:
            if c.block_id == block_id:
                return c
        raise KeyError(f"Bloco com block_id {block_id} não encontrado.")


def prepare_file_upload(
    file_path: str | Path,
    remote_path: str,
    chunk_size: int = 4 * 1024 * 1024,
    key: Optional[bytes] = None,
) -> PreparedUpload:
    """
    Lê o arquivo local, calcula os hashes SHA-256 e divide em blocos lógicos.
    Se a chave de criptografia for fornecida, aplica AES-256-GCM em cada bloco.
    """
    local = Path(file_path).resolve()
    if not local.is_file():
        raise FileNotFoundError(f"Arquivo local não encontrado: {local}")

    file_size = local.stat().st_size
    encryption_mode = (
        pb.ENCRYPTION_AES_256_GCM if key is not None else pb.ENCRYPTION_NONE
    )

    # Cálculo do hash global do arquivo original
    hasher = hashlib.sha256()
    with open(local, "rb") as f:
        while True:
            chunk = f.read(256 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    plaintext_sha256 = hasher.digest()

    prepared = PreparedUpload(
        local_path=local,
        remote_path=remote_path,
        plaintext_size_bytes=file_size,
        plaintext_sha256=plaintext_sha256,
        encryption_mode=encryption_mode,
        key=key,
    )

    # Se o arquivo for vazio (0 bytes), não cria blocos (chunk_count = 0)
    if file_size == 0:
        return prepared

    # Fatiamento em chunks
    chunks: List[PreparedChunk] = []
    with open(local, "rb") as f:
        index = 0
        while True:
            raw_bytes = f.read(chunk_size)
            if not raw_bytes:
                break

            block_id = str(uuid4())
            if encryption_mode == pb.ENCRYPTION_AES_256_GCM and key:
                nonce = generate_nonce()
                stored_bytes = encrypt_chunk(raw_bytes, key, nonce)
            else:
                nonce = b""
                stored_bytes = raw_bytes

            storage_sha256 = hashlib.sha256(stored_bytes).digest()
            stored_size_bytes = len(stored_bytes)

            manifest = pb.ChunkManifest(
                block_id=block_id,
                index=index,
                stored_size_bytes=stored_size_bytes,
                storage_sha256=storage_sha256,
                nonce=nonce,
            )

            chunks.append(
                PreparedChunk(
                    index=index,
                    block_id=block_id,
                    raw_bytes=raw_bytes,
                    stored_bytes=stored_bytes,
                    nonce=nonce,
                    stored_size_bytes=stored_size_bytes,
                    storage_sha256=storage_sha256,
                    manifest=manifest,
                )
            )
            index += 1

    prepared.chunks = chunks
    return prepared

