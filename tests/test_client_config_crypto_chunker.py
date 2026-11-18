"""
Testes de Configuração, Criptografia AES-256-GCM e Fatiamento em Blocos (Chunker) do Cliente.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path

import pytest

import dcomp_drive_pb2 as pb
from client.chunker import prepare_file_upload
from client.config import ClientConfig, load_client_config
from client.crypto import (
    DecryptionError,
    InvalidKeyError,
    decrypt_chunk,
    derive_or_validate_key,
    encrypt_chunk,
    generate_nonce,
)


def test_config_defaults(monkeypatch: pytest.MonkeyPatch):
    """Verifica os valores padrão exigidos pelo contrato na configuração do cliente."""
    for var in (
        "DCOMP_CONFIG",
        "DCOMP_CLIENT__CENTRAL_TARGET",
        "DCOMP_CONTROLLER_TARGET",
        "DCOMP_CENTRAL_TARGET",
        "DCOMP_CLIENT_EVENTS_PATH",
        "DCOMP_OBSERVABILITY__LOG_FILE",
        "DCOMP_CLIENT__ENCRYPTION_ENABLED",
        "DCOMP_ENCRYPTION_ENABLED",
    ):
        monkeypatch.delenv(var, raising=False)

    cfg = ClientConfig()
    assert cfg.central_target == "controller:50051"
    assert cfg.logical_chunk_size_bytes == 4 * 1024 * 1024
    assert cfg.grpc_frame_size_bytes == 256 * 1024
    assert cfg.lease_renew_interval_seconds == 180.0
    assert cfg.upload_retry_max_attempts == 3
    assert cfg.encryption_enabled is False


def test_config_yaml_and_env_overrides(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Valida leitura de arquivo YAML e sobrescrita por variáveis de ambiente."""
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text(
        "client:\n"
        "  central_target: '10.0.0.5:50051'\n"
        "  logical_chunk_size_bytes: 1048576\n"
        "  grpc_frame_size_bytes: 65536\n"
        "  encryption_enabled: true\n"
        "observability:\n"
        "  client_events_path: '/tmp/events.jsonl'\n",
        encoding="utf-8",
    )

    cfg = load_client_config(yaml_file)
    assert cfg.central_target == "10.0.0.5:50051"
    assert cfg.logical_chunk_size_bytes == 1048576
    assert cfg.grpc_frame_size_bytes == 65536
    assert cfg.encryption_enabled is True
    assert cfg.client_events_path == "/tmp/events.jsonl"

    monkeypatch.setenv("DCOMP_CLIENT__CENTRAL_TARGET", "central:50051")
    monkeypatch.setenv("DCOMP_CLIENT__ENCRYPTION_ENABLED", "false")
    monkeypatch.setenv("DCOMP_CLIENT_EVENTS_PATH", "/logs/custom.jsonl")

    cfg_env = load_client_config(yaml_file)
    assert cfg_env.central_target == "central:50051"
    assert cfg_env.encryption_enabled is False
    assert cfg_env.client_events_path == "/logs/custom.jsonl"


def test_crypto_key_validation_and_aes_256_gcm_roundtrip(monkeypatch: pytest.MonkeyPatch):
    """Valida aceitação de chaves de 32B/64-hex/32-ASCII, cifragem/decifragem GCM e detecção de adulteração."""
    raw_key = os.urandom(32)
    hex_key = raw_key.hex()
    ascii_key = "K" * 32

    assert derive_or_validate_key(raw_key) == raw_key
    assert derive_or_validate_key(hex_key) == raw_key
    assert derive_or_validate_key(ascii_key) == b"K" * 32

    monkeypatch.setenv("DCOMP_ENCRYPTION_KEY", hex_key)
    assert derive_or_validate_key(None) == raw_key

    with pytest.raises(InvalidKeyError):
        derive_or_validate_key(b"curta")
    with pytest.raises(InvalidKeyError):
        derive_or_validate_key("12345")

    nonce = generate_nonce()
    assert len(nonce) == 12

    plaintext = b"Bloco confidencial do SD-Drive" * 30
    ciphertext = encrypt_chunk(plaintext, raw_key, nonce)
    assert len(ciphertext) == len(plaintext) + 16
    assert decrypt_chunk(ciphertext, raw_key, nonce) == plaintext

    # Adulteração de 1 bit nos dados cifrados deve disparar DecryptionError
    corrupted = bytearray(ciphertext)
    corrupted[5] ^= 0x01
    with pytest.raises(DecryptionError):
        decrypt_chunk(bytes(corrupted), raw_key, nonce)

    # Nonce incorreto deve disparar DecryptionError
    with pytest.raises(DecryptionError):
        decrypt_chunk(ciphertext, raw_key, generate_nonce())


def test_chunker_splitting_hashes_and_regenerate_attempt(tmp_path: Path):
    """Valida divisão em chunks, hashes SHA-256, modo cifrado, arquivo vazio e regenerate_attempt()."""
    sample = tmp_path / "amostra.bin"
    raw = os.urandom(2500)
    sample.write_bytes(raw)

    # Sem criptografia (chunk de 1000 bytes -> 3 chunks: 1000, 1000, 500)
    prep = prepare_file_upload(sample, "/docs/amostra.bin", chunk_size=1000, key=None)
    assert prep.plaintext_size_bytes == 2500
    assert prep.plaintext_sha256 == hashlib.sha256(raw).digest()
    assert prep.encryption_mode == pb.ENCRYPTION_NONE
    assert len(prep.chunks) == 3
    assert [c.index for c in prep.chunks] == [0, 1, 2]
    assert [c.stored_size_bytes for c in prep.chunks] == [1000, 1000, 500]

    # Com criptografia AES-256-GCM
    key = os.urandom(32)
    prep_enc = prepare_file_upload(sample, "/docs/enc.bin", chunk_size=1000, key=key)
    assert prep_enc.encryption_mode == pb.ENCRYPTION_AES_256_GCM
    for c in prep_enc.chunks:
        assert len(c.nonce) == 12
        assert c.stored_size_bytes == len(c.raw_bytes) + 16
        assert c.storage_sha256 == hashlib.sha256(c.stored_bytes).digest()
        assert decrypt_chunk(c.stored_bytes, key, c.nonce) == c.raw_bytes

    # Regeneração para retry deve trocar block_id e nonce de todos os blocos
    old_ids = [c.block_id for c in prep_enc.chunks]
    old_nonces = [c.nonce for c in prep_enc.chunks]
    prep_enc.regenerate_attempt()
    assert all(old != new.block_id for old, new in zip(old_ids, prep_enc.chunks))
    assert all(old != new.nonce for old, new in zip(old_nonces, prep_enc.chunks))

    # Arquivo vazio (0 bytes)
    empty = tmp_path / "vazio.txt"
    empty.write_bytes(b"")
    prep_empty = prepare_file_upload(empty, "/vazio.txt")
    assert prep_empty.plaintext_size_bytes == 0
    assert prep_empty.chunks == []

