"""SD-Drive Client Module (Pessoa 4 - Cliente e CLI)."""

from client.config import ClientConfig, load_client_config
from client.crypto import (
    decrypt_chunk,
    derive_or_validate_key,
    encrypt_chunk,
    generate_nonce,
)
from client.chunker import PreparedUpload, prepare_file_upload

__all__ = [
    "ClientConfig",
    "load_client_config",
    "derive_or_validate_key",
    "generate_nonce",
    "encrypt_chunk",
    "decrypt_chunk",
    "PreparedUpload",
    "prepare_file_upload",
]

