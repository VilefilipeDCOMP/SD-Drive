from __future__ import annotations

import os
from typing import Optional

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


class CryptoError(Exception):
    """Exceção base para erros de criptografia."""
    pass


class InvalidKeyError(CryptoError, ValueError):
    """Chave inválida para AES-256."""
    pass


class DecryptionError(CryptoError):
    """Falha de autenticação ou decifragem (ex: tag inválida ou dados adulterados)."""
    pass


def derive_or_validate_key(key_input: str | bytes | None = None) -> bytes:
    """
    Valida e normaliza uma chave AES-256 (32 bytes).
    Aceita:
    - 32 bytes brutos.
    - String hexadecimal de 64 caracteres (ex: '0123...cdef').
    - String de exatamente 32 caracteres ASCII/UTF-8.
    Se não for fornecida, busca em os.environ['DCOMP_ENCRYPTION_KEY'].
    """
    if key_input is None:
        key_input = os.getenv("DCOMP_ENCRYPTION_KEY")

    if key_input is None:
        raise InvalidKeyError(
            "Chave de criptografia não fornecida. Passe via argumento CLI (--key) "
            "ou defina a variável de ambiente DCOMP_ENCRYPTION_KEY."
        )

    if isinstance(key_input, bytes):
        if len(key_input) == 32:
            return key_input
        raise InvalidKeyError(
            f"Chave AES-256 em bytes deve ter exatamente 32 bytes (recebido: {len(key_input)} bytes)."
        )

    if isinstance(key_input, str):
        cleaned = key_input.strip()
        # Se for string hexadecimal de 64 caracteres
        if len(cleaned) == 64:
            try:
                raw_bytes = bytes.fromhex(cleaned)
                if len(raw_bytes) == 32:
                    return raw_bytes
            except ValueError:
                pass
        # Se for string de 32 bytes
        encoded = cleaned.encode("utf-8")
        if len(encoded) == 32:
            return encoded

        raise InvalidKeyError(
            f"Chave AES-256 deve ter 32 bytes ou 64 caracteres hexadecimais (recebido tamanho: {len(cleaned)})."
        )

    raise InvalidKeyError(f"Tipo de chave não suportado: {type(key_input)}")


def generate_nonce() -> bytes:
    """Gera um nonce aleatório de 12 bytes (padrão AES-GCM)."""
    return os.urandom(12)


def encrypt_chunk(
    data: bytes,
    key: bytes,
    nonce: bytes,
    associated_data: Optional[bytes] = None,
) -> bytes:
    """
    Cifra os dados com AES-256-GCM.
    Retorna o texto cifrado com a tag de autenticação de 16 bytes anexada ao final.
    """
    if len(nonce) != 12:
        raise CryptoError(f"Nonce do AES-GCM deve ter 12 bytes (recebido: {len(nonce)}).")
    try:
        aesgcm = AESGCM(key)
        return aesgcm.encrypt(nonce, data, associated_data)
    except Exception as e:
        raise CryptoError(f"Erro ao cifrar bloco: {e}") from e


def decrypt_chunk(
    ciphertext: bytes,
    key: bytes,
    nonce: bytes,
    associated_data: Optional[bytes] = None,
) -> bytes:
    """
    Decifra os dados cifrados e valida a tag de autenticação GCM.
    Lança DecryptionError se o nonce, chave ou dados forem divergentes/adulterados.
    """
    if len(nonce) != 12:
        raise CryptoError(f"Nonce do AES-GCM deve ter 12 bytes (recebido: {len(nonce)}).")
    try:
        aesgcm = AESGCM(key)
        return aesgcm.decrypt(nonce, ciphertext, associated_data)
    except InvalidTag as e:
        raise DecryptionError(
            "Falha de autenticação AES-GCM: dados adulterados, nonce incorreto ou chave inválida."
        ) from e
    except Exception as e:
        raise CryptoError(f"Erro ao decifrar bloco: {e}") from e

