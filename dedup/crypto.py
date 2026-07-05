from __future__ import annotations

import base64
import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


MAGIC = b"HPCENC1"
NONCE_SIZE = 12


def encryption_enabled() -> bool:
    value = os.getenv("CHUNK_ENCRYPTION_ENABLED", "false").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _key_bytes() -> bytes:
    raw = os.getenv("CHUNK_ENCRYPTION_KEY")
    if not raw:
        raise RuntimeError("CHUNK_ENCRYPTION_KEY es requerido cuando CHUNK_ENCRYPTION_ENABLED=true")

    candidates: list[bytes] = []
    try:
        candidates.append(base64.b64decode(raw, validate=True))
    except Exception:
        pass
    try:
        candidates.append(bytes.fromhex(raw))
    except ValueError:
        pass
    candidates.append(raw.encode("utf-8"))

    for candidate in candidates:
        if len(candidate) in {16, 24, 32}:
            return candidate

    raise RuntimeError("CHUNK_ENCRYPTION_KEY debe tener 16, 24 o 32 bytes, en base64, hex o texto")


def encrypt_payload(data: bytes, aad: bytes = b"") -> bytes:
    nonce = os.urandom(NONCE_SIZE)
    encrypted = AESGCM(_key_bytes()).encrypt(nonce, data, aad)
    return MAGIC + nonce + encrypted


def decrypt_payload(data: bytes, aad: bytes = b"") -> bytes:
    if not data.startswith(MAGIC):
        return data
    nonce_start = len(MAGIC)
    nonce_end = nonce_start + NONCE_SIZE
    nonce = data[nonce_start:nonce_end]
    encrypted = data[nonce_end:]
    return AESGCM(_key_bytes()).decrypt(nonce, encrypted, aad)
