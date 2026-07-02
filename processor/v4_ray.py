from __future__ import annotations

import hashlib
import os
import zlib
from datetime import datetime, timezone

import ray


def _ensure_ray() -> None:
    if ray.is_initialized():
        return

    address = os.getenv("RAY_ADDRESS", "auto")
    try:
        ray.init(address=address, ignore_reinit_error=True)
    except Exception as exc:
        raise RuntimeError(
            f"No se pudo inicializar Ray con RAY_ADDRESS={address!r}. "
            "Verifica que ray-head este disponible y que la direccion sea correcta."
        ) from exc


@ray.remote
def _process_chunk(index: int, chunk: bytes) -> dict:
    return {
        "chunk_index": index,
        "data": chunk,
        "sha256": hashlib.sha256(chunk).hexdigest(),
        "crc32": zlib.crc32(chunk) & 0xFFFFFFFF,
        "size_bytes": len(chunk),
        "processed_at": datetime.now(timezone.utc).isoformat(),
    }


def preprocess_chunks(chunks: list[bytes]) -> list[dict]:
    _ensure_ray()
    try:
        futures = [_process_chunk.remote(index, chunk) for index, chunk in enumerate(chunks)]
        return ray.get(futures)
    except Exception as exc:
        raise RuntimeError(
            "Ray fallo durante el procesamiento distribuido de chunks. "
            "Revisa ray-head, ray-worker y conectividad de red."
        ) from exc
