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
        if address.lower() in {"local", "none"}:
            ray.init(
                address=None,
                ignore_reinit_error=True,
                num_cpus=int(os.getenv("RAY_LOCAL_CPUS", os.cpu_count() or 1)),
            )
        else:
            ray.init(address=address, ignore_reinit_error=True)
    except Exception as exc:
        raise RuntimeError(
            f"No se pudo inicializar Ray con RAY_ADDRESS={address!r}. "
            "Verifica que ray-head este disponible y que la direccion sea correcta."
        ) from exc


def _chunk_batch_size() -> int:
    value = int(os.getenv("RAY_CHUNK_BATCH_SIZE", "16"))
    return max(1, value)


@ray.remote
def _process_chunk_batch(items: list[tuple[int, bytes]]) -> list[dict]:
    return [
        {
            "chunk_index": index,
            "sha256": hashlib.sha256(chunk).hexdigest(),
            "crc32": zlib.crc32(chunk) & 0xFFFFFFFF,
            "size_bytes": len(chunk),
            "processed_at": datetime.now(timezone.utc).isoformat(),
        }
        for index, chunk in items
    ]


def preprocess_chunks(chunks: list[bytes]) -> list[dict]:
    _ensure_ray()
    try:
        indexed_chunks = list(enumerate(chunks))
        batch_size = _chunk_batch_size()
        batches = [
            indexed_chunks[offset : offset + batch_size]
            for offset in range(0, len(indexed_chunks), batch_size)
        ]
        futures = [_process_chunk_batch.remote(batch) for batch in batches]
        metadata = [
            item
            for batch_result in ray.get(futures)
            for item in batch_result
        ]
        metadata.sort(key=lambda item: item["chunk_index"])
        return [
            {
                **item,
                "data": chunks[item["chunk_index"]],
            }
            for item in metadata
        ]
    except Exception as exc:
        raise RuntimeError(
            "Ray fallo durante el procesamiento distribuido de chunks. "
            "Revisa ray-head, ray-worker y conectividad de red."
        ) from exc
