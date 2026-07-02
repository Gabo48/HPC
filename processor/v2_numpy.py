from __future__ import annotations

import hashlib
import zlib
from datetime import datetime, timezone

import numpy as np


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def preprocess_chunks(chunks: list[bytes]) -> list[dict]:
    results: list[dict] = []
    for index, chunk in enumerate(chunks):
        array = np.frombuffer(chunk, dtype=np.uint8)
        results.append(
            {
                "chunk_index": index,
                "data": chunk,
                "sha256": hashlib.sha256(array.tobytes()).hexdigest(),
                "crc32": zlib.crc32(array) & 0xFFFFFFFF,
                "size_bytes": int(array.nbytes),
                "processed_at": _utc_now(),
            }
        )
    return results
