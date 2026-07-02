from __future__ import annotations

import hashlib
import zlib
from datetime import datetime, timezone


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def preprocess_chunks(chunks: list[bytes]) -> list[dict]:
    results: list[dict] = []
    for index, chunk in enumerate(chunks):
        results.append(
            {
                "chunk_index": index,
                "data": chunk,
                "sha256": hashlib.sha256(chunk).hexdigest(),
                "crc32": zlib.crc32(chunk) & 0xFFFFFFFF,
                "size_bytes": len(chunk),
                "processed_at": _utc_now(),
            }
        )
    return results
