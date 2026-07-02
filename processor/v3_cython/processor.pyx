# cython: language_level=3, boundscheck=False, wraparound=False, initializedcheck=False, cdivision=True
from __future__ import annotations

import hashlib
from datetime import datetime, timezone

from cython.parallel cimport prange


cdef unsigned int _crc32_bytes(const unsigned char[:] data) noexcept nogil:
    cdef unsigned int crc = 0xFFFFFFFF
    cdef unsigned int poly = 0xEDB88320
    cdef Py_ssize_t i
    cdef int bit

    # CRC32 is stateful, so each byte depends on the state produced by the
    # previous byte. This keeps the exact zlib-compatible value in compiled C.
    for i in range(data.shape[0]):
        crc = crc ^ data[i]
        for bit in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ poly
            else:
                crc = crc >> 1
    return crc ^ 0xFFFFFFFF


cdef unsigned long long _parallel_byte_sum(const unsigned char[:] data) noexcept nogil:
    cdef Py_ssize_t i
    cdef unsigned long long total = 0

    # OpenMP/prange demonstration over chunk bytes. The canonical CRC32 value is
    # still calculated by _crc32_bytes because CRC32 state cannot be naively
    # split per byte without a combine step.
    for i in prange(data.shape[0], nogil=True, schedule="static"):
        total += data[i]
    return total


cpdef unsigned int crc32_cython(bytes chunk):
    cdef const unsigned char[:] view = chunk
    _parallel_byte_sum(view)
    return _crc32_bytes(view)


def preprocess_chunks(chunks: list[bytes]) -> list[dict]:
    cdef int index
    cdef bytes chunk
    results = []

    for index, chunk in enumerate(chunks):
        results.append(
            {
                "chunk_index": index,
                "data": chunk,
                "sha256": hashlib.sha256(chunk).hexdigest(),
                "crc32": int(crc32_cython(chunk)),
                "size_bytes": len(chunk),
                "processed_at": datetime.now(timezone.utc).isoformat(),
            }
        )
    return results
