from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
from datetime import datetime, timezone
from pathlib import Path


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def file_bytes(dataset: str, size_bytes: int, chunk_size_kb: int, index: int, seed: int) -> bytes:
    rng = random.Random(seed + index)
    chunk_size = chunk_size_kb * 1024

    if dataset == "random":
        return rng.randbytes(size_bytes)

    if dataset == "repeated":
        block = random.Random(seed).randbytes(chunk_size)
        return (block * ((size_bytes // chunk_size) + 1))[:size_bytes]

    if dataset == "modified":
        base = bytearray(random.Random(seed).randbytes(chunk_size))
        chunks: list[bytes] = []
        for chunk_index in range((size_bytes + chunk_size - 1) // chunk_size):
            current = bytearray(base)
            if (chunk_index + index) % 8 == 0:
                pos = (chunk_index + index) % len(current)
                current[pos] = (current[pos] + chunk_index + index + 1) % 256
            chunks.append(bytes(current))
        return b"".join(chunks)[:size_bytes]

    if dataset == "mixed":
        repeated_block = random.Random(seed).randbytes(chunk_size)
        chunks = []
        for chunk_index in range((size_bytes + chunk_size - 1) // chunk_size):
            if chunk_index % 2 == 0:
                chunks.append(repeated_block)
            else:
                chunks.append(rng.randbytes(chunk_size))
        return b"".join(chunks)[:size_bytes]

    raise ValueError(f"dataset invalido: {dataset}")


def generate_batch(
    *,
    dataset: str,
    num_files: int,
    file_size_mb: int,
    output: Path,
    chunk_size_kb: int,
    seed: int,
    batch_id: str | None = None,
) -> dict:
    output.mkdir(parents=True, exist_ok=True)
    batch_id = batch_id or output.name
    size_bytes = file_size_mb * 1024 * 1024
    files = []

    for index in range(num_files):
        filename = f"file_{index:03d}.bin"
        path = output / filename
        data = file_bytes(dataset, size_bytes, chunk_size_kb, index, seed)
        path.write_bytes(data)
        files.append(
            {
                "filename": filename,
                "size_bytes": len(data),
                "size_mb": file_size_mb,
                "sha256": hashlib.sha256(data).hexdigest(),
            }
        )

    manifest = {
        "batch_id": batch_id,
        "dataset_type": dataset,
        "num_files": num_files,
        "file_size_mb": file_size_mb,
        "chunk_size_kb": chunk_size_kb,
        "total_size_mb": num_files * file_size_mb,
        "seed": seed,
        "files": files,
        "created_at": utc_now(),
    }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description="Genera batches reproducibles para benchmark HPC.")
    parser.add_argument("--dataset", choices=["random", "repeated", "modified", "mixed"], required=True)
    parser.add_argument("--num-files", type=int, required=True)
    parser.add_argument("--file-size-mb", type=int, required=True)
    parser.add_argument("--chunk-size-kb", type=int, default=1024)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=int(os.getenv("BENCHMARK_SEED", "12345")))
    args = parser.parse_args()

    manifest = generate_batch(
        dataset=args.dataset,
        num_files=args.num_files,
        file_size_mb=args.file_size_mb,
        output=args.output,
        chunk_size_kb=args.chunk_size_kb,
        seed=args.seed,
    )
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
