from __future__ import annotations

import io
from dataclasses import dataclass

import psycopg2
from minio import Minio
from minio.error import S3Error


@dataclass(frozen=True)
class DedupStats:
    chunk_count: int
    unique_chunks: int
    duplicate_chunks: int
    bytes_original: int
    bytes_stored: int

    @property
    def bytes_saved(self) -> int:
        return self.bytes_original - self.bytes_stored

    @property
    def storage_saving_pct(self) -> float:
        if self.bytes_original == 0:
            return 0.0
        return (self.bytes_saved / self.bytes_original) * 100

    @property
    def dedup_ratio(self) -> float:
        if self.bytes_stored == 0:
            return 1.0
        return self.bytes_original / self.bytes_stored

    def as_dict(self) -> dict:
        return {
            "chunk_count": self.chunk_count,
            "unique_chunks": self.unique_chunks,
            "duplicate_chunks": self.duplicate_chunks,
            "bytes_original": self.bytes_original,
            "bytes_stored": self.bytes_stored,
            "bytes_saved": self.bytes_saved,
            "storage_saving_pct": self.storage_saving_pct,
            "dedup_ratio": self.dedup_ratio,
        }


def ensure_bucket(client: Minio, bucket: str) -> None:
    try:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
    except S3Error as exc:
        raise RuntimeError(f"No se pudo preparar el bucket MinIO {bucket!r}") from exc


def object_key_for_hash(chunk_hash: str) -> str:
    return f"chunks/{chunk_hash}"


def store_file_chunks(
    *,
    postgres_dsn: str,
    minio_client: Minio,
    bucket: str,
    file_id: str,
    filename: str,
    total_size: int,
    processed_chunks: list[dict],
) -> DedupStats:
    ensure_bucket(minio_client, bucket)

    unique_chunks = 0
    duplicate_chunks = 0
    bytes_stored = 0
    seen_in_file: set[str] = set()

    with psycopg2.connect(postgres_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO files (file_id, filename, total_size, chunk_count)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (file_id) DO UPDATE
                SET filename = EXCLUDED.filename,
                    total_size = EXCLUDED.total_size,
                    chunk_count = EXCLUDED.chunk_count
                """,
                (file_id, filename, total_size, len(processed_chunks)),
            )

            for chunk in processed_chunks:
                chunk_hash = chunk["sha256"]
                object_key = object_key_for_hash(chunk_hash)

                if chunk_hash in seen_in_file:
                    duplicate_chunks += 1
                else:
                    seen_in_file.add(chunk_hash)
                    unique_chunks += 1
                    bytes_stored += chunk["size_bytes"]

                cur.execute(
                    "SELECT object_key FROM unique_chunks WHERE chunk_hash = %s FOR UPDATE",
                    (chunk_hash,),
                )
                existing = cur.fetchone()

                if existing is None:
                    minio_client.put_object(
                        bucket,
                        object_key,
                        data=io.BytesIO(chunk["data"]),
                        length=chunk["size_bytes"],
                        content_type="application/octet-stream",
                    )
                    cur.execute(
                        """
                        INSERT INTO unique_chunks
                            (chunk_hash, size_bytes, crc32, object_key, ref_count)
                        VALUES (%s, %s, %s, %s, 1)
                        ON CONFLICT (chunk_hash) DO UPDATE
                        SET ref_count = unique_chunks.ref_count + 1
                        """,
                        (chunk_hash, chunk["size_bytes"], chunk["crc32"], object_key),
                    )
                else:
                    object_key = existing[0]
                    cur.execute(
                        "UPDATE unique_chunks SET ref_count = ref_count + 1 WHERE chunk_hash = %s",
                        (chunk_hash,),
                    )

                cur.execute(
                    """
                    INSERT INTO file_chunk_refs
                        (file_id, chunk_index, chunk_hash, logical_size)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (file_id, chunk["chunk_index"], chunk_hash, chunk["size_bytes"]),
                )

                # Compatibility view for the original endpoints/schema.
                cur.execute(
                    """
                    INSERT INTO chunk_metadata
                        (file_id, chunk_index, sha256, crc32, size_bytes, object_key, processed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        file_id,
                        chunk["chunk_index"],
                        chunk_hash,
                        chunk["crc32"],
                        chunk["size_bytes"],
                        object_key,
                        chunk["processed_at"],
                    ),
                )

    return DedupStats(
        chunk_count=len(processed_chunks),
        unique_chunks=unique_chunks,
        duplicate_chunks=duplicate_chunks,
        bytes_original=total_size,
        bytes_stored=bytes_stored,
    )
