from __future__ import annotations

import io

import psycopg2
from minio import Minio


def reconstruct_file(*, postgres_dsn: str, minio_client: Minio, bucket: str, file_id: str) -> bytes:
    parts: list[bytes] = []

    with psycopg2.connect(postgres_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT u.object_key
                FROM file_chunk_refs r
                JOIN unique_chunks u ON u.chunk_hash = r.chunk_hash
                WHERE r.file_id = %s
                ORDER BY r.chunk_index
                """,
                (file_id,),
            )
            rows = cur.fetchall()

    if not rows:
        raise FileNotFoundError(f"No hay chunks para file_id={file_id}")

    for (object_key,) in rows:
        response = minio_client.get_object(bucket, object_key)
        try:
            parts.append(response.read())
        finally:
            response.close()
            response.release_conn()

    buffer = io.BytesIO()
    for part in parts:
        buffer.write(part)
    return buffer.getvalue()
