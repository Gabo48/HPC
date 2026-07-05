from __future__ import annotations

import io
import json
import os
import uuid
from datetime import datetime, timezone

import pika
import psycopg2
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from minio import Minio

from dedup.deduplicator import ensure_bucket
from dedup.reconstruct import reconstruct_file


TASK_QUEUE = "upload.tasks"


app = FastAPI(title="HPC Chunk Storage API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def env(name: str) -> str:
    """
    Obtiene una variable de entorno requerida por el servicio.

    Parameters:
    name (str): Nombre de la variable de entorno a leer.

    Returns:
    str: Valor configurado para la variable solicitada.
    """
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Falta variable de entorno requerida: {name}")
    return value


def log(message: str) -> None:
    print(f"[{datetime.now(timezone.utc).isoformat()}] backend {message}", flush=True)


def rabbitmq_connection() -> pika.BlockingConnection:
    """
    Crea una conexion bloqueante hacia RabbitMQ.

    Parameters:
    None

    Returns:
    pika.BlockingConnection: Conexion activa usando RABBITMQ_URL.
    """
    return pika.BlockingConnection(pika.URLParameters(env("RABBITMQ_URL")))


def postgres_connection():
    """
    Crea una conexion hacia PostgreSQL.

    Parameters:
    None

    Returns:
    connection: Conexion psycopg2 usando POSTGRES_DSN.
    """
    return psycopg2.connect(env("POSTGRES_DSN"))


def minio_client() -> Minio:
    """
    Construye un cliente MinIO con la configuracion del entorno.

    Parameters:
    None

    Returns:
    Minio: Cliente listo para leer y escribir objetos.
    """
    return Minio(
        env("MINIO_ENDPOINT"),
        access_key=env("MINIO_ACCESS_KEY"),
        secret_key=env("MINIO_SECRET_KEY"),
        secure=False,
    )


def ensure_timing_column(cur) -> None:
    cur.execute("ALTER TABLE files ADD COLUMN IF NOT EXISTS processing_timings JSONB")


@app.post("/upload")
async def upload(file: UploadFile = File(...)) -> dict:
    """
    Recibe un archivo y publica una tarea de procesamiento en RabbitMQ.

    Parameters:
    file (UploadFile): Archivo enviado por multipart/form-data.

    Returns:
    dict: Identificador, nombre y tamano del archivo recibido.
    """
    file_id = str(uuid.uuid4())
    filename = os.path.basename(file.filename or "upload.bin")
    data = await file.read()
    bucket = env("MINIO_BUCKET")
    staged_object_key = f"uploads/{file_id}/{filename}"
    log(f"upload received file_id={file_id} filename={filename} size_mb={len(data) / (1024 * 1024):.2f}")

    try:
        client = minio_client()
        ensure_bucket(client, bucket)
        client.put_object(
            bucket,
            staged_object_key,
            data=io.BytesIO(data),
            length=len(data),
            content_type="application/octet-stream",
        )
        log(f"upload staged file_id={file_id} object={staged_object_key}")
    except Exception as exc:
        log(f"upload staging failed file_id={file_id} error={exc}")
        raise HTTPException(status_code=503, detail=f"MinIO no disponible: {exc}") from exc

    payload = {
        "file_id": file_id,
        "filename": filename,
        "bucket": bucket,
        "staged_object_key": staged_object_key,
        "size_bytes": len(data),
    }

    try:
        connection = rabbitmq_connection()
        channel = connection.channel()
        channel.queue_declare(queue=TASK_QUEUE, durable=True)
        channel.basic_publish(
            exchange="",
            routing_key=TASK_QUEUE,
            body=json.dumps(payload).encode("utf-8"),
            properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
        )
        connection.close()
        log(f"upload queued file_id={file_id}")
    except Exception as exc:
        try:
            client.remove_object(bucket, staged_object_key)
        except Exception:
            pass
        log(f"upload queue failed file_id={file_id} error={exc}")
        raise HTTPException(status_code=503, detail=f"RabbitMQ no disponible: {exc}") from exc

    return {"file_id": file_id, "filename": payload["filename"], "size_bytes": len(data)}


@app.get("/file/{file_id}")
def get_file(file_id: str) -> dict:
    """
    Obtiene metadata de un archivo y sus referencias deduplicadas.

    Parameters:
    file_id (str): Identificador del archivo consultado.

    Returns:
    dict: Metadata del archivo y lista ordenada de chunks logicos.
    """
    with postgres_connection() as conn:
        with conn.cursor() as cur:
            ensure_timing_column(cur)
            cur.execute(
                """
                SELECT file_id, filename, total_size, chunk_count, uploaded_at, processing_timings
                FROM files
                WHERE file_id = %s
                """,
                (file_id,),
            )
            file_row = cur.fetchone()
            if not file_row:
                raise HTTPException(status_code=404, detail="Archivo no encontrado")
            cur.execute(
                """
                SELECT
                    r.chunk_index,
                    r.chunk_hash,
                    u.crc32,
                    r.logical_size,
                    u.object_key,
                    r.created_at,
                    u.ref_count
                FROM file_chunk_refs r
                JOIN unique_chunks u ON u.chunk_hash = r.chunk_hash
                WHERE r.file_id = %s
                ORDER BY r.chunk_index
                """,
                (file_id,),
            )
            chunks = [
                {
                    "chunk_index": row[0],
                    "chunk_hash": row[1],
                    "sha256": row[1],
                    "crc32": row[2],
                    "size_bytes": row[3],
                    "object_key": row[4],
                    "referenced_at": row[5].isoformat(),
                    "ref_count": row[6],
                }
                for row in cur.fetchall()
            ]

    return {
        "file_id": str(file_row[0]),
        "filename": file_row[1],
        "total_size": file_row[2],
        "chunk_count": file_row[3],
        "uploaded_at": file_row[4].isoformat(),
        "processing_timings": file_row[5] or {},
        "chunks": chunks,
    }


@app.get("/file/{file_id}/integrity")
def get_integrity(file_id: str) -> dict:
    """
    Obtiene informacion de integridad por chunk para un archivo.

    Parameters:
    file_id (str): Identificador del archivo consultado.

    Returns:
    dict: Hash, CRC32 y tamano de cada chunk del archivo.
    """
    with postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM files WHERE file_id = %s", (file_id,))
            if cur.fetchone() is None:
                raise HTTPException(status_code=404, detail="Archivo no encontrado")
            cur.execute(
                """
                SELECT r.chunk_index, r.chunk_hash, u.crc32, r.logical_size
                FROM file_chunk_refs r
                JOIN unique_chunks u ON u.chunk_hash = r.chunk_hash
                WHERE r.file_id = %s
                ORDER BY r.chunk_index
                """,
                (file_id,),
            )
            chunks = [
                {"chunk_index": row[0], "chunk_hash": row[1], "crc32": row[2], "size_bytes": row[3]}
                for row in cur.fetchall()
            ]
    return {"file_id": file_id, "chunk_count": len(chunks), "chunks": chunks}


@app.get("/file/{file_id}/download")
def download_file(file_id: str) -> Response:
    """
    Reconstruye y descarga un archivo desde chunks deduplicados.

    Parameters:
    file_id (str): Identificador del archivo a reconstruir.

    Returns:
    Response: Archivo binario reconstruido con cabecera de descarga.
    """
    with postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT filename FROM files WHERE file_id = %s", (file_id,))
            row = cur.fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="Archivo no encontrado")
            filename = row[0]

    try:
        data = reconstruct_file(
            postgres_dsn=env("POSTGRES_DSN"),
            minio_client=minio_client(),
            bucket=env("MINIO_BUCKET"),
            file_id=file_id,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"No se pudo reconstruir el archivo: {exc}") from exc

    return Response(
        content=data,
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/health")
def health() -> dict:
    """
    Revisa el estado basico de las dependencias del backend.

    Parameters:
    None

    Returns:
    dict: Estado de RabbitMQ, PostgreSQL y salud general del servicio.
    """
    status = {"rabbitmq": "down", "postgres": "down"}

    try:
        connection = rabbitmq_connection()
        connection.close()
        status["rabbitmq"] = "up"
    except Exception:
        pass

    try:
        with postgres_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        status["postgres"] = "up"
    except Exception:
        pass

    return {"status": "ok" if all(value == "up" for value in status.values()) else "degraded", **status}
