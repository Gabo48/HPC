from __future__ import annotations

import base64
import json
import os
import time
from datetime import datetime, timezone
from typing import Any

import pika
from minio import Minio

from dedup.deduplicator import store_file_chunks, update_file_timings
from processor.interface import preprocess_chunks


TASK_QUEUE = "upload.tasks"
RESULT_QUEUE = "upload.results"


def env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Falta variable de entorno requerida: {name}")
    return value


def log(message: str) -> None:
    print(f"[{datetime.now(timezone.utc).isoformat()}] upload-worker {message}", flush=True)


def chunk_bytes(data: bytes, chunk_size_kb: int) -> list[bytes]:
    chunk_size = chunk_size_kb * 1024
    return [data[offset : offset + chunk_size] for offset in range(0, len(data), chunk_size)]


def chunk_size_kb() -> int:
    value = os.getenv("CHUNK_SIZE_KB")
    if value:
        return int(value)
    return int(env("CHUNK_SIZE_MB", "1")) * 1024


def connect_rabbitmq() -> pika.BlockingConnection:
    return pika.BlockingConnection(pika.URLParameters(env("RABBITMQ_URL")))


def publish_result(channel: pika.adapters.blocking_connection.BlockingChannel, payload: dict[str, Any]) -> None:
    channel.queue_declare(queue=RESULT_QUEUE, durable=True)
    channel.basic_publish(
        exchange="",
        routing_key=RESULT_QUEUE,
        body=json.dumps(payload).encode("utf-8"),
        properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
    )


def read_upload_payload(message: dict[str, Any], minio_client: Minio) -> bytes:
    if "data_base64" in message:
        return base64.b64decode(message["data_base64"])

    bucket = message.get("bucket") or env("MINIO_BUCKET")
    staged_object_key = message["staged_object_key"]
    response = minio_client.get_object(bucket, staged_object_key)
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


def cleanup_staged_upload(message: dict[str, Any], minio_client: Minio) -> None:
    staged_object_key = message.get("staged_object_key")
    if not staged_object_key:
        return
    bucket = message.get("bucket") or env("MINIO_BUCKET")
    try:
        minio_client.remove_object(bucket, staged_object_key)
    except Exception as exc:
        print(f"No se pudo limpiar upload temporal {staged_object_key}: {exc}", flush=True)


def handle_message(channel, method, _properties, body: bytes) -> None:
    minio_client = None
    message: dict[str, Any] | None = None
    worker_start = time.perf_counter()
    try:
        message = json.loads(body)
        file_id = message["file_id"]
        filename = message["filename"]
        timings: dict[str, float] = {}
        log(f"message received file_id={file_id} filename={filename}")
        minio_client = Minio(
            env("MINIO_ENDPOINT"),
            access_key=env("MINIO_ACCESS_KEY"),
            secret_key=env("MINIO_SECRET_KEY"),
            secure=False,
        )
        stage_start = time.perf_counter()
        data = read_upload_payload(message, minio_client)
        timings["payload_load_seconds"] = time.perf_counter() - stage_start
        log(f"payload loaded file_id={file_id} size_mb={len(data) / (1024 * 1024):.2f}")
        configured_chunk_size_kb = chunk_size_kb()
        stage_start = time.perf_counter()
        chunks = chunk_bytes(data, configured_chunk_size_kb)
        timings["chunk_split_seconds"] = time.perf_counter() - stage_start
        log(f"chunks ready file_id={file_id} chunks={len(chunks)} chunk_size_kb={configured_chunk_size_kb}")
        stage_start = time.perf_counter()
        processed_chunks = preprocess_chunks(chunks)
        timings["preprocess_seconds"] = time.perf_counter() - stage_start
        log(f"preprocess done file_id={file_id} chunks={len(processed_chunks)}")

        bucket = env("MINIO_BUCKET")
        stage_start = time.perf_counter()
        dedup_stats = store_file_chunks(
            postgres_dsn=env("POSTGRES_DSN"),
            minio_client=minio_client,
            bucket=bucket,
            file_id=file_id,
            filename=filename,
            total_size=len(data),
            processed_chunks=processed_chunks,
            processing_timings=timings,
        )
        timings["dedup_store_seconds"] = time.perf_counter() - stage_start
        timings["encryption_seconds"] = dedup_stats.encryption_seconds
        timings["object_upload_seconds"] = dedup_stats.object_upload_seconds
        log(
            f"dedup stored file_id={file_id} unique={dedup_stats.unique_chunks} "
            f"duplicates={dedup_stats.duplicate_chunks}"
        )
        stage_start = time.perf_counter()
        cleanup_staged_upload(message, minio_client)
        timings["cleanup_seconds"] = time.perf_counter() - stage_start
        timings["worker_total_seconds"] = time.perf_counter() - worker_start
        update_file_timings(postgres_dsn=env("POSTGRES_DSN"), file_id=file_id, processing_timings=timings)
        publish_result(
            channel,
            {
                "file_id": file_id,
                "filename": filename,
                "status": "ok",
                **dedup_stats.as_dict(),
                "processing_timings": timings,
                "completed_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        log(f"message done file_id={file_id}")
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as exc:
        try:
            payload = message or json.loads(body)
            file_id = payload.get("file_id")
            filename = payload.get("filename")
        except Exception:
            file_id = None
            filename = None
        publish_result(
            channel,
            {
                "file_id": file_id,
                "filename": filename,
                "status": "error",
                "error": str(exc),
                "completed_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        log(f"message failed file_id={file_id} filename={filename} error={exc}")
        if message is not None and minio_client is not None:
            cleanup_staged_upload(message, minio_client)
        channel.basic_ack(delivery_tag=method.delivery_tag)


def main() -> None:
    while True:
        try:
            connection = connect_rabbitmq()
            channel = connection.channel()
            channel.queue_declare(queue=TASK_QUEUE, durable=True)
            channel.queue_declare(queue=RESULT_QUEUE, durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=TASK_QUEUE, on_message_callback=handle_message)
            log("ready waiting for messages")
            channel.start_consuming()
        except Exception as exc:
            log(f"reconnecting after error: {exc}")
            time.sleep(5)


if __name__ == "__main__":
    main()
