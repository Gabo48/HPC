from __future__ import annotations

import base64
import json
import os
import time
from datetime import datetime, timezone
from typing import Any

import pika
from minio import Minio

from dedup.deduplicator import store_file_chunks
from processor.interface import preprocess_chunks


TASK_QUEUE = "upload.tasks"
RESULT_QUEUE = "upload.results"


def env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Falta variable de entorno requerida: {name}")
    return value


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


def handle_message(channel, method, _properties, body: bytes) -> None:
    try:
        message = json.loads(body)
        file_id = message["file_id"]
        filename = message["filename"]
        data = base64.b64decode(message["data_base64"])
        chunks = chunk_bytes(data, chunk_size_kb())
        processed_chunks = preprocess_chunks(chunks)

        minio_client = Minio(
            env("MINIO_ENDPOINT"),
            access_key=env("MINIO_ACCESS_KEY"),
            secret_key=env("MINIO_SECRET_KEY"),
            secure=False,
        )
        bucket = env("MINIO_BUCKET")
        dedup_stats = store_file_chunks(
            postgres_dsn=env("POSTGRES_DSN"),
            minio_client=minio_client,
            bucket=bucket,
            file_id=file_id,
            filename=filename,
            total_size=len(data),
            processed_chunks=processed_chunks,
        )
        publish_result(
            channel,
            {
                "file_id": file_id,
                "filename": filename,
                "status": "ok",
                **dedup_stats.as_dict(),
                "completed_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as exc:
        try:
            payload = json.loads(body)
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
            print("upload-worker listo; esperando mensajes", flush=True)
            channel.start_consuming()
        except Exception as exc:
            print(f"upload-worker reconectando tras error: {exc}", flush=True)
            time.sleep(5)


if __name__ == "__main__":
    main()
