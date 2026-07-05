from __future__ import annotations

import argparse
import concurrent.futures
import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path

import requests


MIN_POLL_SECONDS = 0.25
MAX_POLL_SECONDS = 30.0


def utc_now() -> str:
    """
    Genera un timestamp UTC en formato ISO 8601.

    Parameters:
    None

    Returns:
    str: Fecha y hora actual en UTC.
    """
    return datetime.now(timezone.utc).isoformat()


def log(message: str) -> None:
    print(f"[{utc_now()}] {message}", flush=True)


def upload_one(backend_url: str, path: Path) -> dict:
    """
    Sube un archivo individual al endpoint HTTP de upload.

    Parameters:
    backend_url (str): URL base del backend FastAPI.
    path (Path): Ruta local del archivo a enviar.

    Returns:
    dict: Nombre, file_id y duracion de la request de upload.
    """
    start = time.perf_counter()
    log(f"upload start filename={path.name} size_mb={path.stat().st_size / (1024 * 1024):.2f}")
    with path.open("rb") as file_obj:
        response = requests.post(
            f"{backend_url.rstrip('/')}/upload",
            files={"file": (path.name, file_obj, "application/octet-stream")},
            timeout=300,
        )
    response.raise_for_status()
    payload = response.json()
    elapsed = time.perf_counter() - start
    log(f"upload done filename={path.name} file_id={payload['file_id']} elapsed_s={elapsed:.2f}")
    return {
        "filename": path.name,
        "file_id": payload["file_id"],
        "upload_request_seconds": elapsed,
    }


def next_poll_delay(current_delay: float) -> float:
    """
    Calcula el siguiente intervalo de polling con backoff exponencial y jitter.
    """
    jitter = random.uniform(0.8, 1.2)
    return min(MAX_POLL_SECONDS, max(MIN_POLL_SECONDS, current_delay * 2.0 * jitter))


def wait_for_file(
    backend_url: str,
    file_id: str,
    timeout_seconds: int,
    poll_seconds: float,
    max_poll_attempts: int | None = None,
) -> dict:
    """
    Espera hasta que un archivo procesado este disponible en el backend.

    Parameters:
    backend_url (str): URL base del backend FastAPI.
    file_id (str): Identificador del archivo a consultar.
    timeout_seconds (int): Tiempo maximo de espera en segundos.
    poll_seconds (float): Intervalo entre consultas sucesivas.

    Returns:
    dict: Metadata del archivo procesado.
    """
    deadline = time.monotonic() + timeout_seconds
    last_error = None
    attempts = 0
    delay = max(MIN_POLL_SECONDS, poll_seconds)
    started_at = time.monotonic()
    log(f"poll start file_id={file_id} timeout_s={timeout_seconds} initial_delay_s={delay:.2f}")
    while time.monotonic() < deadline:
        if max_poll_attempts is not None and attempts >= max_poll_attempts:
            raise TimeoutError(f"Max poll attempts esperando file_id={file_id}: {last_error}")
        attempts += 1
        try:
            response = requests.get(f"{backend_url.rstrip('/')}/file/{file_id}", timeout=30)
            if response.status_code == 200:
                elapsed = time.monotonic() - started_at
                log(f"poll done file_id={file_id} attempts={attempts} elapsed_s={elapsed:.2f}")
                return response.json()
            last_error = f"HTTP {response.status_code}: {response.text}"
        except requests.RequestException as exc:
            last_error = str(exc)
        if attempts == 1 or attempts % 5 == 0:
            elapsed = time.monotonic() - started_at
            remaining = max(0.0, deadline - time.monotonic())
            log(
                f"poll waiting file_id={file_id} attempts={attempts} "
                f"elapsed_s={elapsed:.2f} remaining_s={remaining:.2f} last_error={last_error}"
            )
        time.sleep(min(delay, max(0.0, deadline - time.monotonic())))
        delay = next_poll_delay(delay)
    raise TimeoutError(f"Timeout esperando file_id={file_id}: {last_error}")


def send_batch(
    *,
    batch_dir: Path,
    backend_url: str,
    concurrency: int,
    wait_results: bool,
    timeout_seconds: int,
    poll_seconds: float,
    max_poll_attempts: int | None = None,
) -> dict:
    """
    Envia todos los archivos de un batch al backend con concurrencia configurable.

    Parameters:
    batch_dir (Path): Directorio que contiene archivos y manifest.json.
    backend_url (str): URL base del backend FastAPI.
    concurrency (int): Numero maximo de uploads simultaneos.
    wait_results (bool): Indica si debe esperar procesamiento completo.
    timeout_seconds (int): Tiempo maximo de espera por archivo.
    poll_seconds (float): Intervalo de consulta mientras espera resultados.

    Returns:
    dict: Manifest de envio con file_id y tiempos por archivo.
    """
    manifest = json.loads((batch_dir / "manifest.json").read_text(encoding="utf-8"))
    started_at = utc_now()
    sent_files = []
    log(
        f"send batch start batch_id={manifest['batch_id']} files={len(manifest['files'])} "
        f"concurrency={concurrency} wait_results={wait_results}"
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [
            executor.submit(upload_one, backend_url, batch_dir / item["filename"])
            for item in manifest["files"]
        ]
        for future in concurrent.futures.as_completed(futures):
            sent_files.append(future.result())
            log(f"upload progress completed={len(sent_files)}/{len(manifest['files'])}")

    sent_files.sort(key=lambda item: item["filename"])
    log(f"all uploads accepted batch_id={manifest['batch_id']} files={len(sent_files)}")

    completed_files = []
    if wait_results:
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [
                executor.submit(
                    wait_for_file,
                    backend_url,
                    item["file_id"],
                    timeout_seconds,
                    poll_seconds,
                    max_poll_attempts,
                )
                for item in sent_files
            ]
            for future in concurrent.futures.as_completed(futures):
                completed_files.append(future.result())
                log(f"processing progress completed={len(completed_files)}/{len(sent_files)}")

    sent_manifest = {
        "batch_id": manifest["batch_id"],
        "backend_url": backend_url,
        "concurrency": concurrency,
        "sent_files": sent_files,
        "completed_files": completed_files,
        "completed_file_count": len(completed_files),
        "started_at": started_at,
        "completed_at": utc_now(),
    }
    (batch_dir / "sent_manifest.json").write_text(json.dumps(sent_manifest, indent=2), encoding="utf-8")
    log(
        f"send batch done batch_id={manifest['batch_id']} "
        f"completed_files={len(completed_files)}"
    )
    return sent_manifest


def main() -> None:
    """
    Ejecuta la interfaz CLI para enviar un batch por HTTP.

    Parameters:
    None

    Returns:
    None: Escribe sent_manifest.json e imprime el resultado del envio.
    """
    parser = argparse.ArgumentParser(description="Envia un batch al backend HPC por HTTP.")
    parser.add_argument("--batch", type=Path, required=True)
    parser.add_argument("--backend-url", required=True)
    parser.add_argument("--concurrency", type=int, default=3)
    parser.add_argument("--wait-results", action="store_true")
    parser.add_argument("--timeout-seconds", type=int, default=3600)
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument("--max-poll-attempts", type=int, default=0)
    args = parser.parse_args()

    result = send_batch(
        batch_dir=args.batch,
        backend_url=args.backend_url,
        concurrency=args.concurrency,
        wait_results=args.wait_results,
        timeout_seconds=args.timeout_seconds,
        poll_seconds=args.poll_seconds,
        max_poll_attempts=args.max_poll_attempts or None,
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
