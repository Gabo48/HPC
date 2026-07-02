from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import statistics
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests

try:
    import psutil
except ImportError:  # pragma: no cover
    psutil = None

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from benchmark.generate_batch import file_bytes, generate_batch
from benchmark.send_batch import send_batch


CSV_COLUMNS = [
    "batch_id",
    "dataset_type",
    "num_files",
    "file_size_mb",
    "total_input_mb",
    "chunk_size_kb",
    "backend",
    "run_number",
    "total_elapsed_seconds",
    "batch_throughput_mb_s",
    "files_per_second",
    "total_chunks",
    "total_unique_chunks",
    "total_duplicate_chunks",
    "total_bytes_original",
    "total_bytes_stored",
    "total_bytes_saved",
    "storage_saving_pct",
    "dedup_ratio",
    "integrity_ok_count",
    "integrity_failed_count",
    "cpu_avg_pct",
    "cpu_max_pct",
    "ram_delta_mb",
    "peak_memory_mb",
    "ray_workers",
    "upload_workers",
    "concurrency",
]

SUMMARY_COLUMNS = [
    "dataset_type",
    "num_files",
    "file_size_mb",
    "total_input_mb",
    "chunk_size_kb",
    "backend",
    "concurrency",
    "repetitions",
    "elapsed_avg_seconds",
    "elapsed_std_seconds",
    "elapsed_min_seconds",
    "elapsed_max_seconds",
    "throughput_avg_mb_s",
    "throughput_std_mb_s",
    "throughput_min_mb_s",
    "throughput_max_mb_s",
]


class ResourceSampler:
    """
    Muestrea CPU y memoria mientras corre un benchmark batch.

    Parameters:
    None

    Returns:
    ResourceSampler: Context manager que expone metricas agregadas.
    """

    def __init__(self) -> None:
        """
        Inicializa el estado interno del muestreador de recursos.

        Parameters:
        None

        Returns:
        None: Prepara buffers y banderas de control.
        """
        self.cpu_samples: list[float] = []
        self.peak_memory_mb = 0.0
        self.ram_start_mb = 0.0
        self.ram_end_mb = 0.0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def __enter__(self):
        if psutil is None:
            return self
        process = psutil.Process()
        self.ram_start_mb = process.memory_info().rss / (1024 * 1024)
        process.cpu_percent(interval=None)

        def sample() -> None:
            while not self._stop.is_set():
                self.cpu_samples.append(process.cpu_percent(interval=0.1))
                memory_mb = process.memory_info().rss / (1024 * 1024)
                self.peak_memory_mb = max(self.peak_memory_mb, memory_mb)

        self._thread = threading.Thread(target=sample, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *_exc) -> None:
        if psutil is None:
            return
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1)
        process = psutil.Process()
        self.ram_end_mb = process.memory_info().rss / (1024 * 1024)
        self.peak_memory_mb = max(self.peak_memory_mb, self.ram_end_mb)

    def metrics(self) -> dict:
        """
        Calcula las metricas finales de CPU y memoria.

        Parameters:
        None

        Returns:
        dict: Promedio/maximo de CPU, delta de RAM y memoria peak.
        """
        if psutil is None:
            return {"cpu_avg_pct": None, "cpu_max_pct": None, "ram_delta_mb": None, "peak_memory_mb": None}
        return {
            "cpu_avg_pct": statistics.mean(self.cpu_samples) if self.cpu_samples else 0.0,
            "cpu_max_pct": max(self.cpu_samples) if self.cpu_samples else 0.0,
            "ram_delta_mb": self.ram_end_mb - self.ram_start_mb,
            "peak_memory_mb": self.peak_memory_mb,
        }


def utc_stamp() -> str:
    """
    Genera un timestamp UTC compacto para nombres de batch.

    Parameters:
    None

    Returns:
    str: Timestamp en formato YYYYMMDDTHHMMSSZ.
    """
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def parse_preset(preset: str) -> list[tuple[int, int]]:
    """
    Convierte un nombre de preset batch en configuraciones de archivos.

    Parameters:
    preset (str): Nombre del preset solicitado.

    Returns:
    list[tuple[int, int]]: Pares de cantidad de archivos y tamano en MB.
    """
    presets = {
        "batch_small": [(10, 10)],
        "batch_medium": [(10, 100)],
        "batch_large": [(5, 500)],
        "batch_mixed": [(5, 10), (5, 100), (2, 500)],
    }
    if preset not in presets:
        raise ValueError(f"Preset invalido: {preset}")
    return presets[preset]


def generate_mixed_size_batch(
    *,
    dataset: str,
    configs: list[tuple[int, int]],
    output: Path,
    chunk_size_kb: int,
    seed: int,
    batch_id: str,
) -> dict:
    """
    Genera un batch con archivos de multiples tamanos.

    Parameters:
    dataset (str): Tipo de dataset sintetico.
    configs (list[tuple[int, int]]): Pares de cantidad de archivos y tamano en MB.
    output (Path): Directorio de salida del batch.
    chunk_size_kb (int): Tamano de chunk usado para construir datos.
    seed (int): Semilla base para reproducibilidad.
    batch_id (str): Identificador del batch generado.

    Returns:
    dict: Manifest del batch con hashes globales por archivo.
    """
    output.mkdir(parents=True, exist_ok=True)
    files = []
    file_index = 0
    total_size_mb = 0

    for num_files, file_size_mb in configs:
        size_bytes = file_size_mb * 1024 * 1024
        total_size_mb += num_files * file_size_mb
        for _ in range(num_files):
            filename = f"file_{file_index:03d}_{file_size_mb}mb.bin"
            data = file_bytes(dataset, size_bytes, chunk_size_kb, file_index, seed)
            (output / filename).write_bytes(data)
            files.append(
                {
                    "filename": filename,
                    "size_bytes": len(data),
                    "size_mb": file_size_mb,
                    "sha256": hashlib.sha256(data).hexdigest(),
                }
            )
            file_index += 1

    manifest = {
        "batch_id": batch_id,
        "dataset_type": dataset,
        "num_files": len(files),
        "file_size_mb": 0,
        "chunk_size_kb": chunk_size_kb,
        "total_size_mb": total_size_mb,
        "files": files,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def wait_file(backend_url: str, file_id: str, timeout_seconds: int = 3600) -> dict:
    """
    Espera hasta que el backend exponga metadata de un archivo.

    Parameters:
    backend_url (str): URL base del backend FastAPI.
    file_id (str): Identificador del archivo esperado.
    timeout_seconds (int): Tiempo maximo de espera en segundos.

    Returns:
    dict: Metadata del archivo procesado.
    """
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        response = requests.get(f"{backend_url.rstrip('/')}/file/{file_id}", timeout=30)
        if response.status_code == 200:
            return response.json()
        time.sleep(2)
    raise TimeoutError(f"No se completo file_id={file_id}")


def validate_download(backend_url: str, file_id: str, expected_sha256: str) -> bool:
    """
    Descarga un archivo reconstruido y valida su SHA-256 global.

    Parameters:
    backend_url (str): URL base del backend FastAPI.
    file_id (str): Identificador del archivo a descargar.
    expected_sha256 (str): Hash global esperado del archivo original.

    Returns:
    bool: True si el archivo reconstruido coincide con el original.
    """
    response = requests.get(f"{backend_url.rstrip('/')}/file/{file_id}/download", timeout=600)
    if response.status_code != 200:
        return False
    return hashlib.sha256(response.content).hexdigest() == expected_sha256


def aggregate_metrics(backend_url: str, batch_dir: Path, sent_manifest: dict) -> dict:
    """
    Agrega metricas de deduplicacion e integridad para un batch enviado.

    Parameters:
    backend_url (str): URL base del backend FastAPI.
    batch_dir (Path): Directorio que contiene manifest.json del batch.
    sent_manifest (dict): Manifest de envio con file_id por archivo.

    Returns:
    dict: Metricas globales del batch procesado.
    """
    manifest = json.loads((batch_dir / "manifest.json").read_text(encoding="utf-8"))
    expected_hashes = {item["filename"]: item["sha256"] for item in manifest["files"]}
    all_hashes: dict[str, int] = {}
    total_chunks = 0
    total_bytes_original = 0
    integrity_ok = 0
    integrity_failed = 0

    for sent in sent_manifest["sent_files"]:
        metadata = wait_file(backend_url, sent["file_id"])
        total_bytes_original += int(metadata["total_size"])
        chunks = metadata["chunks"]
        total_chunks += len(chunks)
        for chunk in chunks:
            all_hashes.setdefault(chunk["chunk_hash"], int(chunk["size_bytes"]))

        if validate_download(backend_url, sent["file_id"], expected_hashes[sent["filename"]]):
            integrity_ok += 1
        else:
            integrity_failed += 1

    total_unique_chunks = len(all_hashes)
    total_bytes_stored = sum(all_hashes.values())
    total_duplicate_chunks = total_chunks - total_unique_chunks
    total_bytes_saved = total_bytes_original - total_bytes_stored
    return {
        "total_chunks": total_chunks,
        "total_unique_chunks": total_unique_chunks,
        "total_duplicate_chunks": total_duplicate_chunks,
        "total_bytes_original": total_bytes_original,
        "total_bytes_stored": total_bytes_stored,
        "total_bytes_saved": total_bytes_saved,
        "storage_saving_pct": (total_bytes_saved / total_bytes_original) * 100 if total_bytes_original else 0.0,
        "dedup_ratio": total_bytes_original / total_bytes_stored if total_bytes_stored else 1.0,
        "integrity_ok_count": integrity_ok,
        "integrity_failed_count": integrity_failed,
    }


def save_result(row: dict) -> None:
    """
    Guarda una fila de resultado batch en PostgreSQL.

    Parameters:
    row (dict): Resultado consolidado de una corrida batch.

    Returns:
    None: Inserta datos en batch_benchmark_results si POSTGRES_DSN existe.
    """
    dsn = os.getenv("POSTGRES_DSN")
    if not dsn:
        return
    import psycopg2

    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO batch_benchmark_results (
                    batch_id, dataset_type, num_files, file_size_mb, total_input_mb,
                    chunk_size_kb, backend, run_number, total_elapsed_seconds,
                    batch_throughput_mb_s, files_per_second, total_chunks,
                    total_unique_chunks, total_duplicate_chunks, total_bytes_original,
                    total_bytes_stored, total_bytes_saved, storage_saving_pct,
                    dedup_ratio, integrity_ok_count, integrity_failed_count,
                    cpu_avg_pct, cpu_max_pct, ram_delta_mb, peak_memory_mb,
                    ray_workers, upload_workers, concurrency
                )
                VALUES (
                    %(batch_id)s, %(dataset_type)s, %(num_files)s, %(file_size_mb)s,
                    %(total_input_mb)s, %(chunk_size_kb)s, %(backend)s, %(run_number)s,
                    %(total_elapsed_seconds)s, %(batch_throughput_mb_s)s,
                    %(files_per_second)s, %(total_chunks)s, %(total_unique_chunks)s,
                    %(total_duplicate_chunks)s, %(total_bytes_original)s,
                    %(total_bytes_stored)s, %(total_bytes_saved)s,
                    %(storage_saving_pct)s, %(dedup_ratio)s, %(integrity_ok_count)s,
                    %(integrity_failed_count)s, %(cpu_avg_pct)s, %(cpu_max_pct)s,
                    %(ram_delta_mb)s, %(peak_memory_mb)s, %(ray_workers)s,
                    %(upload_workers)s, %(concurrency)s
                )
                """,
                row,
            )


def write_csv(rows: list[dict], output: Path) -> None:
    """
    Escribe resultados batch detallados en un archivo CSV.

    Parameters:
    rows (list[dict]): Filas de resultados a serializar.
    output (Path): Ruta del CSV de salida.

    Returns:
    None: Crea o reemplaza el archivo CSV.
    """
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def write_summary_csv(rows: list[dict], output: Path) -> None:
    """
    Escribe un resumen estadistico agrupado del benchmark batch.

    Parameters:
    rows (list[dict]): Filas de resultados detallados.
    output (Path): Ruta base del CSV detallado.

    Returns:
    None: Crea un CSV adicional con promedio, desviacion, minimo y maximo.
    """
    grouped: dict[tuple, list[dict]] = {}
    for row in rows:
        key = (
            row["dataset_type"],
            row["num_files"],
            row["file_size_mb"],
            row["total_input_mb"],
            row["chunk_size_kb"],
            row["backend"],
            row["concurrency"],
        )
        grouped.setdefault(key, []).append(row)

    summary_rows = []
    for key, subset in sorted(grouped.items()):
        elapsed = [row["total_elapsed_seconds"] for row in subset]
        throughput = [row["batch_throughput_mb_s"] for row in subset]
        summary_rows.append(
            {
                "dataset_type": key[0],
                "num_files": key[1],
                "file_size_mb": key[2],
                "total_input_mb": key[3],
                "chunk_size_kb": key[4],
                "backend": key[5],
                "concurrency": key[6],
                "repetitions": len(subset),
                "elapsed_avg_seconds": statistics.mean(elapsed),
                "elapsed_std_seconds": statistics.stdev(elapsed) if len(elapsed) > 1 else 0.0,
                "elapsed_min_seconds": min(elapsed),
                "elapsed_max_seconds": max(elapsed),
                "throughput_avg_mb_s": statistics.mean(throughput),
                "throughput_std_mb_s": statistics.stdev(throughput) if len(throughput) > 1 else 0.0,
                "throughput_min_mb_s": min(throughput),
                "throughput_max_mb_s": max(throughput),
            }
        )

    summary_output = output.with_name(output.stem + "_summary.csv")
    with summary_output.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=SUMMARY_COLUMNS)
        writer.writeheader()
        writer.writerows(summary_rows)


def run_config(args, num_files: int, file_size_mb: int, run_number: int) -> dict:
    """
    Ejecuta una configuracion batch de tamano uniforme.

    Parameters:
    args: Argumentos CLI parseados.
    num_files (int): Cantidad de archivos del batch.
    file_size_mb (int): Tamano de cada archivo en MB.
    run_number (int): Numero de repeticion experimental.

    Returns:
    dict: Fila de resultados consolidada para la corrida.
    """
    batch_id = f"batch_{args.dataset}_{num_files}x{file_size_mb}_{args.chunk_size_kb}kb_r{run_number}_{utc_stamp()}"
    batch_dir = args.output_dir / batch_id
    generate_batch(
        dataset=args.dataset,
        num_files=num_files,
        file_size_mb=file_size_mb,
        output=batch_dir,
        chunk_size_kb=args.chunk_size_kb,
        seed=args.seed + run_number,
        batch_id=batch_id,
    )

    total_input_mb = num_files * file_size_mb
    with ResourceSampler() as sampler:
        start = time.perf_counter()
        sent_manifest = send_batch(
            batch_dir=batch_dir,
            backend_url=args.backend_url,
            concurrency=args.concurrency,
            wait_results=True,
            timeout_seconds=args.timeout_seconds,
            poll_seconds=args.poll_seconds,
        )
        aggregate = aggregate_metrics(args.backend_url, batch_dir, sent_manifest)
        elapsed = time.perf_counter() - start

    row = {
        "batch_id": batch_id,
        "dataset_type": args.dataset,
        "num_files": num_files,
        "file_size_mb": file_size_mb,
        "total_input_mb": total_input_mb,
        "chunk_size_kb": args.chunk_size_kb,
        "backend": args.backend,
        "run_number": run_number,
        "total_elapsed_seconds": elapsed,
        "batch_throughput_mb_s": total_input_mb / elapsed,
        "files_per_second": num_files / elapsed,
        **aggregate,
        **sampler.metrics(),
        "ray_workers": args.ray_workers,
        "upload_workers": args.upload_workers,
        "concurrency": args.concurrency,
    }
    save_result(row)
    return row


def run_preset_config(args, configs: list[tuple[int, int]], run_number: int) -> dict:
    """
    Ejecuta una configuracion batch basada en preset.

    Parameters:
    args: Argumentos CLI parseados.
    configs (list[tuple[int, int]]): Pares de cantidad de archivos y tamano en MB.
    run_number (int): Numero de repeticion experimental.

    Returns:
    dict: Fila de resultados consolidada para la corrida.
    """
    num_files = sum(item[0] for item in configs)
    total_input_mb = sum(num_files * file_size_mb for num_files, file_size_mb in configs)
    reported_file_size_mb = configs[0][1] if len(configs) == 1 else 0
    batch_id = f"{args.batch_preset}_{args.dataset}_{args.chunk_size_kb}kb_r{run_number}_{utc_stamp()}"
    batch_dir = args.output_dir / batch_id
    generate_mixed_size_batch(
        dataset=args.dataset,
        configs=configs,
        output=batch_dir,
        chunk_size_kb=args.chunk_size_kb,
        seed=args.seed + run_number,
        batch_id=batch_id,
    )

    with ResourceSampler() as sampler:
        start = time.perf_counter()
        sent_manifest = send_batch(
            batch_dir=batch_dir,
            backend_url=args.backend_url,
            concurrency=args.concurrency,
            wait_results=True,
            timeout_seconds=args.timeout_seconds,
            poll_seconds=args.poll_seconds,
        )
        aggregate = aggregate_metrics(args.backend_url, batch_dir, sent_manifest)
        elapsed = time.perf_counter() - start

    row = {
        "batch_id": batch_id,
        "dataset_type": args.dataset,
        "num_files": num_files,
        "file_size_mb": reported_file_size_mb,
        "total_input_mb": total_input_mb,
        "chunk_size_kb": args.chunk_size_kb,
        "backend": args.backend,
        "run_number": run_number,
        "total_elapsed_seconds": elapsed,
        "batch_throughput_mb_s": total_input_mb / elapsed,
        "files_per_second": num_files / elapsed,
        **aggregate,
        **sampler.metrics(),
        "ray_workers": args.ray_workers,
        "upload_workers": args.upload_workers,
        "concurrency": args.concurrency,
    }
    save_result(row)
    return row


def print_summary(rows: list[dict]) -> None:
    """
    Imprime una tabla compacta con resultados batch.

    Parameters:
    rows (list[dict]): Filas de resultados batch.

    Returns:
    None: Escribe el resumen en stdout.
    """
    print("\nResumen batch benchmark")
    print("dataset files size_mb run elapsed_s MB/s files/s save% ok/fail")
    for row in rows:
        print(
            f"{row['dataset_type']:8s} {row['num_files']:5d} {row['file_size_mb']:7d} "
            f"{row['run_number']:3d} {row['total_elapsed_seconds']:9.2f} "
            f"{row['batch_throughput_mb_s']:6.2f} {row['files_per_second']:7.3f} "
            f"{row['storage_saving_pct']:5.1f} "
            f"{row['integrity_ok_count']}/{row['integrity_failed_count']}"
        )


def main() -> None:
    """
    Ejecuta el benchmark batch end-to-end desde CLI.

    Parameters:
    None

    Returns:
    None: Genera batches, envia archivos, valida integridad y escribe CSVs.
    """
    parser = argparse.ArgumentParser(description="Benchmark batch end-to-end por HTTP.")
    parser.add_argument("--dataset", choices=["random", "repeated", "modified", "mixed"], required=True)
    parser.add_argument("--num-files", type=int)
    parser.add_argument("--file-size-mb", type=int)
    parser.add_argument("--batch-preset", choices=["batch_small", "batch_medium", "batch_large", "batch_mixed"])
    parser.add_argument("--chunk-size-kb", type=int, default=1024)
    parser.add_argument("--backend-url", default=os.getenv("BENCHMARK_BACKEND_URL", "http://backend:8000"))
    parser.add_argument("--backend", default=os.getenv("HPC_BACKEND", "ray"))
    parser.add_argument("--concurrency", type=int, default=int(os.getenv("BENCHMARK_CONCURRENCY", "3")))
    parser.add_argument("--repetitions", type=int, default=int(os.getenv("BENCHMARK_REPETITIONS", "5")))
    parser.add_argument("--output-dir", type=Path, default=Path("benchmark/datasets"))
    parser.add_argument("--csv-output", type=Path, default=Path("/results/batch_benchmark.csv"))
    parser.add_argument("--seed", type=int, default=int(os.getenv("BENCHMARK_SEED", "12345")))
    parser.add_argument("--timeout-seconds", type=int, default=3600)
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument("--ray-workers", type=int, default=int(os.getenv("RAY_WORKERS", "0") or "0"))
    parser.add_argument("--upload-workers", type=int, default=int(os.getenv("UPLOAD_WORKERS", "1") or "1"))
    args = parser.parse_args()

    if args.batch_preset:
        configs = parse_preset(args.batch_preset)
    else:
        if args.num_files is None or args.file_size_mb is None:
            raise SystemExit("--num-files y --file-size-mb son requeridos si no usas --batch-preset")
        configs = [(args.num_files, args.file_size_mb)]

    rows = []
    for run_number in range(1, args.repetitions + 1):
        if args.batch_preset:
            rows.append(run_preset_config(args, configs, run_number))
        else:
            for num_files, file_size_mb in configs:
                rows.append(run_config(args, num_files, file_size_mb, run_number))

    write_csv(rows, args.csv_output)
    write_summary_csv(rows, args.csv_output)
    print_summary(rows)


if __name__ == "__main__":
    main()
