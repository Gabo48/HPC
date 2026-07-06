from __future__ import annotations

import csv
import hashlib
import importlib
import os
import statistics
import threading
import time
import uuid
from pathlib import Path

import psycopg2

try:
    import psutil
except ImportError:  # pragma: no cover - psutil is installed in the benchmark image.
    psutil = None


BACKENDS = {
    "sequential": "processor.v1_sequential",
    "numpy": "processor.v2_numpy",
    "cython": "processor.v3_cython",
    "ray": "processor.v4_ray",
}
DATASET_TYPES = ["random", "repeated", "modified", "mixed"]
DEFAULT_FILE_SIZES_MB = [10, 100, 500]
DEFAULT_CHUNK_SIZES_KB = [256, 1024, 4096]
CSV_COLUMNS = [
    "benchmark_mode",
    "backend",
    "dataset_type",
    "file_size_mb",
    "chunk_size_kb",
    "run_number",
    "elapsed_seconds",
    "throughput_mb_s",
    "speedup_vs_sequential",
    "chunk_count",
    "unique_chunks",
    "duplicate_chunks",
    "bytes_original",
    "bytes_stored",
    "bytes_saved",
    "storage_saving_pct",
    "dedup_ratio",
    "integrity_ok",
    "cpu_avg_pct",
    "cpu_max_pct",
    "ram_delta_mb",
    "peak_memory_mb",
    "ray_workers",
]


class ResourceSampler:
    """
    Muestrea CPU y memoria durante una seccion medida del benchmark.

    Parameters:
    None

    Returns:
    ResourceSampler: Context manager con metricas recolectadas.
    """

    def __init__(self) -> None:
        """
        Inicializa buffers y estado interno del muestreador.

        Parameters:
        None

        Returns:
        None: Configura el objeto para medir recursos posteriormente.
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
        Resume las metricas de CPU y memoria recolectadas.

        Parameters:
        None

        Returns:
        dict: Promedio/maximo de CPU y memoria usada durante la muestra.
        """
        if psutil is None:
            return {
                "cpu_avg_pct": None,
                "cpu_max_pct": None,
                "ram_delta_mb": None,
                "peak_memory_mb": None,
            }
        return {
            "cpu_avg_pct": statistics.mean(self.cpu_samples) if self.cpu_samples else 0.0,
            "cpu_max_pct": max(self.cpu_samples) if self.cpu_samples else 0.0,
            "ram_delta_mb": self.ram_end_mb - self.ram_start_mb,
            "peak_memory_mb": self.peak_memory_mb,
        }


def parse_int_list(env_name: str, default: list[int]) -> list[int]:
    """
    Lee una lista de enteros desde una variable de entorno.

    Parameters:
    env_name (str): Nombre de la variable de entorno.
    default (list[int]): Valores usados cuando la variable no existe.

    Returns:
    list[int]: Lista parseada desde texto separado por comas.
    """
    raw = os.getenv(env_name)
    if not raw:
        return default
    return [int(item.strip()) for item in raw.split(",") if item.strip()]


def parse_str_list(env_name: str, default: list[str]) -> list[str]:
    """
    Lee una lista de strings desde una variable de entorno.

    Parameters:
    env_name (str): Nombre de la variable de entorno.
    default (list[str]): Valores usados cuando la variable no existe.

    Returns:
    list[str]: Lista parseada desde texto separado por comas.
    """
    raw = os.getenv(env_name)
    if not raw:
        return default
    return [item.strip() for item in raw.split(",") if item.strip()]


def chunks_for(data: bytes, chunk_size_kb: int) -> list[bytes]:
    """
    Divide un archivo en chunks de tamano fijo.

    Parameters:
    data (bytes): Contenido binario del archivo completo.
    chunk_size_kb (int): Tamano de cada chunk en KB.

    Returns:
    list[bytes]: Lista ordenada de chunks.
    """
    chunk_size = chunk_size_kb * 1024
    return [data[offset : offset + chunk_size] for offset in range(0, len(data), chunk_size)]


def generate_dataset(dataset_type: str, file_size_mb: int, chunk_size_kb: int) -> bytes:
    """
    Genera un archivo sintetico para benchmark.

    Parameters:
    dataset_type (str): Tipo de dataset: random, repeated, modified o mixed.
    file_size_mb (int): Tamano total del archivo en MB.
    chunk_size_kb (int): Tamano de chunk usado para construir patrones.

    Returns:
    bytes: Archivo sintetico generado en memoria.
    """
    total_size = file_size_mb * 1024 * 1024
    chunk_size = chunk_size_kb * 1024

    if dataset_type == "random":
        return os.urandom(total_size)

    if dataset_type == "repeated":
        block = os.urandom(chunk_size)
        return (block * ((total_size // chunk_size) + 1))[:total_size]

    if dataset_type == "modified":
        block = bytearray(os.urandom(chunk_size))
        chunks: list[bytes] = []
        for index in range((total_size + chunk_size - 1) // chunk_size):
            current = bytearray(block)
            if index % 8 == 0:
                current[index % len(current)] = (current[index % len(current)] + index + 1) % 256
            chunks.append(bytes(current))
        return b"".join(chunks)[:total_size]

    if dataset_type == "mixed":
        repeated_block = os.urandom(chunk_size)
        chunks = []
        for index in range((total_size + chunk_size - 1) // chunk_size):
            chunks.append(repeated_block if index % 2 == 0 else os.urandom(chunk_size))
        return b"".join(chunks)[:total_size]

    raise ValueError(f"dataset_type invalido: {dataset_type}")


def preprocess(backend: str, chunks: list[bytes]) -> list[dict]:
    """
    Ejecuta el processor seleccionado sobre una lista de chunks.

    Parameters:
    backend (str): Nombre del backend HPC a usar.
    chunks (list[bytes]): Chunks a preprocesar.

    Returns:
    list[dict]: Metadata calculada por chunk.
    """
    return importlib.import_module(BACKENDS[backend]).preprocess_chunks(chunks)


def dedup_metrics_from_processed(processed_chunks: list[dict], bytes_original: int) -> dict:
    """
    Calcula metricas de deduplicacion a partir de chunks procesados.

    Parameters:
    processed_chunks (list[dict]): Chunks con sha256, tamano y metadata.
    bytes_original (int): Tamano original del archivo en bytes.

    Returns:
    dict: Conteos de chunks, bytes almacenados y ahorro estimado.
    """
    seen: set[str] = set()
    bytes_stored = 0
    duplicate_chunks = 0

    for chunk in processed_chunks:
        chunk_hash = chunk["sha256"]
        if chunk_hash in seen:
            duplicate_chunks += 1
        else:
            seen.add(chunk_hash)
            bytes_stored += chunk["size_bytes"]

    bytes_saved = bytes_original - bytes_stored
    return {
        "chunk_count": len(processed_chunks),
        "unique_chunks": len(seen),
        "duplicate_chunks": duplicate_chunks,
        "bytes_original": bytes_original,
        "bytes_stored": bytes_stored,
        "bytes_saved": bytes_saved,
        "storage_saving_pct": (bytes_saved / bytes_original) * 100 if bytes_original else 0.0,
        "dedup_ratio": bytes_original / bytes_stored if bytes_stored else 1.0,
    }


def detect_ray_workers(backend: str) -> int | None:
    """
    Detecta la cantidad de workers Ray disponibles o configurados.

    Parameters:
    backend (str): Backend en evaluacion.

    Returns:
    int | None: Numero de ray-workers si aplica, o None si no se conoce.
    """
    raw = os.getenv("RAY_WORKERS")
    if raw:
        return int(raw)
    if backend != "ray":
        return None
    try:
        import ray

        if not ray.is_initialized():
            return None
        nodes = [node for node in ray.nodes() if node.get("Alive")]
        return max(len(nodes) - 1, 0)
    except Exception:
        return None


def save_result(row: dict) -> None:
    """
    Guarda una fila de benchmark en PostgreSQL.

    Parameters:
    row (dict): Resultado completo de una corrida.

    Returns:
    None: Inserta la fila en benchmark_results.
    """
    with psycopg2.connect(os.environ["POSTGRES_DSN"]) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO benchmark_results (
                    benchmark_mode, backend, dataset_type, file_size_mb, chunk_size_kb,
                    run_number, elapsed_seconds, throughput_mb_s, speedup_vs_sequential,
                    chunk_count, unique_chunks, duplicate_chunks, bytes_original,
                    bytes_stored, bytes_saved, storage_saving_pct, dedup_ratio,
                    integrity_ok, cpu_avg_pct, cpu_max_pct, ram_delta_mb,
                    peak_memory_mb, ray_workers
                )
                VALUES (
                    %(benchmark_mode)s, %(backend)s, %(dataset_type)s, %(file_size_mb)s,
                    %(chunk_size_kb)s, %(run_number)s, %(elapsed_seconds)s,
                    %(throughput_mb_s)s, %(speedup_vs_sequential)s, %(chunk_count)s,
                    %(unique_chunks)s, %(duplicate_chunks)s, %(bytes_original)s,
                    %(bytes_stored)s, %(bytes_saved)s, %(storage_saving_pct)s,
                    %(dedup_ratio)s, %(integrity_ok)s, %(cpu_avg_pct)s,
                    %(cpu_max_pct)s, %(ram_delta_mb)s, %(peak_memory_mb)s,
                    %(ray_workers)s
                )
                """,
                row,
            )


def run_processor_once(
    backend: str, data: bytes, chunks: list[bytes], _filename: str
) -> tuple[float, dict, bool, dict]:
    """
    Ejecuta una corrida de microbenchmark sobre el processor.

    Parameters:
    backend (str): Backend HPC a evaluar.
    data (bytes): Archivo original completo.
    chunks (list[bytes]): Chunks derivados del archivo.
    _filename (str): Nombre reservado para compatibilidad con otros runners.

    Returns:
    tuple[float, dict, bool, dict]: Tiempo, metricas dedup, integridad y recursos.
    """
    original_hash = hashlib.sha256(data).hexdigest()
    with ResourceSampler() as sampler:
        start = time.perf_counter()
        processed_chunks = preprocess(backend, chunks)
        elapsed = time.perf_counter() - start
    reconstructed = b"".join(chunk["data"] for chunk in sorted(processed_chunks, key=lambda item: item["chunk_index"]))
    integrity_ok = hashlib.sha256(reconstructed).hexdigest() == original_hash
    return elapsed, dedup_metrics_from_processed(processed_chunks, len(data)), integrity_ok, sampler.metrics()


def run_end_to_end_once(backend: str, data: bytes, chunks: list[bytes], filename: str) -> tuple[float, dict, bool, dict]:
    """
    Ejecuta una corrida end-to-end sin HTTP usando deduplicacion real.

    Parameters:
    backend (str): Backend HPC a evaluar.
    data (bytes): Archivo original completo.
    chunks (list[bytes]): Chunks derivados del archivo.
    filename (str): Nombre logico del archivo almacenado.

    Returns:
    tuple[float, dict, bool, dict]: Tiempo, metricas dedup, integridad y recursos.
    """
    from minio import Minio

    from dedup.deduplicator import store_file_chunks
    from dedup.reconstruct import reconstruct_file

    minio_client = Minio(
        os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=False,
    )
    file_id = str(uuid.uuid4())
    original_hash = hashlib.sha256(data).hexdigest()

    with ResourceSampler() as sampler:
        start = time.perf_counter()
        processed_chunks = preprocess(backend, chunks)
        stats = store_file_chunks(
            postgres_dsn=os.environ["POSTGRES_DSN"],
            minio_client=minio_client,
            bucket=os.environ["MINIO_BUCKET"],
            file_id=file_id,
            filename=filename,
            total_size=len(data),
            processed_chunks=processed_chunks,
        )
        reconstructed = reconstruct_file(
            postgres_dsn=os.environ["POSTGRES_DSN"],
            minio_client=minio_client,
            bucket=os.environ["MINIO_BUCKET"],
            file_id=file_id,
        )
        elapsed = time.perf_counter() - start

    integrity_ok = hashlib.sha256(reconstructed).hexdigest() == original_hash
    return elapsed, stats.as_dict(), integrity_ok, sampler.metrics()


def build_row(
    *,
    benchmark_mode: str,
    backend: str,
    dataset_type: str,
    file_size_mb: int,
    chunk_size_kb: int,
    run_number: int,
    elapsed: float,
    speedup: float,
    dedup_metrics: dict,
    integrity_ok: bool,
    resource_metrics: dict,
) -> dict:
    """
    Construye una fila normalizada para CSV y PostgreSQL.

    Parameters:
    benchmark_mode (str): Modo de benchmark ejecutado.
    backend (str): Backend HPC usado.
    dataset_type (str): Tipo de dataset generado.
    file_size_mb (int): Tamano del archivo en MB.
    chunk_size_kb (int): Tamano de chunk en KB.
    run_number (int): Numero de repeticion.
    elapsed (float): Duracion medida en segundos.
    speedup (float): Aceleracion relativa al baseline secuencial.
    dedup_metrics (dict): Metricas de deduplicacion.
    integrity_ok (bool): Resultado de validacion de integridad.
    resource_metrics (dict): Metricas de CPU y memoria.

    Returns:
    dict: Fila lista para persistencia y CSV.
    """
    csv_dedup_metrics = {key: value for key, value in dedup_metrics.items() if key in CSV_COLUMNS}
    return {
        "benchmark_mode": benchmark_mode,
        "backend": backend,
        "dataset_type": dataset_type,
        "file_size_mb": file_size_mb,
        "chunk_size_kb": chunk_size_kb,
        "run_number": run_number,
        "elapsed_seconds": elapsed,
        "throughput_mb_s": file_size_mb / elapsed,
        "speedup_vs_sequential": speedup,
        **csv_dedup_metrics,
        "integrity_ok": integrity_ok,
        **resource_metrics,
        "ray_workers": detect_ray_workers(backend),
    }


def print_summary(rows: list[dict]) -> None:
    """
    Imprime un resumen agrupado de las corridas ejecutadas.

    Parameters:
    rows (list[dict]): Filas de resultados del benchmark.

    Returns:
    None: Escribe la tabla resumen en stdout.
    """
    print("\nResumen benchmark")
    print("mode        dataset   file chunk backend      avg_s     MB/s  save%  ok")
    print("----------- -------- ----- ----- ------------ ------- ------- ------ ---")
    grouped = {}
    for row in rows:
        key = (
            row["benchmark_mode"],
            row["dataset_type"],
            row["file_size_mb"],
            row["chunk_size_kb"],
            row["backend"],
        )
        grouped.setdefault(key, []).append(row)

    for key, subset in sorted(grouped.items()):
        mode, dataset, file_size, chunk_size, backend = key
        avg_time = statistics.mean(row["elapsed_seconds"] for row in subset)
        avg_throughput = statistics.mean(row["throughput_mb_s"] for row in subset)
        avg_saving = statistics.mean(row["storage_saving_pct"] for row in subset)
        ok = all(row["integrity_ok"] for row in subset)
        print(
            f"{mode:11s} {dataset:8s} {file_size:5d} {chunk_size:5d} "
            f"{backend:12s} {avg_time:7.3f} {avg_throughput:7.2f} {avg_saving:6.1f} {str(ok):3s}"
        )


def main() -> None:
    """
    Ejecuta el benchmark configurado por variables de entorno.

    Parameters:
    None

    Returns:
    None: Guarda resultados en PostgreSQL y /results/benchmark.csv.
    """
    benchmark_mode = os.getenv("BENCHMARK_MODE", "processor").lower()
    if benchmark_mode not in {"processor", "end_to_end"}:
        raise ValueError("BENCHMARK_MODE debe ser processor o end_to_end")

    runs = int(os.getenv("BENCHMARK_RUNS", "5"))
    file_sizes_mb = parse_int_list("BENCHMARK_FILE_SIZES_MB", DEFAULT_FILE_SIZES_MB)
    if os.getenv("BENCHMARK_INCLUDE_1GB", "false").lower() == "true" and 1024 not in file_sizes_mb:
        file_sizes_mb.append(1024)
    chunk_sizes_kb = parse_int_list("BENCHMARK_CHUNK_SIZES_KB", DEFAULT_CHUNK_SIZES_KB)
    dataset_types = parse_str_list("BENCHMARK_DATASETS", DATASET_TYPES)
    backends = parse_str_list("BENCHMARK_BACKENDS", list(BACKENDS))

    rows: list[dict] = []
    runner = run_processor_once if benchmark_mode == "processor" else run_end_to_end_once

    for dataset_type in dataset_types:
        for file_size_mb in file_sizes_mb:
            for chunk_size_kb in chunk_sizes_kb:
                data = generate_dataset(dataset_type, file_size_mb, chunk_size_kb)
                chunks = chunks_for(data, chunk_size_kb)
                sequential_times: list[float] = []

                for backend in backends:
                    backend_rows: list[dict] = []
                    for run_number in range(1, runs + 1):
                        elapsed, dedup_metrics, integrity_ok, resource_metrics = runner(
                            backend,
                            data,
                            chunks,
                            f"{dataset_type}_{file_size_mb}mb_{chunk_size_kb}kb.bin",
                        )
                        if backend == "sequential":
                            sequential_times.append(elapsed)
                        sequential_avg = statistics.mean(sequential_times) if sequential_times else elapsed
                        row = build_row(
                            benchmark_mode=benchmark_mode,
                            backend=backend,
                            dataset_type=dataset_type,
                            file_size_mb=file_size_mb,
                            chunk_size_kb=chunk_size_kb,
                            run_number=run_number,
                            elapsed=elapsed,
                            speedup=sequential_avg / elapsed,
                            dedup_metrics=dedup_metrics,
                            integrity_ok=integrity_ok,
                            resource_metrics=resource_metrics,
                        )
                        rows.append(row)
                        backend_rows.append(row)
                        save_result(row)
                    if backend == "sequential" and not sequential_times:
                        sequential_times = [row["elapsed_seconds"] for row in backend_rows]

    output_path = Path(os.getenv("BENCHMARK_CSV_OUTPUT", "/results/benchmark.csv"))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    print_summary(rows)


if __name__ == "__main__":
    main()
