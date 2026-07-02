# Benchmark HPC, Deduplicacion y Batches

## Pregunta Experimental

Evaluar si una capa de preprocesamiento basada en chunks mejora el procesamiento, integridad y almacenamiento de archivos al comparar implementaciones secuenciales, vectorizadas, compiladas y distribuidas.

## Objetivos

1. Comparar rendimiento de `sequential`, `numpy`, `cython` y `ray`.
2. Medir throughput y speedup de la capa de preprocesamiento.
3. Medir uso de CPU y memoria.
4. Evaluar escalabilidad con `ray-workers`.
5. Evaluar concurrencia con `upload-workers`.
6. Medir ahorro de almacenamiento por deduplicacion.
7. Validar que la reconstruccion conserva la integridad del archivo original.

## Modos

Microbenchmark:

```text
archivo/chunks -> preprocess_chunks()
```

Sirve para medir la capa HPC pura. Escribe `/results/benchmark.csv`.

```bash
docker compose --profile benchmark run --rm benchmark
```

End-to-end por archivo:

```text
archivo -> preprocess_chunks() -> deduplicador -> MinIO -> PostgreSQL -> reconstruccion
```

```bash
BENCHMARK_MODE=end_to_end docker compose --profile benchmark run --rm benchmark
```

Batch end-to-end:

```text
batch -> HTTP -> RabbitMQ -> upload-worker -> processor -> deduplicador -> MinIO/PostgreSQL -> reconstruccion
```

Sirve para medir comportamiento distribuido con multiples archivos. Escribe `/results/batch_benchmark.csv` y `/results/batch_benchmark_summary.csv`.

## Scripts Batch

Generar dataset reproducible:

```bash
python benchmark/generate_batch.py \
  --dataset repeated \
  --num-files 10 \
  --file-size-mb 100 \
  --chunk-size-kb 1024 \
  --output benchmark/datasets/batch_repeated_10x100
```

Enviar batch por HTTP:

```bash
python benchmark/send_batch.py \
  --batch benchmark/datasets/batch_repeated_10x100 \
  --backend-url http://backend:8000 \
  --concurrency 3 \
  --wait-results
```

Orquestar experimento completo:

```bash
python benchmark/run_batch_benchmark.py \
  --dataset repeated \
  --num-files 10 \
  --file-size-mb 100 \
  --chunk-size-kb 1024 \
  --backend-url http://backend:8000 \
  --concurrency 3 \
  --repetitions 5
```

Usar presets:

```bash
python benchmark/run_batch_benchmark.py \
  --dataset mixed \
  --batch-preset batch_mixed \
  --chunk-size-kb 1024 \
  --backend-url http://backend:8000 \
  --concurrency 5 \
  --repetitions 5
```

Presets disponibles:

- `batch_small`: 10 archivos de 10 MB.
- `batch_medium`: 10 archivos de 100 MB.
- `batch_large`: 5 archivos de 500 MB.
- `batch_mixed`: 5 archivos de 10 MB, 5 archivos de 100 MB y 2 archivos de 500 MB.

## Variables Independientes

- `backend`: `sequential`, `numpy`, `cython`, `ray`.
- `dataset_type`: `random`, `repeated`, `modified`, `mixed`.
- `file_size_mb`: `10`, `100`, `500`.
- `chunk_size_kb`: `256`, `1024`, `4096`.
- `num_files`: `1`, `5`, `10`.
- `ray_workers`: `1`, `2`, `4`.
- `upload_workers`: `1`, `2`, `3`.
- `concurrency`: `1`, `3`, `5`.

Para el batch HTTP, el `chunk_size_kb` efectivo lo usa el `upload-worker`. Levanta el sistema con:

```bash
CHUNK_SIZE_KB=1024 docker compose up -d --build
```

## Variables Dependientes

- `elapsed_seconds`
- `throughput_mb_s`
- `speedup_vs_sequential`
- `cpu_avg_pct`
- `cpu_max_pct`
- `ram_delta_mb`
- `peak_memory_mb`
- `chunk_count`
- `unique_chunks`
- `duplicate_chunks`
- `storage_saving_pct`
- `dedup_ratio`
- `integrity_ok`
- `batch_throughput_mb_s`
- `files_per_second`

## Metricas Batch

`batch_benchmark.csv` incluye:

- `batch_id`
- `dataset_type`
- `num_files`
- `file_size_mb`
- `total_input_mb`
- `chunk_size_kb`
- `backend`
- `run_number`
- `total_elapsed_seconds`
- `batch_throughput_mb_s`
- `files_per_second`
- `total_chunks`
- `total_unique_chunks`
- `total_duplicate_chunks`
- `total_bytes_original`
- `total_bytes_stored`
- `total_bytes_saved`
- `storage_saving_pct`
- `dedup_ratio`
- `integrity_ok_count`
- `integrity_failed_count`
- `cpu_avg_pct`
- `cpu_max_pct`
- `ram_delta_mb`
- `peak_memory_mb`
- `ray_workers`
- `upload_workers`
- `concurrency`

`batch_benchmark_summary.csv` registra promedio, desviacion estandar, minimo y maximo para tiempo y throughput por configuracion.

## Interpretacion

- `throughput_mb_s`: MB procesados por segundo.
- `speedup_vs_sequential`: aceleracion contra el promedio secuencial de la misma configuracion.
- `dedup_ratio`: `bytes_original / bytes_stored`; mayor es mejor.
- `storage_saving_pct`: porcentaje de bytes evitados por deduplicacion.
- `integrity_ok`: confirma que el SHA-256 global reconstruido coincide con el original.
- `integrity_ok_count` y `integrity_failed_count`: validacion por archivo dentro del batch.

## Hipotesis Esperadas

1. `sequential` sera el baseline mas simple.
2. `numpy` puede no mejorar significativamente si el costo principal esta en SHA-256 y CRC32.
3. `cython`/OpenMP puede mejorar partes CPU-bound, especialmente CRC32 si esta implementado en Cython.
4. `ray` puede tener overhead en archivos pequenos, pero deberia mejorar con archivos grandes o batches grandes.
5. Escalar `upload-workers` mejora el procesamiento de multiples archivos concurrentes, no necesariamente de un unico archivo.
6. Escalar `ray-workers` mejora el procesamiento paralelo de chunks cuando `HPC_BACKEND=ray`.
7. Datasets `random` tendran bajo o nulo ahorro por deduplicacion.
8. Datasets `repeated` tendran alto ahorro por deduplicacion.
9. Datasets `modified` y `mixed` representan escenarios intermedios.

## Control de Condiciones

Registra junto a cada corrida:

- CPU disponible.
- RAM disponible.
- numero de `ray-workers`.
- numero de `upload-workers`.
- tamano de chunk.
- tipo de dataset.
- numero de archivos.
- backend usado.
- concurrencia HTTP.

## Ray

Ray no utiliza GPU automaticamente. En este proyecto Ray se utiliza como framework de distribucion de tareas sobre CPU. Cada chunk puede ser enviado como una tarea remota a `ray-workers`. La aceleracion esperada viene de paralelismo CPU/distribuido, no de procesamiento GPU.

## Escalado

`upload-worker` escala el consumo de tareas completas. Ayuda cuando hay varios archivos o mensajes en cola, pero no necesariamente acelera un unico archivo:

```bash
UPLOAD_WORKERS=3 docker compose up -d --scale upload-worker=3
```

`ray-worker` escala el procesamiento interno de chunks cuando `HPC_BACKEND=ray`:

```bash
docker compose up -d --scale ray-worker=1
RAY_WORKERS=1 docker compose --profile benchmark run --rm benchmark

docker compose up -d --scale ray-worker=2
RAY_WORKERS=2 docker compose --profile benchmark run --rm benchmark

docker compose up -d --scale ray-worker=4
RAY_WORKERS=4 docker compose --profile benchmark run --rm benchmark
```

Si reutilizas un volumen antiguo de PostgreSQL, `init.sql` no se ejecuta de nuevo. Para una base limpia durante pruebas, elimina el volumen de Postgres antes de levantar el sistema.
