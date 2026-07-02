# Sistema HPC de Preprocesamiento de Chunks

## Arquitectura

Este proyecto implementa un sistema distribuido de almacenamiento por chunks, independiente de cualquier codigo externo a `hpc/`.

Servicios unicos de infraestructura:

- `rabbitmq`: broker de mensajes para `upload.tasks` y `upload.results`.
- `postgres`: base de datos de metadata de archivos, chunks y benchmark.
- `minio`: almacenamiento de objetos para los bytes originales de cada chunk.
- `ray-head`: nodo coordinador de Ray y dashboard.

Servicios replicables:

- `ray-worker`: nodos de computo Ray para distribuir el preprocesamiento.
- `backend`: API FastAPI stateless para recibir uploads y consultar metadata.
- `upload-worker`: consumidores de RabbitMQ que dividen archivos, calculan metadata y almacenan chunks.

Benchmark aislado:

- `benchmark`: contenedor bajo perfil `benchmark` que mide computo puro llamando directamente a los processors.

Los bytes almacenados en MinIO son identicos a los bytes originales. El preprocesamiento calcula `sha256`, `crc32`, tamano y timestamp. La capa de deduplicacion usa `sha256` como fingerprint principal: si un chunk ya existe en `unique_chunks`, no crea un nuevo objeto en MinIO y solo agrega una referencia logica en `file_chunk_refs`.

## Levantar el sistema

```bash
cd hpc
docker compose up -d
```

## Escalar componentes

```bash
# Escalar upload-workers
docker compose up -d --scale upload-worker=3

# Escalar backend
docker compose up -d --scale backend=2
```

Si escalas `backend`, evita publicar el mismo puerto host para multiples replicas en una sola maquina. En produccion, pon un balanceador delante o ajusta el mapeo de puertos.

`upload-worker` permite procesar varios archivos o tareas concurrentes. No necesariamente acelera un unico archivo, porque cada worker consume mensajes completos desde RabbitMQ.

`ray-worker` aumenta la capacidad interna de computo para chunks cuando usas `HPC_BACKEND=ray`.

## Escenarios experimentales Ray

```bash
# Escenario A: baseline secuencial
HPC_BACKEND=sequential docker compose up -d

# Escenario B: Ray con 1 worker
docker compose up -d --scale ray-worker=1

# Escenario C: Ray con 2 workers
docker compose up -d --scale ray-worker=2
```

## Subir archivos desde otro equipo en la red

```bash
# En el equipo cliente generar archivos de prueba
dd if=/dev/urandom of=archivo_10mb.bin bs=1M count=10
dd if=/dev/urandom of=archivo_100mb.bin bs=1M count=100
dd if=/dev/urandom of=archivo_500mb.bin bs=1M count=500

# Subir al sistema (reemplazar IP con la del servidor)
curl -X POST http://192.168.x.x:8000/upload \
     -F "file=@archivo_10mb.bin"
```

## Verificar integridad

```bash
curl http://192.168.x.x:8000/file/{file_id}/integrity
```

## Descargar archivo reconstruido

```bash
curl -o reconstruido.bin http://192.168.x.x:8000/file/{file_id}/download
```

## Correr benchmark

```bash
docker compose --profile benchmark run benchmark
```

Modo end-to-end con deduplicacion, MinIO, PostgreSQL y reconstruccion:

```bash
BENCHMARK_MODE=end_to_end docker compose --profile benchmark run --rm benchmark
```

Corrida rapida para pruebas:

```bash
BENCHMARK_FILE_SIZES_MB=10 \
BENCHMARK_CHUNK_SIZES_KB=1024 \
BENCHMARK_DATASETS=random,repeated \
docker compose --profile benchmark run --rm benchmark
```

## Ver resultados

```bash
cat results/benchmark.csv
```

Mas detalles del benchmark: [docs/benchmark.md](docs/benchmark.md).

## Benchmark por batch

El benchmark por batch mide el flujo completo:

```text
batch -> HTTP /upload -> RabbitMQ -> upload-workers -> processor -> deduplicador -> MinIO/PostgreSQL -> reconstruccion
```

Ejemplo:

```bash
docker compose --profile benchmark run --rm benchmark \
  python benchmark/run_batch_benchmark.py \
  --dataset repeated \
  --num-files 10 \
  --file-size-mb 100 \
  --chunk-size-kb 1024 \
  --backend-url http://backend:8000 \
  --concurrency 3 \
  --repetitions 5
```

Presets:

```bash
docker compose --profile benchmark run --rm benchmark \
  python benchmark/run_batch_benchmark.py \
  --dataset mixed \
  --batch-preset batch_mixed \
  --chunk-size-kb 1024 \
  --backend-url http://backend:8000 \
  --concurrency 5 \
  --repetitions 5
```

Resultados:

```bash
cat results/batch_benchmark.csv
cat results/batch_benchmark_summary.csv
```

## Dashboards

| Servicio | URL |
|----------|-----|
| Ray | http://localhost:8265 |
| RabbitMQ | http://localhost:15672 |
| MinIO | http://localhost:9001 |
| Backend | http://localhost:8000/docs |
