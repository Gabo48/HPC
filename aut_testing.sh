#!/usr/bin/env bash
set -euo pipefail

BACKENDS=("sequential" "numpy" "cython" "ray")
DATASET="repeated"
NUM_FILES=3
FILE_SIZE_MB=10
CHUNK_SIZE_KB=1024
CONCURRENCY=1
REPETITIONS=5

for BACKEND in "${BACKENDS[@]}"; do
  echo "=== Ejecutando backend: ${BACKEND} ==="

  if [ "$BACKEND" = "ray" ]; then
    HPC_BACKEND="$BACKEND" \
    CHUNK_SIZE_KB="$CHUNK_SIZE_KB" \
    RAY_WORKERS=2 \
    docker compose up -d --build --scale ray-worker=2 --scale upload-worker=1
  else
    HPC_BACKEND="$BACKEND" \
    CHUNK_SIZE_KB="$CHUNK_SIZE_KB" \
    docker compose up -d --build --scale upload-worker=1
  fi

  docker compose --profile benchmark run --rm benchmark \
    python benchmark/run_batch_benchmark.py \
    --dataset "$DATASET" \
    --num-files "$NUM_FILES" \
    --file-size-mb "$FILE_SIZE_MB" \
    --chunk-size-kb "$CHUNK_SIZE_KB" \
    --backend-url http://backend:8000 \
    --backend "$BACKEND" \
    --concurrency "$CONCURRENCY" \
    --repetitions "$REPETITIONS"

  echo "=== Terminado backend: ${BACKEND} ==="
done

echo "Resultados:"
echo "  results/batch_benchmark.csv"
echo "  results/batch_benchmark_summary.csv"