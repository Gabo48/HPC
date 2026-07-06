#!/usr/bin/env bash
set -euo pipefail

# Diagnostico para separar metodologia vs implementacion:
# 1) microbenchmark solo de preprocess (sin HTTP, RabbitMQ, Postgres ni MinIO)
# 2) Ray con distintos batch sizes
# 3) Cython/OpenMP con threads controlados
# 4) barrido de granularidad por chunk_size_kb
#
# Uso:
#   ./processor_diagnostics.sh
#   FILE_SIZE_MB=500 DATASET=random RUNS=5 ./processor_diagnostics.sh
#
# Resultados:
#   results/processor_diagnostics_<timestamp>/

DATASET="${DATASET:-random}"
FILE_SIZE_MB="${FILE_SIZE_MB:-200}"
RUNS="${RUNS:-3}"
CHUNK_SIZES_KB=(${CHUNK_SIZES_KB:-1024 4096 8192})

CYTHON_OMP_THREADS="${CYTHON_OMP_THREADS:-6}"
RAY_WORKERS_LIST=(${RAY_WORKERS_LIST:-2 3})
RAY_BATCH_SIZES=(${RAY_BATCH_SIZES:-1 8 16 32})
RAY_WORKER_CPUS="${RAY_WORKER_CPUS:-1}"

RUN_ID="processor_diagnostics_$(date +%Y%m%d_%H%M%S)"
RESULTS_ROOT="results"
RESULTS_DIR="${RESULTS_ROOT}/${RUN_ID}"

compose_cleanup() {
    docker compose down -v --remove-orphans || true
}

run_processor_case() {
    local backend="$1"
    local chunk_size_kb="$2"
    local ray_workers="$3"
    local ray_batch_size="$4"
    local omp_threads="$5"
    local label="$6"

    echo "=== Processor diagnostic: ${label} ==="
    compose_cleanup

    if [ "$backend" = "ray" ]; then
        RAY_WORKER_CPUS="$RAY_WORKER_CPUS" docker compose up -d \
            --scale ray-worker="$ray_workers" \
            ray-head ray-worker
    fi

    BENCHMARK_MODE=processor \
        BENCHMARK_BACKENDS="$backend" \
        BENCHMARK_DATASETS="$DATASET" \
        BENCHMARK_FILE_SIZES_MB="$FILE_SIZE_MB" \
        BENCHMARK_CHUNK_SIZES_KB="$chunk_size_kb" \
        BENCHMARK_RUNS="$RUNS" \
        HPC_BACKEND="$backend" \
        CHUNK_SIZE_KB="$chunk_size_kb" \
        RAY_WORKERS="$ray_workers" \
        RAY_CHUNK_BATCH_SIZE="$ray_batch_size" \
        OMP_NUM_THREADS="$omp_threads" \
        OPENBLAS_NUM_THREADS=1 \
        MKL_NUM_THREADS=1 \
        NUMEXPR_NUM_THREADS=1 \
        docker compose --profile benchmark run --rm benchmark \
            python benchmark/run_benchmark.py

    cp "${RESULTS_ROOT}/benchmark.csv" "${RESULTS_DIR}/${label}.csv"
}

main() {
    mkdir -p "$RESULTS_DIR"
    echo "Resultados: ${RESULTS_DIR}"
    echo "DATASET=${DATASET} FILE_SIZE_MB=${FILE_SIZE_MB} RUNS=${RUNS}"
    echo "CHUNK_SIZES_KB=${CHUNK_SIZES_KB[*]}"

    docker compose build benchmark ray-head ray-worker

    for chunk_size_kb in "${CHUNK_SIZES_KB[@]}"; do
        run_processor_case \
            "sequential" "$chunk_size_kb" 0 1 1 \
            "sequential_${DATASET}_${FILE_SIZE_MB}mb_${chunk_size_kb}kb"

        run_processor_case \
            "numpy" "$chunk_size_kb" 0 1 1 \
            "numpy_${DATASET}_${FILE_SIZE_MB}mb_${chunk_size_kb}kb"

        run_processor_case \
            "cython" "$chunk_size_kb" 0 1 "$CYTHON_OMP_THREADS" \
            "cython_${DATASET}_${FILE_SIZE_MB}mb_${chunk_size_kb}kb_omp${CYTHON_OMP_THREADS}"

        for ray_workers in "${RAY_WORKERS_LIST[@]}"; do
            for ray_batch_size in "${RAY_BATCH_SIZES[@]}"; do
                run_processor_case \
                    "ray" "$chunk_size_kb" "$ray_workers" "$ray_batch_size" 1 \
                    "ray_${DATASET}_${FILE_SIZE_MB}mb_${chunk_size_kb}kb_rw${ray_workers}_batch${ray_batch_size}"
            done
        done
    done

    compose_cleanup

    echo
    echo "Diagnostico listo en ${RESULTS_DIR}"
    echo "Columnas clave: elapsed_seconds, throughput_mb_s, speedup_vs_sequential, cpu_avg_pct, peak_memory_mb"
}

main "$@"
