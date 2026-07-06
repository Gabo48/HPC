#!/usr/bin/env bash
set -euo pipefail

# Probe enfocado en paralelizacion por chunks con un solo archivo grande.
# Uso:
#   ./parallel_probe.sh
#   FILE_SIZE_MB=1000 REPS=3 ./parallel_probe.sh
#
# Los CSV quedan en:
#   results/parallel_probe_<timestamp>/

BACKENDS=("sequential" "numpy" "cython" "ray")
FILE_SIZE_MB="${FILE_SIZE_MB:-500}"
DATASET="${DATASET:-random}"
REPS="${REPS:-3}"
CHUNK_SIZE_KB="${CHUNK_SIZE_KB:-1024}"
NUM_FILES=1

CYTHON_OMP_THREADS="${CYTHON_OMP_THREADS:-6}"
RAY_WORKERS_LIST=(${RAY_WORKERS_LIST:-2 3})
RAY_WORKER_CPUS="${RAY_WORKER_CPUS:-1}"
RAY_CHUNK_BATCH_SIZE="${RAY_CHUNK_BATCH_SIZE:-16}"

RUN_ID="parallel_probe_$(date +%Y%m%d_%H%M%S)"
RESULTS_ROOT="results"
RESULTS_DIR="${RESULTS_ROOT}/${RUN_ID}"

compose_cleanup() {
    docker compose down -v --remove-orphans || true
}

wait_for_backend() {
    local deadline=$((SECONDS + 120))
    until curl -fsS http://localhost:8000/docs >/dev/null 2>&1; do
        if (( SECONDS >= deadline )); then
            echo "Backend no se levantó en tiempo" >&2
            return 1
        fi
        sleep 2
    done
}

run_case() {
    local backend="$1"
    local ray_workers="$2"
    local omp_threads="$3"
    local experiment="$4"
    local csv_output="/results/${RUN_ID}/${experiment}.csv"

    echo "=== Iniciando ${experiment} ==="
    compose_cleanup

    if [ "$backend" = "ray" ]; then
        HPC_BACKEND="$backend" CHUNK_SIZE_KB="$CHUNK_SIZE_KB" RAY_WORKERS="$ray_workers" \
            RAY_WORKER_CPUS="$RAY_WORKER_CPUS" RAY_CHUNK_BATCH_SIZE="$RAY_CHUNK_BATCH_SIZE" \
            OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 NUMEXPR_NUM_THREADS=1 \
            docker compose up -d \
                --scale upload-worker=1 \
                --scale ray-worker="$ray_workers" \
                backend upload-worker ray-head ray-worker rabbitmq postgres minio
    else
        HPC_BACKEND="$backend" CHUNK_SIZE_KB="$CHUNK_SIZE_KB" RAY_WORKERS=0 \
            RAY_WORKER_CPUS="$RAY_WORKER_CPUS" RAY_CHUNK_BATCH_SIZE="$RAY_CHUNK_BATCH_SIZE" \
            OMP_NUM_THREADS="$omp_threads" OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 NUMEXPR_NUM_THREADS=1 \
            docker compose up -d \
                --scale upload-worker=1 \
                backend upload-worker rabbitmq postgres minio
    fi

    wait_for_backend

    HPC_BACKEND="$backend" CHUNK_SIZE_KB="$CHUNK_SIZE_KB" RAY_WORKERS="$ray_workers" \
        RAY_WORKER_CPUS="$RAY_WORKER_CPUS" RAY_CHUNK_BATCH_SIZE="$RAY_CHUNK_BATCH_SIZE" \
        OMP_NUM_THREADS="$omp_threads" OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 NUMEXPR_NUM_THREADS=1 \
        docker compose --profile benchmark run --rm benchmark \
            python benchmark/run_batch_benchmark.py \
                --dataset "$DATASET" \
                --num-files "$NUM_FILES" \
                --file-size-mb "$FILE_SIZE_MB" \
                --chunk-size-kb "$CHUNK_SIZE_KB" \
                --backend-url http://backend:8000 \
                --backend "$backend" \
                --concurrency 1 \
                --repetitions "$REPS" \
                --csv-output "$csv_output" \
                --experiment "$experiment" \
                --ray-workers "$ray_workers" \
                --upload-workers 1

    echo "=== Finalizado ${experiment} ==="
}

main() {
    mkdir -p "$RESULTS_DIR"
    echo "Resultados: ${RESULTS_DIR}"
    echo "Dataset=${DATASET} file_size_mb=${FILE_SIZE_MB} reps=${REPS} chunk_size_kb=${CHUNK_SIZE_KB}"

    docker compose build

    run_case "sequential" 0 1 "sequential_${DATASET}_${FILE_SIZE_MB}mb_1file_uw1"
    run_case "numpy" 0 1 "numpy_${DATASET}_${FILE_SIZE_MB}mb_1file_uw1"
    run_case "cython" 0 "$CYTHON_OMP_THREADS" "cython_${DATASET}_${FILE_SIZE_MB}mb_1file_uw1_omp${CYTHON_OMP_THREADS}"

    for ray_workers in "${RAY_WORKERS_LIST[@]}"; do
        run_case "ray" "$ray_workers" 1 "ray_${DATASET}_${FILE_SIZE_MB}mb_1file_uw1_rw${ray_workers}"
    done

    compose_cleanup

    echo
    echo "Probe terminado. Revisa:"
    echo "  ${RESULTS_DIR}"
    echo
    echo "Columnas clave:"
    echo "  total_elapsed_seconds, preprocess_avg_seconds, worker_total_avg_seconds"
}

main "$@"
