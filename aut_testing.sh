#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# CONFIGURACIÓN
# ============================================================
BACKENDS=("sequential" "numpy" "cython" "ray")
DATASETS=("random" "repeated" "modified" "mixed")
FILE_SIZES=(10 50 100)

# num_files fijo para la matriz principal (barrido completo).
# El efecto de num_files se explora aparte, en un experimento reducido (ver EXTRA abajo).
NUM_FILES_MAIN=20

# Dos modos comparables:
# - uw1: comparación limpia del método de procesamiento dentro de un archivo.
# - uw2: throughput del sistema con dos archivos en paralelo.
UPLOAD_WORKERS=(1 2)

# En un i5-12450H, cada ray-worker se limita a 1 CPU. Usamos rw3 solo en uw1
# para medir escalamiento sin mezclarlo con dos upload-workers.
RAY_WORKERS_UW1=(2 3)
RAY_WORKERS_UW2=(2)

# OpenMP/Cython: mantener upload_workers * OMP_NUM_THREADS <= 6 aprox.
CYTHON_OMP_UW1=4
CYTHON_OMP_UW2=3

RAY_WORKER_CPUS=1
RAY_CHUNK_BATCH_SIZE=16

CHUNK_SIZE_KB=1024
RESULTS_DIR="results"

# Repeticiones adaptativas: menos reps para archivos grandes (500MB)
# para no volarte el tiempo total. Ajusta según lo que necesites.
reps_for_size() {
    local size="$1"
    if [ "$size" -ge 500 ]; then
        echo 2
    elif [ "$size" -ge 100 ]; then
        echo 3
    else
        echo 5
    fi
}

omp_threads_for() {
    local backend="$1"
    local upload_workers="$2"

    if [ "$backend" != "cython" ]; then
        echo 1
    elif [ "$upload_workers" -eq 1 ]; then
        echo "$CYTHON_OMP_UW1"
    else
        echo "$CYTHON_OMP_UW2"
    fi
}

ray_workers_for_upload_workers() {
    local upload_workers="$1"

    if [ "$upload_workers" -eq 1 ]; then
        printf "%s\n" "${RAY_WORKERS_UW1[@]}"
    else
        printf "%s\n" "${RAY_WORKERS_UW2[@]}"
    fi
}

# ============================================================
# ENERGÍA (Intel RAPL vía powercap)
# ============================================================
RAPL_PATH="/sys/class/powercap/intel-rapl:0/energy_uj"
ENERGY_CSV="${RESULTS_DIR}/energy.csv"
RAPL_AVAILABLE=0

check_rapl() {
    if [ -r "$RAPL_PATH" ]; then
        RAPL_AVAILABLE=1
        echo "RAPL disponible: se medirá energía por experimento."
    else
        RAPL_AVAILABLE=0
        echo "AVISO: $RAPL_PATH no accesible (¿estás en una VM sin passthrough, o sin permisos?)."
        echo "Los experimentos van a correr igual, pero SIN datos de energía."
        echo "Prueba: sudo chmod -R a+r /sys/class/powercap/intel-rapl:0/ (o corré el script con sudo)."
    fi
}

read_rapl_uj() {
    cat "$RAPL_PATH" 2>/dev/null || echo 0
}

# ============================================================
# HELPERS
# ============================================================
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

run_experiment() {
    local backend="$1"
    local dataset="$2"
    local file_size_mb="$3"
    local num_files="$4"
    local upload_workers="$5"
    local ray_workers="$6"
    local experiment="$7"
    local repetitions="$8"
    local omp_threads="$9"

    local csv_output="/results/${experiment}.csv"
    echo "=== Iniciando experimento: ${experiment} (reps=${repetitions}) ==="

    compose_cleanup

    if [ "$backend" = "ray" ]; then
        HPC_BACKEND="$backend" CHUNK_SIZE_KB="$CHUNK_SIZE_KB" RAY_WORKERS="$ray_workers" \
            RAY_WORKER_CPUS="$RAY_WORKER_CPUS" RAY_CHUNK_BATCH_SIZE="$RAY_CHUNK_BATCH_SIZE" \
            OMP_NUM_THREADS="$omp_threads" OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 NUMEXPR_NUM_THREADS=1 \
            docker compose up -d \
                --scale upload-worker="$upload_workers" \
                --scale ray-worker="$ray_workers" \
                backend upload-worker ray-head ray-worker rabbitmq postgres minio
    else
        HPC_BACKEND="$backend" CHUNK_SIZE_KB="$CHUNK_SIZE_KB" RAY_WORKERS=0 \
            RAY_WORKER_CPUS="$RAY_WORKER_CPUS" RAY_CHUNK_BATCH_SIZE="$RAY_CHUNK_BATCH_SIZE" \
            OMP_NUM_THREADS="$omp_threads" OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 NUMEXPR_NUM_THREADS=1 \
            docker compose up -d \
                --scale upload-worker="$upload_workers" \
                backend upload-worker rabbitmq postgres minio
    fi

    if ! wait_for_backend; then
        echo "${experiment}: FALLO al levantar backend" >> "${RESULTS_DIR}/failed.log"
        return 1
    fi

    local energy_before energy_after wall_start wall_end
    energy_before=$(read_rapl_uj)
    wall_start=$(date +%s.%N)

    if ! HPC_BACKEND="$backend" CHUNK_SIZE_KB="$CHUNK_SIZE_KB" RAY_WORKERS="$ray_workers" \
        RAY_WORKER_CPUS="$RAY_WORKER_CPUS" RAY_CHUNK_BATCH_SIZE="$RAY_CHUNK_BATCH_SIZE" \
        OMP_NUM_THREADS="$omp_threads" OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 NUMEXPR_NUM_THREADS=1 \
        docker compose --profile benchmark run --rm benchmark \
        python benchmark/run_batch_benchmark.py \
            --dataset "$dataset" \
            --num-files "$num_files" \
            --file-size-mb "$file_size_mb" \
            --chunk-size-kb "$CHUNK_SIZE_KB" \
            --backend-url http://backend:8000 \
            --backend "$backend" \
            --concurrency "$upload_workers" \
            --repetitions "$repetitions" \
            --csv-output "$csv_output" \
            --experiment "$experiment" \
            --ray-workers "$ray_workers" \
            --upload-workers "$upload_workers"; then
        echo "${experiment}: FALLO en run_batch_benchmark.py" >> "${RESULTS_DIR}/failed.log"
        return 1
    fi

    wall_end=$(date +%s.%N)
    energy_after=$(read_rapl_uj)

    if [ "$RAPL_AVAILABLE" -eq 1 ]; then
        # energy_uj es un contador acumulado que puede hacer overflow y resetear;
        # si el delta da negativo, lo descartamos (NA) en vez de reportar un valor falso.
        local delta_uj wall_seconds
        wall_seconds=$(awk -v a="$wall_start" -v b="$wall_end" 'BEGIN{printf "%.3f", b-a}')
        if [ "$energy_after" -ge "$energy_before" ] 2>/dev/null; then
            delta_uj=$((energy_after - energy_before))
            local delta_joules avg_watts
            delta_joules=$(awk -v uj="$delta_uj" 'BEGIN{printf "%.3f", uj/1000000}')
            avg_watts=$(awk -v j="$delta_joules" -v s="$wall_seconds" 'BEGIN{ if (s>0) printf "%.3f", j/s; else print "NA" }')
            echo "${experiment},${delta_joules},${avg_watts},${wall_seconds}" >> "$ENERGY_CSV"
        else
            echo "${experiment},NA,NA,${wall_seconds}" >> "$ENERGY_CSV"
            echo "${experiment}: overflow/reset detectado en contador RAPL, energía descartada" >> "${RESULTS_DIR}/failed.log"
        fi
    fi

    echo "=== Experimento finalizado: ${experiment} ==="
}

# ============================================================
# MATRIZ PRINCIPAL: backend x dataset x file_size x upload_workers (x ray_workers si aplica)
# num_files fijo en NUM_FILES_MAIN
# ============================================================
main_matrix() {
    for backend in "${BACKENDS[@]}"; do
        for dataset in "${DATASETS[@]}"; do
            for file_size_mb in "${FILE_SIZES[@]}"; do
                local repetitions
                repetitions=$(reps_for_size "$file_size_mb")
                for upload_workers in "${UPLOAD_WORKERS[@]}"; do
                    local omp_threads
                    omp_threads=$(omp_threads_for "$backend" "$upload_workers")
                    if [ "$backend" = "ray" ]; then
                        while IFS= read -r ray_workers; do
                            local experiment="ray_${dataset}_${file_size_mb}mb_uw${upload_workers}_rw${ray_workers}"
                            run_experiment "$backend" "$dataset" "$file_size_mb" "$NUM_FILES_MAIN" \
                                "$upload_workers" "$ray_workers" "$experiment" "$repetitions" "$omp_threads" \
                                || echo "Continuando pese al fallo en ${experiment}"
                        done < <(ray_workers_for_upload_workers "$upload_workers")
                    elif [ "$backend" = "cython" ]; then
                        local experiment="cython_${dataset}_${file_size_mb}mb_uw${upload_workers}_omp${omp_threads}"
                        run_experiment "$backend" "$dataset" "$file_size_mb" "$NUM_FILES_MAIN" \
                            "$upload_workers" 0 "$experiment" "$repetitions" "$omp_threads" \
                            || echo "Continuando pese al fallo en ${experiment}"
                    else
                        local experiment="${backend}_${dataset}_${file_size_mb}mb_uw${upload_workers}"
                        run_experiment "$backend" "$dataset" "$file_size_mb" "$NUM_FILES_MAIN" \
                            "$upload_workers" 0 "$experiment" "$repetitions" "$omp_threads" \
                            || echo "Continuando pese al fallo en ${experiment}"
                    fi
                done
            done
        done
    done
}

# ============================================================
# EXPERIMENTO EXTRA: efecto de num_files, aislado
# Solo dataset=mixed, file_size=100MB, backend representativo (sequential y ray)
# para no repetir toda la matriz con num_files variable.
# ============================================================
extra_num_files_experiment() {
    local dataset="mixed"
    local file_size_mb=100
    local repetitions=3
    local num_files_values=(5 20 50)

    for backend in "sequential" "ray"; do
        for num_files in "${num_files_values[@]}"; do
            if [ "$backend" = "ray" ]; then
                local experiment="extra_nf_ray_${num_files}files_rw2"
                run_experiment "$backend" "$dataset" "$file_size_mb" "$num_files" \
                    2 2 "$experiment" "$repetitions" 1 \
                    || echo "Continuando pese al fallo en ${experiment}"
            else
                local experiment="extra_nf_sequential_${num_files}files"
                run_experiment "$backend" "$dataset" "$file_size_mb" "$num_files" \
                    2 0 "$experiment" "$repetitions" 1 \
                    || echo "Continuando pese al fallo en ${experiment}"
            fi
        done
    done
}

# ============================================================
# MAIN
# ============================================================
main() {
    mkdir -p "$RESULTS_DIR"
    : > "${RESULTS_DIR}/failed.log"

    check_rapl
    if [ "$RAPL_AVAILABLE" -eq 1 ]; then
        echo "experiment,energy_joules,avg_watts,wall_seconds" > "$ENERGY_CSV"
    fi

    echo "--- Build de imágenes (una sola vez) ---"
    docker compose build

    echo "--- Matriz principal ---"
    main_matrix

    echo "--- Experimento extra: efecto de num_files ---"
    extra_num_files_experiment

    compose_cleanup
    echo "Resultados generados en ${RESULTS_DIR}/"
    echo "Fallos (si hubo) registrados en ${RESULTS_DIR}/failed.log"
}

main "$@"
