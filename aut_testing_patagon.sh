#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# CONFIGURACIÓN
# ============================================================
BACKENDS=("sequential" "numpy" "cython" "ray")
DATASETS=("random" "repeated" "modified" "mixed")
FILE_SIZES=(10 50 100)

NUM_FILES_MAIN=20

# En HPC controlamos los hilos directamente con las variables que lee Python,
# ya no escalamos contenedores independientes con Docker.
UPLOAD_WORKERS=(1 2)
RAY_WORKERS=(2 4)

CHUNK_SIZE_KB=1024
RESULTS_DIR="results"
SIF_IMAGE="benchmark_image.sif" # Tu contenedor de Apptainer

ENABLE_ENERGY_MEASUREMENT=0

reps_for_size() {
    local size="$1"
    if [ "$size" -ge 500 ]; then echo 2; elif [ "$size" -ge 100 ]; then echo 3; else echo 5; fi
}

# ============================================================
# HELPERS (Limpiados de Docker)
# ============================================================
cluster_cleanup() {
    echo "Limpiando entornos temporales si aplica..."
}

wait_for_backend() {
    # En HPC, si el backend corre local/nativo o en el mismo nodo, validamos su puerto.
    local deadline=$((SECONDS + 120))
    until curl -fsS http://localhost:8000/docs >/dev/null 2>&1; do
        if (( SECONDS >= deadline )); then
            echo "El servicio Backend no responde en el puerto 8000" >&2
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

    local csv_output="/app/results/${experiment}.csv"
    echo "=== Iniciando experimento: ${experiment} (reps=${repetitions}) ==="

    cluster_cleanup

    # --- CAMBIO CLAVE HPC ---
    # En lugar de levantar contenedores aislados con Docker Compose, ejecutamos el script 
    # de Python directamente dentro de tu entorno seguro aislado de Apptainer (.sif)
    # Pasamos las variables de entorno para que las librerías internas las configuren.
    
    if ! apptainer exec --bind .:/app "$SIF_IMAGE" \
        env HPC_BACKEND="$backend" CHUNK_SIZE_KB="$CHUNK_SIZE_KB" RAY_WORKERS="$ray_workers" \
        python /app/benchmark/run_batch_benchmark.py \
            --dataset "$dataset" \
            --num-files "$num_files" \
            --file-size-mb "$file_size_mb" \
            --chunk-size-kb "$CHUNK_SIZE_KB" \
            --backend-url "http://localhost:8000" \
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

    echo "=== Experimento finalizado: ${experiment} ==="
}

# ============================================================
# MATRIZ PRINCIPAL
# ============================================================
main_matrix() {
    for backend in "${BACKENDS[@]}"; do
        for dataset in "${DATASETS[@]}"; do
            for file_size_mb in "${FILE_SIZES[@]}"; do
                local repetitions
                repetitions=$(reps_for_size "$file_size_mb")
                for upload_workers in "${UPLOAD_WORKERS[@]}"; do
                    if [ "$backend" = "ray" ]; then
                        for ray_workers in "${RAY_WORKERS[@]}"; do
                            local experiment="ray_${dataset}_${file_size_mb}mb_uw${upload_workers}_rw${ray_workers}"
                            run_experiment "$backend" "$dataset" "$file_size_mb" "$NUM_FILES_MAIN" \
                                "$upload_workers" "$ray_workers" "$experiment" "$repetitions" \
                                || echo "Continuando pese al fallo en ${experiment}"
                        done
                    else
                        local experiment="${backend}_${dataset}_${file_size_mb}mb_uw${upload_workers}"
                        run_experiment "$backend" "$dataset" "$file_size_mb" "$NUM_FILES_MAIN" \
                            "$upload_workers" 0 "$experiment" "$repetitions" \
                            || echo "Continuando pese al fallo en ${experiment}"
                    fi
                done
            done
        done
    done
}

# ============================================================
# EXPERIMENTO EXTRA
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
                    2 2 "$experiment" "$repetitions" \
                    || echo "Continuando pese al fallo en ${experiment}"
            else
                local experiment="extra_nf_sequential_${num_files}files"
                run_experiment "$backend" "$dataset" "$file_size_mb" "$num_files" \
                    2 0 "$experiment" "$repetitions" \
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

    echo "--- Matriz principal en HPC ---"
    main_matrix

    echo "--- Experimento extra: efecto de num_files ---"
    extra_num_files_experiment

    cluster_cleanup
    echo "Resultados generados en ${RESULTS_DIR}/"
}

main "$@"