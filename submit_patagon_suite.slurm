#!/bin/bash
#SBATCH --job-name=hpc_suite
#SBATCH --partition=cpu
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=08:00:00
#SBATCH --output=slurm_patagon_suite_%j.out
#SBATCH --error=slurm_patagon_suite_%j.err

set -euo pipefail

# Suite para Patagon/SLURM.
#
# Replica en Patagon las pruebas que SI calzan con un contenedor SLURM:
#   1) processor_diagnostics.sh: microbenchmark de preprocess con chunk/batch sweep.
#   2) parallel_probe.sh: un archivo grande para ver si aparece paralelizacion.
#   3) aut_testing.sh: equivalente de matriz, pero SOLO en modo processor.
#
# Importante:
#   aut_testing.sh end-to-end completo usa Docker Compose + backend + RabbitMQ +
#   Postgres + MinIO. Eso no se replica aqui porque Patagon usa SLURM/pyxis,
#   no docker compose dentro del job. Esta suite produce una comparacion
#   computacional aislada y comparable entre metodos.
#
# Ejemplos:
#   sbatch submit_patagon_suite.slurm
#   sbatch --export=ALL,SUITE_MODE=diagnostics submit_patagon_suite.slurm
#   sbatch --export=ALL,SUITE_MODE=all,SLURM_CPUS=32 submit_patagon_suite.slurm

SUITE_MODE="${SUITE_MODE:-all}" # all | diagnostics | probe | matrix

CONTAINER_IMAGE="${CONTAINER_IMAGE:-rayproject/ray:2.10.0-py311}"
CONTAINER_NAME="${CONTAINER_NAME:-hpc-suite-${SLURM_JOB_ID}}"

DIAG_DATASET="${DIAG_DATASET:-random}"
DIAG_FILE_SIZE_MB="${DIAG_FILE_SIZE_MB:-200}"
DIAG_RUNS="${DIAG_RUNS:-3}"
DIAG_CHUNK_SIZES_KB="${DIAG_CHUNK_SIZES_KB:-1024 4096 8192}"
DIAG_RAY_WORKERS_LIST="${DIAG_RAY_WORKERS_LIST:-2 3}"
DIAG_RAY_BATCH_SIZES="${DIAG_RAY_BATCH_SIZES:-1 8 16 32}"

PROBE_DATASET="${PROBE_DATASET:-random}"
PROBE_FILE_SIZE_MB="${PROBE_FILE_SIZE_MB:-500}"
PROBE_RUNS="${PROBE_RUNS:-3}"
PROBE_CHUNK_SIZE_KB="${PROBE_CHUNK_SIZE_KB:-1024}"
PROBE_RAY_WORKERS_LIST="${PROBE_RAY_WORKERS_LIST:-2 3}"
PROBE_RAY_BATCH_SIZE="${PROBE_RAY_BATCH_SIZE:-16}"

MATRIX_DATASETS="${MATRIX_DATASETS:-random repeated modified mixed}"
MATRIX_FILE_SIZES_MB="${MATRIX_FILE_SIZES_MB:-10 50 100}"
MATRIX_CHUNK_SIZE_KB="${MATRIX_CHUNK_SIZE_KB:-1024}"
MATRIX_RAY_WORKERS_LIST="${MATRIX_RAY_WORKERS_LIST:-2 3}"
MATRIX_RAY_BATCH_SIZE="${MATRIX_RAY_BATCH_SIZE:-16}"

CYTHON_OMP_THREADS="${CYTHON_OMP_THREADS:-8}"
RAY_LOCAL_CPUS_DEFAULT="${RAY_LOCAL_CPUS_DEFAULT:-${SLURM_CPUS_ON_NODE:-16}}"

RESULTS_DIR="${PWD}/results/patagon_suite_${SLURM_JOB_ID}"
mkdir -p "$RESULTS_DIR"

echo "Job ${SLURM_JOB_ID}"
echo "PWD=${PWD}"
echo "Container=${CONTAINER_IMAGE}"
echo "SUITE_MODE=${SUITE_MODE}"
echo "Resultados=${RESULTS_DIR}"
echo "SLURM_CPUS_ON_NODE=${SLURM_CPUS_ON_NODE:-unknown}"

srun \
    --container-image="${CONTAINER_IMAGE}" \
    --container-name="${CONTAINER_NAME}" \
    --container-workdir="${PWD}" \
    bash -lc "
        set -euo pipefail

        echo 'Preparando entorno Python...'
        pip install --user -r requirements.benchmark.txt
        cd processor/v3_cython
        python setup.py build_ext --inplace
        cd ../..

        export PYTHONPATH='${PWD}'
        export OPENBLAS_NUM_THREADS=1
        export MKL_NUM_THREADS=1
        export NUMEXPR_NUM_THREADS=1

        reps_for_size() {
            local size=\"\$1\"
            if [ \"\$size\" -ge 500 ]; then
                echo 2
            elif [ \"\$size\" -ge 100 ]; then
                echo 3
            else
                echo 5
            fi
        }

        run_processor_case() {
            local suite_dir=\"\$1\"
            local backend=\"\$2\"
            local dataset=\"\$3\"
            local file_size_mb=\"\$4\"
            local chunk_size_kb=\"\$5\"
            local runs=\"\$6\"
            local ray_cpus=\"\$7\"
            local ray_batch_size=\"\$8\"
            local omp_threads=\"\$9\"
            local label=\"\${10}\"

            mkdir -p \"\$suite_dir\"
            echo \"=== \${label} ===\"
            export BENCHMARK_MODE=processor
            export BENCHMARK_BACKENDS=\"\$backend\"
            export BENCHMARK_DATASETS=\"\$dataset\"
            export BENCHMARK_FILE_SIZES_MB=\"\$file_size_mb\"
            export BENCHMARK_CHUNK_SIZES_KB=\"\$chunk_size_kb\"
            export BENCHMARK_RUNS=\"\$runs\"
            export BENCHMARK_CSV_OUTPUT=\"\$suite_dir/\${label}.csv\"
            export HPC_BACKEND=\"\$backend\"
            export CHUNK_SIZE_KB=\"\$chunk_size_kb\"
            export OMP_NUM_THREADS=\"\$omp_threads\"
            export RAY_ADDRESS=local
            export RAY_LOCAL_CPUS=\"\$ray_cpus\"
            export RAY_WORKERS=\"\$ray_cpus\"
            export RAY_CHUNK_BATCH_SIZE=\"\$ray_batch_size\"
            python benchmark/run_benchmark.py
        }

        run_diagnostics() {
            local out='${RESULTS_DIR}/processor_diagnostics'
            mkdir -p \"\$out\"
            for chunk_size_kb in ${DIAG_CHUNK_SIZES_KB}; do
                run_processor_case \"\$out\" sequential '${DIAG_DATASET}' '${DIAG_FILE_SIZE_MB}' \"\$chunk_size_kb\" '${DIAG_RUNS}' 1 1 1 \
                    \"sequential_${DIAG_DATASET}_${DIAG_FILE_SIZE_MB}mb_\${chunk_size_kb}kb\"
                run_processor_case \"\$out\" numpy '${DIAG_DATASET}' '${DIAG_FILE_SIZE_MB}' \"\$chunk_size_kb\" '${DIAG_RUNS}' 1 1 1 \
                    \"numpy_${DIAG_DATASET}_${DIAG_FILE_SIZE_MB}mb_\${chunk_size_kb}kb\"
                run_processor_case \"\$out\" cython '${DIAG_DATASET}' '${DIAG_FILE_SIZE_MB}' \"\$chunk_size_kb\" '${DIAG_RUNS}' 1 1 '${CYTHON_OMP_THREADS}' \
                    \"cython_${DIAG_DATASET}_${DIAG_FILE_SIZE_MB}mb_\${chunk_size_kb}kb_omp${CYTHON_OMP_THREADS}\"
                for ray_workers in ${DIAG_RAY_WORKERS_LIST}; do
                    for ray_batch_size in ${DIAG_RAY_BATCH_SIZES}; do
                        run_processor_case \"\$out\" ray '${DIAG_DATASET}' '${DIAG_FILE_SIZE_MB}' \"\$chunk_size_kb\" '${DIAG_RUNS}' \"\$ray_workers\" \"\$ray_batch_size\" 1 \
                            \"ray_${DIAG_DATASET}_${DIAG_FILE_SIZE_MB}mb_\${chunk_size_kb}kb_rw\${ray_workers}_batch\${ray_batch_size}\"
                    done
                done
            done
        }

        run_probe() {
            local out='${RESULTS_DIR}/parallel_probe'
            mkdir -p \"\$out\"
            run_processor_case \"\$out\" sequential '${PROBE_DATASET}' '${PROBE_FILE_SIZE_MB}' '${PROBE_CHUNK_SIZE_KB}' '${PROBE_RUNS}' 1 1 1 \
                \"sequential_${PROBE_DATASET}_${PROBE_FILE_SIZE_MB}mb_1file_${PROBE_CHUNK_SIZE_KB}kb\"
            run_processor_case \"\$out\" numpy '${PROBE_DATASET}' '${PROBE_FILE_SIZE_MB}' '${PROBE_CHUNK_SIZE_KB}' '${PROBE_RUNS}' 1 1 1 \
                \"numpy_${PROBE_DATASET}_${PROBE_FILE_SIZE_MB}mb_1file_${PROBE_CHUNK_SIZE_KB}kb\"
            run_processor_case \"\$out\" cython '${PROBE_DATASET}' '${PROBE_FILE_SIZE_MB}' '${PROBE_CHUNK_SIZE_KB}' '${PROBE_RUNS}' 1 1 '${CYTHON_OMP_THREADS}' \
                \"cython_${PROBE_DATASET}_${PROBE_FILE_SIZE_MB}mb_1file_${PROBE_CHUNK_SIZE_KB}kb_omp${CYTHON_OMP_THREADS}\"
            for ray_workers in ${PROBE_RAY_WORKERS_LIST}; do
                run_processor_case \"\$out\" ray '${PROBE_DATASET}' '${PROBE_FILE_SIZE_MB}' '${PROBE_CHUNK_SIZE_KB}' '${PROBE_RUNS}' \"\$ray_workers\" '${PROBE_RAY_BATCH_SIZE}' 1 \
                    \"ray_${PROBE_DATASET}_${PROBE_FILE_SIZE_MB}mb_1file_${PROBE_CHUNK_SIZE_KB}kb_rw\${ray_workers}_batch${PROBE_RAY_BATCH_SIZE}\"
            done
        }

        run_matrix() {
            local out='${RESULTS_DIR}/aut_matrix_processor'
            mkdir -p \"\$out\"
            for dataset in ${MATRIX_DATASETS}; do
                for file_size_mb in ${MATRIX_FILE_SIZES_MB}; do
                    runs=\$(reps_for_size \"\$file_size_mb\")
                    run_processor_case \"\$out\" sequential \"\$dataset\" \"\$file_size_mb\" '${MATRIX_CHUNK_SIZE_KB}' \"\$runs\" 1 1 1 \
                        \"sequential_\${dataset}_\${file_size_mb}mb_${MATRIX_CHUNK_SIZE_KB}kb\"
                    run_processor_case \"\$out\" numpy \"\$dataset\" \"\$file_size_mb\" '${MATRIX_CHUNK_SIZE_KB}' \"\$runs\" 1 1 1 \
                        \"numpy_\${dataset}_\${file_size_mb}mb_${MATRIX_CHUNK_SIZE_KB}kb\"
                    run_processor_case \"\$out\" cython \"\$dataset\" \"\$file_size_mb\" '${MATRIX_CHUNK_SIZE_KB}' \"\$runs\" 1 1 '${CYTHON_OMP_THREADS}' \
                        \"cython_\${dataset}_\${file_size_mb}mb_${MATRIX_CHUNK_SIZE_KB}kb_omp${CYTHON_OMP_THREADS}\"
                    for ray_workers in ${MATRIX_RAY_WORKERS_LIST}; do
                        run_processor_case \"\$out\" ray \"\$dataset\" \"\$file_size_mb\" '${MATRIX_CHUNK_SIZE_KB}' \"\$runs\" \"\$ray_workers\" '${MATRIX_RAY_BATCH_SIZE}' 1 \
                            \"ray_\${dataset}_\${file_size_mb}mb_${MATRIX_CHUNK_SIZE_KB}kb_rw\${ray_workers}_batch${MATRIX_RAY_BATCH_SIZE}\"
                    done
                done
            done
        }

        case '${SUITE_MODE}' in
            all)
                run_diagnostics
                run_probe
                run_matrix
                ;;
            diagnostics)
                run_diagnostics
                ;;
            probe)
                run_probe
                ;;
            matrix)
                run_matrix
                ;;
            *)
                echo 'SUITE_MODE invalido: ${SUITE_MODE}' >&2
                exit 2
                ;;
        esac
    "

echo "Listo. Resultados en ${RESULTS_DIR}"
