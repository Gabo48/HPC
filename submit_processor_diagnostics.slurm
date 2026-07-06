#!/bin/bash
#SBATCH --job-name=hpc_proc_diag
#SBATCH --partition=cpu
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=04:00:00
#SBATCH --output=slurm_processor_diag_%j.out
#SBATCH --error=slurm_processor_diag_%j.err

set -euo pipefail

# Ajustables al enviar:
#   sbatch --export=ALL,FILE_SIZE_MB=500,RUNS=5 submit_processor_diagnostics.slurm
DATASET="${DATASET:-random}"
FILE_SIZE_MB="${FILE_SIZE_MB:-200}"
RUNS="${RUNS:-3}"
CHUNK_SIZES_KB="${CHUNK_SIZES_KB:-1024 4096 8192}"
CYTHON_OMP_THREADS="${CYTHON_OMP_THREADS:-8}"
RAY_WORKERS_LIST="${RAY_WORKERS_LIST:-4 8 16}"
RAY_BATCH_SIZES="${RAY_BATCH_SIZES:-8 16 32}"

RESULTS_DIR="${PWD}/results/patagon_processor_diagnostics_${SLURM_JOB_ID}"
mkdir -p "$RESULTS_DIR"

echo "Job ${SLURM_JOB_ID}"
echo "PWD=${PWD}"
echo "Resultados=${RESULTS_DIR}"
echo "DATASET=${DATASET} FILE_SIZE_MB=${FILE_SIZE_MB} RUNS=${RUNS}"
echo "SLURM_CPUS_ON_NODE=${SLURM_CPUS_ON_NODE:-unknown}"

run_case() {
    local backend="$1"
    local chunk_size_kb="$2"
    local ray_workers="$3"
    local ray_batch_size="$4"
    local omp_threads="$5"
    local label="$6"
    local output_csv="${RESULTS_DIR}/${label}.csv"

    echo "=== ${label} ==="
    srun \
        --container-image=rayproject/ray:2.10.0-py311 \
        --container-name=hpc-ray-benchmark \
        --container-workdir="${PWD}" \
        bash -lc "
            set -euo pipefail
            pip install --user -r requirements.benchmark.txt
            cd processor/v3_cython
            python setup.py build_ext --inplace
            cd ../..
            export PYTHONPATH=\"${PWD}\"
            export BENCHMARK_MODE=processor
            export BENCHMARK_BACKENDS=${backend}
            export BENCHMARK_DATASETS=${DATASET}
            export BENCHMARK_FILE_SIZES_MB=${FILE_SIZE_MB}
            export BENCHMARK_CHUNK_SIZES_KB=${chunk_size_kb}
            export BENCHMARK_RUNS=${RUNS}
            export BENCHMARK_CSV_OUTPUT=${output_csv}
            export HPC_BACKEND=${backend}
            export CHUNK_SIZE_KB=${chunk_size_kb}
            export OMP_NUM_THREADS=${omp_threads}
            export OPENBLAS_NUM_THREADS=1
            export MKL_NUM_THREADS=1
            export NUMEXPR_NUM_THREADS=1
            export RAY_ADDRESS=local
            export RAY_LOCAL_CPUS=${ray_workers}
            export RAY_WORKERS=${ray_workers}
            export RAY_CHUNK_BATCH_SIZE=${ray_batch_size}
            python benchmark/run_benchmark.py
        "
}

for chunk_size_kb in ${CHUNK_SIZES_KB}; do
    run_case "sequential" "$chunk_size_kb" 1 1 1 \
        "sequential_${DATASET}_${FILE_SIZE_MB}mb_${chunk_size_kb}kb"

    run_case "numpy" "$chunk_size_kb" 1 1 1 \
        "numpy_${DATASET}_${FILE_SIZE_MB}mb_${chunk_size_kb}kb"

    run_case "cython" "$chunk_size_kb" 1 1 "$CYTHON_OMP_THREADS" \
        "cython_${DATASET}_${FILE_SIZE_MB}mb_${chunk_size_kb}kb_omp${CYTHON_OMP_THREADS}"

    for ray_workers in ${RAY_WORKERS_LIST}; do
        for ray_batch_size in ${RAY_BATCH_SIZES}; do
            run_case "ray" "$chunk_size_kb" "$ray_workers" "$ray_batch_size" 1 \
                "ray_${DATASET}_${FILE_SIZE_MB}mb_${chunk_size_kb}kb_rw${ray_workers}_batch${ray_batch_size}"
        done
    done
done

echo "Listo. CSV en ${RESULTS_DIR}"
