#!/usr/bin/env bash
#SBATCH --job-name=dedup_matrix
#SBATCH --output=matrix_output_%j.log
#SBATCH --error=matrix_errors_%j.log
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G
#SBATCH --partition=enel  # Reemplaza por tu partición autorizada de Patagon

# Ejecutamos el orquestador de la matriz
chmod +x aut_testing_patagon.sh
./aut_testing_patagon.sh