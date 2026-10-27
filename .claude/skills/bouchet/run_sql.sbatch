#!/bin/bash
#SBATCH --partition=day
#SBATCH --cpus-per-task=4
#SBATCH --mem=64G
#SBATCH --chdir=/home/jr2728/ptax_explore
#SBATCH --output=/home/jr2728/ptax_explore/logs/%x_%j.out
# Usage: sbatch $(~/ptax_explore/jobsize.sh HH:MM:SS) --job-name=NAME --time=HH:MM:SS run_sql.sbatch file.sql
# (jobsize.sh picks the largest allocation that starts now, max 16 cores / 320G; default 4 cores / 64G.)
# corelogic.db is ATTACHed READ_ONLY into an in-memory DuckDB session; temp/spill goes to ~/ptax_explore/tmp.
# DuckDB threads = allocated CPUs, memory_limit = 80% of allocated memory.
# Nothing is written under /nfs/roberts/pi/.
set -euo pipefail
DB=/nfs/roberts/pi/pi_csl53/corelogic/database/corelogic.db
MEMLIM=$(( ${SLURM_MEM_PER_NODE:-65536} * 8 / 10 / 1024 ))GB
echo "host=$(hostname) start=$(date -Is) cpus=${SLURM_CPUS_PER_TASK} mem=${SLURM_MEM_PER_NODE:-?}M memory_limit=$MEMLIM sql=$1"
{ echo "SET temp_directory='$HOME/ptax_explore/tmp'; SET threads=${SLURM_CPUS_PER_TASK}; SET memory_limit='$MEMLIM';"
  echo "ATTACH '$DB' AS cl (READ_ONLY); USE cl;"
  echo ".timer on"
  cat "$1"; } | ~/bin/duckdb
echo "end=$(date -Is)"
