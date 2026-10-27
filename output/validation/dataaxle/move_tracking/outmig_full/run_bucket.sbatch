#!/bin/bash
#SBATCH --partition=day
#SBATCH --chdir=/home/jr2728/ptax_explore
#SBATCH --output=/home/jr2728/ptax_explore/logs/%x_%A_%a.out
# Array version of run_sql.sbatch: task i runs hh_strat/outmig_full/bucket_<i>.sql (generated from bucket.sql by sed).
# corelogic.db is ATTACHed READ_ONLY (unused here, the buckets read scratch parquet); nothing is written under /nfs/roberts/pi/.
set -euo pipefail
DB=/nfs/roberts/pi/pi_csl53/corelogic/database/corelogic.db
SQL=hh_strat/outmig_full/bucket_${SLURM_ARRAY_TASK_ID}.sql
MEMLIM=$(( ${SLURM_MEM_PER_NODE:-65536} * 8 / 10 / 1024 ))GB
echo "host=$(hostname) start=$(date -Is) cpus=${SLURM_CPUS_PER_TASK} mem=${SLURM_MEM_PER_NODE:-?}M memory_limit=$MEMLIM sql=$SQL"
{ echo "SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp'; SET threads=${SLURM_CPUS_PER_TASK}; SET memory_limit='$MEMLIM';"
  echo "ATTACH '$DB' AS cl (READ_ONLY); USE cl;"
  echo ".timer on"
  cat "$SQL"; } | ~/bin/duckdb
echo "end=$(date -Is)"
