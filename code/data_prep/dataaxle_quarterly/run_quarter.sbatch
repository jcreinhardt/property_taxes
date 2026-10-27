#!/bin/bash
#SBATCH --partition=bigmem,day
#SBATCH --cpus-per-task=16
#SBATCH --mem=256G
#SBATCH --time=06:00:00
#SBATCH --chdir=/home/jr2728/ptax_explore
#SBATCH --output=/home/jr2728/ptax_explore/logs/%x_%j.out
# Usage: sbatch --job-name=daq_2019_06 run_quarter.sbatch hh_strat/convert_2019_06.sql
# Converts one quarterly Data Axle gzip CSV to parquet on scratch. Nothing is written under /nfs/roberts/pi/.
set -euo pipefail
OUTDIR=$(grep -o "/nfs/roberts/scratch/pi_csl53/jr2728/da_quarterly/fy=[0-9]*/fm=[0-9]*" "$1" | head -1); mkdir -p "$OUTDIR" /nfs/roberts/scratch/pi_csl53/jr2728/da_quarterly/raw /nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp
MEMLIM=$(( ${SLURM_MEM_PER_NODE:-65536} * 8 / 10 / 1024 ))GB
echo "host=$(hostname) start=$(date -Is) cpus=${SLURM_CPUS_PER_TASK} mem=${SLURM_MEM_PER_NODE:-?}M memory_limit=$MEMLIM sql=$1"
{ echo "SET threads=${SLURM_CPUS_PER_TASK}; SET memory_limit='$MEMLIM';"; cat "$1"; } | ~/bin/duckdb-1.5.6
echo "end=$(date -Is)"
ls -la /nfs/roberts/scratch/pi_csl53/jr2728/da_quarterly/raw /nfs/roberts/scratch/pi_csl53/jr2728/da_quarterly/fy=*/fm=*/
