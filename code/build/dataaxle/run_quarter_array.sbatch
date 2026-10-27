#!/bin/bash
#SBATCH --partition=bigmem,day
#SBATCH --cpus-per-task=16
#SBATCH --mem=256G
#SBATCH --time=04:00:00
#SBATCH --array=1-50%8
#SBATCH --job-name=daq
#SBATCH --chdir=/home/jr2728/ptax_explore
#SBATCH --output=/home/jr2728/ptax_explore/logs/%x_%A_%a.out
# Converts quarterly Data Axle gzip CSVs to typed parquet on scratch, one array task per file (line N of quarters.txt).
# Usage: sbatch run_quarter_array.sbatch            (SQL files pre-generated in hh_strat/daq/convert_YYYY_MM.sql)
# Deletes the raw intermediate parquet when the typed summary exists. Nothing is written under /nfs/roberts/pi/.
set -euo pipefail
LINE=$(sed -n "${SLURM_ARRAY_TASK_ID}p" /home/jr2728/ptax_explore/quarters.txt)
Y=${LINE% *}; M=${LINE#* }
case $M in March) MM=03;; June) MM=06;; September) MM=09;; December) MM=12;; esac
SQL=/home/jr2728/ptax_explore/hh_strat/daq/convert_${Y}_${MM}.sql
OUT=/nfs/roberts/scratch/pi_csl53/jr2728/da_quarterly
mkdir -p "$OUT/fy=$Y/fm=$MM" "$OUT/raw" "$OUT/../duckdb_tmp"
MEMLIM=$(( ${SLURM_MEM_PER_NODE:-65536} * 8 / 10 / 1024 ))GB
echo "host=$(hostname) start=$(date -Is) cpus=${SLURM_CPUS_PER_TASK} mem=${SLURM_MEM_PER_NODE:-?}M memory_limit=$MEMLIM file=$Y $M sql=$SQL"
{ echo "SET threads=${SLURM_CPUS_PER_TASK}; SET memory_limit='$MEMLIM';"; cat "$SQL"; } | ~/bin/duckdb-1.5.6
SUMMARY=/home/jr2728/ptax_explore/hh_strat/out/daq_${Y}_${MM}_typed_summary.csv
if [ -s "$SUMMARY" ] && [ "$(wc -l < "$SUMMARY")" -eq 2 ]; then
  rm -f "$OUT/raw/da_${Y}_${MM}_raw.parquet"; echo "raw parquet removed"
else
  echo "WARNING: typed summary missing, raw parquet kept"
fi
echo "end=$(date -Is)"; du -sh "$OUT/fy=$Y/fm=$MM"
