#!/bin/bash
#SBATCH -J kpanel
#SBATCH -p default_queue
#SBATCH --cpus-per-task=16
#SBATCH --mem=100G
#SBATCH --time=04:00:00
#SBATCH -o /home/jr2728/ptax_explore/logs/kpanel_%j.out
# Builds ~/ptax_explore/kpanel/{roll_tract,roll_area,sales}_YYYY.parquet for years Y0..Y1 (sbatch --export=ALL,Y0=..,Y1=..).
D=/home/jr2728/ptax_explore/kpanel; mkdir -p $D; cd /home/jr2728/ptax_explore
{ echo "SET threads=16; SET memory_limit='85GB';"; cat k_panel_head.sql
  for y in $(seq $Y0 $Y1); do sed -e "s#__DIR__#$D#g" -e "s/__Y__/$y/g" k_panel_year.sql; done; } > $D/k_run_${Y0}.sql
date; ~/.local/bin/duckdb -c ".read $D/k_run_${Y0}.sql"; date; ls -la $D
