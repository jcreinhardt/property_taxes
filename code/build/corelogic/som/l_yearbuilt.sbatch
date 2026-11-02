#!/bin/bash
#SBATCH -J yearbuilt
#SBATCH -p default_queue
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --time=04:00:00
#SBATCH -o /home/jr2728/ptax_explore/logs/yearbuilt_%j.out
# Builds ~/ptax_explore/kpanel/roll_built_YYYY.parquet (year-built counts on the residential roll) for years Y0..Y1
# (sbatch --export=ALL,Y0=2013,Y1=2024 l_yearbuilt.sbatch). Copy the files to data/built/corelogic/kpanel/ and rerun
# code/build/corelogic/10_county_tract_year_panels.R (which picks them up if present).
D=/home/jr2728/ptax_explore/kpanel; mkdir -p $D; cd /home/jr2728/ptax_explore
{ echo "SET threads=8; SET memory_limit='52GB'; SET temp_directory='/home/jr2728/ptax_explore/tmp'; SET preserve_insertion_order=false;"
  echo "ATTACH '/gpfs/project/corelogic_database/corelogic_public.db' AS cl (READ_ONLY); .timer on"
  for y in $(seq $Y0 $Y1); do sed -e "s#__DIR__#$D#g" -e "s/__Y__/$y/g" l_yearbuilt_year.sql; done; } > $D/l_run_${Y0}.sql
date; ~/.local/bin/duckdb -c ".read $D/l_run_${Y0}.sql"; date; ls -la $D
