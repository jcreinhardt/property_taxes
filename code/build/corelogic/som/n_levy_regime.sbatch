#!/bin/bash
#SBATCH -J levyreg
#SBATCH -p default_queue
#SBATCH --cpus-per-task=16
#SBATCH --mem=120G
#SBATCH --time=04:00:00
#SBATCH -o /home/jr2728/ptax_explore/logs/levyreg_%j.out
# Builds $D/levy_area_YYYY.parquet for Y0..Y1 (sbatch --export=ALL,Y0=..,Y1=..[,TAG=_test,FIPS="36059 48201"]).
# The first year only seeds the same-parcel join (its stable-parcel columns are empty).
FILTER=""; [ -n "$FIPS" ] && FILTER="AND fips_code IN ($(echo $FIPS | sed "s/\([0-9]*\)/'\1'/g; s/ /, /g"))"
D=/home/jr2728/ptax_explore/levyreg${TAG}; mkdir -p $D; cd /home/jr2728/ptax_explore
{ echo "SET threads=$SLURM_CPUS_PER_TASK; SET memory_limit='$((SLURM_MEM_PER_NODE * 8 / 10240))GB';"; cat n_levy_regime_head.sql
  for y in $(seq $Y0 $Y1); do sed -e "s#__DIR__#$D#g" -e "s/__Y__/$y/g" -e "s/__FILTER__/$FILTER/" n_levy_regime_year.sql; done; } > $D/run.sql
date; ~/.local/bin/duckdb -c ".read $D/run.sql"; date; ls -la $D
