#!/bin/bash
#SBATCH -J rate16
#SBATCH -p cpunormal
#SBATCH --cpus-per-task=32
#SBATCH --mem=150G
#SBATCH -o /home/jr2728/ptax_explore/logs/rate16_%j.out
# Full roll: A extract (2016 parcels, sales ratio, fit sample), then B layer assignment (TIGER/Line 2016 point-in-polygon).
set -eu
D=/home/jr2728/ptax_explore/full; mkdir -p $D; cd /home/jr2728/ptax_explore
{ echo "SET threads=32; SET memory_limit='130GB';"; sed -e 's/__SAMPLE__//' -e "s#__OUT__#$D#g" a_extract16.sql; } > $D/a_full.sql
date; ~/.local/bin/duckdb -c ".read $D/a_full.sql"
date
./b_layers.sh $D
date
