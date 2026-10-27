#!/bin/bash
#SBATCH -J grid
#SBATCH -p default_queue
#SBATCH --cpus-per-task=16
#SBATCH --mem=100G
#SBATCH --time=04:00:00
#SBATCH -o /home/jr2728/ptax_explore/logs/grid_%j.out
D=/home/jr2728/ptax_explore/grid5k; mkdir -p $D; cd /home/jr2728/ptax_explore
{ echo "SET threads=16; SET memory_limit='80GB';"; sed -e "s#__DIR__#$D#g" h_grid_eff.sql; } > $D/h_run.sql
date; ~/.local/bin/duckdb -c ".read $D/h_run.sql"; date
