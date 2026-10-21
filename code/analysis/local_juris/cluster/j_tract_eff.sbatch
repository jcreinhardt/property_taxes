#!/bin/bash
#SBATCH -J tract
#SBATCH -p gpunormal
#SBATCH --cpus-per-task=16
#SBATCH --mem=100G
#SBATCH --time=04:00:00
#SBATCH -o /home/jr2728/ptax_explore/logs/tract_%j.out
D=/home/jr2728/ptax_explore/tract; mkdir -p $D; cd /home/jr2728/ptax_explore
{ echo "SET threads=16; SET memory_limit='80GB';"; sed -e "s#__DIR__#$D#g" j_tract_eff.sql; } > $D/j_run.sql
date; ~/.local/bin/duckdb -c ".read $D/j_run.sql"; date
