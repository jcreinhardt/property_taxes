#!/bin/bash
#SBATCH -J border1
#SBATCH -p default_queue
#SBATCH --cpus-per-task=16
#SBATCH --mem=120G
#SBATCH --time=04:00:00
#SBATCH -o /home/jr2728/ptax_explore/logs/border1_%j.out
cd /home/jr2728/ptax_explore
{ echo "SET threads=16; SET memory_limit='100GB';"; cat e_border_1pct.sql; } > border1/e_run.sql
date; ~/.local/bin/duckdb -c ".read border1/e_run.sql"; date
