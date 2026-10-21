#!/bin/bash
#SBATCH -p default_queue
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=00:30:00
#SBATCH -o /home/jr2728/ptax_explore/logs/%x_%j.out
# Usage: sbatch -J NAME run_small.sbatch file.sql   (small checks; SQL attaches the DB READ_ONLY itself)
cd /home/jr2728/ptax_explore; ~/.local/bin/duckdb < "$1"
