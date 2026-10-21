#!/bin/bash
#SBATCH -J bsample
#SBATCH -p default_queue
#SBATCH --cpus-per-task=16
#SBATCH --mem=100G
#SBATCH --time=04:00:00
#SBATCH -o /home/jr2728/ptax_explore/logs/bsample_%j.out
D=/home/jr2728/ptax_explore/bsample1024; mkdir -p $D; cd /home/jr2728/ptax_explore
{ echo "SET threads=16; SET memory_limit='80GB';"; sed -e "s#__DIR__#$D#g" -e "s#__P16__#/home/jr2728/ptax_explore/full/parcels16.parquet#" -e "s/__Y0__/2010/g" -e "s/__Y1__/2024/g" f_border_sample.sql; } > $D/f_run.sql
date; ~/.local/bin/duckdb -c ".read $D/f_run.sql"; date
