#!/bin/bash
#SBATCH --partition=day --cpus-per-task=16 --mem=320G --time=20:00:00 --job-name=r10pct
#SBATCH --chdir=/home/jr2728/ptax_explore/salt/repo --output=/home/jr2728/ptax_explore/logs/%x_%j.out
# Runs the SALT-cap R pipeline on the 10% sample on Bouchet. Usage: sbatch run_r_10pct.sbatch "01_prepare 02_descriptives s1_area_did ..."
# Code is rsynced from the laptop into ~/ptax_explore/salt/repo/code/analysis/salt_cap/R before submission.
set -uo pipefail
module load R/4.4.2-gfbf-2024a
export R_LIBS_USER=$HOME/R/4.4 OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK
export SALT_SAMPLE=bld10pct SALT_ROOT=$HOME/ptax_explore/salt/repo SALT_PANEL_DIR=$HOME/ptax_explore/salt/out10
cd $SALT_ROOT
for s in $1; do echo "=== $s $(date)"; Rscript code/analysis/salt_cap/R/$s.R; echo "=== exit $? $(date)"; done
