#!/bin/bash
#
#
#SBATCH --account=lapoint # The account name for the job.
#SBATCH --job-name=owner # The job name.
#SBATCH --partition=bigmem
#SBATCH --time=00:20:00 # The time the job will take to run.
#SBATCH --nodes=1
#SBATCH --ntasks=1 
#SBATCH --cpus-per-task=40
#SBATCH --mem=3900G

# Run the extraction
duckdb < extract_taxes.sql