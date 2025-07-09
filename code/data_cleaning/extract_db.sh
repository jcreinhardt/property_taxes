#!/bin/bash
#SBATCH --job-name=extract_taxes
#SBATCH --output=extract_taxes_%j.out
#SBATCH --error=extract_taxes_%j.err
#SBATCH --time=01:00:00
#SBATCH --mem=400G

# Run the SQL extraction
duckdb < extract_taxes.sql