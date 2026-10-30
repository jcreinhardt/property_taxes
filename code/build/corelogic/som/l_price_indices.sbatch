#!/bin/bash
#SBATCH -J pidx
#SBATCH -p cpunormal
#SBATCH --cpus-per-task=16
#SBATCH --mem=128G
#SBATCH --time=18:00:00
#SBATCH -o /home/jr2728/ptax_explore/logs/pidx_%j.out
# Sales-with-attributes extract (l_hedonic_sales.sql) then the three price indices (l_price_indices.R).
# sbatch --export=ALL,STEP=sql|r|all[,SAMPLE='TABLESAMPLE 0.05% (system)',OUT=~/ptax_explore/hedonic_test] l_price_indices.sbatch
STEP=${STEP:-all}; OUT=${OUT:-/home/jr2728/ptax_explore/hedonic}; SAMPLE=${SAMPLE:-}; mkdir -p "$OUT" /home/jr2728/ptax_explore/tmp
cd /home/jr2728/ptax_explore
if [ "$STEP" != "r" ]; then
  { echo "SET threads=$SLURM_CPUS_PER_TASK; SET memory_limit='$((SLURM_MEM_PER_NODE * 8 / 10 / 1024))GB';"
    sed -e "s#__OUT__#$OUT#g" -e "s#__SAMPLE__#$SAMPLE#g" l_hedonic_sales.sql; } > "$OUT/l_run.sql"
  date; ~/.local/bin/duckdb -c ".read $OUT/l_run.sql" || exit 1; date; ls -la "$OUT"
fi
if [ "$STEP" != "sql" ]; then
  module load r/4.3.2-3hmj
  export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK OPENBLAS_NUM_THREADS=1
  date; Rscript l_price_indices.R "$OUT/sales_attr.parquet" tract_leaid.csv "$OUT" || exit 1; date; ls -la "$OUT"
fi
