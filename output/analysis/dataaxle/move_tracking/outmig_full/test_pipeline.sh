#!/bin/bash
# Runs the whole pipeline on the 1% building sample inside one srun (minutes): extract_test -> 10 buckets -> agg; the result
# outmig_full_test_by_tenure.csv must equal outmig_by_tenure.csv (built directly by outmig_by_tenure.sql).
set -euo pipefail
cd ~/ptax_explore/hh_strat/outmig_full
mkdir -p /nfs/roberts/scratch/pi_csl53/jr2728/outmig_full /home/jr2728/ptax_explore/hh_strat/out/outmig_full_test_parts
ROOT=/nfs/roberts/scratch/pi_csl53/jr2728/outmig_full/test_da_s
PARTS=/home/jr2728/ptax_explore/hh_strat/out/outmig_full_test_parts
PRE="SET threads=${SLURM_CPUS_PER_TASK:-4}; SET memory_limit='24GB';"
{ echo "$PRE"; cat extract_test.sql; } | ~/bin/duckdb
for b in 0 1 2 3 4 5 6 7 8 9; do
  sed -e "s#__B__#$b#g" -e "s#__ROOT__#$ROOT#g" -e "s#__PARTS__#$PARTS#g" bucket.sql > bucket_test_$b.sql
  { echo "$PRE"; cat bucket_test_$b.sql; } | ~/bin/duckdb
done
sed -e "s#__PARTS__#$PARTS#g" -e "s#__PREFIX__#outmig_full_test#g" agg.sql > agg_test.sql
{ echo "$PRE"; cat agg_test.sql; } | ~/bin/duckdb
echo "test pipeline done $(date -Is)"
