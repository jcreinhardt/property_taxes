SET preserve_insertion_order=false; USE memory; SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
-- Verify the bucketed parquet written by extract.sql: rows, primaries and years per bucket.
COPY (
  SELECT bucket, count(*) n, count(*) FILTER (WHERE prim) n_prim, min(y) miny, max(y) maxy, count(DISTINCT y) nyears, count(DISTINCT bkey) nbld
  FROM read_parquet('/nfs/roberts/scratch/pi_csl53/jr2728/outmig_full/da_s/*/*.parquet', hive_partitioning = true) GROUP BY 1 ORDER BY 1
) TO '/home/jr2728/ptax_explore/hh_strat/out/outmig_full_extract_check.csv' (HEADER);
COPY (
  SELECT y, count(*) n, count(*) FILTER (WHERE prim) n_prim, max(recency_date) cutoff
  FROM read_parquet('/nfs/roberts/scratch/pi_csl53/jr2728/outmig_full/da_s/*/*.parquet') GROUP BY 1 ORDER BY 1
) TO '/home/jr2728/ptax_explore/hh_strat/out/outmig_full_extract_years.csv' (HEADER);
