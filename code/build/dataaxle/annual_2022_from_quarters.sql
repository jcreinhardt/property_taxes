SET preserve_insertion_order=false; USE memory; SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
-- Replacement for the truncated annual 2022 Data Axle file: union of the four 2022 quarterly files, one row per family_id
-- (quarters never disagree for the same family; the earliest quarter's row is kept). record_year = 2022 added.
COPY (
  SELECT * EXCLUDE (fm, rn), 2022::USMALLINT AS record_year FROM (
    SELECT *, row_number() OVER (PARTITION BY family_id ORDER BY fm) rn
    FROM read_parquet('/nfs/roberts/scratch/pi_csl53/jr2728/da_quarterly/fy=2022/fm=*/*/*.parquet', hive_partitioning = true)
  ) WHERE rn = 1
) TO '/nfs/roberts/scratch/pi_csl53/jr2728/da_quarterly/annual_2022_from_quarters' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 1000000, PARTITION_BY (state), OVERWRITE_OR_IGNORE true);
SELECT count(*) n, count(DISTINCT family_id) fam, min(recency_date), max(recency_date), avg(primary_family_ind::INT) prim, count(DISTINCT state) states
FROM read_parquet('/nfs/roberts/scratch/pi_csl53/jr2728/da_quarterly/annual_2022_from_quarters/*/*.parquet', hive_partitioning = true);
