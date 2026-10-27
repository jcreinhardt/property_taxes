SET preserve_insertion_order=false; USE memory; SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
-- Test version of extract.sql on the 1% building sample (must reproduce outmig_by_tenure.csv exactly).
COPY (
  SELECT record_year::INTEGER y, bkey, coalesce(upper(trim(unit_num)), '') unit, family_id fid, owner_renter_status ors,
         length_of_residence lor, recency_date, primary_family_ind prim, ((bkey // 100) % 10)::TINYINT bucket  -- sample is bkey % 100 = 0, so bucket on the next digit
  FROM read_parquet('/home/jr2728/ptax_explore/samples/da_bld1pct.parquet') WHERE address_type = 'S' AND bkey IS NOT NULL AND record_year <> 2022
  UNION ALL
  SELECT record_year::INTEGER, bkey, coalesce(upper(trim(unit_num)), ''), family_id, owner_renter_status, length_of_residence, recency_date, primary_family_ind, ((bkey // 100) % 10)::TINYINT
  FROM read_parquet('/home/jr2728/ptax_explore/samples/da_annual_2022_from_quarters/*/*.parquet', hive_partitioning = true)
  WHERE address_type = 'S' AND bkey IS NOT NULL AND bkey % 100 = 0
) TO '/nfs/roberts/scratch/pi_csl53/jr2728/outmig_full/test_da_s' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 1000000, PARTITION_BY (bucket), OVERWRITE_OR_IGNORE true);
SELECT bucket, count(*) n, count(*) FILTER (WHERE prim) n_prim, min(y), max(y), count(DISTINCT y) nyears
FROM read_parquet('/nfs/roberts/scratch/pi_csl53/jr2728/outmig_full/test_da_s/*/*.parquet', hive_partitioning = true) GROUP BY 1 ORDER BY 1;
