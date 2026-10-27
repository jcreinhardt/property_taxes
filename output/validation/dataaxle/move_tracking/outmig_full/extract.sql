SET preserve_insertion_order=false; USE memory; SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
-- Stage 1 of the full-data out-/in-migration by tenure (outmig_by_tenure.sql on all buildings): one pass over dataaxle_annual
-- pulling only the columns the panel needs for street-address records with a building key, all years except the truncated 2022,
-- plus the 2022 replacement built from the quarterly files. Written to scratch as parquet bucketed by bkey % 10 so that the
-- unit-year panel (stage 2, bucket.sql) can be built one bucket at a time.
COPY (
  SELECT record_year::INTEGER y, bkey, coalesce(upper(trim(unit_num)), '') unit, family_id fid, owner_renter_status ors,
         length_of_residence lor, recency_date, primary_family_ind prim, (bkey % 10)::TINYINT bucket
  FROM (SELECT record_year, family_id, unit_num, owner_renter_status, length_of_residence, recency_date, primary_family_ind,
               hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num))) bkey
        FROM cl.dataaxle_annual
        WHERE record_year <> 2022 AND address_type = 'S'
          AND coalesce(trim(house_num),'') <> '' AND coalesce(trim(street_name),'') <> '' AND zip IS NOT NULL)
  UNION ALL
  SELECT record_year::INTEGER, bkey, coalesce(upper(trim(unit_num)), ''), family_id, owner_renter_status, length_of_residence, recency_date, primary_family_ind, (bkey % 10)::TINYINT
  FROM read_parquet('/home/jr2728/ptax_explore/samples/da_annual_2022_from_quarters/*/*.parquet', hive_partitioning = true)
  WHERE address_type = 'S' AND bkey IS NOT NULL
) TO '/nfs/roberts/scratch/pi_csl53/jr2728/outmig_full/da_s' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 1000000, PARTITION_BY (bucket), OVERWRITE_OR_IGNORE true);
SELECT bucket, count(*) n, count(*) FILTER (WHERE prim) n_prim, min(y), max(y), count(DISTINCT y) nyears
FROM read_parquet('/nfs/roberts/scratch/pi_csl53/jr2728/outmig_full/da_s/*/*.parquet', hive_partitioning = true) GROUP BY 1 ORDER BY 1;
