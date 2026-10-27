SET preserve_insertion_order=false; USE memory; SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
-- Does the quarterly 2022 data replace the truncated annual 2022 file? 1% of family_ids.
CREATE OR REPLACE TABLE q AS
  SELECT file_month fm, family_id, bkey, recency_date, length_of_residence lor, primary_family_ind prim, household_status hs, state
  FROM read_parquet('/nfs/roberts/scratch/pi_csl53/jr2728/da_quarterly/fy=2022/fm=*/*/*.parquet', hive_partitioning=true)
  WHERE hash(family_id) % 100 = 0;
CREATE OR REPLACE TABLE a AS
  SELECT record_year, family_id, bkey, recency_date, length_of_residence lor, primary_family_ind prim, household_status hs, state
  FROM read_parquet('/home/jr2728/ptax_explore/samples/da_fam1pct.parquet') WHERE record_year IN (2021, 2022, 2023);
.print == rows: annual 2021/2022/2023 vs 2022 quarters and their union
SELECT 'annual ' || record_year src, count(*) n FROM a GROUP BY 1
UNION ALL SELECT 'q2022 fm=' || fm, count(*) FROM q GROUP BY 1
UNION ALL SELECT 'q2022 union of families', count(DISTINCT family_id) FROM q ORDER BY 1;
.print == is the truncated annual 2022 a subset of the quarters, with identical values?
SELECT count(*) annual22_rows, round(avg((u.family_id IS NOT NULL)::INT),4) in_some_quarter,
       round(avg((u.family_id IS NOT NULL AND a.bkey IS NOT DISTINCT FROM u.bkey AND a.recency_date = u.recency_date AND a.lor = u.lor AND a.prim = u.prim)::INT),4) identical
FROM (SELECT * FROM a WHERE record_year = 2022) a
LEFT JOIN (SELECT family_id, any_value(bkey) bkey, any_value(recency_date) recency_date, any_value(lor) lor, any_value(prim) prim FROM q GROUP BY 1) u USING (family_id);
.print == do the quarters agree with each other for the same family (2022)?
WITH p AS (SELECT a.family_id, a.bkey b0, b.bkey b1, a.recency_date r0, b.recency_date r1, a.lor l0, b.lor l1 FROM q a JOIN q b ON a.family_id = b.family_id AND b.fm = a.fm + 3)
SELECT count(*) n, round(avg((b0 IS DISTINCT FROM b1 OR r0 <> r1 OR l0 <> l1)::INT),5) any_change FROM p;
.print == continuity: annual-2021 families found in annual 2022 vs in 2022 quarters; 2022-quarter families found in annual 2023
SELECT count(*) fam21, round(avg((a22.family_id IS NOT NULL)::INT),4) in_annual22, round(avg((u.family_id IS NOT NULL)::INT),4) in_q22
FROM (SELECT family_id FROM a WHERE record_year = 2021) a21
LEFT JOIN (SELECT family_id FROM a WHERE record_year = 2022) a22 USING (family_id)
LEFT JOIN (SELECT DISTINCT family_id FROM q) u USING (family_id);
SELECT count(*) fam_q22, round(avg((a23.family_id IS NOT NULL)::INT),4) in_annual23
FROM (SELECT DISTINCT family_id FROM q) u LEFT JOIN (SELECT family_id FROM a WHERE record_year = 2023) a23 USING (family_id);
.print == state coverage of the 2022 quarters vs annual 2021 (rows per state, June 2022 / annual 2021)
SELECT state, n21, n22q6, round(n22q6::DOUBLE / n21, 2) ratio FROM
 (SELECT state, count(*) n21 FROM a WHERE record_year = 2021 GROUP BY 1) x JOIN (SELECT state, count(*) n22q6 FROM q WHERE fm = 6 GROUP BY 1) y USING (state)
ORDER BY ratio LIMIT 8;
