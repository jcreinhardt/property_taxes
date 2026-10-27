SET threads=4;
-- Step 2b (revised): the odd year y holds a disjoint set of parcels from y-1. Check whether that split is by town (situs_city),
-- what the tax-year dates say, whether the raw address key matches at all, and why rows > distinct CLIPs in year y.
CREATE OR REPLACE TABLE e AS SELECT *, upper(trim(situs_city)) city, left(situs_zip_code,5) z,
  CASE WHEN coalesce(trim(situs_house_number),'')<>'' AND coalesce(trim(situs_street_name),'')<>'' AND length(situs_zip_code)>=5
       THEN hash(left(situs_zip_code,5), upper(trim(situs_street_name)), upper(trim(situs_house_number))) END bkey
  FROM read_parquet(['/home/jr2728/ptax_explore/hh_strat/out/odd_cy_25017.parquet','/home/jr2728/ptax_explore/hh_strat/out/odd_cy_33015.parquet','/home/jr2728/ptax_explore/hh_strat/out/odd_cy_23005.parquet','/home/jr2728/ptax_explore/hh_strat/out/odd_cy_9011.parquet']);
CREATE OR REPLACE TABLE dd AS SELECT * FROM (VALUES (25017,2014),(33015,2014),(23005,2014),(9011,2015)) v(fips, y);
.mode box
-- town x year matrix: number of rows per town in y-1, y, y+1
CREATE OR REPLACE TABLE tw AS
  SELECT d.fips, d.y, e.city, count(*) FILTER (WHERE yr=d.y-1) n_prev, count(*) FILTER (WHERE yr=d.y) n_y, count(*) FILTER (WHERE yr=d.y+1) n_next
  FROM e JOIN dd d ON e.fips=d.fips AND e.yr BETWEEN d.y-1 AND d.y+1 GROUP BY ALL;
SELECT fips, y, count(*) towns, count(*) FILTER (WHERE n_prev>0) towns_prev, count(*) FILTER (WHERE n_y>0) towns_y, count(*) FILTER (WHERE n_next>0) towns_next,
       count(*) FILTER (WHERE n_prev>0 AND n_y>0) towns_prev_and_y, count(*) FILTER (WHERE n_prev>0 AND n_y>0 AND least(n_prev,n_y) > 0.2*greatest(n_prev,n_y)) towns_both_substantial,
       count(*) FILTER (WHERE n_prev>0 AND n_next>0) towns_prev_and_next
FROM tw GROUP BY ALL ORDER BY ALL;
SELECT * FROM tw WHERE fips=25017 ORDER BY n_next DESC LIMIT 40;
SELECT * FROM tw WHERE fips=9011 ORDER BY n_next DESC LIMIT 25;
-- tax-year dates by year
SELECT fips, yr, beginning_tax_year_date b, ending_tax_year_date en, count(*) n FROM e GROUP BY ALL ORDER BY fips, yr, n DESC;
-- raw address-key match y-1 -> y and y-1 -> y+1, residential parcels with an address
WITH k AS (SELECT DISTINCT fips, yr, bkey FROM e WHERE bkey IS NOT NULL AND pic IN (10,11,21,22))
SELECT d.fips, d.y, count(*) n_prev_keys,
  round(100*avg((EXISTS (SELECT 1 FROM k b WHERE b.fips=d.fips AND b.yr=d.y AND b.bkey=a.bkey))::INT),2) p_match_y,
  round(100*avg((EXISTS (SELECT 1 FROM k b WHERE b.fips=d.fips AND b.yr=d.y+1 AND b.bkey=a.bkey))::INT),2) p_match_next
FROM k a JOIN dd d ON a.fips=d.fips AND a.yr=d.y-1 GROUP BY ALL ORDER BY ALL;
-- rows per clip in each year
SELECT fips, yr, count(*) n_rows, count(DISTINCT clip) n_clip, count(*) FILTER (WHERE clip IS NULL) clip_null,
       count(DISTINCT apn_sequence_number) n_seq, max(cnt) max_rows_per_clip
FROM (SELECT *, count(*) OVER (PARTITION BY fips, yr, clip) cnt FROM e) GROUP BY ALL ORDER BY ALL;
-- example of a multi-row clip in year y (situs fields only)
SELECT fips, yr, apn_sequence_number, pic, situs_house_number, situs_street_name, situs_mode, situs_unit_number, city, z, beginning_tax_year_date
FROM e WHERE (fips, yr, clip) = (SELECT (fips, yr, clip) FROM e WHERE fips=9011 AND yr=2014 GROUP BY ALL HAVING count(*) > 1 ORDER BY count(*) DESC LIMIT 1) LIMIT 10;
