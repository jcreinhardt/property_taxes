SET threads=4;
-- Step 2a: profile the situs fields per (fips, year) for the 4 deep-dive county-years, and CLIP overlap across years.
CREATE OR REPLACE TABLE e AS SELECT * FROM read_parquet(['/home/jr2728/ptax_explore/hh_strat/out/odd_cy_25017.parquet','/home/jr2728/ptax_explore/hh_strat/out/odd_cy_33015.parquet','/home/jr2728/ptax_explore/hh_strat/out/odd_cy_23005.parquet','/home/jr2728/ptax_explore/hh_strat/out/odd_cy_9011.parquet']);
.mode box
SELECT fips, yr, count(*) n, count(DISTINCT clip) n_clip, count(DISTINCT apn_parcel_number_unformatted) n_apn,
       round(100*avg((length(situs_zip_code)>=5)::INT),1) p_zip5, round(100*avg((situs_zip_code IS NULL)::INT),1) p_zip_null,
       round(100*avg((coalesce(trim(situs_house_number),'')<>'')::INT),1) p_hn,
       round(100*avg((coalesce(trim(situs_street_name),'')<>'')::INT),1) p_str,
       round(100*avg((coalesce(trim(situs_mode),'')<>'')::INT),1) p_mode,
       round(100*avg((coalesce(trim(situs_direction),'')<>'')::INT),1) p_dir,
       round(100*avg((coalesce(trim(situs_unit_number),'')<>'')::INT),1) p_unit,
       round(100*avg((coalesce(trim(situs_street_address),'')<>'')::INT),1) p_stradr,
       round(100*avg((coalesce(trim(situs_city),'')<>'')::INT),1) p_city,
       round(100*avg((coalesce(trim(situs_house_number_suffix),'')<>'')::INT),1) p_hnsuf,
       round(100*avg((coalesce(trim(situs_house_number_2),'')<>'')::INT),1) p_hn2,
       round(100*avg(regexp_matches(trim(situs_house_number),'^[0-9]+$')::INT),1) p_hn_numeric,
       round(100*avg((trim(situs_mode)<>'' AND upper(trim(situs_street_name)) LIKE '% '||upper(trim(situs_mode)))::INT),1) p_str_endswith_mode,
       round(100*avg((pic IN (10,11,21,22))::INT),1) p_res,
       count(DISTINCT left(situs_zip_code,5)) n_zip, count(DISTINCT upper(trim(situs_city))) n_city,
       any_value(situs_city_state_zip_source) src_ex, count(DISTINCT situs_city_state_zip_source) n_src
FROM e GROUP BY ALL ORDER BY ALL;
-- source of city/state/zip by year
SELECT fips, yr, situs_city_state_zip_source src, count(*) n FROM e GROUP BY ALL ORDER BY fips, yr, n DESC;
-- CLIP overlap: y-1 -> y, y-1 -> y+1, y -> y+1 (deep-dive pairs)
CREATE OR REPLACE TABLE dd AS SELECT * FROM (VALUES (25017,2014),(33015,2014),(23005,2014),(9011,2015)) v(fips, y);
WITH c AS (SELECT DISTINCT fips, yr, clip FROM e WHERE clip IS NOT NULL)
SELECT d.fips, d.y,
  (SELECT count(*) FROM c WHERE c.fips=d.fips AND c.yr=d.y-1) clips_prev,
  (SELECT count(*) FROM c WHERE c.fips=d.fips AND c.yr=d.y) clips_y,
  (SELECT count(*) FROM c WHERE c.fips=d.fips AND c.yr=d.y+1) clips_next,
  (SELECT count(*) FROM c a JOIN c b ON a.clip=b.clip AND a.fips=b.fips AND a.yr=d.y-1 AND b.yr=d.y WHERE a.fips=d.fips) prev_in_y,
  (SELECT count(*) FROM c a JOIN c b ON a.clip=b.clip AND a.fips=b.fips AND a.yr=d.y-1 AND b.yr=d.y+1 WHERE a.fips=d.fips) prev_in_next,
  (SELECT count(*) FROM c a JOIN c b ON a.clip=b.clip AND a.fips=b.fips AND a.yr=d.y AND b.yr=d.y+1 WHERE a.fips=d.fips) y_in_next
FROM dd d ORDER BY 1;
-- Same by APN (unformatted)
WITH c AS (SELECT DISTINCT fips, yr, apn_parcel_number_unformatted apn FROM e WHERE coalesce(apn_parcel_number_unformatted,'')<>'')
SELECT d.fips, d.y,
  (SELECT count(*) FROM c WHERE c.fips=d.fips AND c.yr=d.y-1) apns_prev,
  (SELECT count(*) FROM c a JOIN c b ON a.apn=b.apn AND a.fips=b.fips AND a.yr=d.y-1 AND b.yr=d.y WHERE a.fips=d.fips) prev_in_y,
  (SELECT count(*) FROM c a JOIN c b ON a.apn=b.apn AND a.fips=b.fips AND a.yr=d.y-1 AND b.yr=d.y+1 WHERE a.fips=d.fips) prev_in_next
FROM dd d ORDER BY 1;
-- Distribution of cities in y vs y-1 (top 8 each) for the deep-dive pairs
SELECT fips, yr, upper(trim(situs_city)) city, count(*) n FROM e GROUP BY ALL QUALIFY row_number() OVER (PARTITION BY fips, yr ORDER BY count(*) DESC) <= 8 ORDER BY fips, yr, n DESC;
