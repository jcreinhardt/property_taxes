SET threads=4;
-- Step 3: for ALL 44 odd county-years, share of (y-1) residential parcels (pic 10/11/21/22, with situs address) that match a
-- year-y row by (a) raw key, (b) normalised keys, (c) CLIP, and the same for the control pair y-1 -> y+1.
-- Also: share of (y-1) parcels whose town (situs_city) has essentially no roll in year y (town-level delivery gap).
CREATE OR REPLACE TABLE odd AS SELECT fips::INT fips, y::INT y FROM read_csv('/home/jr2728/ptax_explore/hh_strat/odd_county_years.csv');
CREATE OR REPLACE TABLE e AS
  SELECT fips, yr, clip, pic, upper(trim(situs_city)) city,
         left(situs_zip_code,5) z5, upper(trim(situs_street_name)) s, upper(trim(situs_house_number)) h,
         -- normalisation: numeric prefix of the house number; street name with a trailing suffix token (if equal to situs_mode) removed
         regexp_extract(upper(trim(situs_house_number)), '^[0-9]+') hn,
         regexp_replace(upper(trim(situs_street_name)), '\s+' || coalesce(upper(trim(situs_mode)),'§') || '$', '') s_nomode,
         coalesce(trim(situs_house_number),'')<>'' AND coalesce(trim(situs_street_name),'')<>'' AND length(situs_zip_code)>=5 has_addr
  FROM read_parquet('/home/jr2728/ptax_explore/hh_strat/out/odd_cy_all.parquet');
CREATE OR REPLACE TABLE k AS
  SELECT fips, yr, clip, city, pic,
         CASE WHEN has_addr THEN hash(z5, s, h) END k_raw,
         CASE WHEN has_addr THEN hash(z5, s_nomode, hn) END k_norm,                    -- zip5 + street (suffix stripped) + numeric house no.
         CASE WHEN coalesce(hn,'')<>'' AND coalesce(s,'')<>'' AND coalesce(city,'')<>'' THEN hash(city, s_nomode, hn) END k_city  -- city instead of zip
  FROM e;
-- town coverage in each year relative to y+1
CREATE OR REPLACE TABLE tw AS
  SELECT o.fips, o.y, k.city, count(*) FILTER (WHERE yr=o.y-1) n_prev, count(*) FILTER (WHERE yr=o.y) n_y, count(*) FILTER (WHERE yr=o.y+1) n_next
  FROM k JOIN odd o ON k.fips=o.fips AND k.yr BETWEEN o.y-1 AND o.y+1 GROUP BY ALL;
-- base: residential parcels with address in y-1 (one row per clip; NULL clips kept as rows)
CREATE OR REPLACE TABLE base AS
  SELECT o.fips, o.y, k.clip, k.city, k.k_raw, k.k_norm, k.k_city FROM k JOIN odd o ON k.fips=o.fips AND k.yr=o.y-1
  WHERE k.pic IN (10,11,21,22) AND k.k_raw IS NOT NULL;
CREATE OR REPLACE TABLE res AS
SELECT b.fips, b.y, count(*) n_prev_res,
  round(100*avg((EXISTS (SELECT 1 FROM k t WHERE t.fips=b.fips AND t.yr=b.y AND t.k_raw=b.k_raw))::INT),2) raw_in_y,
  round(100*avg((EXISTS (SELECT 1 FROM k t WHERE t.fips=b.fips AND t.yr=b.y AND t.k_norm=b.k_norm))::INT),2) norm_in_y,
  round(100*avg((EXISTS (SELECT 1 FROM k t WHERE t.fips=b.fips AND t.yr=b.y AND t.k_city=b.k_city))::INT),2) city_in_y,
  round(100*avg((b.clip IS NOT NULL AND EXISTS (SELECT 1 FROM k t WHERE t.fips=b.fips AND t.yr=b.y AND t.clip=b.clip))::INT),2) clip_in_y,
  round(100*avg((EXISTS (SELECT 1 FROM k t WHERE t.fips=b.fips AND t.yr=b.y+1 AND t.k_raw=b.k_raw))::INT),2) raw_in_next,
  round(100*avg((EXISTS (SELECT 1 FROM k t WHERE t.fips=b.fips AND t.yr=b.y+1 AND t.k_norm=b.k_norm))::INT),2) norm_in_next,
  round(100*avg((EXISTS (SELECT 1 FROM k t WHERE t.fips=b.fips AND t.yr=b.y+1 AND t.k_city=b.k_city))::INT),2) city_in_next,
  round(100*avg((b.clip IS NOT NULL AND EXISTS (SELECT 1 FROM k t WHERE t.fips=b.fips AND t.yr=b.y+1 AND t.clip=b.clip))::INT),2) clip_in_next,
  round(100*avg((EXISTS (SELECT 1 FROM tw WHERE tw.fips=b.fips AND tw.y=b.y AND tw.city IS NOT DISTINCT FROM b.city AND tw.n_y < 0.05*greatest(tw.n_next,1)))::INT),2) town_absent_in_y,
  round(100*avg((EXISTS (SELECT 1 FROM tw WHERE tw.fips=b.fips AND tw.y=b.y AND tw.city IS NOT DISTINCT FROM b.city AND tw.n_next < 0.05*greatest(tw.n_prev,1)))::INT),2) town_absent_in_next
FROM base b GROUP BY ALL ORDER BY ALL;
COPY res TO '/home/jr2728/ptax_explore/hh_strat/out/odd_cy_recovery.csv' (HEADER);
COPY (SELECT * FROM tw ORDER BY fips, y, n_next DESC) TO '/home/jr2728/ptax_explore/hh_strat/out/odd_cy_towns_3yr.csv' (HEADER);
.mode box
SELECT * FROM res;
-- town-level summary per county-year
SELECT fips, y, count(*) towns, count(*) FILTER (WHERE n_prev>0) t_prev, count(*) FILTER (WHERE n_y>0) t_y, count(*) FILTER (WHERE n_next>0) t_next,
       count(*) FILTER (WHERE n_prev>=20 AND n_y>=20) t_prev_and_y, count(*) FILTER (WHERE n_prev>=20 AND n_y>=20 AND least(n_prev,n_y) > 0.2*greatest(n_prev,n_y)) t_both_substantial
FROM tw GROUP BY ALL ORDER BY ALL;
-- the "control": where the y-1 town IS present in year y, do the raw and normalised keys match?
SELECT b.fips, b.y, count(*) n,
  round(100*avg((EXISTS (SELECT 1 FROM k t WHERE t.fips=b.fips AND t.yr=b.y AND t.k_raw=b.k_raw))::INT),1) raw_in_y,
  round(100*avg((EXISTS (SELECT 1 FROM k t WHERE t.fips=b.fips AND t.yr=b.y AND t.k_norm=b.k_norm))::INT),1) norm_in_y,
  round(100*avg((b.clip IS NOT NULL AND EXISTS (SELECT 1 FROM k t WHERE t.fips=b.fips AND t.yr=b.y AND t.clip=b.clip))::INT),1) clip_in_y
FROM base b JOIN tw ON tw.fips=b.fips AND tw.y=b.y AND tw.city IS NOT DISTINCT FROM b.city WHERE tw.n_y >= 0.2*greatest(tw.n_next,1)
GROUP BY ALL ORDER BY ALL;
