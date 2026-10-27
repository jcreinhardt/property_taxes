SET preserve_insertion_order=false; USE memory; SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
SET threads=4; SET memory_limit='26GB';
-- Brand-new family_ids (never seen in an earlier file) and record counts by year and state, 1% family sample (hash(family_id) % 100 = 0; all years).
CREATE OR REPLACE TABLE f AS
  SELECT family_id fid, record_year::INT y, primary_family_ind prim, state, address_type atype, length_of_residence lor, location_type ltype,
         bkey IS NOT NULL has_bkey, individual_id_1 iid1, ln_1
  FROM '/home/jr2728/ptax_explore/samples/da_fam1pct.parquet' WHERE record_year <> 2022;
CREATE OR REPLACE TABLE f1 AS SELECT fid, min(y) y_first, max(y) y_last, count(*) n_years FROM f GROUP BY fid;
CREATE OR REPLACE TABLE fx AS SELECT f.*, f1.y_first, f1.y_last, f1.n_years, f.y = f1.y_first AND f.y > 2006 brand_new,
  EXISTS (SELECT 1 FROM f g WHERE g.fid = f.fid AND g.y = f.y + 1) in_next FROM f JOIN f1 USING (fid);
-- national
COPY (
  SELECT y, count(*) records, count(*) FILTER (WHERE prim) primaries, count(*) FILTER (WHERE atype = 'S') street_addr,
         round(100*avg(brand_new::INT),2) pct_brand_new, round(100*avg(brand_new::INT) FILTER (WHERE prim),2) pct_brand_new_prim,
         round(100*avg((brand_new AND lor <= 1)::INT),2) pct_new_lor1, round(100*avg((brand_new AND lor >= 2)::INT),2) pct_new_lor_ge2,
         round(100*avg((brand_new AND n_years = 1)::INT),2) pct_new_oneyear_only, round(100*avg((NOT in_next AND y < 2021)::INT),2) pct_absent_next,
         round(100*avg((brand_new AND y_last = y)::INT),2) pct_new_never_again
  FROM fx GROUP BY y ORDER BY y
) TO '/home/jr2728/ptax_explore/hh_strat/out/spike_newfids_national.csv' (HEADER);
-- by state: records and brand-new share for 2013-2020, wide
COPY (
  SELECT state, count(*) FILTER (WHERE y = 2017) records_2017,
         round(100.0*count(*) FILTER (WHERE y = 2016)/count(*) FILTER (WHERE y = 2015) - 100, 1) rec_growth_2016,
         round(100.0*count(*) FILTER (WHERE y = 2018)/count(*) FILTER (WHERE y = 2017) - 100, 1) rec_growth_2018,
         round(100.0*count(*) FILTER (WHERE y = 2017)/count(*) FILTER (WHERE y = 2016) - 100, 1) rec_growth_2017,
         round(100.0*count(*) FILTER (WHERE y = 2019)/count(*) FILTER (WHERE y = 2018) - 100, 1) rec_growth_2019,
         round(100*avg(brand_new::INT) FILTER (WHERE y = 2014),1) new_2014, round(100*avg(brand_new::INT) FILTER (WHERE y = 2015),1) new_2015,
         round(100*avg(brand_new::INT) FILTER (WHERE y = 2016),1) new_2016, round(100*avg(brand_new::INT) FILTER (WHERE y = 2017),1) new_2017,
         round(100*avg(brand_new::INT) FILTER (WHERE y = 2018),1) new_2018, round(100*avg(brand_new::INT) FILTER (WHERE y = 2019),1) new_2019,
         round(100*avg((NOT in_next)::INT) FILTER (WHERE y = 2014),1) gone_2014_15, round(100*avg((NOT in_next)::INT) FILTER (WHERE y = 2015),1) gone_2015_16,
         round(100*avg((NOT in_next)::INT) FILTER (WHERE y = 2016),1) gone_2016_17, round(100*avg((NOT in_next)::INT) FILTER (WHERE y = 2017),1) gone_2017_18,
         round(100*avg((NOT in_next)::INT) FILTER (WHERE y = 2018),1) gone_2018_19
  FROM fx WHERE state IS NOT NULL GROUP BY state HAVING count(*) FILTER (WHERE y = 2017) >= 2000 ORDER BY new_2018 DESC
) TO '/home/jr2728/ptax_explore/hh_strat/out/spike_newfids_state.csv' (HEADER);
SELECT * FROM read_csv('/home/jr2728/ptax_explore/hh_strat/out/spike_newfids_national.csv');
SELECT * FROM read_csv('/home/jr2728/ptax_explore/hh_strat/out/spike_newfids_state.csv') LIMIT 15;
