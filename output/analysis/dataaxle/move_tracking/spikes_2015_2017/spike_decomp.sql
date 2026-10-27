SET preserve_insertion_order=false; USE memory; SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
SET threads=4; SET memory_limit='26GB';
-- Decomposition of the 2015->16 and 2017->18 spikes in primary-family-ID turnover at residential addresses (1% building sample).
-- Panel as in moveout_proxies.sql (residential = latest tax pic 10/11/21/22 with structure; 2012-2021; backfilled from last record).
CREATE OR REPLACE TABLE tx AS
  SELECT bkey, arg_max(struct_pack(pic := property_indicator_code,
                 noimpr := coalesce(calculated_improvement_value,0) = 0 AND coalesce(total_living_area_square_feet_all_buildings,0) = 0), tax_year) a
  FROM '/home/jr2728/ptax_explore/samples/tax_bld1pct.parquet' WHERE tax_year BETWEEN 2012 AND 2021 GROUP BY bkey;
CREATE OR REPLACE TABLE res AS SELECT bkey FROM tx WHERE a.pic IN (10, 11, 21, 22) AND NOT a.noimpr;
-- Data Axle family records at residential addresses, with the fields to compare across years
CREATE OR REPLACE TABLE fam AS
  SELECT bkey, record_year::INT y, family_id fid, primary_family_ind prim, length_of_residence lor, state, city,
         list_filter([individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5], x -> x IS NOT NULL) iids,
         list_filter([ln_1, ln_2, ln_3, ln_4, ln_5], x -> x IS NOT NULL) lns,
         ln_1, fn_1, age_1, location_type ltype, household_status hhst, address_type atype, street_pre_dir predir, street_suffix sfx, street_post_dir postdir,
         nullif(upper(trim(unit_num)), '') unit, zip4, dpbc, round(ge_latitude_2010, 4) lat, round(ge_longitude_2010, 4) lon, ge_census_level_2010 glev,
         ge_census_state_2010::INT*1000 + ge_als_county_code_2010::INT cty, ge_als_census_tract_2010 tract, owner_renter_status ors, recency_date rec,
         head_hh_age_code agec, estmtd_home_val ehv, mailability_score mail, vacant, usps_nostats nostats, filename, len(iids) npers
  FROM '/home/jr2728/ptax_explore/samples/da_bld1pct.parquet' WHERE record_year BETWEEN 2012 AND 2021 AND bkey IN (SELECT bkey FROM res);
CREATE OR REPLACE TABLE obs AS
  SELECT bkey, y, min(fid) FILTER (WHERE prim) prim_fid, count(*) FILTER (WHERE prim) n_prim, count(*) n_fam, any_value(state) st FROM fam GROUP BY bkey, y;
CREATE OR REPLACE TABLE grid AS SELECT r.bkey, yy.y FROM res r CROSS JOIN (SELECT unnest(range(2012, 2022)) y) yy;
CREATE OR REPLACE TABLE pan AS
  SELECT g.bkey, g.y, o.prim_fid IS NOT NULL observed, o.n_fam IS NOT NULL any_rec, last_value(o.n_prim IGNORE NULLS) OVER w n_prim,
         last_value(o.prim_fid IGNORE NULLS) OVER w prim_fid, last_value(o.st IGNORE NULLS) OVER w st
  FROM grid g LEFT JOIN obs o ON o.bkey = g.bkey AND o.y = g.y
  WINDOW w AS (PARTITION BY g.bkey ORDER BY g.y ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
CREATE OR REPLACE TABLE pr AS
  SELECT a.bkey, a.y, a.st, a.observed obs0, b.observed obs1, b.any_rec anyrec1, (a.n_prim = 1 AND b.n_prim = 1) one_prim, a.prim_fid fid0, b.prim_fid fid1, (a.prim_fid <> b.prim_fid) id_chg
  FROM pan a JOIN pan b ON b.bkey = a.bkey AND b.y = a.y + 1
  WHERE a.prim_fid IS NOT NULL AND b.prim_fid IS NOT NULL;
-- (A) rates by pair and sample; plus disappearance of the address from the file in t+1
COPY (
  SELECT y, count(*) pairs_backfilled, round(100*avg(id_chg::INT),2) id_chg_backfilled,
         count(*) FILTER (WHERE obs0 AND obs1) pairs_obs_both, round(100*avg(id_chg::INT) FILTER (WHERE obs0 AND obs1),2) id_chg_obs_both,
         count(*) FILTER (WHERE obs0 AND obs1 AND one_prim) pairs_obs_both_1prim, round(100*avg(id_chg::INT) FILTER (WHERE obs0 AND obs1 AND one_prim),2) id_chg_obs_both_1prim,
         round(100*avg((NOT obs1)::INT) FILTER (WHERE obs0),2) pct_obs_t_not_t1, round(100*avg((NOT anyrec1)::INT) FILTER (WHERE obs0),2) pct_obs_t_norecord_t1,
         round(100*avg((NOT obs0)::INT),2) pct_backfilled_t
  FROM pr GROUP BY y ORDER BY y
) TO '/home/jr2728/ptax_explore/hh_strat/out/spike_rates.csv' (HEADER);
-- all records of these families anywhere in Data Axle (to follow fid0 and trace fid1's history)
CREATE OR REPLACE TABLE fp AS SELECT family_id fid, record_year::INT y, bkey, primary_family_ind prim, length_of_residence lor
  FROM '/home/jr2728/ptax_explore/samples/da_bld1pct_fampos.parquet' WHERE record_year <> 2022;
CREATE OR REPLACE TABLE fp1 AS SELECT fid, min(y) y_first, max(y) y_last FROM fp GROUP BY fid;
-- id-change pairs, both observed, one primary in both years: old and new primary records
CREATE OR REPLACE TABLE ch AS
  SELECT p.y, p.bkey, p.st, p.fid0, p.fid1, o.lor lor0, n.lor lor1, o.ors ors0, n.ors ors1, o.npers np0, n.npers np1,
         len(list_intersect(o.iids, n.iids)) > 0 same_iid, len(list_intersect(o.lns, n.lns)) > 0 same_ln,
         f1.y_first = p.y + 1 fid1_new, f0.y_last = p.y fid0_last,
         EXISTS (SELECT 1 FROM fp q WHERE q.fid = p.fid1 AND q.y = p.y AND q.bkey = p.bkey) fid1_here_t,
         EXISTS (SELECT 1 FROM fp q WHERE q.fid = p.fid1 AND q.y = p.y AND q.bkey IS DISTINCT FROM p.bkey) fid1_elsewhere_t,
         EXISTS (SELECT 1 FROM fp q WHERE q.fid = p.fid0 AND q.y = p.y + 1 AND q.bkey = p.bkey) fid0_here_t1,
         EXISTS (SELECT 1 FROM fp q WHERE q.fid = p.fid0 AND q.y = p.y + 1 AND q.bkey IS DISTINCT FROM p.bkey) fid0_elsewhere_t1,
         EXISTS (SELECT 1 FROM fp q WHERE q.fid = p.fid0 AND q.y = p.y + 2 AND q.bkey = p.bkey AND q.prim) fid0_back_t2,
         EXISTS (SELECT 1 FROM fp q WHERE q.fid = p.fid0 AND q.y >= p.y + 2 AND q.bkey = p.bkey) fid0_back_later,
         EXISTS (SELECT 1 FROM fp q WHERE q.fid = p.fid1 AND q.y = p.y + 2 AND q.bkey = p.bkey AND q.prim) fid1_stays_t2,
         -- field comparisons old vs new record at the same address
         o.unit IS DISTINCT FROM n.unit d_unit, o.zip4 IS DISTINCT FROM n.zip4 d_zip4, o.dpbc IS DISTINCT FROM n.dpbc d_dpbc,
         (o.lat IS DISTINCT FROM n.lat OR o.lon IS DISTINCT FROM n.lon) d_latlon, o.glev IS DISTINCT FROM n.glev d_glev, o.ltype IS DISTINCT FROM n.ltype d_ltype,
         o.hhst IS DISTINCT FROM n.hhst d_hhst, o.cty IS DISTINCT FROM n.cty d_cty, o.tract IS DISTINCT FROM n.tract d_tract, o.sfx IS DISTINCT FROM n.sfx d_sfx,
         o.predir IS DISTINCT FROM n.predir d_predir, o.city IS DISTINCT FROM n.city d_city, o.ors IS DISTINCT FROM n.ors d_ors, o.agec IS DISTINCT FROM n.agec d_agec,
         o.ehv IS DISTINCT FROM n.ehv d_ehv, o.rec IS DISTINCT FROM n.rec d_rec, o.npers <> n.npers d_npers, n.lor <= 1 lor_reset, n.lor = o.lor + 1 lor_cont,
         o.fn_1 = n.fn_1 AND o.ln_1 = n.ln_1 same_p1_name, o.age_1 BETWEEN n.age_1 - 2 AND n.age_1 same_p1_age
  FROM pr p JOIN fam o ON o.bkey = p.bkey AND o.y = p.y AND o.fid = p.fid0 JOIN fam n ON n.bkey = p.bkey AND n.y = p.y + 1 AND n.fid = p.fid1
  LEFT JOIN fp1 f1 ON f1.fid = p.fid1 LEFT JOIN fp1 f0 ON f0.fid = p.fid0
  WHERE p.id_chg AND p.obs0 AND p.obs1 AND p.one_prim;
-- control: same primary id both years (both observed, one primary), same field comparisons
CREATE OR REPLACE TABLE ct AS
  SELECT p.y, p.bkey, p.st, o.lor lor0, n.lor lor1,
         o.unit IS DISTINCT FROM n.unit d_unit, o.zip4 IS DISTINCT FROM n.zip4 d_zip4, o.dpbc IS DISTINCT FROM n.dpbc d_dpbc,
         (o.lat IS DISTINCT FROM n.lat OR o.lon IS DISTINCT FROM n.lon) d_latlon, o.glev IS DISTINCT FROM n.glev d_glev, o.ltype IS DISTINCT FROM n.ltype d_ltype,
         o.hhst IS DISTINCT FROM n.hhst d_hhst, o.cty IS DISTINCT FROM n.cty d_cty, o.tract IS DISTINCT FROM n.tract d_tract, o.sfx IS DISTINCT FROM n.sfx d_sfx,
         o.predir IS DISTINCT FROM n.predir d_predir, o.city IS DISTINCT FROM n.city d_city, o.ors IS DISTINCT FROM n.ors d_ors, o.agec IS DISTINCT FROM n.agec d_agec,
         o.ehv IS DISTINCT FROM n.ehv d_ehv, o.rec IS DISTINCT FROM n.rec d_rec, o.npers <> n.npers d_npers, n.lor <= 1 lor_reset, n.lor = o.lor + 1 lor_cont,
         len(list_intersect(o.iids, n.iids)) > 0 same_iid, len(list_intersect(o.lns, n.lns)) > 0 same_ln, o.fn_1 = n.fn_1 AND o.ln_1 = n.ln_1 same_p1_name
  FROM pr p JOIN fam o ON o.bkey = p.bkey AND o.y = p.y AND o.fid = p.fid0 JOIN fam n ON n.bkey = p.bkey AND n.y = p.y + 1 AND n.fid = p.fid1
  WHERE NOT p.id_chg AND p.obs0 AND p.obs1 AND p.one_prim;
-- (B) decomposition by pair: categories as % of all one-primary observed pairs (so excess reads in pp) and as % of id changes
CREATE OR REPLACE TABLE chc AS
  SELECT *, CASE WHEN same_iid THEN '1_same_person_id'
                 WHEN same_ln THEN '2_same_surname_new_persons'
                 WHEN fid1_here_t THEN '3_diff_surname_promoted_subfamily'
                 WHEN fid1_elsewhere_t THEN '4_diff_surname_tracked_movein'
                 WHEN fid1_new AND lor1 <= 1 THEN '5_diff_surname_newid_lor_reset'
                 WHEN fid1_new THEN '6_diff_surname_newid_lor_carried'
                 ELSE '7_diff_surname_oldid_gap_return' END cat,
            CASE WHEN fid0_here_t1 THEN 'a_demoted_same_address' WHEN fid0_elsewhere_t1 THEN 'b_moved_elsewhere'
                 WHEN fid0_back_later THEN 'c_vanished_returns_here' WHEN NOT fid0_last THEN 'd_vanished_returns_elsewhere' ELSE 'e_vanished_for_good' END fate0
  FROM ch;
CREATE OR REPLACE TABLE denom AS SELECT y, count(*) pairs FROM pr WHERE obs0 AND obs1 AND one_prim GROUP BY y;
COPY (
  SELECT c.y, c.cat, count(*) n, round(100.0*count(*)/any_value(d.pairs),3) pct_of_pairs, round(100.0*count(*)/sum(count(*)) OVER (PARTITION BY c.y),1) pct_of_changes,
         round(100*avg(lor_reset::INT),1) lor1_le1, round(100*avg(fid0_here_t1::INT),1) fid0_demoted_here, round(100*avg(fid0_elsewhere_t1::INT),1) fid0_elsewhere,
         round(100*avg((NOT fid0_here_t1 AND NOT fid0_elsewhere_t1)::INT),1) fid0_vanished, round(100*avg(fid0_back_t2::INT),1) fid0_back_prim_t2,
         round(100*avg(fid0_back_later::INT),1) fid0_back_later, round(100*avg(fid1_stays_t2::INT),1) fid1_prim_t2,
         round(100*avg(d_latlon::INT),1) d_latlon, round(100*avg(d_dpbc::INT),1) d_dpbc, round(100*avg(d_zip4::INT),1) d_zip4, round(100*avg(d_unit::INT),1) d_unit,
         round(100*avg(d_ors::INT),1) d_ors, round(100*avg((ors1 >= 7)::INT),1) new_owner_status, round(avg(np1),2) npers_new, round(avg(np0),2) npers_old
  FROM chc c JOIN denom d USING (y) GROUP BY c.y, c.cat ORDER BY c.y, c.cat
) TO '/home/jr2728/ptax_explore/hh_strat/out/spike_decomp.csv' (HEADER);
COPY (
  SELECT c.y, c.fate0, count(*) n, round(100.0*count(*)/any_value(d.pairs),3) pct_of_pairs, round(100.0*count(*)/sum(count(*)) OVER (PARTITION BY c.y),1) pct_of_changes
  FROM chc c JOIN denom d USING (y) GROUP BY c.y, c.fate0 ORDER BY c.y, c.fate0
) TO '/home/jr2728/ptax_explore/hh_strat/out/spike_fate0.csv' (HEADER);
-- (C) field differences t vs t+1 at the same address: control (same id) vs id-change groups
COPY (
  SELECT grp, y, count(*) n, round(100*avg(d_latlon::INT),1) d_latlon, round(100*avg(d_glev::INT),1) d_glev, round(100*avg(d_dpbc::INT),1) d_dpbc,
         round(100*avg(d_zip4::INT),1) d_zip4, round(100*avg(d_unit::INT),1) d_unit, round(100*avg(d_sfx::INT),1) d_sfx, round(100*avg(d_predir::INT),1) d_predir,
         round(100*avg(d_city::INT),1) d_city, round(100*avg(d_cty::INT),1) d_cty, round(100*avg(d_tract::INT),1) d_tract, round(100*avg(d_ltype::INT),1) d_ltype,
         round(100*avg(d_hhst::INT),1) d_hhst, round(100*avg(d_ors::INT),1) d_ors, round(100*avg(d_agec::INT),1) d_agec, round(100*avg(d_ehv::INT),1) d_ehv,
         round(100*avg(d_rec::INT),1) d_recency, round(100*avg(d_npers::INT),1) d_npers, round(100*avg(lor_reset::INT),1) lor1_le1, round(100*avg(lor_cont::INT),1) lor1_eq_lor0_plus1,
         round(100*avg(same_iid::INT),1) same_iid, round(100*avg(same_ln::INT),1) same_ln, round(100*avg(same_p1_name::INT),1) same_p1_name
  FROM (SELECT 'ctrl_same_fid' grp, * EXCLUDE (bkey, st) FROM ct
        UNION ALL BY NAME SELECT 'chg_same_person_or_surname' grp, y, lor0, lor1, d_unit, d_zip4, d_dpbc, d_latlon, d_glev, d_ltype, d_hhst, d_cty, d_tract, d_sfx, d_predir, d_city, d_ors, d_agec, d_ehv, d_rec, d_npers, lor_reset, lor_cont, same_iid, same_ln, same_p1_name FROM chc WHERE cat IN ('1_same_person_id', '2_same_surname_new_persons')
        UNION ALL BY NAME SELECT 'chg_diff_surname' grp, y, lor0, lor1, d_unit, d_zip4, d_dpbc, d_latlon, d_glev, d_ltype, d_hhst, d_cty, d_tract, d_sfx, d_predir, d_city, d_ors, d_agec, d_ehv, d_rec, d_npers, lor_reset, lor_cont, same_iid, same_ln, same_p1_name FROM chc WHERE cat NOT IN ('1_same_person_id', '2_same_surname_new_persons'))
  GROUP BY grp, y ORDER BY grp, y
) TO '/home/jr2728/ptax_explore/hh_strat/out/spike_fields.csv' (HEADER);
-- (D) geography: id-change rate by state and pair (backfilled, all residential), with excess of the spike pairs over adjacent pairs
CREATE OR REPLACE TABLE sty AS SELECT st, y, count(*) pairs, 100*avg(id_chg::INT) r FROM pr WHERE st IS NOT NULL GROUP BY st, y;
COPY (
  SELECT s.st, sum(pairs) FILTER (WHERE y = 2015) pairs_2015,
         round(any_value(r) FILTER (WHERE y = 2014),2) r2014, round(any_value(r) FILTER (WHERE y = 2015),2) r2015, round(any_value(r) FILTER (WHERE y = 2016),2) r2016,
         round(any_value(r) FILTER (WHERE y = 2017),2) r2017, round(any_value(r) FILTER (WHERE y = 2018),2) r2018, round(any_value(r) FILTER (WHERE y = 2019),2) r2019,
         round(any_value(r) FILTER (WHERE y = 2015) - (any_value(r) FILTER (WHERE y = 2014) + any_value(r) FILTER (WHERE y = 2016))/2, 2) excess_2015,
         round(any_value(r) FILTER (WHERE y = 2017) - (any_value(r) FILTER (WHERE y = 2016) + any_value(r) FILTER (WHERE y = 2018))/2, 2) excess_2017,
         (any_value(r) FILTER (WHERE y = 2015) - (any_value(r) FILTER (WHERE y = 2014) + any_value(r) FILTER (WHERE y = 2016))/2) > 3 flag_2015,
         (any_value(r) FILTER (WHERE y = 2017) - (any_value(r) FILTER (WHERE y = 2016) + any_value(r) FILTER (WHERE y = 2018))/2) > 3 flag_2017
  FROM sty s GROUP BY s.st ORDER BY excess_2017 DESC
) TO '/home/jr2728/ptax_explore/hh_strat/out/spike_by_state.csv' (HEADER);
COPY (SELECT st, y, pairs, round(r,2) id_chg FROM sty ORDER BY st, y) TO '/home/jr2728/ptax_explore/hh_strat/out/spike_by_state_long.csv' (HEADER);
-- county-level: top counties by excess in each spike pair (counties with >= 300 pairs)
CREATE OR REPLACE TABLE ctyy AS
  SELECT o.cty, p.y, count(*) pairs, 100*avg(p.id_chg::INT) r FROM pr p JOIN fam o ON o.bkey = p.bkey AND o.y = p.y AND o.fid = p.fid0 WHERE p.obs0 GROUP BY o.cty, p.y;
COPY (
  SELECT cty, any_value(pairs) FILTER (WHERE y = 2017) pairs_2017,
         round(any_value(r) FILTER (WHERE y = 2014),1) r2014, round(any_value(r) FILTER (WHERE y = 2015),1) r2015, round(any_value(r) FILTER (WHERE y = 2016),1) r2016,
         round(any_value(r) FILTER (WHERE y = 2017),1) r2017, round(any_value(r) FILTER (WHERE y = 2018),1) r2018,
         round(any_value(r) FILTER (WHERE y = 2015) - (any_value(r) FILTER (WHERE y = 2014) + any_value(r) FILTER (WHERE y = 2016))/2, 1) excess_2015,
         round(any_value(r) FILTER (WHERE y = 2017) - (any_value(r) FILTER (WHERE y = 2016) + any_value(r) FILTER (WHERE y = 2018))/2, 1) excess_2017
  FROM ctyy GROUP BY cty HAVING min(pairs) >= 300 AND count(*) FILTER (WHERE y BETWEEN 2014 AND 2018) = 5 ORDER BY greatest(excess_2015, excess_2017) DESC
) TO '/home/jr2728/ptax_explore/hh_strat/out/spike_by_county.csv' (HEADER);
-- (E) continuing primaries: LOR increments by pair (is LOR re-derived in vintage years?)
COPY (
  SELECT y, count(*) n, round(100*avg((lor1 = lor0 + 1)::INT),1) lor_plus1, round(100*avg((lor1 = lor0)::INT),1) lor_same, round(100*avg((lor1 <= 1)::INT),1) lor_reset,
         round(100*avg((lor1 > lor0 + 1)::INT),1) lor_jump_up, round(100*avg((lor1 < lor0 AND lor1 > 1)::INT),1) lor_down_not_reset
  FROM ct GROUP BY y ORDER BY y
) TO '/home/jr2728/ptax_explore/hh_strat/out/spike_lor_ctrl.csv' (HEADER);
-- (F) file vintage: filenames and recency dates per record year
COPY (
  SELECT y, count(*) n_fam_records, list(DISTINCT filename) files, round(100*avg((left(rec,4)::INT = y)::INT),1) pct_recency_same_year,
         round(100*avg((left(rec,4)::INT = y - 1)::INT),1) pct_recency_prev_year, quantile_cont(rec::INT, 0.5) median_recency, round(100*avg(prim::INT),1) pct_primary
  FROM fam WHERE rec IS NOT NULL AND length(rec) = 6 GROUP BY y ORDER BY y
) TO '/home/jr2728/ptax_explore/hh_strat/out/spike_files.csv' (HEADER);
SELECT * FROM read_csv('/home/jr2728/ptax_explore/hh_strat/out/spike_rates.csv');
SELECT * FROM read_csv('/home/jr2728/ptax_explore/hh_strat/out/spike_decomp.csv') WHERE y BETWEEN 2014 AND 2018;
SELECT * FROM read_csv('/home/jr2728/ptax_explore/hh_strat/out/spike_fate0.csv') WHERE y BETWEEN 2014 AND 2018;
SELECT * FROM read_csv('/home/jr2728/ptax_explore/hh_strat/out/spike_by_state.csv') WHERE flag_2015 OR flag_2017;
SELECT * FROM read_csv('/home/jr2728/ptax_explore/hh_strat/out/spike_files.csv');
