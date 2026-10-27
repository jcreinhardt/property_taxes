SET preserve_insertion_order=false; USE memory; SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
-- Who gets tracked across a move? Primary families at the 1% sample buildings in t (2006-2020) that are not at the same building in
-- t+1: tracked (same family_id at another address in t+1), re-keyed (a person id under another family_id elsewhere), temporarily
-- absent (family_id back at the same building in t+2..t+3), or lost. Shares by household characteristics in t, and the distance of
-- tracked moves. Seller subset: owner-occupied SFR with an arm's-length deed in t or t+1.
CREATE OR REPLACE TABLE fp AS
  SELECT family_id fid, record_year::INT y, bkey, zip, state, cty, length_of_residence lor,
         [individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5] iids
  FROM '/home/jr2728/ptax_explore/samples/da_bld1pct_fampos.parquet' WHERE record_year <> 2022;
CREATE OR REPLACE TABLE p AS
  SELECT family_id fid, record_year::INT y, bkey, zip, state, ge_census_state_2010::INT * 1000 + ge_als_county_code_2010::INT cty,
         CASE WHEN owner_renter_status >= 7 THEN 'owner' WHEN owner_renter_status <= 3 THEN 'renter' ELSE 'unknown' END tenure,
         CASE WHEN find < 30000 THEN '1_<30k' WHEN find < 50000 THEN '2_30-50k' WHEN find < 75000 THEN '3_50-75k' WHEN find < 100000 THEN '4_75-100k' WHEN find < 150000 THEN '5_100-150k' ELSE '6_150k+' END income,
         CASE WHEN estmtd_home_val IS NULL OR estmtd_home_val = 0 THEN '0_none' WHEN estmtd_home_val < 100000 THEN '1_<100k' WHEN estmtd_home_val < 200000 THEN '2_100-200k' WHEN estmtd_home_val < 350000 THEN '3_200-350k' WHEN estmtd_home_val < 600000 THEN '4_350-600k' ELSE '5_600k+' END homeval,
         CASE WHEN age_1 IS NULL THEN '0_unknown' WHEN age_1 < 30 THEN '1_<30' WHEN age_1 < 40 THEN '2_30s' WHEN age_1 < 50 THEN '3_40s' WHEN age_1 < 65 THEN '4_50-64' WHEN age_1 < 75 THEN '5_65-74' ELSE '6_75+' END age,
         CASE WHEN length_of_residence <= 1 THEN '1_<=1' WHEN length_of_residence <= 3 THEN '2_2-3' WHEN length_of_residence <= 9 THEN '3_4-9' WHEN length_of_residence <= 19 THEN '4_10-19' ELSE '5_20+' END lorbin,
         CASE WHEN children_ind THEN 'yes' ELSE 'no' END children,
         least(len(list_filter([individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5], lambda x: x IS NOT NULL)), 4)::VARCHAR persons,
         CASE WHEN marital_status = 9 THEN 'married_reported' WHEN marital_status = 0 THEN 'single_reported' WHEN marital_status >= 5 THEN 'likely_married' ELSE 'likely_single' END marital,
         coalesce(cbsa_type::VARCHAR, 'none') cbsa_type,
         CASE WHEN wealth_finder_score < 50000 THEN '1_<50k' WHEN wealth_finder_score < 250000 THEN '2_50-250k' WHEN wealth_finder_score < 1000000 THEN '3_250k-1m' ELSE '4_1m+' END wealth,
         coalesce(left(ethnicity_code_1, 1), '?') eth1, unit_num IS NOT NULL has_unit, record_year::VARCHAR yr,
         [individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5] iids
  FROM '/home/jr2728/ptax_explore/samples/da_bld1pct.parquet'
  WHERE primary_family_ind AND address_type = 'S' AND bkey IS NOT NULL AND record_year BETWEEN 2006 AND 2020;
-- next year by family_id
CREATE OR REPLACE TABLE nx AS
  SELECT p.*, n.bkey nbkey, n.zip nzip, n.state nstate, n.cty ncty, n.lor nlor
  FROM p LEFT JOIN fp n ON n.fid = p.fid AND n.y = p.y + 1;
-- re-key via person id elsewhere in t+1
CREATE OR REPLACE TABLE pi AS SELECT unnest(iids) iid, fid, y, bkey FROM fp;
CREATE OR REPLACE TABLE pi AS SELECT * FROM pi WHERE iid IS NOT NULL;
CREATE OR REPLACE TABLE rk AS
  SELECT q.fid, q.y, bool_or(z.bkey IS DISTINCT FROM q.bkey) rk_elsewhere
  FROM (SELECT fid, y, bkey, unnest(iids) iid FROM p) q JOIN pi z ON z.iid = q.iid AND z.y = q.y + 1 AND z.fid <> q.fid GROUP BY 1, 2;
-- back at the same building in t+2 or t+3
CREATE OR REPLACE TABLE ret AS
  SELECT DISTINCT q.fid, q.y FROM p q JOIN fp r ON r.fid = q.fid AND r.bkey = q.bkey AND r.y BETWEEN q.y + 2 AND q.y + 3;
-- sales at owner-occupied SFR
CREATE OR REPLACE TABLE oo AS
  SELECT bkey FROM (SELECT bkey, arg_max(struct_pack(pic := property_indicator_code, occ := owner_occupancy_code, corp := coalesce(owner_1_corporate_indicator,false)), tax_year) a
                    FROM '/home/jr2728/ptax_explore/samples/tax_bld1pct.parquet' GROUP BY 1) WHERE a.pic = 10 AND NOT a.corp AND a.occ = 'O';
CREATE OR REPLACE TABLE sales AS
  SELECT DISTINCT bkey, year(sale_derived_date) sy FROM '/home/jr2728/ptax_explore/samples/deeds_bld1pct.parquet'
  WHERE primary_category_code = 'A' AND NOT coalesce(interfamily_related_indicator,false) AND sale_amount > 0 AND bkey IN (SELECT bkey FROM oo);
-- leavers
CREATE OR REPLACE TABLE lv AS
  SELECT x.*, 
         CASE WHEN x.nbkey IS NOT NULL THEN 'tracked' WHEN coalesce(r.rk_elsewhere,false) THEN 'rekeyed_elsewhere' WHEN t.fid IS NOT NULL THEN 'absent_returns' ELSE 'lost' END outcome,
         CASE WHEN x.nbkey IS NULL THEN NULL WHEN x.nzip = x.zip THEN '1_same_zip' WHEN x.ncty = x.cty THEN '2_same_county'
              WHEN x.nstate = x.state THEN '3_same_state' ELSE '4_other_state' END dist,
         x.nlor <= 1 lor_reset,
         EXISTS (SELECT 1 FROM sales s WHERE s.bkey = x.bkey AND s.sy IN (x.y, x.y + 1)) sold, x.bkey IN (SELECT bkey FROM oo) oo_sfr
  FROM nx x LEFT JOIN rk r ON r.fid = x.fid AND r.y = x.y LEFT JOIN ret t ON t.fid = x.fid AND t.y = x.y
  WHERE x.nbkey IS DISTINCT FROM x.bkey;
CREATE OR REPLACE TABLE lvl AS
  SELECT fid, y, outcome, dist, lor_reset, sold, oo_sfr, dim, bin
  FROM lv UNPIVOT (bin FOR dim IN (tenure, income, homeval, age, lorbin, children, persons, marital, cbsa_type, wealth, eth1, yr, state));
CREATE OR REPLACE MACRO shares() AS TABLE
  SELECT dim, bin, count(*) leavers,
         round(100*avg((outcome='tracked')::INT),2) tracked, round(100*avg((outcome='tracked' AND lor_reset)::INT),2) tracked_lor_reset,
         round(100*avg((outcome='rekeyed_elsewhere')::INT),2) rekeyed, round(100*avg((outcome='absent_returns')::INT),2) absent_returns,
         round(100*avg((outcome='lost')::INT),2) lost,
         round(100*avg((dist='1_same_zip')::INT) FILTER (WHERE outcome='tracked'),1) t_same_zip, round(100*avg((dist='2_same_county')::INT) FILTER (WHERE outcome='tracked'),1) t_same_county,
         round(100*avg((dist='3_same_state')::INT) FILTER (WHERE outcome='tracked'),1) t_same_state, round(100*avg((dist='4_other_state')::INT) FILTER (WHERE outcome='tracked'),1) t_other_state
  FROM lvl GROUP BY 1, 2;
COPY (SELECT 'all_leavers' smpl, * FROM shares() UNION ALL SELECT 'sold_oo_sfr', * FROM (SELECT dim, bin, count(*) leavers,
         round(100*avg((outcome='tracked')::INT),2), round(100*avg((outcome='tracked' AND lor_reset)::INT),2), round(100*avg((outcome='rekeyed_elsewhere')::INT),2),
         round(100*avg((outcome='absent_returns')::INT),2), round(100*avg((outcome='lost')::INT),2),
         round(100*avg((dist='1_same_zip')::INT) FILTER (WHERE outcome='tracked'),1), round(100*avg((dist='2_same_county')::INT) FILTER (WHERE outcome='tracked'),1),
         round(100*avg((dist='3_same_state')::INT) FILTER (WHERE outcome='tracked'),1), round(100*avg((dist='4_other_state')::INT) FILTER (WHERE outcome='tracked'),1)
       FROM lvl WHERE sold AND oo_sfr GROUP BY 1, 2) ORDER BY smpl, dim, bin) TO '/home/jr2728/ptax_explore/hh_strat/out/tracked_bias.csv' (HEADER);
-- composition of the base (all primaries in t) vs tracked movers vs lost, for the main dims
COPY (
  SELECT dim, bin, round(100*count(*) FILTER (WHERE grp='base')/ (SELECT count(*) FROM p),2) base_pct,
         round(100*count(*) FILTER (WHERE grp='tracked') / (SELECT count(*) FROM lv WHERE outcome='tracked'),2) tracked_pct,
         round(100*count(*) FILTER (WHERE grp='lost') / (SELECT count(*) FROM lv WHERE outcome='lost'),2) lost_pct
  FROM (SELECT 'base' grp, dim, bin FROM p UNPIVOT (bin FOR dim IN (tenure, income, homeval, age, lorbin, children, persons, marital, cbsa_type, wealth, eth1))
        UNION ALL SELECT outcome, dim, bin FROM lvl WHERE outcome IN ('tracked','lost'))
  GROUP BY 1, 2 ORDER BY 1, 2) TO '/home/jr2728/ptax_explore/hh_strat/out/tracked_bias_composition.csv' (HEADER);
