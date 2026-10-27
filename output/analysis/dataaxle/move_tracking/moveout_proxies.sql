-- Move-out proxies at residential parcels (1% building sample; all residential, owner-occupied SFR, and arm's-length-sold SFR), 2012-2021, with Data Axle gap years backfilled
-- from the most recent previous record. Proxies for t -> t+1: (1) primary family_id changes, (2) modal surname of primary-family persons
-- changes, (3) modal surname across all persons at the address changes. Validated against arm's-length deed sales and tax owner-name changes.
SET threads=4;
-- residential addresses (latest tax year), structure on roll; flag owner-occupied non-corporate single family
CREATE OR REPLACE TABLE tx AS
  SELECT bkey, arg_max(struct_pack(pic := property_indicator_code, occ := owner_occupancy_code, corp := coalesce(owner_1_corporate_indicator,false),
                 noimpr := coalesce(calculated_improvement_value,0) = 0 AND coalesce(total_living_area_square_feet_all_buildings,0) = 0,
                 mail_here := left(coalesce(mailing_zip_code,''),5) = left(situs_zip_code,5) AND upper(trim(coalesce(mailing_house_number,''))) = upper(trim(situs_house_number))
                              AND upper(trim(coalesce(mailing_street_name,''))) = upper(trim(situs_street_name))), tax_year) a
  FROM '/home/jr2728/ptax_explore/samples/tax_bld1pct.parquet' WHERE tax_year BETWEEN 2012 AND 2021 GROUP BY bkey;
CREATE OR REPLACE TABLE sfr AS SELECT bkey, a.pic = 10 AND NOT a.corp AND (a.occ = 'O' OR a.mail_here) oo_sfr FROM tx WHERE a.pic IN (10, 11, 21, 22) AND NOT a.noimpr;
-- tax owner surname by year (hashed), for the tax-based turnover at the same parcels
CREATE OR REPLACE TABLE town AS
  SELECT bkey, tax_year::INT y, min(o1_ln) o1 FROM '/home/jr2728/ptax_explore/samples/tax_bld1pct.parquet'
  WHERE tax_year BETWEEN 2012 AND 2021 AND bkey IN (SELECT bkey FROM sfr) GROUP BY 1, 2;
-- arm's-length sales by address-year
CREATE OR REPLACE TABLE sales AS
  SELECT DISTINCT bkey, year(sale_derived_date) y FROM '/home/jr2728/ptax_explore/samples/deeds_bld1pct.parquet'
  WHERE primary_category_code = 'A' AND NOT coalesce(interfamily_related_indicator, false) AND sale_amount > 0
    AND year(sale_derived_date) BETWEEN 2010 AND 2023 AND bkey IN (SELECT bkey FROM sfr WHERE oo_sfr);
CREATE OR REPLACE TABLE sold_ever AS SELECT DISTINCT bkey FROM sales WHERE y BETWEEN 2013 AND 2020;
-- Data Axle persons at these addresses
CREATE OR REPLACE TABLE per AS
  SELECT bkey, record_year::INT y, family_id fid, primary_family_ind prim, unnest([ln_1, ln_2, ln_3, ln_4, ln_5]) ln
  FROM '/home/jr2728/ptax_explore/samples/da_bld1pct.parquet' WHERE record_year BETWEEN 2012 AND 2021 AND record_year <> 2022 AND bkey IN (SELECT bkey FROM sfr);
CREATE OR REPLACE TABLE modal AS
  SELECT bkey, y, scp, ln FROM (
    SELECT bkey, y, scp, ln, row_number() OVER (PARTITION BY bkey, y, scp ORDER BY n DESC, ln) rn FROM (
      SELECT bkey, y, 'prim' scp, ln, count(*) n FROM per WHERE prim AND ln IS NOT NULL GROUP BY ALL
      UNION ALL SELECT bkey, y, 'all', ln, count(*) FROM per WHERE ln IS NOT NULL GROUP BY ALL)) WHERE rn = 1;
CREATE OR REPLACE TABLE obs AS
  SELECT p.bkey, p.y, min(p.fid) FILTER (WHERE p.prim) prim_fid, count(DISTINCT p.fid) FILTER (WHERE p.prim) n_prim, any_value(mp.ln) ln_prim, any_value(ma.ln) ln_all
  FROM per p LEFT JOIN modal mp ON mp.bkey = p.bkey AND mp.y = p.y AND mp.scp = 'prim' LEFT JOIN modal ma ON ma.bkey = p.bkey AND ma.y = p.y AND ma.scp = 'all'
  GROUP BY p.bkey, p.y;
-- panel 2012-2021 (2022 missing in DA -> always filled), backfilled from the most recent previous record
CREATE OR REPLACE TABLE grid AS SELECT s.bkey, s.oo_sfr, yy.y FROM sfr s CROSS JOIN (SELECT unnest(range(2012, 2022)) y) yy;
CREATE OR REPLACE TABLE pan AS
  SELECT g.bkey, g.oo_sfr, g.y, o.prim_fid IS NOT NULL observed, last_value(o.n_prim IGNORE NULLS) OVER w n_prim,
         last_value(o.prim_fid IGNORE NULLS) OVER w prim_fid, last_value(o.ln_prim IGNORE NULLS) OVER w ln_prim, last_value(o.ln_all IGNORE NULLS) OVER w ln_all
  FROM grid g LEFT JOIN obs o ON o.bkey = g.bkey AND o.y = g.y
  WINDOW w AS (PARTITION BY g.bkey ORDER BY g.y ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
CREATE OR REPLACE TABLE pr AS
  SELECT a.bkey, a.oo_sfr, (a.n_prim = 1 AND b.n_prim = 1) one_prim, a.y, a.observed obs0, b.observed obs1, a.prim_fid IS NOT NULL has0, b.prim_fid IS NOT NULL has1,
         (a.prim_fid <> b.prim_fid) id_chg, (a.ln_prim <> b.ln_prim) lnp_chg, (a.ln_all <> b.ln_all) lna_chg,
         (a.bkey, a.y + 1) IN (SELECT (bkey, y) FROM sales) sale_t1, (a.bkey, a.y) IN (SELECT (bkey, y) FROM sales) sale_t0,
         EXISTS (SELECT 1 FROM sales s WHERE s.bkey = a.bkey AND s.y BETWEEN a.y - 1 AND a.y + 2) sale_near,
         a.bkey IN (SELECT bkey FROM sold_ever) sold_ever,
         (t0.o1 IS NOT NULL AND t1.o1 IS NOT NULL AND t0.o1 <> t1.o1) tax_owner_chg, (t0.o1 IS NOT NULL AND t1.o1 IS NOT NULL) tax_both
  FROM pan a JOIN pan b ON b.bkey = a.bkey AND b.y = a.y + 1
  LEFT JOIN town t0 ON t0.bkey = a.bkey AND t0.y = a.y LEFT JOIN town t1 ON t1.bkey = a.bkey AND t1.y = a.y + 1
  WHERE a.prim_fid IS NOT NULL AND b.prim_fid IS NOT NULL;
-- (1) annual rates: all owner-occupied SFR and the arm's-length subsample; backfilled panel and observed-both-years pairs
COPY (
  SELECT smp, fill, y, count(*) pairs, round(100*avg(id_chg::INT),2) id_change, round(100*avg(lnp_chg::INT),2) modal_ln_prim_change,
         round(100*avg(lna_chg::INT),2) modal_ln_all_change, round(100*avg(sale_t1::INT),2) sale_in_t1,
         round(100*avg(tax_owner_chg::INT) FILTER (WHERE tax_both),2) tax_owner_ln_change
  FROM (SELECT '1_all_residential' smp, 'backfilled' fill, * FROM pr
        UNION ALL SELECT '1_all_residential', 'observed_only', * FROM pr WHERE obs0 AND obs1
        UNION ALL SELECT '2_all_res_one_primary', 'backfilled', * FROM pr WHERE one_prim
        UNION ALL SELECT '3_oo_sfr', 'backfilled', * FROM pr WHERE oo_sfr
        UNION ALL SELECT '3_oo_sfr', 'observed_only', * FROM pr WHERE oo_sfr AND obs0 AND obs1
        UNION ALL SELECT '4_oo_sfr_sold_2013_2020', 'backfilled', * FROM pr WHERE sold_ever
        UNION ALL SELECT '4_oo_sfr_sold_2013_2020', 'observed_only', * FROM pr WHERE sold_ever AND obs0 AND obs1)
  GROUP BY GROUPING SETS ((smp, fill, y), (smp, fill)) ORDER BY smp, fill, y NULLS LAST
) TO '/home/jr2728/ptax_explore/hh_strat/out/moveout_rates.csv' (HEADER);
-- (2) validation against sales: proxy firing rates by sale timing (arm's-length subsample, backfilled)
COPY (
  SELECT CASE WHEN sale_t1 THEN '1_sale_in_t+1' WHEN sale_t0 THEN '2_sale_in_t' WHEN sale_near THEN '3_sale_in_t-1_or_t+2' ELSE '4_no_sale_within_window' END sale_timing,
         count(*) pairs, round(100*avg(id_chg::INT),1) id_change, round(100*avg(lnp_chg::INT),1) modal_ln_prim_change, round(100*avg(lna_chg::INT),1) modal_ln_all_change,
         round(100*avg((id_chg AND lnp_chg)::INT),1) both_id_and_ln_prim, round(100*avg((id_chg AND NOT lnp_chg)::INT),1) id_only, round(100*avg((lnp_chg AND NOT id_chg)::INT),1) ln_prim_only,
         round(100*avg(tax_owner_chg::INT) FILTER (WHERE tax_both),1) tax_owner_ln_change, round(100*avg((obs0 AND obs1)::INT),1) pct_both_observed
  FROM pr WHERE sold_ever GROUP BY 1 ORDER BY 1
) TO '/home/jr2728/ptax_explore/hh_strat/out/moveout_validation.csv' (HEADER);
-- (3) 2x2 vs tax owner-name change, all owner-occupied SFR, backfilled
COPY (
  SELECT tax_owner_chg, count(*) pairs, round(100*avg(id_chg::INT),1) id_change, round(100*avg(lnp_chg::INT),1) modal_ln_prim_change, round(100*avg(lna_chg::INT),1) modal_ln_all_change
  FROM pr WHERE tax_both AND oo_sfr GROUP BY 1 ORDER BY 1
) TO '/home/jr2728/ptax_explore/hh_strat/out/moveout_vs_taxowner.csv' (HEADER);
SELECT * FROM read_csv('/home/jr2728/ptax_explore/hh_strat/out/moveout_rates.csv') WHERE y IS NULL;
SELECT * FROM read_csv('/home/jr2728/ptax_explore/hh_strat/out/moveout_validation.csv');
SELECT * FROM read_csv('/home/jr2728/ptax_explore/hh_strat/out/moveout_vs_taxowner.csv');
