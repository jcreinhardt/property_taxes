-- (1) Why do tax owner surnames change more often than homes sell arm's-length? Owner-occupied non-corporate SFR, 1% building sample, pairs t->t+1.
-- (2) Timing of the Data Axle primary change around each arm's-length sale (union over adjacent pairs).
SET threads=4;
CREATE OR REPLACE TABLE tx AS
  SELECT bkey, tax_year::INT y, min(o1_ln) o1, min(o1_full) o1f, min(o2_ln) o2, min(o2_full) o2f, bool_or(coalesce(owner_1_corporate_indicator,false)) corp,
         any_value(property_indicator_code) pic, any_value(owner_occupancy_code) occ,
         bool_or(left(coalesce(mailing_zip_code,''),5) = left(situs_zip_code,5) AND upper(trim(coalesce(mailing_house_number,''))) = upper(trim(situs_house_number))
                 AND upper(trim(coalesce(mailing_street_name,''))) = upper(trim(situs_street_name))) mail_here,
         bool_or(coalesce(calculated_improvement_value,0) = 0 AND coalesce(total_living_area_square_feet_all_buildings,0) = 0) noimpr
  FROM '/home/jr2728/ptax_explore/samples/tax_bld1pct.parquet' WHERE tax_year BETWEEN 2012 AND 2021 GROUP BY 1, 2;
CREATE OR REPLACE TABLE oo AS SELECT bkey FROM tx WHERE y = (SELECT max(y) FROM tx t2 WHERE t2.bkey = tx.bkey) AND pic = 10 AND NOT corp AND NOT noimpr AND (occ = 'O' OR mail_here);
CREATE OR REPLACE TABLE deeds AS
  SELECT bkey, year(sale_derived_date) y, primary_category_code cat, coalesce(interfamily_related_indicator,false) fam, sale_amount amt, deed_category_type_code dtype
  FROM '/home/jr2728/ptax_explore/samples/deeds_bld1pct.parquet' WHERE year(sale_derived_date) BETWEEN 2010 AND 2023 AND bkey IN (SELECT bkey FROM oo);
CREATE OR REPLACE TABLE pr AS
  SELECT a.bkey, a.y, a.o1 <> b.o1 chg, a.o1 = b.o1 same, b.o1 = a.o2 new_is_old_owner2, a.o1f = b.o1f full_same, a.corp <> b.corp corp_flip,
         c.o1 = a.o1 reverts,
         EXISTS (SELECT 1 FROM deeds d WHERE d.bkey = a.bkey AND d.y IN (a.y, a.y + 1) AND d.cat = 'A' AND NOT d.fam AND d.amt > 0) arms,
         EXISTS (SELECT 1 FROM deeds d WHERE d.bkey = a.bkey AND d.y IN (a.y, a.y + 1) AND NOT (d.cat = 'A' AND NOT d.fam AND d.amt > 0)) other_deed,
         EXISTS (SELECT 1 FROM deeds d WHERE d.bkey = a.bkey AND d.y IN (a.y - 1, a.y + 2)) deed_adjacent
  FROM tx a JOIN tx b ON b.bkey = a.bkey AND b.y = a.y + 1 LEFT JOIN tx c ON c.bkey = a.bkey AND c.y = a.y + 2
  WHERE a.bkey IN (SELECT bkey FROM oo) AND a.o1 IS NOT NULL AND b.o1 IS NOT NULL;
SELECT count(*) pairs, round(100*avg(chg::INT),2) owner_ln_change_pct FROM pr;
SELECT CASE WHEN arms THEN '1_arms_length_deed_in_t_or_t+1' WHEN other_deed THEN '2_other_deed_in_t_or_t+1 (interfamily/trust/quitclaim/etc)'
            WHEN deed_adjacent THEN '3_deed_in_t-1_or_t+2' WHEN new_is_old_owner2 THEN '4_no_deed: new owner1 = old owner2 (order swap/survivor)'
            WHEN full_same THEN '5_no_deed: full name same, last-name parse differs' WHEN coalesce(reverts,false) THEN '6_no_deed: reverts to old surname in t+2 (flicker)'
            ELSE '7_no_deed: other' END why,
       count(*) n, round(100*count(*)/sum(count(*)) OVER (),1) pct_of_changes, round(100*count(*)/(SELECT count(*) FROM pr),2) pct_of_all_pairs
FROM pr WHERE chg GROUP BY 1 ORDER BY 1;
-- deed categories behind "other deed" changes
SELECT d.cat, d.fam, d.amt > 0 priced, count(*) n FROM pr p JOIN deeds d ON d.bkey = p.bkey AND d.y IN (p.y, p.y + 1)
WHERE p.chg AND NOT p.arms GROUP BY ALL ORDER BY n DESC LIMIT 8;
-- (2) timing of DA primary change around each arm's-length sale year s (backfilled panel from moveout_proxies logic, rebuilt briefly)
CREATE OR REPLACE TABLE per AS
  SELECT bkey, record_year::INT y, min(family_id) FILTER (WHERE primary_family_ind) prim_fid
  FROM '/home/jr2728/ptax_explore/samples/da_bld1pct.parquet' WHERE record_year BETWEEN 2010 AND 2023 AND record_year <> 2022 AND bkey IN (SELECT bkey FROM oo) GROUP BY 1, 2;
CREATE OR REPLACE TABLE grid AS SELECT o.bkey, yy.y FROM oo o CROSS JOIN (SELECT unnest(range(2010, 2024)) y) yy;
CREATE OR REPLACE TABLE pan AS
  SELECT g.bkey, g.y, last_value(p.prim_fid IGNORE NULLS) OVER (PARTITION BY g.bkey ORDER BY g.y ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) fid
  FROM grid g LEFT JOIN per p USING (bkey, y);
CREATE OR REPLACE TABLE chg AS SELECT a.bkey, a.y, a.fid <> b.fid c FROM pan a JOIN pan b ON b.bkey = a.bkey AND b.y = a.y + 1 WHERE a.fid IS NOT NULL AND b.fid IS NOT NULL;
CREATE OR REPLACE TABLE sl AS SELECT DISTINCT bkey, y s FROM deeds WHERE cat = 'A' AND NOT fam AND amt > 0 AND y BETWEEN 2013 AND 2020;
CREATE OR REPLACE TABLE st AS
  SELECT s.bkey, s.s, c0.c c_before, c1.c c_same, c2.c c_after, c3.c c_after2
  FROM sl s LEFT JOIN chg c0 ON c0.bkey = s.bkey AND c0.y = s.s - 2   -- pair s-2 -> s-1 (before the sale)
            LEFT JOIN chg c1 ON c1.bkey = s.bkey AND c1.y = s.s - 1   -- pair s-1 -> s  (file of the sale year)
            LEFT JOIN chg c2 ON c2.bkey = s.bkey AND c2.y = s.s       -- pair s -> s+1
            LEFT JOIN chg c3 ON c3.bkey = s.bkey AND c3.y = s.s + 1;  -- pair s+1 -> s+2
SELECT count(*) sales, round(100*avg(c_before::INT),1) chg_pair_before_sale_year, round(100*avg(c_same::INT),1) chg_pair_ending_in_sale_year,
       round(100*avg(c_after::INT),1) chg_pair_after, round(100*avg(c_after2::INT),1) chg_pair_after2,
       round(100*avg((c_same OR c_after)::INT),1) chg_in_either_of_two_pairs, round(100*avg((c_same AND c_after)::INT),1) chg_in_both,
       round(100*avg((c_before OR c_same OR c_after OR c_after2)::INT),1) chg_in_any_of_four
FROM st WHERE c_same IS NOT NULL AND c_after IS NOT NULL;
-- base rate over 2 and 4 consecutive pairs at homes with no sale 2010-2023
SELECT round(100*avg((a.c OR b.c)::INT),1) base_two_pairs, round(100*avg((z.c OR a.c OR b.c OR d.c)::INT),1) base_four_pairs
FROM chg a JOIN chg b ON b.bkey = a.bkey AND b.y = a.y + 1 JOIN chg z ON z.bkey = a.bkey AND z.y = a.y - 1 JOIN chg d ON d.bkey = a.bkey AND d.y = a.y + 2
WHERE a.bkey NOT IN (SELECT bkey FROM deeds) AND a.y BETWEEN 2013 AND 2019;
