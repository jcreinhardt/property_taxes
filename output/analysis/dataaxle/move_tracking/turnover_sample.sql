-- Owner turnover at single-family parcels, 1% building sample: tax rolls and deeds vs Data Axle at the same buildings.
SET threads=4;
CREATE OR REPLACE TABLE t AS
  SELECT clip, tax_year::INT y, bkey, property_indicator_code pic, owner_occupancy_code occ, coalesce(owner_1_corporate_indicator, false) corp,
         o1_ln, o1_full,
         (left(coalesce(mailing_zip_code,''),5) = left(situs_zip_code,5) AND upper(trim(coalesce(mailing_house_number,''))) = upper(trim(situs_house_number))
          AND upper(trim(coalesce(mailing_street_name,''))) = upper(trim(situs_street_name))) mail_here
  FROM '/home/jr2728/ptax_explore/samples/tax_bld1pct.parquet' WHERE property_indicator_code = 10 AND tax_year BETWEEN 2006 AND 2022;
-- (1) tax roll: owner name change t -> t+1 at owner-occupied, non-corporate SFR parcels
CREATE OR REPLACE TABLE tr AS
  SELECT a.y, count(*) n,
         avg((a.o1_ln IS DISTINCT FROM b.o1_ln)::INT) FILTER (WHERE a.o1_ln IS NOT NULL AND b.o1_ln IS NOT NULL) owner_last_name_changed,
         avg((a.o1_full IS DISTINCT FROM b.o1_full)::INT) FILTER (WHERE a.o1_full IS NOT NULL AND b.o1_full IS NOT NULL) owner_full_name_changed
  FROM t a JOIN t b ON b.clip = a.clip AND b.y = a.y + 1
  WHERE (a.occ = 'O' OR a.mail_here) AND NOT a.corp GROUP BY a.y;
-- (2) deeds: arm's-length sales (amount > 0, not inter-family) per year / SFR parcels in that tax year
CREATE OR REPLACE TABLE d AS
  SELECT year(sale_derived_date) y, count(DISTINCT clip) sold
  FROM '/home/jr2728/ptax_explore/samples/deeds_bld1pct.parquet'
  WHERE property_indicator_code = 10 AND sale_amount > 0 AND primary_category_code = 'A' AND NOT coalesce(interfamily_related_indicator, false)
    AND clip IN (SELECT clip FROM t) GROUP BY 1;
CREATE OR REPLACE TABLE ds AS SELECT y, count(DISTINCT clip) stock FROM t GROUP BY y;
-- (3) Data Axle at the same SFR buildings: primary family changes t -> t+1 (address-level turnover), owners (status >= 7) only
CREATE OR REPLACE TABLE da AS
  SELECT record_year::INT y, bkey, family_id fid, owner_renter_status ors, length_of_residence lor
  FROM '/home/jr2728/ptax_explore/samples/da_bld1pct.parquet'
  WHERE primary_family_ind AND unit_num IS NULL AND record_year <> 2022 AND bkey IN (SELECT bkey FROM t);
CREATE OR REPLACE TABLE dt AS
  SELECT a.y, count(*) n, avg((a.fid <> b.fid)::INT) primary_changed, avg((a.fid <> b.fid AND b.lor <= 1)::INT) primary_changed_lor_reset
  FROM da a JOIN da b ON b.bkey = a.bkey AND b.y = a.y + 1
  WHERE a.ors >= 7 AND a.bkey IN (SELECT bkey FROM t WHERE (occ = 'O' OR mail_here) AND NOT corp) GROUP BY a.y;
SELECT tr.y, tr.n tax_pairs, round(100*owner_last_name_changed,2) tax_owner_ln_chg, round(100*owner_full_name_changed,2) tax_owner_full_chg,
       round(100*d.sold/ds.stock,2) deed_sales_pct, dt.n da_pairs, round(100*primary_changed,2) da_primary_chg, round(100*primary_changed_lor_reset,2) da_primary_chg_lor1
FROM tr LEFT JOIN d USING (y) LEFT JOIN ds USING (y) LEFT JOIN dt USING (y) ORDER BY y;
