-- Residential tax addresses (1% building sample) matched to Data Axle in some year 2012-2021: in how many of the 10 years does DA have a
-- record there? Address = bkey (ZIP5 + street + house number). Tax attributes from the latest tax year 2012-2021 at the address.
SET threads=4;
CREATE OR REPLACE TABLE tx AS
  SELECT bkey, arg_max(struct_pack(pic := property_indicator_code, occ := owner_occupancy_code, corp := coalesce(owner_1_corporate_indicator, false),
                                   noimpr := coalesce(calculated_improvement_value, 0) = 0 AND coalesce(total_living_area_square_feet_all_buildings, 0) = 0,
                                   mail_here := left(coalesce(mailing_zip_code,''),5) = left(situs_zip_code,5)
                                                AND upper(trim(coalesce(mailing_house_number,''))) = upper(trim(situs_house_number))
                                                AND upper(trim(coalesce(mailing_street_name,''))) = upper(trim(situs_street_name)),
                                   yb := try_cast(year_built AS INT)), tax_year) a,
         count(DISTINCT tax_year) tax_years
  FROM '/home/jr2728/ptax_explore/samples/tax_bld1pct.parquet' WHERE tax_year BETWEEN 2012 AND 2021 GROUP BY bkey;
CREATE OR REPLACE TABLE r AS
  SELECT bkey, a.pic pic, CASE WHEN a.corp THEN 'corporate' WHEN a.occ = 'O' OR a.mail_here THEN 'owner-occupied' ELSE 'absentee' END own, a.yb yb, tax_years
  FROM tx WHERE a.pic IN (10, 11, 21, 22) AND NOT a.noimpr;
CREATE OR REPLACE TABLE d AS
  SELECT bkey, count(DISTINCT record_year) yrs_any, count(DISTINCT record_year) FILTER (WHERE primary_family_ind) yrs_prim,
         count(DISTINCT family_id) FILTER (WHERE primary_family_ind) n_prim_ids
  FROM '/home/jr2728/ptax_explore/samples/da_bld1pct.parquet' WHERE record_year BETWEEN 2012 AND 2021 GROUP BY bkey;
CREATE OR REPLACE TABLE j AS SELECT r.*, coalesce(d.yrs_any, 0) yrs_any, coalesce(d.yrs_prim, 0) yrs_prim, coalesce(d.n_prim_ids, 0) n_prim_ids FROM r LEFT JOIN d USING (bkey);
-- (1) all residential addresses: matched in any year?
SELECT count(*) residential_addresses, round(100*avg((yrs_any > 0)::INT),1) pct_matched_any_year FROM j;
-- (2) distribution of years with any DA record / with a primary family, among matched addresses
SELECT yrs, sum((yrs_any = yrs)::INT) n_any, round(100*avg((yrs_any = yrs)::INT),1) pct_any, round(100*avg((yrs_prim = yrs)::INT),1) pct_with_primary
FROM (SELECT * FROM j WHERE yrs_any > 0), (SELECT unnest(range(0, 11)) yrs) GROUP BY yrs ORDER BY yrs;
-- (3) by property type and occupancy; also built before 2012 and on the roll all 10 tax years (exposure is the full decade)
SELECT CASE pic WHEN 10 THEN 'single family' WHEN 11 THEN 'condo' WHEN 21 THEN '2-4 units' ELSE 'apartment' END ptype, own,
       count(*) n, round(avg(yrs_any),2) mean_yrs, round(100*avg((yrs_any = 10)::INT),1) pct_10, round(100*avg((yrs_any >= 8)::INT),1) pct_8plus,
       round(100*avg((yrs_any <= 3)::INT),1) pct_le3, round(avg(n_prim_ids),2) mean_primary_ids
FROM j WHERE yrs_any > 0 GROUP BY ALL ORDER BY ptype, own;
SELECT 'built<2012, on roll 10 yrs, SFR owner-occ.' grp, count(*) n, round(avg(yrs_any),2) mean_yrs, round(100*avg((yrs_any = 10)::INT),1) pct_10,
       round(100*avg((yrs_any >= 8)::INT),1) pct_8plus, round(100*avg((yrs_prim = 10)::INT),1) pct_10_primary
FROM j WHERE yrs_any > 0 AND tax_years = 10 AND yb < 2012 AND pic = 10 AND own = 'owner-occupied';
