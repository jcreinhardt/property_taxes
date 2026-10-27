-- Residential tax addresses (1% building sample; structure on roll): share of years observed between the first tax year (>= 2012) and 2021,
-- in the tax table and in Data Axle over the same window.
SET threads=4;
CREATE OR REPLACE TABLE t AS
  SELECT bkey, tax_year::INT y, property_indicator_code pic,
         coalesce(calculated_improvement_value, 0) = 0 AND coalesce(total_living_area_square_feet_all_buildings, 0) = 0 noimpr,
         try_cast(year_built AS INT) yb, owner_occupancy_code occ, coalesce(owner_1_corporate_indicator, false) corp,
         left(coalesce(mailing_zip_code,''),5) = left(situs_zip_code,5) AND upper(trim(coalesce(mailing_house_number,''))) = upper(trim(situs_house_number))
           AND upper(trim(coalesce(mailing_street_name,''))) = upper(trim(situs_street_name)) mail_here
  FROM '/home/jr2728/ptax_explore/samples/tax_bld1pct.parquet' WHERE tax_year BETWEEN 2012 AND 2021;
CREATE OR REPLACE TABLE a AS
  SELECT bkey, arg_max(struct_pack(pic := pic, noimpr := noimpr, yb := yb, occ := occ, corp := corp, mail_here := mail_here), y) l,
         min(y) y0, count(DISTINCT y) n_tax
  FROM t GROUP BY bkey;
CREATE OR REPLACE TABLE r AS
  SELECT bkey, y0, n_tax, 2021 - y0 + 1 win, n_tax / (2021 - y0 + 1) frac_tax, l.pic pic, l.yb yb,
         CASE WHEN l.corp THEN 'corporate' WHEN l.occ = 'O' OR l.mail_here THEN 'owner-occupied' ELSE 'absentee' END own
  FROM a WHERE l.pic IN (10, 11, 21, 22) AND NOT l.noimpr;
CREATE OR REPLACE TABLE d AS SELECT DISTINCT bkey, record_year::INT y FROM '/home/jr2728/ptax_explore/samples/da_bld1pct.parquet'
  WHERE record_year BETWEEN 2012 AND 2021;
CREATE OR REPLACE TABLE j AS
  SELECT r.*, (SELECT count(*) FROM d WHERE d.bkey = r.bkey AND d.y >= r.y0) n_da,
         (SELECT count(*) FROM d WHERE d.bkey = r.bkey) n_da_any
  FROM r;
CREATE OR REPLACE TABLE j AS SELECT *, n_da / win frac_da FROM j;
CREATE OR REPLACE MACRO bin(f) AS CASE WHEN f >= 1 THEN '1: all years' WHEN f >= 0.9 THEN '2: 90-99%' WHEN f >= 0.8 THEN '3: 80-89%'
                                      WHEN f >= 0.5 THEN '4: 50-79%' WHEN f > 0 THEN '5: < 50%' ELSE '6: none' END;
-- (1) tax: share of window observed (all residential; and SFR built before y0)
SELECT 'tax, all residential' smpl, bin(frac_tax) b, count(*) n, round(100*count(*)/sum(count(*)) OVER (),1) pct FROM j GROUP BY ALL
UNION ALL
SELECT 'DA, same addresses & window (matched in any year)', bin(frac_da), count(*), round(100*count(*)/sum(count(*)) OVER (),1) FROM j WHERE n_da_any > 0 GROUP BY ALL
ORDER BY smpl DESC, b;
SELECT round(100*avg(frac_tax),1) mean_tax, round(100*avg(frac_da) FILTER (WHERE n_da_any > 0),1) mean_da_matched, count(*) n,
       round(100*avg((n_da_any > 0)::INT),1) pct_matched FROM j;
-- (2) by first tax year
SELECT y0, count(*) n, round(100*avg(frac_tax),1) mean_tax_pct, round(100*avg((frac_tax = 1)::INT),1) tax_all,
       round(100*avg(frac_da) FILTER (WHERE n_da_any > 0),1) mean_da_pct, round(100*avg((frac_da >= 1)::INT) FILTER (WHERE n_da_any > 0),1) da_all
FROM j GROUP BY y0 ORDER BY y0;
-- (3) houses complete in tax from first year: DA coverage by occupancy and type
SELECT CASE pic WHEN 10 THEN 'single family' WHEN 11 THEN 'condo' WHEN 21 THEN '2-4 units' ELSE 'apartment' END ptype, own, count(*) n,
       round(100*avg(frac_da),1) mean_da_pct, round(100*avg((frac_da >= 1)::INT),1) da_all, round(100*avg((frac_da >= 0.8)::INT),1) da_80plus,
       round(100*avg((frac_da < 0.5)::INT),1) da_below_half
FROM j WHERE frac_tax = 1 AND n_da_any > 0 GROUP BY ALL ORDER BY ptype, own;
