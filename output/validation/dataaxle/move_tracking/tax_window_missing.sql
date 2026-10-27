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
CREATE OR REPLACE TABLE j AS SELECT *, win - n_tax miss_tax, win - n_da miss_da FROM j;
CREATE OR REPLACE MACRO mb(m) AS CASE WHEN m >= 5 THEN '5+' ELSE m::VARCHAR END;
-- (1) missing years since first on the tax roll: tax vs Data Axle (addresses matched to DA in any year)
SELECT mb(miss) missing_years, count(*) FILTER (WHERE src = 'tax') n_tax, round(100*count(*) FILTER (WHERE src = 'tax') / sum(count(*) FILTER (WHERE src = 'tax')) OVER (),1) pct_tax,
       count(*) FILTER (WHERE src = 'da') n_da, round(100*count(*) FILTER (WHERE src = 'da') / sum(count(*) FILTER (WHERE src = 'da')) OVER (),1) pct_da
FROM (SELECT 'tax' src, miss_tax miss FROM j UNION ALL SELECT 'da', miss_da FROM j WHERE n_da_any > 0) GROUP BY ALL ORDER BY missing_years;
-- (2) same, by first tax year (window length)
SELECT y0, count(*) n, round(avg(miss_tax),2) mean_miss_tax, round(avg(miss_da) FILTER (WHERE n_da_any > 0),2) mean_miss_da,
       round(100*avg((miss_tax = 0)::INT),1) tax_0, round(100*avg((miss_tax = 1)::INT),1) tax_1, round(100*avg((miss_tax >= 2)::INT),1) tax_2plus,
       round(100*avg((miss_da = 0)::INT) FILTER (WHERE n_da_any > 0),1) da_0, round(100*avg((miss_da = 1)::INT) FILTER (WHERE n_da_any > 0),1) da_1,
       round(100*avg((miss_da >= 2)::INT) FILTER (WHERE n_da_any > 0),1) da_2plus
FROM j GROUP BY y0 ORDER BY y0;
-- (3) addresses with no missing tax year: DA missing years by type and occupancy
SELECT CASE pic WHEN 10 THEN 'single family' WHEN 11 THEN 'condo' WHEN 21 THEN '2-4 units' ELSE 'apartment' END ptype, own, count(*) n,
       round(avg(miss_da),2) mean_miss_da, round(100*avg((miss_da = 0)::INT),1) da_0, round(100*avg((miss_da = 1)::INT),1) da_1,
       round(100*avg((miss_da = 2)::INT),1) da_2, round(100*avg((miss_da BETWEEN 3 AND 4)::INT),1) da_3_4, round(100*avg((miss_da >= 5)::INT),1) da_5plus
FROM j WHERE miss_tax = 0 AND n_da_any > 0 GROUP BY ALL ORDER BY ptype, own;
