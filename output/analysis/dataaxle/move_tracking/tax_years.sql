-- Residential tax addresses (1% building sample): in how many tax years 2012-2021 does the address appear in the tax table?
-- Missing years split into: county absent from the sample that year (county-level coverage gap) vs address missing while its county is present.
SET threads=4;
CREATE OR REPLACE TABLE t AS
  SELECT bkey, tax_year::INT y, fips_code fips, property_indicator_code pic,
         coalesce(calculated_improvement_value, 0) = 0 AND coalesce(total_living_area_square_feet_all_buildings, 0) = 0 noimpr,
         try_cast(year_built AS INT) yb, owner_occupancy_code occ, coalesce(owner_1_corporate_indicator, false) corp
  FROM '/home/jr2728/ptax_explore/samples/tax_bld1pct.parquet' WHERE tax_year BETWEEN 2012 AND 2021;
CREATE OR REPLACE TABLE a AS
  SELECT bkey, any_value(fips) fips, arg_max(struct_pack(pic := pic, noimpr := noimpr, yb := yb, occ := occ, corp := corp), y) l,
         list(DISTINCT y) ys, count(DISTINCT y) n_years, min(y) y0, max(y) y1
  FROM t GROUP BY bkey;
CREATE OR REPLACE TABLE r AS SELECT * FROM a WHERE l.pic IN (10, 11, 21, 22) AND NOT l.noimpr;
CREATE OR REPLACE TABLE cy AS SELECT DISTINCT fips, y FROM t;
-- (1) distribution of tax years present
SELECT n_years, count(*) n, round(100*count(*)/sum(count(*)) OVER (),1) pct,
       round(100*sum(count(*)) OVER (ORDER BY n_years DESC)/sum(count(*)) OVER (),1) pct_at_least
FROM r GROUP BY n_years ORDER BY n_years;
-- (2) built before 2012, single family: missing years by cause
CREATE OR REPLACE TABLE m AS
  SELECT r.bkey, r.fips, r.n_years, yy.y, list_contains(r.ys, yy.y) present, (r.fips, yy.y) IN (SELECT (fips, y) FROM cy) county_present
  FROM (SELECT * FROM r WHERE l.pic = 10 AND l.yb < 2012) r, (SELECT unnest(range(2012, 2022)) y) yy;
SELECT count(DISTINCT bkey) addresses, round(100*avg((NOT present)::INT),2) pct_address_years_missing,
       round(100*avg((NOT present AND NOT county_present)::INT),2) pct_missing_county_absent,
       round(100*avg((NOT present AND county_present)::INT),2) pct_missing_county_present
FROM m;
SELECT round(100*avg((n_years = 10)::INT),1) pct_all_10, round(100*avg((n_years >= 8)::INT),1) pct_8plus,
       round(100*avg((n_missing_cp = 0)::INT),1) pct_no_gap_when_county_present
FROM (SELECT bkey, any_value(n_years) n_years, sum((NOT present AND county_present)::INT) n_missing_cp FROM m GROUP BY bkey);
-- (3) county-years missing: which years
SELECT y, count(*) counties_in_sample FROM cy GROUP BY y ORDER BY y;
