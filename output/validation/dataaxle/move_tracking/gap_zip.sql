-- Isolated tax gaps (county-year < 10% missing): do they cluster by ZIP-year (town-level rolls)? Same construction as gap_evidence.sql.
SET threads=4;
CREATE OR REPLACE TABLE t AS
  SELECT bkey, tax_year::INT y, any_value(fips_code) fips, any_value(situs_state) st, any_value(left(situs_zip_code,5)) z
  FROM '/home/jr2728/ptax_explore/samples/tax_bld1pct.parquet'
  WHERE tax_year BETWEEN 2012 AND 2021 AND property_indicator_code IN (10, 11, 21, 22) GROUP BY bkey, tax_year;
CREATE OR REPLACE TABLE a AS SELECT bkey, any_value(fips) fips, any_value(st) st, any_value(z) z, list(DISTINCT y) ys FROM t GROUP BY bkey;
CREATE OR REPLACE TABLE br AS
  SELECT a.bkey, a.fips, a.st, a.z, yy.y, list_contains(a.ys, yy.y) present
  FROM a, (SELECT unnest(range(2013, 2021)) y) yy WHERE list_contains(a.ys, yy.y - 1) AND list_contains(a.ys, yy.y + 1);
CREATE OR REPLACE TABLE cyr AS SELECT fips, y, avg((NOT present)::INT) miss_share FROM br GROUP BY fips, y;
CREATE OR REPLACE TABLE zyr AS SELECT z, y, count(*) n_bridge, avg((NOT present)::INT) zmiss FROM br GROUP BY z, y;
CREATE OR REPLACE TABLE iso AS SELECT b.*, zy.zmiss, zy.n_bridge FROM br b JOIN cyr c USING (fips, y) JOIN zyr zy USING (z, y) WHERE NOT b.present AND c.miss_share < 0.1;
SELECT CASE WHEN zmiss >= 0.9 THEN '1: >=90% of ZIP missing' WHEN zmiss >= 0.5 THEN '2: 50-90%' WHEN zmiss >= 0.1 THEN '3: 10-50%' ELSE '4: <10%' END grp,
       count(*) gaps, round(100*count(*)/sum(count(*)) OVER (),1) pct, round(avg(n_bridge),0) mean_zip_parcels
FROM iso GROUP BY grp ORDER BY grp;
SELECT st, count(*) gaps, round(100*avg((zmiss >= 0.5)::INT),1) pct_in_zip_years_half_missing FROM iso GROUP BY st ORDER BY gaps DESC LIMIT 12;
-- state share of isolated gaps vs state share of bridging address-years in covered county-years
SELECT st, round(100*gaps/sum(gaps) OVER (),1) pct_of_isolated_gaps, round(100*base/sum(base) OVER (),1) pct_of_covered_address_years FROM (
  SELECT b.st, sum((NOT b.present)::INT) gaps, count(*) base FROM br b JOIN cyr c USING (fips, y) WHERE c.miss_share < 0.1 GROUP BY b.st) ORDER BY gaps DESC LIMIT 10;
