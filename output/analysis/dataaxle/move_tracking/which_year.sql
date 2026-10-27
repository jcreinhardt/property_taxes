SET threads=4;
CREATE OR REPLACE TABLE t AS SELECT DISTINCT bkey, tax_year::INT y, fips_code fips, situs_state st FROM '/home/jr2728/ptax_explore/samples/tax_bld1pct.parquet'
  WHERE tax_year BETWEEN 2012 AND 2021 AND property_indicator_code IN (10, 11, 21, 22);
CREATE OR REPLACE TABLE a AS SELECT bkey, any_value(st) st, min(y) y0, max(y) y1, list(DISTINCT y) ys, count(DISTINCT y) n FROM t GROUP BY bkey;
CREATE OR REPLACE TABLE g AS
  SELECT a.bkey, a.st, yy.y missing_year FROM a, (SELECT unnest(range(2012, 2022)) y) yy
  WHERE yy.y BETWEEN a.y0 AND a.y1 AND NOT list_contains(a.ys, yy.y) AND a.y1 - a.y0 + 1 - a.n = 1;
SELECT missing_year, count(*) n, round(100*count(*)/sum(count(*)) OVER (),1) pct FROM g GROUP BY 1 ORDER BY 1;
SELECT missing_year, st, count(*) n FROM g GROUP BY ALL ORDER BY n DESC LIMIT 12;
