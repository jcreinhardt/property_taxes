-- Run locally (duckdb CLI) in this folder on odd_cy_town_years.csv (output of odd_cy_town_years.sql on Bouchet).
-- Town x tax_year coverage patterns 2011-2017 for towns with >= 500 rows at peak. A town-year counts as "covered"
-- if it has >= 20% of the town's maximum annual row count (filters stray mis-citied rows).
CREATE TABLE ty AS SELECT * FROM 'odd_cy_town_years.csv' WHERE city IS NOT NULL;
CREATE TABLE cov AS SELECT fips, city, yr, n_rows, n_rows >= 0.2*max(n_rows) OVER (PARTITION BY fips, city) covered, max(n_rows) OVER (PARTITION BY fips, city) mx FROM ty;
CREATE TABLE towns AS SELECT DISTINCT fips, city, mx FROM cov WHERE mx >= 500;
CREATE TABLE yrs AS SELECT unnest(range(2011, 2018)) yr;
CREATE TABLE grid AS SELECT t.fips, t.city, t.mx, y.yr FROM towns t CROSS JOIN yrs y;
CREATE TABLE pat AS SELECT g.fips, g.city, g.mx, string_agg(CASE WHEN coalesce(c.covered,false) THEN '1' ELSE '0' END, '' ORDER BY g.yr) pattern
  FROM grid g LEFT JOIN cov c ON c.fips=g.fips AND c.city=g.city AND c.yr=g.yr GROUP BY ALL;
SELECT fips//1000 st, pattern AS pattern_2011_2017, count(*) towns, sum(mx) parcels FROM pat GROUP BY ALL HAVING count(*)>=3 ORDER BY st, towns DESC;
SELECT fips, count(*) towns, count(*) FILTER (WHERE substr(pattern,3,2)='10') y13_only, count(*) FILTER (WHERE substr(pattern,3,2)='01') y14_only,
  count(*) FILTER (WHERE substr(pattern,3,2)='11') both_13_14, count(*) FILTER (WHERE substr(pattern,3,2)='00') neither,
  count(*) FILTER (WHERE substr(pattern,1,1)='1') y11, count(*) FILTER (WHERE substr(pattern,2,1)='1') y12, count(*) FILTER (WHERE substr(pattern,5,1)='1') y15
FROM pat GROUP BY ALL ORDER BY ALL;
COPY (SELECT * FROM pat ORDER BY fips, city) TO 'odd_cy_town_patterns_2011_2017.csv' (HEADER);
