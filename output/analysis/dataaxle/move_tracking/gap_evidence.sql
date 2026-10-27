-- Are tax-roll gaps whole county-years? Residential addresses (1% building sample), gap = year strictly between first and last tax year
-- with no row. For each county-year: share of "bridging" parcels (present in y-1 and y+1) that are missing in y.
SET threads=4;
CREATE OR REPLACE TABLE t AS
  SELECT bkey, tax_year::INT y, any_value(fips_code) fips, any_value(situs_state) st, list(DISTINCT clip) clips,
         max(try_cast(year_built AS INT)) yb, max(sale_date) sale, any_value(property_indicator_code) pic
  FROM '/home/jr2728/ptax_explore/samples/tax_bld1pct.parquet'
  WHERE tax_year BETWEEN 2012 AND 2021 AND property_indicator_code IN (10, 11, 21, 22) GROUP BY bkey, tax_year;
CREATE OR REPLACE TABLE a AS SELECT bkey, any_value(fips) fips, any_value(st) st, min(y) y0, max(y) y1, list(DISTINCT y) ys FROM t GROUP BY bkey;
-- bridging address-years: present in y-1 and y+1
CREATE OR REPLACE TABLE br AS
  SELECT a.bkey, a.fips, a.st, yy.y, list_contains(a.ys, yy.y) present
  FROM a, (SELECT unnest(range(2013, 2021)) y) yy
  WHERE list_contains(a.ys, yy.y - 1) AND list_contains(a.ys, yy.y + 1);
CREATE OR REPLACE TABLE cyr AS SELECT fips, y, count(*) n_bridge, sum((NOT present)::INT) n_miss, avg((NOT present)::INT) miss_share FROM br GROUP BY fips, y;
CREATE OR REPLACE TABLE syr AS SELECT st, y, count(*) n_bridge, sum((NOT present)::INT) n_miss, avg((NOT present)::INT) miss_share FROM br GROUP BY st, y;
-- (1) distribution of gaps by how much of their county-year / state-year is missing
SELECT 'county-year' lvl, CASE WHEN c.miss_share >= 0.9 THEN '1: >=90% of county missing' WHEN c.miss_share >= 0.5 THEN '2: 50-90%' WHEN c.miss_share >= 0.1 THEN '3: 10-50%' ELSE '4: <10% (isolated parcels)' END grp,
       count(*) gaps, round(100*count(*)/sum(count(*)) OVER (),1) pct
FROM br b JOIN cyr c USING (fips, y) WHERE NOT b.present GROUP BY ALL
UNION ALL
SELECT 'state-year', CASE WHEN s.miss_share >= 0.9 THEN '1: >=90% of state missing' WHEN s.miss_share >= 0.5 THEN '2: 50-90%' WHEN s.miss_share >= 0.1 THEN '3: 10-50%' ELSE '4: <10% (isolated parcels)' END,
       count(*), round(100*count(*)/sum(count(*)) OVER (),1)
FROM br b JOIN syr s USING (st, y) WHERE NOT b.present GROUP BY ALL ORDER BY lvl, grp;
-- (2) overall gap rate, and gap rate in county-years where < 10% missing (the idiosyncratic residual)
SELECT count(*) bridging_address_years, round(100*avg((NOT present)::INT),2) pct_missing_all,
       round(100*avg((NOT present)::INT) FILTER (WHERE c.miss_share < 0.1),2) pct_missing_in_covered_county_years,
       round(100*avg((c.miss_share >= 0.9)::INT),2) pct_address_years_in_dropped_county_years
FROM br b JOIN cyr c USING (fips, y);
-- (3) top county-years by gaps
SELECT fips, st, y, n_bridge, n_miss, round(100*miss_share,1) pct_missing FROM cyr JOIN (SELECT DISTINCT fips, st FROM a) USING (fips) ORDER BY n_miss DESC LIMIT 15;
-- (4) count of county-years that are (near-)fully dropped, by year
SELECT y, count(*) county_years, sum((miss_share >= 0.9)::INT) fully_dropped, sum((miss_share BETWEEN 0.1 AND 0.9)::INT) partially_dropped FROM cyr WHERE n_bridge >= 20 GROUP BY y ORDER BY y;
-- (5) isolated gaps (county-year < 10% missing): parcel-level correlates
CREATE OR REPLACE TABLE iso AS
  SELECT b.bkey, b.y, p.clips c0, n.clips c1, p.yb, n.yb yb1, n.sale sale1, p.sale sale0
  FROM br b JOIN cyr c USING (fips, y) JOIN t p ON p.bkey = b.bkey AND p.y = b.y - 1 JOIN t n ON n.bkey = b.bkey AND n.y = b.y + 1
  WHERE NOT b.present AND c.miss_share < 0.1;
SELECT count(*) isolated_gaps,
       round(100*avg((len(list_intersect(c0, c1)) = 0)::INT),1) pct_clip_changed_across_gap,
       round(100*avg((len(c0) <> len(c1))::INT),1) pct_n_parcels_changed,
       round(100*avg((coalesce(yb1, yb) >= y - 1)::INT),1) pct_built_around_gap,
       round(100*avg((sale1 IS NOT NULL AND year(sale1) BETWEEN y - 1 AND y + 1)::INT),1) pct_sold_around_gap
FROM iso;
-- control: same correlates for bridging address-years that are present (no gap), in covered county-years
SELECT count(*) no_gap,
       round(100*avg((len(list_intersect(p.clips, n.clips)) = 0)::INT),1) pct_clip_changed,
       round(100*avg((len(p.clips) <> len(n.clips))::INT),1) pct_n_parcels_changed,
       round(100*avg((coalesce(n.yb, p.yb) >= b.y - 1)::INT),1) pct_built_around,
       round(100*avg((n.sale IS NOT NULL AND year(n.sale) BETWEEN b.y - 1 AND b.y + 1)::INT),1) pct_sold_around
FROM br b JOIN cyr c USING (fips, y) JOIN t p ON p.bkey = b.bkey AND p.y = b.y - 1 JOIN t n ON n.bkey = b.bkey AND n.y = b.y + 1
WHERE b.present AND c.miss_share < 0.1;
-- export isolated gaps' clips for the full-table check
COPY (SELECT bkey, y, unnest(list_distinct(list_concat(c0, c1))) clip FROM iso) TO '/home/jr2728/ptax_explore/hh_strat/out/iso_gap_clips.parquet' (FORMAT parquet);
COPY (SELECT DISTINCT fips, y FROM cyr WHERE miss_share >= 0.9 AND n_bridge >= 20) TO '/home/jr2728/ptax_explore/hh_strat/out/dropped_county_years.parquet' (FORMAT parquet);
