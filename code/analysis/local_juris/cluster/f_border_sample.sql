-- Per-side border sample (SOM, read-only DB). Universe: 2016 residential parcels of the full roll (parcels16.parquet) within
-- 20 mi of a shared state border, by side (state x neighbour; border20_sides.geojson). On each side draw
-- max(5000, ceil(1% of the side's parcels)) parcels (all if fewer), by hash(clip). For the sample: tax bill per year __Y0__-__Y1__
-- and the year's first arm's-length sale (price > 0, not interfamily; new-construction flag), as in the SALT panel.
-- Outputs in __DIR__: side_counts (pair, st_a, n_border, n_sample), border_sample_sales (sampled clip x year with a sale:
-- bill, price, newcon), border_sample_roll (side x year: sampled parcels in the roll).
SET temp_directory='/home/jr2728/ptax_explore/tmp'; SET preserve_insertion_order=false; LOAD spatial;
ATTACH '/gpfs/project/corelogic_database/corelogic_public.db' AS cl (READ_ONLY);
.timer on
CREATE TEMP TABLE sides AS SELECT st_a, st_b, pair, geom FROM ST_Read('/home/jr2728/ptax_explore/border20_sides.geojson');
CREATE TEMP TABLE bp AS
SELECT p.clip, s.pair, s.st_a FROM read_parquet('__P16__') p JOIN sides s ON ST_Within(ST_Point(p.lon, p.lat), s.geom)
WHERE p.lat IS NOT NULL AND p.lon IS NOT NULL;
CREATE TEMP TABLE smp AS
SELECT clip, pair, st_a FROM (
  SELECT *, count(*) OVER w n, row_number() OVER (w ORDER BY hash(clip)) k FROM bp WINDOW w AS (PARTITION BY pair, st_a))
WHERE k <= greatest(5000, ceil(0.01 * n));
COPY (SELECT b.pair, b.st_a, b.n_border, s.n_sample FROM (SELECT pair, st_a, count(*) n_border FROM bp GROUP BY ALL) b
      JOIN (SELECT pair, st_a, count(*) n_sample FROM smp GROUP BY ALL) s USING (pair, st_a)) TO '__DIR__/side_counts.parquet' (FORMAT parquet);
CREATE TEMP TABLE roll AS
SELECT clip, tax_year y, max(total_tax_amount) bill FROM cl.tax
WHERE tax_year BETWEEN __Y0__ AND __Y1__ AND clip IN (SELECT DISTINCT clip FROM smp) GROUP BY ALL;
CREATE TEMP TABLE sales AS
SELECT clip, year(sale_derived_date) y, arg_min(sale_amount, sale_derived_date) price, bool_or(coalesce(new_construction_indicator, false)) newcon
FROM cl.ownertransfer
WHERE clip IN (SELECT DISTINCT clip FROM smp) AND sale_derived_date BETWEEN DATE '__Y0__-01-01' AND DATE '__Y1__-12-31'
  AND primary_category_code = 'A' AND NOT coalesce(interfamily_related_indicator, false) AND sale_amount > 0
GROUP BY ALL;
COPY (SELECT s.pair, s.st_a, r.clip, r.y, r.bill, x.price, x.newcon FROM smp s JOIN roll r USING (clip) JOIN sales x USING (clip, y))
  TO '__DIR__/border_sample_sales.parquet' (FORMAT parquet);
COPY (SELECT s.pair, s.st_a, r.y, count(*) n_roll FROM smp s JOIN roll r USING (clip) GROUP BY ALL) TO '__DIR__/border_sample_roll.parquet' (FORMAT parquet);
SELECT (SELECT count(*) FROM bp) border_rows, (SELECT count(*) FROM smp) sample_rows, (SELECT count(*) FROM '__DIR__/border_sample_sales.parquet') sale_rows;
