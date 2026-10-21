-- USPS vacancy (situs_delivery_point_validation_vacant_indicator) on the per-side border sample (SOM, read-only DB).
-- Same sample as f_border_sample.sql (2016 residential parcels within 20 mi of a shared border; per side
-- max(5000, ceil(1%)) parcels by hash(clip)). Per side-year 2010-2024: parcels in roll, flag filled, flagged vacant.
-- Output: __DIR__/border_sample_vacancy.parquet
SET temp_directory='/home/jr2728/ptax_explore/tmp'; SET preserve_insertion_order=false; LOAD spatial;
ATTACH '/gpfs/project/corelogic_database/corelogic_public.db' AS cl (READ_ONLY);
.timer on
CREATE TEMP TABLE sides AS SELECT st_a, st_b, pair, geom FROM ST_Read('/home/jr2728/ptax_explore/border20_sides.geojson');
CREATE TEMP TABLE bp AS
SELECT p.clip, s.pair, s.st_a FROM read_parquet('/home/jr2728/ptax_explore/full/parcels16.parquet') p JOIN sides s ON ST_Within(ST_Point(p.lon, p.lat), s.geom)
WHERE p.lat IS NOT NULL AND p.lon IS NOT NULL;
CREATE TEMP TABLE smp AS
SELECT clip, pair, st_a FROM (
  SELECT *, count(*) OVER w n, row_number() OVER (w ORDER BY hash(clip)) k FROM bp WINDOW w AS (PARTITION BY pair, st_a))
WHERE k <= greatest(5000, ceil(0.01 * n));
CREATE TEMP TABLE roll AS
SELECT clip, tax_year y, bool_or(situs_delivery_point_validation_vacant_indicator) vac,
       bool_or(situs_delivery_point_validation_vacant_indicator IS NOT NULL) filled
FROM cl.tax WHERE tax_year BETWEEN 2010 AND 2024 AND clip IN (SELECT DISTINCT clip FROM smp) GROUP BY ALL;
COPY (SELECT s.pair, s.st_a, r.y, count(*) n_roll, count(*) FILTER (WHERE r.filled) n_filled, count(*) FILTER (WHERE r.vac) n_vac
      FROM smp s JOIN roll r USING (clip) GROUP BY ALL) TO '__DIR__/border_sample_vacancy.parquet' (FORMAT parquet);
SELECT y, sum(n_roll) n, round(100 * sum(n_filled) / sum(n_roll), 1) filled, round(100 * sum(n_vac) / sum(n_filled), 2) vac_of_filled
FROM '__DIR__/border_sample_vacancy.parquet' GROUP BY y ORDER BY y;
