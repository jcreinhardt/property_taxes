-- Grid of effective property-tax rates (SOM, read-only DB). Arm's-length sales 2017-2019 of single-family homes and condos
-- (pic 10/11; price > 10k, not interfamily, no new construction; first sale per parcel-year), effective rate = tax bill of the
-- sale year / price, kept if 0.05-10%. Parcel coordinates from that year's roll, projected to EPSG:5070 and binned into
-- 5 km cells. Per cell keep up to 50 sales (by hash, deterministic) and their median effective rate.
-- Outputs in __DIR__: grid_sales (sampled sales: cx, cy, eff, y, st), grid_cells (cx, cy, n_all, n, med_eff, state mode).
SET temp_directory='/home/jr2728/ptax_explore/tmp'; SET preserve_insertion_order=false; LOAD spatial;
ATTACH '/gpfs/project/corelogic_database/corelogic_public.db' AS cl (READ_ONLY);
.timer on
CREATE TEMP TABLE tx AS
SELECT clip, tax_year::INT y, max(total_tax_amount) bill, any_value(situs_state) stf,
       any_value(nullif(parcel_level_latitude, 0)) lat, any_value(nullif(parcel_level_longitude, 0)) lon
FROM cl.tax WHERE tax_year BETWEEN 2017 AND 2019 AND property_indicator_code IN (10, 11) GROUP BY ALL;
CREATE TEMP TABLE sl AS
SELECT clip, year(sale_derived_date)::INT y, arg_min(sale_amount, sale_derived_date) price, bool_or(coalesce(new_construction_indicator, false)) newcon
FROM cl.ownertransfer
WHERE sale_derived_date BETWEEN DATE '2017-01-01' AND DATE '2019-12-31' AND primary_category_code = 'A'
  AND NOT coalesce(interfamily_related_indicator, false) AND sale_amount > 0 GROUP BY ALL;
CREATE TEMP TABLE s AS
SELECT t.clip, t.y, t.stf, t.bill / x.price eff,
       ST_Transform(ST_Point(t.lon, t.lat), 'EPSG:4326', 'EPSG:5070', always_xy := true) g
FROM tx t JOIN sl x USING (clip, y)
WHERE x.price > 10000 AND NOT x.newcon AND t.bill > 0 AND t.bill / x.price BETWEEN 0.0005 AND 0.10
  AND t.lat BETWEEN 18 AND 72 AND t.lon BETWEEN -180 AND -60;
CREATE TEMP TABLE sc AS
SELECT *, count(*) OVER w n_all, row_number() OVER (w ORDER BY hash(clip, y)) k FROM (
  SELECT clip, y, stf, eff, floor(ST_X(g) / 5000)::INT cx, floor(ST_Y(g) / 5000)::INT cy FROM s)
WINDOW w AS (PARTITION BY cx, cy);
COPY (SELECT cx, cy, y, stf, eff FROM sc WHERE k <= 50) TO '__DIR__/grid_sales.parquet' (FORMAT parquet);
COPY (SELECT cx, cy, any_value(n_all) n_all, count(*) n, median(eff) med_eff, mode(stf) stf FROM sc WHERE k <= 50 GROUP BY ALL)
  TO '__DIR__/grid_cells.parquet' (FORMAT parquet);
SELECT count(*) sales, count(DISTINCT (cx, cy)) cells, count(DISTINCT (cx, cy)) FILTER (WHERE n_all >= 10) cells10 FROM sc;
