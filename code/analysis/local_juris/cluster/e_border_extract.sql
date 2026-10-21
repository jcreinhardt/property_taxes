-- Border panel 2013-2021 (SOM, read-only DB). Universe: 2016 residential parcels (parcels16.parquet from a_extract16.sql)
-- whose point lies within 20 mi of a shared state border (border20_sides.geojson; a parcel near a tri-point is in each pair).
-- Outputs in __DIR__: border_parcels (clip, pair, st_a, st_b), border_panel (clip x year rows: cell, hs, bill, av, mv,
-- CL rate), fitsample_border (<= 500 random parcels per cell x year x homestead group, all parcels of cells that contain a
-- border parcel that year, groups with >= 30 parcels with bill, AV > 0), ratio_border (sales ratio MV / price, arm's-length
-- sales in the roll year, by cell-year (>= 30 sales) and county-year). __SAMPLE__ = '' or 'TABLESAMPLE ...' (test).
SET temp_directory='/home/jr2728/ptax_explore/tmp'; SET preserve_insertion_order=false; LOAD spatial;
ATTACH '/gpfs/project/corelogic_database/corelogic_public.db' AS cl (READ_ONLY);
.timer on
CREATE TEMP TABLE sides AS SELECT st_a, st_b, pair, geom FROM ST_Read('/home/jr2728/ptax_explore/border20_sides.geojson');
CREATE TEMP TABLE bp AS
SELECT p.clip, s.pair, s.st_a, s.st_b FROM read_parquet('__P16__') p JOIN sides s ON ST_Within(ST_Point(p.lon, p.lat), s.geom)
WHERE p.lat IS NOT NULL AND p.lon IS NOT NULL;
COPY bp TO '__DIR__/border_parcels.parquet' (FORMAT parquet);

CREATE TEMP TABLE t AS
SELECT clip, tax_year y, lpad(fips_code::VARCHAR, 5, '0') fips, situs_state st,
       lpad(fips_code::VARCHAR, 5, '0') || '|' || coalesce(nullif(trim(tax_area_code), ''), nullif(trim(tax_rate_area_code), ''),
                                                    'M:' || nullif(upper(trim(municipality_name)), ''), '_') cell,
       coalesce(homestead_exempt_indicator, false) hs, total_tax_amount bill, assessed_total_value av, calculated_total_value mv,
       total_property_tax_rate_percent rate_pct
FROM cl.tax __SAMPLE__
WHERE tax_year BETWEEN 2013 AND 2021 AND property_indicator_code IN (10, 11, 21, 22)
  AND (calculated_improvement_value > 0 OR total_living_area_square_feet_all_buildings > 0)
QUALIFY row_number() OVER (PARTITION BY clip, tax_year ORDER BY total_tax_amount DESC NULLS LAST) = 1;

COPY (SELECT t.*, b.pair, b.st_a, b.st_b FROM t JOIN bp b USING (clip)) TO '__DIR__/border_panel.parquet' (FORMAT parquet);

CREATE TEMP TABLE bcells AS SELECT DISTINCT t.cell, t.y FROM t SEMI JOIN bp USING (clip);
COPY (SELECT cell, y, hs, n_grp, bill, av, mv FROM (
        SELECT t.*, count(*) OVER w n_grp, row_number() OVER (w ORDER BY hash(clip)) k
        FROM t SEMI JOIN bcells USING (cell, y) WHERE bill > 0 AND av > 0 WINDOW w AS (PARTITION BY cell, y, hs))
      WHERE n_grp >= 30 AND k <= 500) TO '__DIR__/fitsample_border.parquet' (FORMAT parquet);

CREATE TEMP TABLE rr AS
SELECT t.fips, t.cell, t.y, t.mv / s.sale_amount r
FROM t JOIN cl.ownertransfer s ON s.clip = t.clip AND year(s.sale_derived_date) = t.y
WHERE s.sale_derived_date BETWEEN DATE '2013-01-01' AND DATE '2021-12-31' AND s.primary_category_code = 'A'
  AND NOT coalesce(s.interfamily_related_indicator, false) AND s.sale_amount > 10000 AND t.mv > 1000
  AND t.mv / s.sale_amount BETWEEN 0.05 AND 5;
COPY (SELECT 'cell' lvl, fips, cell, y, count(*) n_sales, median(r) ratio FROM rr SEMI JOIN bcells USING (cell, y) GROUP BY ALL HAVING count(*) >= 30
      UNION ALL SELECT 'county', fips, NULL, y, count(*), median(r) FROM rr GROUP BY fips, y HAVING count(*) >= 30)
  TO '__DIR__/ratio_border.parquet' (FORMAT parquet);
SELECT (SELECT count(*) FROM bp) border_parcels, (SELECT count(DISTINCT clip) FROM bp) border_clips,
       (SELECT count(*) FROM '__DIR__/border_panel.parquet') panel_rows, (SELECT count(*) FROM '__DIR__/fitsample_border.parquet') fit_rows,
       (SELECT count(*) FROM '__DIR__/ratio_border.parquet') ratio_rows;
