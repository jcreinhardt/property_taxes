-- Full-roll extract for the nominal-rate pipeline (SOM copy, read-only). Tax year 2016, residential with structure.
-- Outputs (in ~/ptax_explore/full/): parcels16 (one row per clip), ratio16 (sales ratio by cell and county),
-- fitsample16 (<= 2000 random parcels per cell x homestead group with bill, AV > 0 and >= 30 such parcels).
-- __SAMPLE__ = '' (full) or 'TABLESAMPLE 0.01% (system)' (test); __OUT__ = output dir.
SET temp_directory='/home/jr2728/ptax_explore/tmp'; SET preserve_insertion_order=false;
ATTACH '/gpfs/project/corelogic_database/corelogic_public.db' AS cl (READ_ONLY);
.timer on
CREATE TEMP TABLE p AS
SELECT clip, lpad(fips_code::VARCHAR, 5, '0') fips, situs_state st,
       lpad(fips_code::VARCHAR, 5, '0') || '|' || coalesce(nullif(trim(tax_area_code), ''), nullif(trim(tax_rate_area_code), ''),
                                                    'M:' || nullif(upper(trim(municipality_name)), ''), '_') cell,
       coalesce(homestead_exempt_indicator, false) hs, owner_occupancy_code = 'O' oo,
       total_tax_amount bill, assessed_total_value av, calculated_total_value mv,
       nullif(parcel_level_latitude, 0) lat, nullif(parcel_level_longitude, 0) lon
FROM cl.tax __SAMPLE__
WHERE tax_year = 2016 AND property_indicator_code IN (10, 11, 21, 22)
  AND (calculated_improvement_value > 0 OR total_living_area_square_feet_all_buildings > 0)
QUALIFY row_number() OVER (PARTITION BY clip ORDER BY total_tax_amount DESC NULLS LAST) = 1;
COPY p TO '__OUT__/parcels16.parquet' (FORMAT parquet);

-- sales ratio: CoreLogic MV / arm's-length price, sales 2015-17 matched to the 2016 roll
CREATE TEMP TABLE rr AS
SELECT p.fips, p.cell, p.mv / s.sale_amount r
FROM p JOIN cl.ownertransfer s ON s.clip = p.clip
WHERE s.sale_derived_date BETWEEN DATE '2015-01-01' AND DATE '2017-12-31' AND s.primary_category_code = 'A'
  AND NOT coalesce(s.interfamily_related_indicator, false) AND s.sale_amount > 10000 AND p.mv > 1000
  AND p.mv / s.sale_amount BETWEEN 0.05 AND 5;
COPY (SELECT 'cell' lvl, fips, cell, count(*) n_sales, median(r) ratio FROM rr GROUP BY ALL HAVING count(*) >= 30
      UNION ALL SELECT 'county', fips, NULL, count(*), median(r) FROM rr GROUP BY fips HAVING count(*) >= 30)
  TO '__OUT__/ratio16.parquet' (FORMAT parquet);

COPY (SELECT clip, fips, st, cell, hs, oo, bill, av, mv, n_grp FROM (
        SELECT *, count(*) OVER w n_grp, row_number() OVER (w ORDER BY hash(clip)) k
        FROM p WHERE bill > 0 AND av > 0 WINDOW w AS (PARTITION BY cell, hs)) WHERE n_grp >= 30 AND k <= 2000)
  TO '__OUT__/fitsample16.parquet' (FORMAT parquet);
SELECT (SELECT count(*) FROM p) parcels, (SELECT count(*) FROM rr) sales_pairs,
       (SELECT count(*) FROM '__OUT__/fitsample16.parquet') fit_rows, (SELECT count(DISTINCT cell) FROM '__OUT__/fitsample16.parquet') fit_cells;
