-- Border panel 2013-2021 for the 1% building sample (SOM, read-only DB). Border parcels and their state pairs come from
-- R/09_border_parcels_1pct.R. One filtered pass over cl.tax:
--   border_panel_1pct: the border parcels' rows (cell, homestead flag, bill, AV, MV, CL rate) for each year;
--   fitsample_border: <= 500 random parcels (any parcel, not only the sample) per cell x year x homestead group, for
--                     cells that contain a border parcel that year (groups with >= 30 parcels with bill, AV > 0);
--   ratio_border: sales ratio MV / arm's-length price for sales in the roll year, by cell-year (>= 30) and county-year.
SET temp_directory='/home/jr2728/ptax_explore/tmp'; SET preserve_insertion_order=false;
ATTACH '/gpfs/project/corelogic_database/corelogic_public.db' AS cl (READ_ONLY);
.timer on
CREATE TEMP TABLE bp AS SELECT * FROM '/home/jr2728/ptax_explore/border1/border_parcels_1pct.parquet';
CREATE TEMP TABLE t AS
SELECT clip, tax_year y, lpad(fips_code::VARCHAR, 5, '0') fips, situs_state st,
       lpad(fips_code::VARCHAR, 5, '0') || '|' || coalesce(nullif(trim(tax_area_code), ''), nullif(trim(tax_rate_area_code), ''),
                                                    'M:' || nullif(upper(trim(municipality_name)), ''), '_') cell,
       coalesce(homestead_exempt_indicator, false) hs, total_tax_amount bill, assessed_total_value av, calculated_total_value mv,
       total_property_tax_rate_percent rate_pct
FROM cl.tax
WHERE tax_year BETWEEN 2013 AND 2021 AND property_indicator_code IN (10, 11, 21, 22)
  AND (calculated_improvement_value > 0 OR total_living_area_square_feet_all_buildings > 0)
  AND fips_code IN (SELECT DISTINCT fips_code FROM cl.tax WHERE tax_year = 2016 AND clip IN (SELECT clip FROM bp));
CREATE TEMP TABLE bt AS
SELECT t.* FROM t SEMI JOIN bp USING (clip) QUALIFY row_number() OVER (PARTITION BY clip, y ORDER BY bill DESC NULLS LAST) = 1;
COPY (SELECT bt.*, b.pair, b.st_a, b.st_b FROM bt JOIN bp b USING (clip)) TO '/home/jr2728/ptax_explore/border1/border_panel_1pct.parquet' (FORMAT parquet);
CREATE TEMP TABLE bcells AS SELECT DISTINCT cell, y FROM bt;
COPY (SELECT cell, y, hs, n_grp, bill, av, mv FROM (
        SELECT t.*, count(*) OVER w n_grp, row_number() OVER (w ORDER BY hash(clip)) k
        FROM t SEMI JOIN bcells USING (cell, y) WHERE bill > 0 AND av > 0 WINDOW w AS (PARTITION BY cell, y, hs))
      WHERE n_grp >= 30 AND k <= 500) TO '/home/jr2728/ptax_explore/border1/fitsample_border.parquet' (FORMAT parquet);
CREATE TEMP TABLE rr AS
SELECT t.fips, t.cell, t.y, t.mv / s.sale_amount r
FROM t JOIN cl.ownertransfer s ON s.clip = t.clip AND year(s.sale_derived_date) = t.y
WHERE s.sale_derived_date BETWEEN DATE '2013-01-01' AND DATE '2021-12-31' AND s.primary_category_code = 'A'
  AND NOT coalesce(s.interfamily_related_indicator, false) AND s.sale_amount > 10000 AND t.mv > 1000
  AND t.mv / s.sale_amount BETWEEN 0.05 AND 5;
COPY (SELECT 'cell' lvl, fips, cell, y, count(*) n_sales, median(r) ratio FROM rr SEMI JOIN bcells USING (cell, y) GROUP BY ALL HAVING count(*) >= 30
      UNION ALL SELECT 'county', fips, NULL, y, count(*), median(r) FROM rr GROUP BY fips, y HAVING count(*) >= 30)
  TO '/home/jr2728/ptax_explore/border1/ratio_border.parquet' (FORMAT parquet);
SELECT (SELECT count(*) FROM t) rows_in_border_counties, (SELECT count(*) FROM bt) border_rows,
       (SELECT count(*) FROM '/home/jr2728/ptax_explore/border1/fitsample_border.parquet') fit_rows,
       (SELECT count(*) FROM '/home/jr2728/ptax_explore/border1/ratio_border.parquet') ratio_rows;
