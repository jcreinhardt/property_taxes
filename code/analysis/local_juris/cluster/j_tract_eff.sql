-- Effective property-tax rates by census tract (SOM, read-only DB). Same sales as h_grid_eff.sql: arm's-length sales
-- 2017-2019 of SFR/condos (pic 10/11), price > 10k, no new construction, first sale per parcel-year; effective rate =
-- sale-year bill / price, kept 0.05-10%. Tract GEOID = 5-digit county FIPS || first 6 digits of cl.tax.census_id
-- (tract + 4-digit block). Output __DIR__/tract_eff.parquet: tract, n, median, quartiles, mean lat/lon, modal state.
SET temp_directory='/home/jr2728/ptax_explore/tmp'; SET preserve_insertion_order=false;
ATTACH '/gpfs/project/corelogic_database/corelogic_public.db' AS cl (READ_ONLY);
.timer on
CREATE TEMP TABLE tx AS
SELECT clip, tax_year::INT y, max(total_tax_amount) bill, any_value(situs_state) st,
       any_value(lpad(fips_code::VARCHAR, 5, '0') || left(census_id::VARCHAR, 6)) FILTER (WHERE length(census_id::VARCHAR) >= 6) tract,
       any_value(nullif(parcel_level_latitude, 0)) lat, any_value(nullif(parcel_level_longitude, 0)) lon
FROM cl.tax WHERE tax_year BETWEEN 2017 AND 2019 AND property_indicator_code IN (10, 11) GROUP BY ALL;
CREATE TEMP TABLE sl AS
SELECT clip, year(sale_derived_date)::INT y, arg_min(sale_amount, sale_derived_date) price, bool_or(coalesce(new_construction_indicator, false)) newcon
FROM cl.ownertransfer
WHERE sale_derived_date BETWEEN DATE '2017-01-01' AND DATE '2019-12-31' AND primary_category_code = 'A'
  AND NOT coalesce(interfamily_related_indicator, false) AND sale_amount > 0 GROUP BY ALL;
CREATE TEMP TABLE s AS
SELECT t.tract, t.st, t.lat, t.lon, t.bill / x.price eff FROM tx t JOIN sl x USING (clip, y)
WHERE x.price > 10000 AND NOT x.newcon AND t.bill > 0 AND t.bill / x.price BETWEEN 0.0005 AND 0.10;
COPY (SELECT tract, count(*) n, median(eff) med, quantile_cont(eff, 0.25) q25, quantile_cont(eff, 0.75) q75,
             avg(lat) lat, avg(lon) lon, mode(st) st
      FROM s WHERE tract IS NOT NULL AND right(tract, 6) <> '000000' GROUP BY tract) TO '__DIR__/tract_eff.parquet' (FORMAT parquet);
SELECT count(*) sales, round(100 * avg((tract IS NOT NULL)::INT), 1) pct_tract,
       (SELECT count(*) FROM '__DIR__/tract_eff.parquet') tracts, (SELECT count(*) FROM '__DIR__/tract_eff.parquet' WHERE n >= 30) tracts30 FROM s;
