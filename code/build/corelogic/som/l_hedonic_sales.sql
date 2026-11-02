-- Residential arm's-length sales 2012-2024 with parcel attributes, for the three price indices (SOM copy, read-only DB;
-- l_price_indices.R builds median / repeat-sales / hedonic indices from the output).
-- Attributes from the 2014, 2018 and 2022 rolls (max over the three, so a county missing one delivery keeps its parcels):
--   living area, lot (sq ft, acres), year built, bedrooms, bathrooms, stories, units, property type (pic 10 SFR, 11 condo,
--   21 duplex, 22 apartment); tract = fips || left(census_id, 6), as in k_panel; state and ZIP from the situs.
-- Sales: cl.ownertransfer, primary_category_code = 'A', not interfamily, sale_amount > 10k; one sale per parcel-year (the
--   earliest; its price), flags new construction and distress (REO / foreclosure / short sale) to filter in R.
-- __SAMPLE__ = '' (full) or 'TABLESAMPLE 0.05% (system)' (test); __OUT__ = output dir. Output: __OUT__/sales_attr.parquet.
SET temp_directory='/home/jr2728/ptax_explore/tmp'; SET preserve_insertion_order=false;
ATTACH '/gpfs/project/corelogic_database/corelogic_public.db' AS cl (READ_ONLY);
.timer on
CREATE TEMP TABLE a AS
SELECT clip, any_value(lpad(fips_code::VARCHAR, 5, '0')) fips, any_value(situs_state) st, any_value(left(situs_zip_code, 5)) zip,
       any_value(lpad(fips_code::VARCHAR, 5, '0') || left(census_id::VARCHAR, 6)) FILTER (WHERE length(census_id::VARCHAR) >= 6) tract,
       arg_max(property_indicator_code, tax_year) pic,
       max(total_living_area_square_feet_all_buildings) living, max(total_land_square_footage) lot_sqft, max(total_number_of_acres) acres,
       max(year_built) year_built, max(total_number_of_bedrooms_all_buildings) beds,
       max(coalesce(total_number_of_bathrooms_all_buildings, total_number_of_bathrooms)) baths,
       max(total_number_of_stories) stories, max(total_number_of_units_all_buildings) units
FROM cl.tax __SAMPLE__
WHERE tax_year IN (2014, 2018, 2022) AND property_indicator_code IN (10, 11, 21, 22) AND clip IS NOT NULL
GROUP BY clip;
SELECT 'parcels' k, count(*) n, avg((living > 0)::INT) sh_living, avg((year_built > 0)::INT) sh_yb, avg((beds > 0)::INT) sh_beds, avg((tract IS NOT NULL)::INT) sh_tract FROM a;
CREATE TEMP TABLE s AS
SELECT clip, year(sale_derived_date)::INT y, month(min(sale_derived_date))::INT m, arg_min(sale_amount, sale_derived_date) price,
       bool_or(coalesce(new_construction_indicator, false)) newcon,
       bool_or(coalesce(foreclosure_reo_sale_indicator, false) OR coalesce(foreclosure_reo_indicator, false) OR coalesce(short_sale_indicator, false)) distress,
       count(*) n_deeds
FROM cl.ownertransfer
WHERE sale_derived_date BETWEEN DATE '2012-01-01' AND DATE '2024-12-31' AND primary_category_code = 'A'
  AND NOT coalesce(interfamily_related_indicator, false) AND sale_amount > 10000 AND clip IS NOT NULL
GROUP BY clip, year(sale_derived_date);
COPY (SELECT a.*, s.y, s.m, s.price, s.newcon, s.distress, s.n_deeds FROM s JOIN a USING (clip))
  TO '__OUT__/sales_attr.parquet' (FORMAT parquet, COMPRESSION zstd);
SELECT y, count(*) sales, count(DISTINCT clip) parcels, avg(newcon::INT) sh_newcon, avg(distress::INT) sh_distress, avg((living > 0)::INT) sh_living
FROM '__OUT__/sales_attr.parquet' GROUP BY y ORDER BY y;
