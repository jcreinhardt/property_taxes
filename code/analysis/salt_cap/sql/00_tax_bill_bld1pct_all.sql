SET preserve_insertion_order=false; USE memory; SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
-- Tax bill, exemption, value and tax-area columns for the 1% building sample, ALL tax years (the existing file covers 2012-2021 only).
-- Adds lot size, rooms, CBSA, municipality, school district and block geocode for covariates, Strategy 3 bins and Strategy 5 borders. Output: ~/ptax_explore/samples/tax_bill_bld1pct_all.parquet
COPY (
  SELECT clip, tax_year, fips_code, situs_state, property_indicator_code, owner_occupancy_code, coalesce(owner_1_corporate_indicator,false) corp,
         left(coalesce(mailing_zip_code,''),5) = left(situs_zip_code,5) AND upper(trim(coalesce(mailing_house_number,''))) = upper(trim(situs_house_number))
           AND upper(trim(coalesce(mailing_street_name,''))) = upper(trim(situs_street_name)) mail_here,
         coalesce(trim(mailing_zip_code),'') = '' mail_missing, mailing_opt_out_indicator,
         homestead_exempt_indicator, senior_exempt_indicator, disabled_exempt_indicator, veteran_exempt_indicator, widow_exempt_indicator,
         total_tax_exemption_amount, calculated_total_tax_exemption_amount,
         assessed_total_value, assessed_land_value, assessed_improvement_value, calculated_total_value, calculated_improvement_value,
         market_total_value, appraised_total_value,
         total_tax_amount, net_tax_amount, assessed_year, tax_area_code, tax_rate_area_code, total_property_tax_rate_percent,
         total_land_square_footage, total_number_of_acres, total_number_of_bedrooms_all_buildings, total_number_of_rooms_all_buildings,
         total_number_of_bathrooms_all_buildings, total_number_of_stories, year_built, effective_year_built, total_living_area_square_feet_all_buildings,
         situs_core_based_statistical_area_cbsa cbsa, municipality_code, municipality_name, tax_district_municipality_name, township, school_district_name,
         block_level_latitude, block_level_longitude,
         hash(left(situs_zip_code,5), upper(trim(situs_street_name)), upper(trim(situs_house_number))) bkey
  FROM cl.tax
  WHERE coalesce(trim(situs_house_number),'') <> '' AND coalesce(trim(situs_street_name),'') <> '' AND length(situs_zip_code) >= 5
    AND hash(left(situs_zip_code,5), upper(trim(situs_street_name)), upper(trim(situs_house_number))) % 100 = 0
) TO '/home/jr2728/ptax_explore/samples/tax_bill_bld1pct_all.parquet.tmp' (FORMAT parquet, COMPRESSION zstd);
