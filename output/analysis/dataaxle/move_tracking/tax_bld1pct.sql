SET preserve_insertion_order=false;
USE memory;
SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
-- 1% building sample of the tax table (all tax years), selected columns only; owner names hashed.
-- bkey = hash(ZIP5, upper(trim(street)), upper(trim(house number))) as in da_bld1pct.
CREATE OR REPLACE MACRO hn(x) AS CASE WHEN coalesce(trim(x), '') <> '' THEN hash(upper(trim(x))) END;
COPY (
  SELECT clip, tax_year, fips_code, apn_parcel_number_unformatted, property_indicator_code, land_use_code,
         situs_house_number, situs_house_number_2, situs_street_name, situs_unit_number, situs_city, situs_state, situs_zip_code,
         parcel_level_latitude, parcel_level_longitude,
         owner_occupancy_code, owner_1_corporate_indicator, owner_2_corporate_indicator, owner_etal_code, owner_relationship_type_code,
         hn(owner_1_last_name) o1_ln, hn(owner_1_full_name) o1_full, hn(owner_1_first_name_middle_initial) o1_fn,
         hn(owner_2_last_name) o2_ln, hn(owner_2_full_name) o2_full, hn(owner_2_first_name_middle_initial) o2_fn,
         mailing_house_number, mailing_street_name, mailing_street_address, mailing_city, mailing_state, mailing_zip_code,
         homestead_exempt_indicator, situs_delivery_point_validation_code, situs_delivery_point_validation_vacant_indicator,
         year_built, calculated_improvement_value, calculated_total_value, total_living_area_square_feet_all_buildings,
         number_of_buildings, total_number_of_units_all_buildings,
         sale_date, sale_recording_date, sale_amount, sale_document_type_code, sale_type_code, transaction_batch_date, last_assessor_update_date,
         hash(left(situs_zip_code,5), upper(trim(situs_street_name)), upper(trim(situs_house_number))) bkey
  FROM cl.tax
  WHERE coalesce(trim(situs_house_number),'') <> '' AND coalesce(trim(situs_street_name),'') <> '' AND length(situs_zip_code) >= 5
    AND hash(left(situs_zip_code,5), upper(trim(situs_street_name)), upper(trim(situs_house_number))) % 100 = 0
) TO '/home/jr2728/ptax_explore/samples/tax_bld1pct.parquet' (FORMAT parquet, COMPRESSION zstd);
