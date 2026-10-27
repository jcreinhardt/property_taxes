SET preserve_insertion_order=false; USE memory; SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
-- Tax bill, exemption and tax-area columns for the 1% building sample (same bkey filter as tax_bld1pct.sql), tax years 2012-2021.
ATTACH '/nfs/roberts/pi/pi_csl53/corelogic/database/corelogic.db' AS cl (READ_ONLY);
COPY (
  SELECT clip, tax_year, fips_code, situs_state, property_indicator_code, owner_occupancy_code, coalesce(owner_1_corporate_indicator,false) corp,
         left(coalesce(mailing_zip_code,''),5) = left(situs_zip_code,5) AND upper(trim(coalesce(mailing_house_number,''))) = upper(trim(situs_house_number))
           AND upper(trim(coalesce(mailing_street_name,''))) = upper(trim(situs_street_name)) mail_here,
         coalesce(trim(mailing_zip_code),'') = '' mail_missing, mailing_opt_out_indicator,
         homestead_exempt_indicator, senior_exempt_indicator, disabled_exempt_indicator, veteran_exempt_indicator, widow_exempt_indicator,
         total_tax_exemption_amount, calculated_total_tax_exemption_amount,
         assessed_total_value, assessed_land_value, assessed_improvement_value, calculated_total_value, calculated_improvement_value,
         total_tax_amount, net_tax_amount, assessed_year, tax_area_code, tax_rate_area_code, total_property_tax_rate_percent,
         hash(left(situs_zip_code,5), upper(trim(situs_street_name)), upper(trim(situs_house_number))) bkey
  FROM cl.tax
  WHERE coalesce(trim(situs_house_number),'') <> '' AND coalesce(trim(situs_street_name),'') <> '' AND length(situs_zip_code) >= 5
    AND tax_year BETWEEN 2012 AND 2021
    AND hash(left(situs_zip_code,5), upper(trim(situs_street_name)), upper(trim(situs_house_number))) % 100 = 0
) TO '/home/jr2728/ptax_explore/samples/tax_bill_bld1pct.parquet' (FORMAT parquet, COMPRESSION zstd);
