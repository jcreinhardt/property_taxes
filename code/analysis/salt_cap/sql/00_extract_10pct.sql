SET preserve_insertion_order=false; USE memory; SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
-- 10% building sample (bkey % 10 = 0; superset of the 1% sample) of tax, bill, deeds, mortgage and Data Axle, for the SALT-cap
-- full-scale run (subsample -> full-data rule). Same columns and hashing as the 1% extracts. Outputs: ~/ptax_explore/salt/s10/*.parquet
CREATE OR REPLACE MACRO hn(x) AS CASE WHEN coalesce(trim(x), '') <> '' THEN hash(upper(trim(x))) END;
CREATE OR REPLACE MACRO tkey(z, s, h) AS hash(left(z,5), upper(trim(s)), upper(trim(h)));
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
         tkey(situs_zip_code, situs_street_name, situs_house_number) bkey
  FROM cl.tax
  WHERE coalesce(trim(situs_house_number),'') <> '' AND coalesce(trim(situs_street_name),'') <> '' AND length(situs_zip_code) >= 5
    AND tkey(situs_zip_code, situs_street_name, situs_house_number) % 10 = 0 AND property_indicator_code IN (10, 11)
) TO '/home/jr2728/ptax_explore/salt/s10/tax_bld1pct.parquet' (FORMAT parquet, COMPRESSION zstd);
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
         tkey(situs_zip_code, situs_street_name, situs_house_number) bkey
  FROM cl.tax
  WHERE coalesce(trim(situs_house_number),'') <> '' AND coalesce(trim(situs_street_name),'') <> '' AND length(situs_zip_code) >= 5
    AND tkey(situs_zip_code, situs_street_name, situs_house_number) % 10 = 0 AND property_indicator_code IN (10, 11)
) TO '/home/jr2728/ptax_explore/salt/s10/tax_bill_bld1pct_all.parquet' (FORMAT parquet, COMPRESSION zstd);
CREATE OR REPLACE TABLE sclip AS SELECT DISTINCT clip FROM read_parquet('/home/jr2728/ptax_explore/salt/s10/tax_bld1pct.parquet') WHERE clip IS NOT NULL;
COPY (
  SELECT clip, fips_code, property_indicator_code, primary_category_code, sale_amount, sale_derived_date, interfamily_related_indicator,
         new_construction_indicator, investor_purchase_indicator, buyer_1_corporate_indicator, buyer_ownership_rights_code, buyer_relationship_type_code,
         resale_indicator, foreclosure_reo_sale_indicator, short_sale_indicator, cash_purchase_indicator, mortgage_purchase_indicator,
         buyer_mailing_zip_code, hn(buyer_1_last_name) b1_ln, hn(seller_1_last_name) s1_ln,
         CASE WHEN coalesce(trim(deed_situs_house_number),'') <> '' AND coalesce(trim(deed_situs_street_name),'') <> '' AND length(deed_situs_zip_code) >= 5
              THEN tkey(deed_situs_zip_code, deed_situs_street_name, deed_situs_house_number) END bkey
  FROM cl.ownertransfer o WHERE o.clip IN (SELECT clip FROM sclip)
) TO '/home/jr2728/ptax_explore/salt/s10/deeds_bld1pct.parquet' (FORMAT parquet, COMPRESSION zstd);
COPY (
  SELECT m.clip, m.fips_code, m.property_indicator_code, m.mortgage_composite_transaction_id, m.mortgage_sequence_number, m.primary_category_code,
         m.mortgage_amount, m.mortgage_date, m.mortgage_recording_date, m.mortgage_interest_rate_type_code, m.mortgage_interest_rate,
         m.mortgage_loan_type_code, m.mortgage_document_type_code, m.mortgage_term_code, m.mortgage_term, m.mortgage_type_code, m.mortgage_purpose_code,
         m.conforming_loan_indicator, m.conventional_loan_indicator, m.equity_loan_indicator, m.fha_loan_indicator, m.fixed_rate_indicator,
         m.veterans_administration_loan_indicator, m.variable_rate_loan_indicator, m.construction_loan_indicator, m.private_party_lender_indicator,
         m.refinance_loan_indicator, m.second_home_rider_indicator, m.condominium_rider_indicator,
         hn(m.borrower_1_last_name) bo1_ln, hn(m.borrower_2_last_name) bo2_ln
  FROM cl.mortgage m WHERE m.clip IN (SELECT clip FROM sclip)
) TO '/home/jr2728/ptax_explore/salt/s10/mortgage_bld1pct.parquet' (FORMAT parquet, COMPRESSION zstd);
COPY (
  SELECT family_id, record_year, primary_family_ind, length_of_residence, owner_renter_status, marital_status, find, estmtd_home_val,
         individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5, age_1, unit_num,
         ge_als_census_tract_2010, ge_als_census_bg_2010,
         hn(last_name_1) ln_1, hn(last_name_2) ln_2, hn(last_name_3) ln_3, hn(last_name_4) ln_4, hn(last_name_5) ln_5,
         hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num))) bkey
  FROM cl.dataaxle_annual
  WHERE address_type = 'S' AND coalesce(trim(house_num),'') <> '' AND coalesce(trim(street_name),'') <> '' AND zip IS NOT NULL
    AND record_year BETWEEN 2010 AND 2023 AND record_year <> 2022
    AND hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num))) % 10 = 0
) TO '/home/jr2728/ptax_explore/salt/s10/da_bld1pct.parquet' (FORMAT parquet, COMPRESSION zstd);
