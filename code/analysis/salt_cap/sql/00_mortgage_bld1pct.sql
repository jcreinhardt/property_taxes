SET preserve_insertion_order=false; USE memory; SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
-- 1% building sample of cl.mortgage: rows for the sampled tax parcels (by clip) or whose deed situs key hashes into the same 1%.
-- Borrower names hashed (surname only); lender fields dropped. All years. Output: ~/ptax_explore/samples/mortgage_bld1pct.parquet
CREATE OR REPLACE MACRO hn(x) AS CASE WHEN coalesce(trim(x), '') <> '' THEN hash(upper(trim(x))) END;
CREATE OR REPLACE TABLE sclip AS SELECT DISTINCT clip FROM read_parquet('/home/jr2728/ptax_explore/samples/tax_bld1pct.parquet') WHERE clip IS NOT NULL;
COPY (
  SELECT m.clip, m.fips_code, m.property_indicator_code, m.land_use_code,
         m.deed_situs_zip_code, m.deed_situs_unit_number,
         m.mortgage_composite_transaction_id, m.mortgage_sequence_number, m.transaction_batch_date, m.primary_category_code,
         m.mortgage_amount, m.mortgage_date, m.mortgage_recording_date,
         m.mortgage_interest_rate_type_code, m.mortgage_interest_rate, m.mortgage_loan_type_code, m.mortgage_document_type_code,
         m.mortgage_term_code, m.mortgage_term, m.mortgage_due_date, m.mortgage_type_code, m.mortgage_purpose_code,
         m.mortgage_subordinate_type_code, m.conforming_loan_indicator, m.conventional_loan_indicator, m.equity_loan_indicator,
         m.fha_loan_indicator, m.fixed_rate_indicator, m.veterans_administration_loan_indicator, m.variable_rate_loan_indicator,
         m.construction_loan_indicator, m.private_party_lender_indicator, m.refinance_loan_indicator, m.seller_carry_back_indicator,
         m.second_home_rider_indicator, m.condominium_rider_indicator, m.multifamily_rider_indicator,
         m.borrower_1_corporate_indicator, m.borrower_2_corporate_indicator, m.borrower_etal_code,
         hn(m.borrower_1_last_name) bo1_ln, hn(m.borrower_2_last_name) bo2_ln, hn(m.borrower_1_full_name) bo1_full,
         m.record_action_indicator,
         CASE WHEN coalesce(trim(m.deed_situs_house_number),'') <> '' AND coalesce(trim(m.deed_situs_street_name),'') <> '' AND length(m.deed_situs_zip_code) >= 5
              THEN hash(left(m.deed_situs_zip_code,5), upper(trim(m.deed_situs_street_name)), upper(trim(m.deed_situs_house_number))) END bkey
  FROM cl.mortgage m
  WHERE m.clip IN (SELECT clip FROM sclip)
     OR (coalesce(trim(m.deed_situs_house_number),'') <> '' AND coalesce(trim(m.deed_situs_street_name),'') <> '' AND length(m.deed_situs_zip_code) >= 5
         AND hash(left(m.deed_situs_zip_code,5), upper(trim(m.deed_situs_street_name)), upper(trim(m.deed_situs_house_number))) % 100 = 0)
) TO '/home/jr2728/ptax_explore/samples/mortgage_bld1pct.parquet.tmp' (FORMAT parquet, COMPRESSION zstd);
