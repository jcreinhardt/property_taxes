SET preserve_insertion_order=false;
USE memory;
SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
-- Every Data Axle record (all years, any address) of the families and persons ever seen at the 1% building sample,
-- so moves out of / into sampled buildings can be followed by family_id and by individual_id (re-keyed families).
-- Names hashed; written to ~/ptax_explore/samples/da_bld1pct_fampos.parquet.
CREATE OR REPLACE MACRO hn(x) AS CASE WHEN coalesce(trim(x), '') <> '' THEN hash(upper(trim(x))) END;
CREATE OR REPLACE TABLE sf AS SELECT DISTINCT family_id fid FROM '/home/jr2728/ptax_explore/samples/da_bld1pct.parquet';
CREATE OR REPLACE TABLE si AS
  SELECT DISTINCT iid FROM (SELECT unnest([individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5]) iid
                            FROM '/home/jr2728/ptax_explore/samples/da_bld1pct.parquet') WHERE iid IS NOT NULL;
COPY (
  SELECT family_id, record_year, primary_family_ind, location_id, address_type, zip, zip4, state, city,
         CASE WHEN address_type = 'S' AND coalesce(trim(house_num),'') <> '' AND coalesce(trim(street_name),'') <> '' AND zip IS NOT NULL
              THEN hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num))) END bkey,
         nullif(upper(trim(unit_num)), '') unit, ge_census_state_2010::INT*1000 + ge_als_county_code_2010::INT cty,
         length_of_residence, owner_renter_status, recency_date, household_status,
         individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5,
         hn(last_name_1) ln_1, hn(last_name_2) ln_2, hn(last_name_3) ln_3, hn(last_name_4) ln_4, hn(last_name_5) ln_5,
         hn(first_name_1) fn_1, hn(first_name_2) fn_2, hn(first_name_3) fn_3, hn(first_name_4) fn_4, hn(first_name_5) fn_5,
         age_1, age_2, age_3, age_4, age_5
  FROM cl.dataaxle_annual
  WHERE family_id IN (SELECT fid FROM sf)
     OR individual_id_1 IN (SELECT iid FROM si) OR individual_id_2 IN (SELECT iid FROM si) OR individual_id_3 IN (SELECT iid FROM si)
     OR individual_id_4 IN (SELECT iid FROM si) OR individual_id_5 IN (SELECT iid FROM si)
) TO '/home/jr2728/ptax_explore/samples/da_bld1pct_fampos.parquet' (FORMAT parquet, COMPRESSION zstd);
