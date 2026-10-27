SET preserve_insertion_order=false;
USE memory;
SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
-- Persistent 1% samples (all years), written to ~/ptax_explore/samples/. Personal names are hashed (hash(upper(trim(x)))), never stored raw.
--   da_fam1pct:   Data Axle rows whose family_id hashes into 1% (follow families across addresses)
--   da_bld1pct:   Data Axle street-address rows whose building key hashes into 1%
--   tax_bld1pct:  tax rows (all tax years) whose situs building key hashes into the same 1%
--   deeds_bld1pct: ownertransfer rows for those parcels (by clip) or whose deed situs key is in the 1%
-- Building key bkey = hash(ZIP5, upper(trim(street name)), upper(trim(house number))), identical in both sources.
CREATE OR REPLACE MACRO hn(x) AS CASE WHEN coalesce(trim(x), '') <> '' THEN hash(upper(trim(x))) END;
COPY (
  SELECT * EXCLUDE (first_name_1, middle_name_1, last_name_1, first_name_2, middle_name_2, last_name_2, first_name_3, middle_name_3, last_name_3,
                    first_name_4, middle_name_4, last_name_4, first_name_5, middle_name_5, last_name_5),
         hn(last_name_1) ln_1, hn(last_name_2) ln_2, hn(last_name_3) ln_3, hn(last_name_4) ln_4, hn(last_name_5) ln_5,
         hn(first_name_1) fn_1, hn(first_name_2) fn_2, hn(first_name_3) fn_3, hn(first_name_4) fn_4, hn(first_name_5) fn_5,
         CASE WHEN address_type = 'S' AND coalesce(trim(house_num),'') <> '' AND coalesce(trim(street_name),'') <> '' AND zip IS NOT NULL
              THEN hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num))) END bkey
  FROM cl.dataaxle_annual WHERE hash(family_id) % 100 = 0
) TO '/home/jr2728/ptax_explore/samples/da_fam1pct.parquet' (FORMAT parquet, COMPRESSION zstd);
COPY (
  SELECT * EXCLUDE (first_name_1, middle_name_1, last_name_1, first_name_2, middle_name_2, last_name_2, first_name_3, middle_name_3, last_name_3,
                    first_name_4, middle_name_4, last_name_4, first_name_5, middle_name_5, last_name_5),
         hn(last_name_1) ln_1, hn(last_name_2) ln_2, hn(last_name_3) ln_3, hn(last_name_4) ln_4, hn(last_name_5) ln_5,
         hn(first_name_1) fn_1, hn(first_name_2) fn_2, hn(first_name_3) fn_3, hn(first_name_4) fn_4, hn(first_name_5) fn_5,
         hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num))) bkey
  FROM cl.dataaxle_annual
  WHERE address_type = 'S' AND coalesce(trim(house_num),'') <> '' AND coalesce(trim(street_name),'') <> '' AND zip IS NOT NULL
    AND hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num))) % 100 = 0
) TO '/home/jr2728/ptax_explore/samples/da_bld1pct.parquet' (FORMAT parquet, COMPRESSION zstd);
CREATE OR REPLACE TABLE tx AS
  SELECT * EXCLUDE (owner_1_full_name, owner_1_last_name, owner_1_first_name_middle_initial, owner_2_full_name, owner_2_last_name,
                    owner_2_first_name_middle_initial, owner_3_full_name, owner_3_last_name, owner_3_first_name_middle_initial,
                    owner_4_full_name, owner_4_last_name, owner_4_first_name_middle_initial, owner_care_of_name),
         hn(owner_1_last_name) o1_ln, hn(owner_1_full_name) o1_full, hn(owner_1_first_name_middle_initial) o1_fn,
         hn(owner_2_last_name) o2_ln, hn(owner_2_full_name) o2_full, hn(owner_2_first_name_middle_initial) o2_fn,
         hn(owner_3_last_name) o3_ln, hn(owner_4_last_name) o4_ln, hn(owner_care_of_name) care_of,
         hash(left(situs_zip_code,5), upper(trim(situs_street_name)), upper(trim(situs_house_number))) bkey
  FROM cl.tax
  WHERE coalesce(trim(situs_house_number),'') <> '' AND coalesce(trim(situs_street_name),'') <> '' AND length(situs_zip_code) >= 5
    AND hash(left(situs_zip_code,5), upper(trim(situs_street_name)), upper(trim(situs_house_number))) % 100 = 0;
COPY tx TO '/home/jr2728/ptax_explore/samples/tax_bld1pct.parquet' (FORMAT parquet, COMPRESSION zstd);
CREATE OR REPLACE TABLE sclip AS SELECT DISTINCT clip FROM tx WHERE clip IS NOT NULL;
COPY (
  SELECT * EXCLUDE (buyer_1_full_name, buyer_1_last_name, buyer_1_first_name_and_middle_initial, buyer_2_full_name, buyer_2_last_name,
                    buyer_2_first_name_and_middle_initial, buyer_3_full_name, buyer_3_last_name, buyer_3_first_name_and_middle_initial,
                    buyer_4_full_name, buyer_4_last_name, buyer_4_first_name_and_middle_initial, buyer_care_of_name,
                    seller_1_full_name, seller_1_last_name, seller_1_first_name, seller_2_full_name),
         hn(buyer_1_last_name) b1_ln, hn(buyer_1_full_name) b1_full, hn(buyer_1_first_name_and_middle_initial) b1_fn,
         hn(buyer_2_last_name) b2_ln, hn(buyer_2_full_name) b2_full, hn(buyer_2_first_name_and_middle_initial) b2_fn,
         hn(buyer_3_last_name) b3_ln, hn(buyer_4_last_name) b4_ln, hn(buyer_care_of_name) b_care_of,
         hn(seller_1_last_name) s1_ln, hn(seller_1_full_name) s1_full, hn(seller_1_first_name) s1_fn, hn(seller_2_full_name) s2_full,
         CASE WHEN coalesce(trim(deed_situs_house_number),'') <> '' AND coalesce(trim(deed_situs_street_name),'') <> '' AND length(deed_situs_zip_code) >= 5
              THEN hash(left(deed_situs_zip_code,5), upper(trim(deed_situs_street_name)), upper(trim(deed_situs_house_number))) END bkey
  FROM cl.ownertransfer o
  WHERE o.clip IN (SELECT clip FROM sclip)
     OR (coalesce(trim(deed_situs_house_number),'') <> '' AND coalesce(trim(deed_situs_street_name),'') <> '' AND length(deed_situs_zip_code) >= 5
         AND hash(left(deed_situs_zip_code,5), upper(trim(deed_situs_street_name)), upper(trim(deed_situs_house_number))) % 100 = 0)
) TO '/home/jr2728/ptax_explore/samples/deeds_bld1pct.parquet' (FORMAT parquet, COMPRESSION zstd);
