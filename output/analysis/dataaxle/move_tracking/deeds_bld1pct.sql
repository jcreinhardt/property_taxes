SET preserve_insertion_order=false;
USE memory;
SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
-- 1% building sample of ownertransfer (deeds) by deed situs key; buyer/seller names hashed. Same bkey as da_bld1pct / tax_bld1pct.
CREATE OR REPLACE MACRO hn(x) AS CASE WHEN coalesce(trim(x), '') <> '' THEN hash(upper(trim(x))) END;
COPY (
  SELECT * EXCLUDE (buyer_1_full_name, buyer_1_last_name, buyer_1_first_name_and_middle_initial, buyer_2_full_name, buyer_2_last_name,
                    buyer_2_first_name_and_middle_initial, buyer_3_full_name, buyer_3_last_name, buyer_3_first_name_and_middle_initial,
                    buyer_4_full_name, buyer_4_last_name, buyer_4_first_name_and_middle_initial, buyer_care_of_name,
                    seller_1_full_name, seller_1_last_name, seller_1_first_name, seller_2_full_name),
         hn(buyer_1_last_name) b1_ln, hn(buyer_1_full_name) b1_full, hn(buyer_1_first_name_and_middle_initial) b1_fn,
         hn(buyer_2_last_name) b2_ln, hn(buyer_2_full_name) b2_full, hn(buyer_2_first_name_and_middle_initial) b2_fn,
         hn(seller_1_last_name) s1_ln, hn(seller_1_full_name) s1_full, hn(seller_1_first_name) s1_fn, hn(seller_2_full_name) s2_full,
         hash(left(deed_situs_zip_code,5), upper(trim(deed_situs_street_name)), upper(trim(deed_situs_house_number))) bkey
  FROM cl.ownertransfer
  WHERE coalesce(trim(deed_situs_house_number),'') <> '' AND coalesce(trim(deed_situs_street_name),'') <> '' AND length(deed_situs_zip_code) >= 5
    AND hash(left(deed_situs_zip_code,5), upper(trim(deed_situs_street_name)), upper(trim(deed_situs_house_number))) % 100 = 0
) TO '/home/jr2728/ptax_explore/samples/deeds_bld1pct.parquet' (FORMAT parquet, COMPRESSION zstd);
