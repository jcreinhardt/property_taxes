SET preserve_insertion_order=false;
USE memory;
SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
-- Full-table check of tax-roll gaps found in the 1% sample (gap_evidence.sql).
-- (B) isolated gaps: do the parcel's CLIPs (from y-1 / y+1) have a row in year y anywhere in the table? With what address?
CREATE OR REPLACE TABLE ig AS SELECT bkey, y, clip FROM '/home/jr2728/ptax_explore/hh_strat/out/iso_gap_clips.parquet';
CREATE OR REPLACE TABLE igc AS SELECT DISTINCT clip FROM ig;
CREATE OR REPLACE TABLE rows_y AS
  SELECT t.clip, tax_year::INT y, property_indicator_code pic,
         coalesce(trim(situs_house_number),'') <> '' AND coalesce(trim(situs_street_name),'') <> '' AND length(situs_zip_code) >= 5 has_addr,
         CASE WHEN coalesce(trim(situs_house_number),'') <> '' AND coalesce(trim(situs_street_name),'') <> '' AND length(situs_zip_code) >= 5
              THEN hash(left(situs_zip_code,5), upper(trim(situs_street_name)), upper(trim(situs_house_number))) END bkey
  FROM cl.tax t SEMI JOIN igc ON t.clip = igc.clip WHERE tax_year BETWEEN 2012 AND 2021;
COPY (
  SELECT count(*) isolated_gaps,
         avg(found::INT) clip_row_exists_in_gap_year,
         avg((found AND addr_same)::INT) same_address_but_different_key_fields,
         avg((found AND addr_other)::INT) other_address,
         avg((found AND NOT any_addr)::INT) no_situs_address,
         avg((found AND nonres)::INT) coded_non_residential
  FROM (
    SELECT g.bkey, g.y, bool_or(r.clip IS NOT NULL) found, bool_or(r.has_addr AND r.bkey = g.bkey) addr_same,
           bool_or(r.has_addr AND r.bkey <> g.bkey) addr_other, bool_or(r.has_addr) any_addr, bool_or(r.pic NOT IN (10, 11, 21, 22)) nonres
    FROM ig g LEFT JOIN rows_y r ON r.clip = g.clip AND r.y = g.y GROUP BY g.bkey, g.y)
) TO '/home/jr2728/ptax_explore/hh_strat/out/iso_gaps_full.csv' (HEADER);
