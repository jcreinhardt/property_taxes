SET preserve_insertion_order=false;
USE memory;
SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
-- Full-table check of tax-roll gaps found in the 1% sample (gap_evidence.sql).
-- (A) dropped county-years (>= 90% of sampled bridging parcels missing): rows in cl.tax that year, total and with a usable situs address,
--     vs the same county in the adjacent years.
CREATE OR REPLACE TABLE dcy AS SELECT fips, y FROM '/home/jr2728/ptax_explore/hh_strat/out/dropped_county_years.parquet';
CREATE OR REPLACE TABLE dfips AS SELECT DISTINCT fips FROM dcy;
CREATE OR REPLACE TABLE cnt AS
  SELECT fips_code fips, tax_year::INT y, count(*) n_rows,
         count(*) FILTER (WHERE coalesce(trim(situs_house_number),'') <> '' AND coalesce(trim(situs_street_name),'') <> '' AND length(situs_zip_code) >= 5) n_addr,
         count(*) FILTER (WHERE property_indicator_code IN (10, 11, 21, 22)) n_res
  FROM cl.tax SEMI JOIN dfips ON fips_code = dfips.fips WHERE tax_year BETWEEN 2012 AND 2021 GROUP BY 1, 2;
COPY (
  SELECT d.fips, d.y, coalesce(c.n_rows, 0) rows_in_year, coalesce(c.n_addr, 0) rows_with_address, coalesce(c.n_res, 0) residential_rows,
         p.n_rows rows_prev_year, n.n_rows rows_next_year
  FROM dcy d LEFT JOIN cnt c ON c.fips = d.fips AND c.y = d.y LEFT JOIN cnt p ON p.fips = d.fips AND p.y = d.y - 1 LEFT JOIN cnt n ON n.fips = d.fips AND n.y = d.y + 1
  ORDER BY d.y, d.fips
) TO '/home/jr2728/ptax_explore/hh_strat/out/dropped_county_years_full.csv' (HEADER);
-- (B) isolated gaps: do the parcel's CLIPs (from y-1 / y+1) have a row in year y anywhere in the table? With what address?
CREATE OR REPLACE TABLE ig AS SELECT bkey, y, clip FROM '/home/jr2728/ptax_explore/hh_strat/out/iso_gap_clips.parquet';
CREATE OR REPLACE TABLE igc AS SELECT DISTINCT clip FROM ig;
CREATE OR REPLACE TABLE rows_y AS
  SELECT clip, tax_year::INT y, property_indicator_code pic,
         coalesce(trim(situs_house_number),'') <> '' AND coalesce(trim(situs_street_name),'') <> '' AND length(situs_zip_code) >= 5 has_addr,
         CASE WHEN coalesce(trim(situs_house_number),'') <> '' AND coalesce(trim(situs_street_name),'') <> '' AND length(situs_zip_code) >= 5
              THEN hash(left(situs_zip_code,5), upper(trim(situs_street_name)), upper(trim(situs_house_number))) END bkey
  FROM cl.tax SEMI JOIN igc ON clip = igc.clip WHERE tax_year BETWEEN 2012 AND 2021;
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
