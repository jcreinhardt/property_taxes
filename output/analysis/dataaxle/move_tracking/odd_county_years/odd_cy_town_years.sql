SET preserve_insertion_order=false; USE memory; SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
-- Town (situs_city) x tax_year coverage, all years, for the 44 "odd" fips: are the odd years town-level delivery gaps?
CREATE OR REPLACE TABLE odd AS SELECT DISTINCT fips::INT fips FROM read_csv('/home/jr2728/ptax_explore/hh_strat/odd_county_years.csv');
COPY (
  SELECT fips_code::INT fips, tax_year::INT yr, upper(trim(situs_city)) city, count(*) n_rows, count(DISTINCT t.clip) n_clip,
         count(*) FILTER (WHERE property_indicator_code IN (10,11,21,22)) n_res,
         count(DISTINCT left(situs_zip_code,5)) n_zip
  FROM cl.tax t SEMI JOIN odd ON t.fips_code = odd.fips GROUP BY ALL ORDER BY ALL
) TO '/home/jr2728/ptax_explore/hh_strat/out/odd_cy_town_years.csv' (HEADER);
