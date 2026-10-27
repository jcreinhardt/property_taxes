SET preserve_insertion_order=false; USE memory; SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
-- Extract all tax rows for the 44 "odd" county-years (tax roll exists in year y but parcels fail to match by address key)
-- plus their adjacent years y-1 and y+1. Situs address fields only (no owner names).
CREATE OR REPLACE TABLE odd AS SELECT fips::INT fips, y::INT y FROM read_csv('/home/jr2728/ptax_explore/hh_strat/odd_county_years.csv');
CREATE OR REPLACE TABLE want AS SELECT DISTINCT fips, y + d AS yr FROM odd, (SELECT unnest([-1, 0, 1]) d);
CREATE OR REPLACE TABLE ext AS
  SELECT t.clip, apn_parcel_number_unformatted, apn_sequence_number, fips_code::INT fips, tax_year::INT yr, property_indicator_code pic,
         situs_house_number, situs_house_number_suffix, situs_house_number_2, situs_direction, situs_street_name, situs_mode, situs_quadrant,
         situs_unit_number, situs_city, situs_state, situs_zip_code, situs_street_address, situs_carrier_route, situs_city_state_zip_source,
         beginning_tax_year_date, ending_tax_year_date
  FROM cl.tax t SEMI JOIN want ON t.fips_code::INT = want.fips AND t.tax_year::INT = want.yr;
COPY ext TO '/home/jr2728/ptax_explore/hh_strat/out/odd_cy_all.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
COPY (SELECT * FROM ext WHERE fips = 25017 AND yr BETWEEN 2013 AND 2015) TO '/home/jr2728/ptax_explore/hh_strat/out/odd_cy_25017.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM ext WHERE fips = 33015 AND yr BETWEEN 2013 AND 2015) TO '/home/jr2728/ptax_explore/hh_strat/out/odd_cy_33015.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM ext WHERE fips = 23005 AND yr BETWEEN 2013 AND 2015) TO '/home/jr2728/ptax_explore/hh_strat/out/odd_cy_23005.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM ext WHERE fips = 9011 AND yr BETWEEN 2014 AND 2016) TO '/home/jr2728/ptax_explore/hh_strat/out/odd_cy_9011.parquet' (FORMAT PARQUET);
SELECT fips, yr, count(*) n, count(DISTINCT clip) n_clip FROM ext GROUP BY ALL ORDER BY ALL;
