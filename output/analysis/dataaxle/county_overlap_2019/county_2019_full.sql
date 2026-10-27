SET preserve_insertion_order=false;
USE memory;
CREATE OR REPLACE TABLE dd AS
  SELECT hash(z, s, h) ha, any_value(cty) cty
  FROM (SELECT lpad(zip::VARCHAR, 5, '0') z, upper(trim(street_name)) s, upper(trim(house_num)) h,
               ge_census_state_2010::INT * 1000 + ge_als_county_code_2010::INT cty
        FROM cl.dataaxle_annual 
        WHERE record_year=2019 AND address_type='S' AND coalesce(trim(house_num),'')<>'' AND coalesce(trim(street_name),'')<>'' AND zip IS NOT NULL)
  GROUP BY ALL;
CREATE OR REPLACE TABLE tx AS
  SELECT hash(z, s, h) ha, any_value(fips_code) fips, bool_or(pic IN (10, 11, 21, 22)) res
  FROM (SELECT left(situs_zip_code, 5) z, upper(trim(situs_street_name)) s, upper(trim(situs_house_number)) h, fips_code, property_indicator_code pic
        FROM cl.tax 
        WHERE tax_year=2019 AND coalesce(trim(situs_house_number),'')<>'' AND coalesce(trim(situs_street_name),'')<>'' AND length(situs_zip_code)>=5)
  GROUP BY ALL;
COPY (SELECT d.cty AS fips, count(*) da_addr, count(t.ha) da_in_cl FROM dd d LEFT JOIN tx t USING (ha) GROUP BY ALL ORDER BY ALL) TO '/home/jr2728/ptax_explore/hh_strat/out/county_2019_da.csv' (HEADER);
COPY (SELECT t.fips, count(*) cl_addr, count(d.ha) cl_in_da, count(*) FILTER (WHERE res) cl_res_addr, count(d.ha) FILTER (WHERE res) cl_res_in_da
      FROM tx t LEFT JOIN dd d USING (ha) GROUP BY ALL ORDER BY ALL) TO '/home/jr2728/ptax_explore/hh_strat/out/county_2019_cl.csv' (HEADER);
