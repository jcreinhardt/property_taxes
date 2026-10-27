SET preserve_insertion_order=false;
COPY (SELECT lpad(zip::VARCHAR,5,'0') zip, count(*) fam_records,
             count(DISTINCT (upper(trim(street_name)), upper(trim(house_num)))) FILTER (WHERE address_type='S' AND coalesce(trim(house_num),'')<>'' AND coalesce(trim(street_name),'')<>'') addresses
      FROM cl.dataaxle_annual WHERE record_year=2019 AND state='NY' GROUP BY 1 ORDER BY 1)
  TO '/home/jr2728/ptax_explore/hh_strat/out/ny_da_zip_2019.csv' (HEADER);
COPY (SELECT left(situs_zip_code,5) zip, fips_code, count(*) tax_rows,
             count(DISTINCT (upper(trim(situs_street_name)), upper(trim(situs_house_number)))) FILTER (WHERE coalesce(trim(situs_house_number),'')<>'' AND coalesce(trim(situs_street_name),'')<>'') addresses
      FROM cl.tax WHERE tax_year=2019 AND (situs_state='NY' OR fips_code BETWEEN 36000 AND 36999) GROUP BY ALL ORDER BY ALL)
  TO '/home/jr2728/ptax_explore/hh_strat/out/ny_tax_zip_2019.csv' (HEADER);
COPY (SELECT fips_code, tax_year, count(*) tax_rows, count(*) FILTER (WHERE length(situs_zip_code)>=5) with_zip,
             count(*) FILTER (WHERE coalesce(trim(situs_house_number),'')<>'') with_house_no
      FROM cl.tax WHERE fips_code BETWEEN 36000 AND 36999 AND tax_year BETWEEN 2015 AND 2021 GROUP BY ALL ORDER BY ALL)
  TO '/home/jr2728/ptax_explore/hh_strat/out/ny_tax_fips_year.csv' (HEADER);
