SET preserve_insertion_order=false;
USE memory;
-- Loose address key as in decomp_2019_full.sql: zip5 + street name + house number. "Street" = zip5 + street name.
CREATE OR REPLACE TABLE da AS
  SELECT lpad(zip::VARCHAR,5,'0') z, upper(trim(street_name)) s, upper(trim(house_num)) h, any_value(state) st, true AS in_da
  FROM cl.dataaxle_annual
  WHERE record_year=2019 AND address_type='S' AND coalesce(trim(house_num),'')<>'' AND coalesce(trim(street_name),'')<>'' AND zip IS NOT NULL
  GROUP BY 1,2,3;
CREATE OR REPLACE TABLE tx AS
  SELECT left(situs_zip_code,5) z, upper(trim(situs_street_name)) s, upper(trim(situs_house_number)) h, any_value(situs_state) st, true AS in_tax,
         bool_or(property_indicator_code IN (10,11,21,22)) res
  FROM cl.tax
  WHERE tax_year=2019 AND coalesce(trim(situs_house_number),'')<>'' AND coalesce(trim(situs_street_name),'')<>'' AND length(situs_zip_code)>=5
  GROUP BY 1,2,3;
CREATE OR REPLACE TABLE a AS
  SELECT z, s, h, coalesce(d.st, t.st) st, coalesce(d.in_da,false) in_da, coalesce(t.in_tax,false) in_tax, coalesce(t.res,false) res
  FROM da d FULL JOIN tx t USING (z, s, h);
-- one row per street: counts of addresses in DA, tax, both; tax-only split into residential / non-residential property types
COPY (
  SELECT z, s, any_value(st) st, count(*) n_union,
         count(*) FILTER (WHERE in_da) n_da, count(*) FILTER (WHERE in_tax) n_tax, count(*) FILTER (WHERE in_da AND in_tax) n_both,
         count(*) FILTER (WHERE in_tax AND NOT in_da AND res) n_tax_only_res
  FROM a GROUP BY z, s
) TO '/home/jr2728/ptax_explore/hh_strat/out/streets_2019.parquet' (FORMAT parquet, COMPRESSION zstd);
