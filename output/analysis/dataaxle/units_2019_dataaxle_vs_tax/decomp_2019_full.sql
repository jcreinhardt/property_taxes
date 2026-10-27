SET preserve_insertion_order=false;
USE memory;
-- Data Axle 2019 street addresses (loose key: zip5 + street name + house number), hashed
CREATE OR REPLACE TABLE dd AS
  SELECT hash(z) hz, hash(z, s) hs, hash(z, s, h) ha, any_value(st) st,
         try_cast(regexp_extract(h, '^(\d+)', 1) AS INT) hn
  FROM (SELECT state st, lpad(zip::VARCHAR, 5, '0') z, upper(trim(street_name)) s, upper(trim(house_num)) h
        FROM cl.dataaxle_annual 
        WHERE record_year=2019 AND address_type='S' AND coalesce(trim(house_num),'')<>'' AND coalesce(trim(street_name),'')<>'' AND zip IS NOT NULL)
  GROUP BY ALL;
-- Tax 2019 parcel rows with a street address, hashed the same way
CREATE OR REPLACE TABLE tx AS
  SELECT hash(z) hz, hash(z, s) hs, hash(z, s, h) ha, st, pic,
         try_cast(regexp_extract(h, '^(\d+)', 1) AS INT) hn, try_cast(regexp_extract(h2, '^(\d+)', 1) AS INT) hn2,
         coalesce(nb, 0) nb, coalesce(nu, 0) nu
  FROM (SELECT left(situs_zip_code, 5) z, upper(trim(situs_street_name)) s, upper(trim(situs_house_number)) h, situs_house_number_2 h2,
               situs_state st, property_indicator_code pic, number_of_buildings nb, total_number_of_units_all_buildings nu
        FROM cl.tax 
        WHERE tax_year=2019 AND coalesce(trim(situs_house_number),'')<>'' AND coalesce(trim(situs_street_name),'')<>'' AND length(situs_zip_code)>=5);
CREATE OR REPLACE TABLE tz AS SELECT DISTINCT hz FROM tx;
CREATE OR REPLACE TABLE ts AS SELECT DISTINCT hs FROM tx;
CREATE OR REPLACE TABLE ta AS SELECT DISTINCT ha FROM tx;
-- Classify each Data Axle address: 0 matched, 1 zip not in tax, 2 street not in tax, 3 number not in tax
CREATE OR REPLACE TABLE c AS
  SELECT d.*, CASE WHEN d.ha IN (SELECT ha FROM ta) THEN 0 WHEN d.hz NOT IN (SELECT hz FROM tz) THEN 1
                   WHEN d.hs NOT IN (SELECT hs FROM ts) THEN 2 ELSE 3 END cls
  FROM dd d;
-- Class 3: is the number inside a tax range on that street, or near (±10, same parity) a multi-building/multi-unit parcel?
CREATE OR REPLACE TABLE r3 AS
  SELECT c.ha,
         bool_or(t.hn2 > t.hn AND t.hn2 - t.hn <= 500 AND t.st <> 'HI' AND c.hn BETWEEN t.hn AND t.hn2) in_range,
         bool_or((t.nb > 1 OR t.nu > 1) AND abs(c.hn - t.hn) <= 10 AND (c.hn - t.hn) % 2 = 0) near_multi,
         bool_or(abs(c.hn - t.hn) <= 10 AND (c.hn - t.hn) % 2 = 0) near_any
  FROM c JOIN tx t USING (hs) WHERE c.cls = 3 AND c.hn IS NOT NULL GROUP BY c.ha;
COPY (
  SELECT c.st AS state,
         CASE c.cls WHEN 0 THEN '0_matched' WHEN 1 THEN '1_zip_not_in_tax' WHEN 2 THEN '2_street_not_in_tax'
           ELSE CASE WHEN c.hn IS NULL THEN '3e_number_nonnumeric' WHEN r.in_range THEN '3a_inside_tax_range'
                     WHEN r.near_multi THEN '3b_near_multi_bldg_or_unit_parcel' WHEN r.near_any THEN '3c_near_other_parcel'
                     ELSE '3d_other' END END AS cls,
         count(*) n
  FROM c LEFT JOIN r3 r USING (ha) GROUP BY ALL ORDER BY ALL
) TO '/home/jr2728/ptax_explore/hh_strat/out/decomp_2019_da_classes.csv' (HEADER);
-- Reverse: tax addresses not in Data Axle, by property indicator (any parcel at the address)
COPY (
  WITH a AS (SELECT ha, min(pic) pic, max(nu) nu FROM tx GROUP BY ha)
  SELECT pic AS property_indicator_code, count(*) tax_addresses, count(*) FILTER (WHERE ha IN (SELECT ha FROM dd)) in_dataaxle,
         count(*) FILTER (WHERE nu > 1) multi_unit
  FROM a GROUP BY ALL ORDER BY ALL
) TO '/home/jr2728/ptax_explore/hh_strat/out/decomp_2019_tax_by_pic.csv' (HEADER);
-- Context: tax rows overall and without a usable street address
COPY (
  SELECT property_indicator_code, count(*) rows_2019,
         count(*) FILTER (WHERE coalesce(trim(situs_house_number),'')='' OR coalesce(trim(situs_street_name),'')='' OR coalesce(length(situs_zip_code),0)<5) rows_no_street_address,
         count(*) FILTER (WHERE situs_house_number_2 IS NOT NULL) rows_with_range
  FROM cl.tax  WHERE tax_year=2019 GROUP BY ALL ORDER BY ALL
) TO '/home/jr2728/ptax_explore/hh_strat/out/decomp_2019_tax_rows.csv' (HEADER);
