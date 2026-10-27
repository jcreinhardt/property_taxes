SET preserve_insertion_order=false;
USE memory;
CREATE OR REPLACE MACRO addr_ok(ht, hn, sn, z) AS ht='S' AND coalesce(trim(hn),'')<>'' AND coalesce(trim(sn),'')<>'' AND z IS NOT NULL;
-- Data Axle 2019 addresses with attributes
CREATE OR REPLACE TABLE da AS
  SELECT lpad(zip::VARCHAR,5,'0') z, upper(trim(street_name)) s, upper(trim(house_num)) h,
         any_value(ge_census_state_2010::INT*1000 + ge_als_county_code_2010::INT) cty,
         count(*) n_fam, count(*) FILTER (WHERE primary_family_ind) n_prim, count(DISTINCT unit_num) n_units,
         bool_or(dpbc IS NOT NULL AND trim(dpbc)<>'') has_dpbc, mode(ge_census_level_2010) geo_level,
         bool_or(usps_nostats) nostats, bool_or(vacant) vacant_flag, min(mailability_score) mail_score,
         avg((household_status='S')::INT) share_status_s, max(recency_date) recency,
         median(length_of_residence) FILTER (WHERE primary_family_ind) lor_prim,
         mode(CASE WHEN owner_renter_status <= 3 THEN 'rent' WHEN owner_renter_status <= 6 THEN 'unk' ELSE 'own' END) FILTER (WHERE primary_family_ind) ors
  FROM cl.dataaxle_annual 
  WHERE record_year=2019 AND addr_ok(address_type, house_num, street_name, zip)
  GROUP BY 1,2,3;
CREATE OR REPLACE TABLE da_other AS
  SELECT DISTINCT lpad(zip::VARCHAR,5,'0') z, upper(trim(street_name)) s, upper(trim(house_num)) h, record_year y
  FROM cl.dataaxle_annual  WHERE record_year IN (2017, 2021) AND addr_ok(address_type, house_num, street_name, zip);
-- Tax 2019
CREATE OR REPLACE TABLE txr AS
  SELECT left(situs_zip_code,5) z, upper(trim(situs_street_name)) s, nullif(upper(trim(situs_house_number)),'') h, fips_code fips, situs_state st,
         try_cast(regexp_extract(situs_house_number,'^(\d+)',1) AS INT) hn, try_cast(regexp_extract(situs_house_number_2,'^(\d+)',1) AS INT) hn2
  FROM cl.tax  WHERE tax_year=2019 AND coalesce(trim(situs_street_name),'')<>'' AND length(situs_zip_code)>=5;
CREATE OR REPLACE TABLE tx AS SELECT DISTINCT z, s, h FROM txr WHERE h IS NOT NULL;
CREATE OR REPLACE TABLE tz AS SELECT DISTINCT z FROM txr;
CREATE OR REPLACE TABLE ts AS SELECT DISTINCT z, s FROM txr WHERE h IS NOT NULL;
CREATE OR REPLACE TABLE tsn AS SELECT DISTINCT z, s FROM txr WHERE h IS NULL;
CREATE OR REPLACE TABLE tcty AS SELECT DISTINCT fips FROM txr WHERE h IS NOT NULL;
CREATE OR REPLACE TABLE du AS SELECT * FROM da ANTI JOIN tx USING (z, s, h);
CREATE OR REPLACE TABLE tu AS SELECT * FROM tx ANTI JOIN (SELECT z, s, h FROM da) USING (z, s, h);
CREATE OR REPLACE MACRO near_street(a, b) AS a <> b AND least(length(a), length(b)) >= 4 AND (levenshtein(a, b) <= 2 OR jaro_winkler_similarity(a, b) >= 0.92);
CREATE OR REPLACE TABLE du_c AS
  SELECT u.z, u.s, u.h,
    CASE
      WHEN u.cty NOT IN (SELECT fips FROM tcty) THEN '1_county_not_in_tax'
      WHEN EXISTS (SELECT 1 FROM txr t WHERE t.z=u.z AND t.s=u.s AND t.hn2 > t.hn AND t.hn2 - t.hn <= 500 AND t.st <> 'HI'
                   AND try_cast(regexp_extract(u.h,'^(\d+)',1) AS INT) BETWEEN t.hn AND t.hn2) THEN '2_inside_tax_range'
      WHEN EXISTS (SELECT 1 FROM tu t WHERE t.s=u.s AND t.h=u.h AND t.z<>u.z AND left(t.z,3)=left(u.z,3)) THEN '3_zip_near_miss'
      WHEN EXISTS (SELECT 1 FROM tu t WHERE t.z=u.z AND t.h=u.h AND near_street(u.s, t.s)) THEN '4_street_near_miss'
      WHEN EXISTS (SELECT 1 FROM tu t WHERE t.z=u.z AND t.s=u.s AND length(t.h)=length(u.h) AND levenshtein(t.h,u.h)=1) THEN '5_number_one_char_off'
      WHEN u.z NOT IN (SELECT z FROM tz) THEN '8_zip_not_in_tax'
      WHEN (u.z, u.s) IN (SELECT (z, s) FROM ts) THEN '6_street_in_tax_number_missing'
      WHEN (u.z, u.s) IN (SELECT (z, s) FROM tsn) THEN '7a_street_in_tax_only_without_numbers'
      ELSE '7b_street_not_in_tax' END cls
  FROM du u;
CREATE OR REPLACE TABLE allc AS
  SELECT d.*, coalesce(c.cls, '0_matched') cls,
         EXISTS (SELECT 1 FROM da_other o WHERE o.z=d.z AND o.s=d.s AND o.h=d.h AND o.y=2017) in_2017,
         EXISTS (SELECT 1 FROM da_other o WHERE o.z=d.z AND o.s=d.s AND o.h=d.h AND o.y=2021) in_2021
  FROM da d LEFT JOIN du_c c USING (z, s, h);
COPY (
  SELECT cls, count(*) n_addr, avg(n_fam) fam_per_addr, avg(n_prim) prim_per_addr, avg((n_units>0)::INT) share_with_units,
         avg(has_dpbc::INT) share_dpbc, avg(nostats::INT) share_usps_nostats, avg(vacant_flag::INT) share_vacant,
         avg((mail_score=1)::INT) share_mailscore1, avg(share_status_s) share_status_s,
         avg((try_cast(recency AS INT) >= 201801)::INT) share_recent_2018plus, median(lor_prim) med_lor,
         avg((ors='own')::INT) share_owner, avg((ors='rent')::INT) share_renter,
         avg(in_2017::INT) share_in_2017, avg(in_2021::INT) share_in_2021, avg((in_2017 AND in_2021)::INT) share_in_both
  FROM allc GROUP BY cls ORDER BY cls
) TO '/home/jr2728/ptax_explore/hh_strat/out/resid_2019_by_class.csv' (HEADER);
COPY (SELECT cls, geo_level, count(*) n FROM allc GROUP BY ALL ORDER BY ALL) TO '/home/jr2728/ptax_explore/hh_strat/out/resid_2019_geolevel.csv' (HEADER);
COPY (SELECT cls, cty//1000 state_fips, count(*) n FROM allc GROUP BY ALL ORDER BY ALL) TO '/home/jr2728/ptax_explore/hh_strat/out/resid_2019_state.csv' (HEADER);
COPY (SELECT z, h, s, cls, n_fam, n_prim, n_units, has_dpbc, geo_level, nostats, vacant_flag, mail_score, recency, lor_prim, ors, in_2017, in_2021
      FROM allc WHERE cls IN ('6_street_in_tax_number_missing','7a_street_in_tax_only_without_numbers','7b_street_not_in_tax','8_zip_not_in_tax')
      USING SAMPLE 400 ROWS) TO '/home/jr2728/ptax_explore/hh_strat/out/resid_2019_residual_sample.csv' (HEADER);
