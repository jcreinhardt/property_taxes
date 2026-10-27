SET preserve_insertion_order=false;
USE memory;
CREATE OR REPLACE TEMP MACRO sfx(x) AS CASE upper(trim(x))
  WHEN 'AV' THEN 'AVE' WHEN 'CI' THEN 'CIR' WHEN 'WA' THEN 'WAY' WHEN 'BL' THEN 'BLVD' WHEN 'TR' THEN 'TRL' WHEN 'TE' THEN 'TER'
  WHEN 'HW' THEN 'HWY' WHEN 'LO' THEN 'LOOP' WHEN 'PI' THEN 'PIKE' WHEN 'RU' THEN 'RUN' WHEN 'EX' THEN 'EXT' WHEN 'XI' THEN 'XING'
  WHEN 'HT' THEN 'HTS' WHEN 'TP' THEN 'TPKE' WHEN 'BN' THEN 'BND' WHEN 'RO' THEN 'ROW' WHEN 'MN' THEN 'MNR' WHEN 'GL' THEN 'GLN'
  WHEN 'HO' THEN 'HOLW' WHEN 'FW' THEN 'FWY' WHEN 'VI' THEN 'VIS' WHEN 'AL' THEN 'ALY' WHEN 'SP' THEN 'SPGS' WHEN 'ES' THEN 'EST'
  WHEN 'CY' THEN 'CYN' WHEN 'PZ' THEN 'PLZ' WHEN 'RI' THEN 'RIV' ELSE nullif(upper(trim(x)),'') END;
CREATE OR REPLACE TABLE da AS SELECT DISTINCT
    lpad(zip::VARCHAR, 5, '0') z, upper(trim(house_num)) h, nullif(upper(trim(street_pre_dir)),'') pre,
    upper(trim(street_name)) s, nullif(upper(trim(street_post_dir)),'') post, sfx(street_suffix) sf
  FROM cl.dataaxle_annual 
  WHERE record_year=2019 AND address_type='S' AND coalesce(trim(house_num),'')<>'' AND coalesce(trim(street_name),'')<>'' AND zip IS NOT NULL;
CREATE OR REPLACE TABLE tx AS SELECT DISTINCT
    left(situs_zip_code, 5) z, upper(trim(situs_house_number)) h, nullif(upper(trim(situs_direction)),'') pre,
    upper(trim(situs_street_name)) s, nullif(upper(trim(situs_quadrant)),'') post, nullif(upper(trim(situs_mode)),'') sf
  FROM cl.tax 
  WHERE tax_year=2019 AND coalesce(trim(situs_house_number),'')<>'' AND coalesce(trim(situs_street_name),'')<>'' AND length(situs_zip_code)>=5;
COPY (
  WITH ds AS (SELECT DISTINCT z,h,pre,s,post,sf FROM da), ts AS (SELECT DISTINCT z,h,pre,s,post,sf FROM tx),
       dl AS (SELECT DISTINCT z,h,s FROM da), tl AS (SELECT DISTINCT z,h,s FROM tx)
  SELECT 'strict' AS addr_key, (SELECT count(*) FROM ds) dataaxle, (SELECT count(*) FROM ts) tax,
         (SELECT count(*) FROM ds JOIN ts USING (z,h,s) WHERE ds.pre IS NOT DISTINCT FROM ts.pre AND ds.post IS NOT DISTINCT FROM ts.post AND ds.sf IS NOT DISTINCT FROM ts.sf) both_sources
  UNION ALL
  SELECT 'loose', (SELECT count(*) FROM dl), (SELECT count(*) FROM tl), (SELECT count(*) FROM dl JOIN tl USING (z,h,s))
) TO '/home/jr2728/ptax_explore/hh_strat/out/addr_2019_full.csv' (HEADER);
