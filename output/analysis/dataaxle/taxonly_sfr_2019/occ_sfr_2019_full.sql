SET preserve_insertion_order=false;
USE memory;
CREATE OR REPLACE TABLE da19 AS
  SELECT DISTINCT lpad(zip::VARCHAR,5,'0') z, upper(trim(street_name)) s, upper(trim(house_num)) h, true AS hit
  FROM cl.dataaxle_annual
  WHERE record_year=2019 AND address_type='S' AND coalesce(trim(house_num),'')<>'' AND coalesce(trim(street_name),'')<>'' AND zip IS NOT NULL;
-- single-family parcels 2019: owner-occupancy flag vs mailing address type and corporate owner (parcel level)
CREATE OR REPLACE TABLE p AS
  SELECT left(situs_zip_code,5) z, upper(trim(situs_street_name)) s, upper(trim(situs_house_number)) h,
         coalesce(owner_occupancy_code, 'NULL') occ, coalesce(owner_1_corporate_indicator, false) corp,
         CASE WHEN coalesce(trim(mailing_street_name),'')='' AND coalesce(trim(mailing_street_address),'')='' THEN '0_no_mailing_address'
              WHEN regexp_matches(upper(coalesce(mailing_street_address,'') || ' ' || coalesce(mailing_street_name,'')), 'P ?O BOX|^BOX |PO BX') THEN '2_po_box'
              WHEN left(coalesce(mailing_zip_code,''),5)=left(situs_zip_code,5)
                   AND upper(trim(coalesce(mailing_house_number,'')))=upper(trim(situs_house_number))
                   AND upper(trim(coalesce(mailing_street_name,'')))=upper(trim(situs_street_name)) THEN '1_at_property'
              WHEN upper(trim(coalesce(mailing_state,'')))<>upper(trim(situs_state)) THEN '4_elsewhere_other_state'
              ELSE '3_elsewhere_same_state' END mail
  FROM cl.tax
  WHERE tax_year=2019 AND property_indicator_code=10 AND coalesce(trim(situs_house_number),'')<>''
    AND coalesce(trim(situs_street_name),'')<>'' AND length(situs_zip_code)>=5;
COPY (
  SELECT coalesce(d.hit, false) in_da19, mail, corp, occ, count(*) parcels
  FROM p LEFT JOIN da19 d USING (z, s, h) GROUP BY ALL ORDER BY ALL
) TO '/home/jr2728/ptax_explore/hh_strat/out/occ_sfr_2019.csv' (HEADER);
