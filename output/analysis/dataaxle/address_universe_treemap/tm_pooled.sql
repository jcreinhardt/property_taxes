SET preserve_insertion_order=false;
USE memory;
-- Address universes pooled over 2012-2021: Data Axle street addresses (any record_year) vs tax situs addresses (any tax_year).
-- Loose key = ZIP5 + street name + house number. Matched = key in both, in any (not necessarily the same) year.
-- Same classification logic as resid_2019_full.sql / geo_2019_full.sql (DA side) and taxonly_sfr_2019_full.sql (tax side),
-- with attributes from the latest year a key is seen. Only counts are exported.
CREATE OR REPLACE MACRO near_street(a, b) AS a <> b AND least(length(a), length(b)) >= 4 AND (levenshtein(a, b) <= 2 OR jaro_winkler_similarity(a, b) >= 0.92);
CREATE OR REPLACE TABLE da AS
  SELECT z, s, h, arg_max(struct_pack(cty := cty, geo := geo, lat := lat, lon := lon), y) lst, bit_or(1 << (y - 2012)) dmask
  FROM (SELECT lpad(zip::VARCHAR,5,'0') z, upper(trim(street_name)) s, upper(trim(house_num)) h, record_year::INT y,
               ge_census_state_2010::INT*1000 + ge_als_county_code_2010::INT cty, ge_census_level_2010 geo,
               ge_latitude_2010 lat, ge_longitude_2010 lon
        FROM cl.dataaxle_annual
        WHERE record_year BETWEEN 2012 AND 2021 AND address_type='S' AND coalesce(trim(house_num),'')<>''
          AND coalesce(trim(street_name),'')<>'' AND zip IS NOT NULL)
  GROUP BY z, s, h;
CREATE OR REPLACE TABLE da AS SELECT z, s, h, lst.cty cty, lst.geo geo, lst.lat lat, lst.lon lon, dmask FROM da;
-- tax address keys with latest-year attributes
CREATE OR REPLACE TABLE tx AS
  SELECT z, s, h, any_value(fips) fips, bit_or(1 << (y - 2012)) tmask,
         arg_max(struct_pack(pic := pic, lat := lat, lon := lon, occ := occ, absentee := absentee, out_of_state := out_of_state,
                             po_box := po_box, corp := corp, dpv := dpv, dpv_vacant := dpv_vacant, yb := yb, no_impr := no_impr), y) a
  FROM (SELECT tax_year::INT y, left(situs_zip_code,5) z, upper(trim(situs_street_name)) s, upper(trim(situs_house_number)) h, fips_code fips,
               property_indicator_code pic, parcel_level_latitude lat, parcel_level_longitude lon, owner_occupancy_code occ,
               CASE WHEN coalesce(trim(mailing_street_name),'')='' THEN NULL
                    ELSE NOT (left(coalesce(mailing_zip_code,''),5)=left(situs_zip_code,5)
                              AND upper(trim(coalesce(mailing_house_number,'')))=upper(trim(situs_house_number))
                              AND upper(trim(mailing_street_name))=upper(trim(situs_street_name))) END absentee,
               CASE WHEN coalesce(trim(mailing_state),'')='' THEN NULL ELSE upper(trim(mailing_state))<>upper(trim(situs_state)) END out_of_state,
               regexp_matches(upper(coalesce(mailing_street_address,'') || ' ' || coalesce(mailing_street_name,'')), 'P ?O BOX|^BOX |PO BX') po_box,
               coalesce(owner_1_corporate_indicator,false) corp,
               situs_delivery_point_validation_code dpv, situs_delivery_point_validation_vacant_indicator dpv_vacant,
               nullif(try_cast(year_built AS INT),0) yb,
               (coalesce(calculated_improvement_value,0)=0 AND coalesce(total_living_area_square_feet_all_buildings,0)=0) no_impr
        FROM cl.tax
        WHERE tax_year BETWEEN 2012 AND 2021 AND coalesce(trim(situs_house_number),'')<>'' AND coalesce(trim(situs_street_name),'')<>''
          AND length(situs_zip_code)>=5)
  GROUP BY z, s, h;
-- helper sets over all tax rows with a street name (numbered or not)
CREATE OR REPLACE TABLE txs AS
  SELECT DISTINCT left(situs_zip_code,5) z, upper(trim(situs_street_name)) s, coalesce(trim(situs_house_number),'')<>'' numbered
  FROM cl.tax WHERE tax_year BETWEEN 2012 AND 2021 AND coalesce(trim(situs_street_name),'')<>'' AND length(situs_zip_code)>=5;
CREATE OR REPLACE TABLE tz AS SELECT DISTINCT z FROM txs;
CREATE OR REPLACE TABLE ts AS SELECT DISTINCT z, s FROM txs WHERE numbered;
CREATE OR REPLACE TABLE tsn AS SELECT DISTINCT z, s FROM txs WHERE NOT numbered;
CREATE OR REPLACE TABLE tcty AS SELECT DISTINCT fips FROM tx;
CREATE OR REPLACE TABLE trng AS
  SELECT DISTINCT left(situs_zip_code,5) z, upper(trim(situs_street_name)) s,
         try_cast(regexp_extract(situs_house_number,'^(\d+)',1) AS INT) hn, try_cast(regexp_extract(situs_house_number_2,'^(\d+)',1) AS INT) hn2
  FROM cl.tax
  WHERE tax_year BETWEEN 2012 AND 2021 AND coalesce(trim(situs_street_name),'')<>'' AND length(situs_zip_code)>=5 AND situs_state <> 'HI'
    AND try_cast(regexp_extract(situs_house_number_2,'^(\d+)',1) AS INT) > try_cast(regexp_extract(situs_house_number,'^(\d+)',1) AS INT)
    AND try_cast(regexp_extract(situs_house_number_2,'^(\d+)',1) AS INT) - try_cast(regexp_extract(situs_house_number,'^(\d+)',1) AS INT) <= 500;
CREATE OR REPLACE TABLE du AS SELECT * FROM da ANTI JOIN tx USING (z, s, h);
CREATE OR REPLACE TABLE tu AS SELECT z, s, h FROM tx ANTI JOIN da USING (z, s, h);
COPY (SELECT (SELECT count(*) FROM da) n_da, (SELECT count(*) FROM tx) n_tax,
             (SELECT count(*) FROM da JOIN tx USING (z, s, h)) n_both,
             (SELECT count(*) FROM da JOIN tx USING (z, s, h) WHERE dmask & tmask <> 0) n_both_same_year,
             (SELECT count(*) FROM du) n_da_only, (SELECT count(*) FROM tu) n_tax_only)
  TO '/home/jr2728/ptax_explore/hh_strat/out/tm_totals.csv' (HEADER);
-- DA-only waterfall
CREATE OR REPLACE TABLE du_c AS
  SELECT u.*,
    CASE
      WHEN u.cty NOT IN (SELECT fips FROM tcty) THEN '1_county_not_in_tax'
      WHEN EXISTS (SELECT 1 FROM trng t WHERE t.z=u.z AND t.s=u.s AND try_cast(regexp_extract(u.h,'^(\d+)',1) AS INT) BETWEEN t.hn AND t.hn2) THEN '2_inside_tax_range'
      WHEN EXISTS (SELECT 1 FROM tu t WHERE t.s=u.s AND t.h=u.h AND t.z<>u.z AND left(t.z,3)=left(u.z,3)) THEN '3_zip_near_miss'
      WHEN EXISTS (SELECT 1 FROM tu t WHERE t.z=u.z AND t.h=u.h AND near_street(u.s, t.s)) THEN '4_street_near_miss'
      WHEN EXISTS (SELECT 1 FROM tu t WHERE t.z=u.z AND t.s=u.s AND length(t.h)=length(u.h) AND levenshtein(t.h,u.h)=1) THEN '5_number_one_char_off'
      WHEN u.z NOT IN (SELECT z FROM tz) THEN '8_zip_not_in_tax'
      WHEN (u.z, u.s) IN (SELECT (z, s) FROM ts) THEN '6_street_in_tax_number_missing'
      WHEN (u.z, u.s) IN (SELECT (z, s) FROM tsn) THEN '7a_street_in_tax_only_without_numbers'
      ELSE '7b_street_not_in_tax' END cls
  FROM du u;
-- nearest tax parcel (latest location per clip, ~50 m grid) for P-geocoded classes 6 and 7b
CREATE OR REPLACE TABLE pg AS
  SELECT p.*, floor(lat/0.0005)::BIGINT cy, floor(lon/0.0005)::BIGINT cx FROM (
    SELECT clip, arg_max(struct_pack(z := left(situs_zip_code,5), s := upper(trim(situs_street_name)), h := nullif(upper(trim(situs_house_number)),''),
                                     hn := try_cast(regexp_extract(situs_house_number,'^(\d+)',1) AS INT),
                                     lat := parcel_level_latitude, lon := parcel_level_longitude), tax_year) a
    FROM cl.tax WHERE tax_year BETWEEN 2012 AND 2021 AND parcel_level_latitude IS NOT NULL AND parcel_level_longitude IS NOT NULL
      AND coalesce(trim(situs_street_name),'')<>'' AND length(situs_zip_code)>=5
    GROUP BY clip) q, LATERAL (SELECT q.a.z z, q.a.s s, q.a.h h, q.a.hn hn, q.a.lat lat, q.a.lon lon) p;
CREATE OR REPLACE TABLE pp AS
  SELECT u.z, u.s, u.h, u.lat, u.lon, try_cast(regexp_extract(u.h,'^(\d+)',1) AS INT) hn,
         floor(u.lat/0.0005)::BIGINT + dy cy, floor(u.lon/0.0005)::BIGINT + dx cx
  FROM (SELECT * FROM du_c WHERE cls IN ('6_street_in_tax_number_missing','7b_street_not_in_tax') AND geo = 'P' AND lat IS NOT NULL) u,
       (SELECT unnest([-1,0,1]) dy), (SELECT unnest([-1,0,1]) dx);
CREATE OR REPLACE TABLE nn AS
  SELECT p.z, p.s, p.h, p.hn,
         arg_min(struct_pack(ps := g.s, ph := g.h, phn := g.hn),
                 sqrt(power((g.lon - p.lon) * cos(radians(p.lat)) * 111320, 2) + power((g.lat - p.lat) * 110540, 2))) nb,
         min(sqrt(power((g.lon - p.lon) * cos(radians(p.lat)) * 111320, 2) + power((g.lat - p.lat) * 110540, 2))) dist_m
  FROM pp p JOIN pg g USING (cy, cx) GROUP BY ALL;
CREATE OR REPLACE TABLE nn2 AS
  SELECT n.z, n.s, n.h, n.dist_m, (n.nb.ps = n.s) same_street, abs(n.nb.phn - n.hn) num_gap,
         EXISTS (SELECT 1 FROM da d WHERE d.z = n.z AND d.s = n.nb.ps AND d.h = n.nb.ph) parcel_addr_in_da
  FROM nn n;
CREATE OR REPLACE TABLE du_f AS
  SELECT c.cls, c.geo,
    CASE WHEN c.cls NOT IN ('6_street_in_tax_number_missing','7b_street_not_in_tax') THEN c.cls
         WHEN c.geo IS DISTINCT FROM 'P' OR c.lat IS NULL THEN c.cls || '__not_testable'
         WHEN n.dist_m IS NULL OR n.dist_m > 50 THEN c.cls || '__no_parcel_50m'
         WHEN n.same_street AND n.num_gap <= 10 AND n.parcel_addr_in_da THEN c.cls || '__multi_number_parcel'
         WHEN NOT n.same_street AND n.parcel_addr_in_da THEN c.cls || '__corner_parcel'
         ELSE c.cls || '__parcel_nearby_no_link' END fcls
  FROM du_c c LEFT JOIN nn2 n USING (z, s, h);
COPY (SELECT fcls, count(*) n FROM du_f GROUP BY ALL ORDER BY ALL) TO '/home/jr2728/ptax_explore/hh_strat/out/tm_da_classes.csv' (HEADER);
-- tax-only waterfall (all property types)
CREATE OR REPLACE TABLE t2 AS
  SELECT t.z, t.s, t.h, try_cast(regexp_extract(t.h,'^(\d+)',1) AS INT) hn,
         x.a.pic pic, x.a.lat lat, x.a.lon lon, x.a.occ occ, x.a.absentee absentee, x.a.out_of_state out_of_state, x.a.po_box po_box,
         x.a.corp corp, x.a.dpv dpv, x.a.dpv_vacant dpv_vacant, x.a.yb yb, x.a.no_impr no_impr
  FROM tu t JOIN tx x USING (z, s, h);
CREATE OR REPLACE TABLE near AS
  SELECT t.z, t.s, t.h, bool_or(length(u.h)=length(t.h) AND levenshtein(u.h, t.h)=1) one_off
  FROM t2 t JOIN du u USING (z, s) GROUP BY ALL;
CREATE OR REPLACE TABLE dp AS
  SELECT d.z, d.s, d.h, (u.z IS NOT NULL) da_only, d.lat, d.lon, floor(d.lat/0.0005)::BIGINT cy, floor(d.lon/0.0005)::BIGINT cx
  FROM da d LEFT JOIN (SELECT z, s, h FROM du) u USING (z, s, h) WHERE d.geo = 'P' AND d.lat IS NOT NULL;
CREATE OR REPLACE TABLE tpp AS
  SELECT t.z, t.s, t.h, t.lat, t.lon, floor(t.lat/0.0005)::BIGINT + dy cy, floor(t.lon/0.0005)::BIGINT + dx cx
  FROM (SELECT * FROM t2 WHERE lat IS NOT NULL AND lon IS NOT NULL) t, (SELECT unnest([-1,0,1]) dy), (SELECT unnest([-1,0,1]) dx);
CREATE OR REPLACE TABLE tnn AS
  SELECT p.z, p.s, p.h,
         arg_min(g.da_only, sqrt(power((g.lon - p.lon) * cos(radians(p.lat)) * 111320, 2) + power((g.lat - p.lat) * 110540, 2))) nb_da_only,
         min(sqrt(power((g.lon - p.lon) * cos(radians(p.lat)) * 111320, 2) + power((g.lat - p.lat) * 110540, 2))) dist_m
  FROM tpp p JOIN dp g USING (cy, cx) GROUP BY ALL;
CREATE OR REPLACE TABLE tc AS
  SELECT t.pic, CASE
      WHEN t.pic IN (10, 11, 21, 22) AND (coalesce(n.one_off, false) OR (g.dist_m <= 15 AND g.nb_da_only)) THEN '1_da_has_it_under_other_number'
      WHEN t.pic = 80 THEN '2_nonres_vacant_land'
      WHEN t.pic = 20 THEN '2_nonres_commercial'
      WHEN t.pic = 70 THEN '2_nonres_agricultural'
      WHEN t.pic IS NULL OR t.pic NOT IN (10, 11, 21, 22) THEN '2_nonres_other'
      WHEN t.no_impr THEN '3_no_structure_on_roll'
      WHEN t.yb >= 2020 THEN '4_built_2020_plus'
      WHEN t.corp OR t.out_of_state OR (t.absentee AND NOT t.po_box) THEN '5_absentee_or_corporate_owner'
      WHEN t.dpv_vacant OR t.dpv = 'N' THEN '6_usps_vacant_or_not_deliverable'
      ELSE '7_other' END cls,
    t.po_box, t.occ
  FROM t2 t LEFT JOIN near n USING (z, s, h) LEFT JOIN tnn g USING (z, s, h);
COPY (SELECT cls, pic, count(*) n, count(*) FILTER (WHERE po_box) n_po_box, count(*) FILTER (WHERE occ = 'O') n_occ_o
      FROM tc GROUP BY ALL ORDER BY ALL) TO '/home/jr2728/ptax_explore/hh_strat/out/tm_tax_classes.csv' (HEADER);
