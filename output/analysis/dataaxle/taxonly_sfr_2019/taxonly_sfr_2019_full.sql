SET preserve_insertion_order=false;
USE memory;
-- Data Axle 2019 addresses (loose key) with geocode; 2017/2021 keys
CREATE OR REPLACE TABLE da19 AS
  SELECT lpad(zip::VARCHAR,5,'0') z, upper(trim(street_name)) s, upper(trim(house_num)) h,
         mode(ge_census_level_2010) geo, avg(ge_latitude_2010) lat, avg(ge_longitude_2010) lon
  FROM cl.dataaxle_annual
  WHERE record_year=2019 AND address_type='S' AND coalesce(trim(house_num),'')<>'' AND coalesce(trim(street_name),'')<>'' AND zip IS NOT NULL
  GROUP BY 1,2,3;
CREATE OR REPLACE TABLE daoth AS
  SELECT lpad(zip::VARCHAR,5,'0') z, upper(trim(street_name)) s, upper(trim(house_num)) h,
         bool_or(record_year=2017) in17, bool_or(record_year=2021) in21
  FROM cl.dataaxle_annual
  WHERE record_year IN (2017,2021) AND address_type='S' AND coalesce(trim(house_num),'')<>'' AND coalesce(trim(street_name),'')<>'' AND zip IS NOT NULL
  GROUP BY 1,2,3;
-- all tax 2019 address keys (any property type), and DA addresses not in tax ("DA-only")
CREATE OR REPLACE TABLE txall AS
  SELECT DISTINCT left(situs_zip_code,5) z, upper(trim(situs_street_name)) s, upper(trim(situs_house_number)) h
  FROM cl.tax WHERE tax_year=2019 AND coalesce(trim(situs_house_number),'')<>'' AND coalesce(trim(situs_street_name),'')<>'' AND length(situs_zip_code)>=5;
CREATE OR REPLACE TABLE du AS SELECT * FROM da19 ANTI JOIN txall USING (z, s, h);
-- single-family (code 10) tax addresses with attributes, one row per address
CREATE OR REPLACE TABLE sfr AS
  SELECT z, s, h, any_value(st) st, any_value(fips) fips, count(*) n_parcels, avg(lat) lat, avg(lon) lon,
         mode(occ) occ, bool_or(absentee) absentee, bool_or(out_of_state) out_of_state, bool_or(po_box) po_box,
         bool_or(corp) corp, bool_or(homestead) homestead, mode(dpv) dpv, bool_or(dpv_vacant) dpv_vacant,
         max(yb) yb, bool_and(no_impr) no_impr
  FROM (SELECT left(situs_zip_code,5) z, upper(trim(situs_street_name)) s, upper(trim(situs_house_number)) h, situs_state st, fips_code fips,
               parcel_level_latitude lat, parcel_level_longitude lon, owner_occupancy_code occ,
               CASE WHEN coalesce(trim(mailing_street_name),'')='' THEN NULL
                    ELSE NOT (left(coalesce(mailing_zip_code,''),5)=left(situs_zip_code,5)
                              AND upper(trim(coalesce(mailing_house_number,'')))=upper(trim(situs_house_number))
                              AND upper(trim(mailing_street_name))=upper(trim(situs_street_name))) END absentee,
               CASE WHEN coalesce(trim(mailing_state),'')='' THEN NULL ELSE upper(trim(mailing_state))<>upper(trim(situs_state)) END out_of_state,
               regexp_matches(upper(coalesce(mailing_street_address,'') || ' ' || coalesce(mailing_street_name,'')), 'P ?O BOX|^BOX |PO BX') po_box,
               coalesce(owner_1_corporate_indicator,false) corp, homestead_exempt_indicator homestead,
               situs_delivery_point_validation_code dpv, situs_delivery_point_validation_vacant_indicator dpv_vacant,
               nullif(try_cast(year_built AS INT),0) yb,
               (coalesce(calculated_improvement_value,0)=0 AND coalesce(total_living_area_square_feet_all_buildings,0)=0) no_impr
        FROM cl.tax
        WHERE tax_year=2019 AND property_indicator_code=10 AND coalesce(trim(situs_house_number),'')<>''
          AND coalesce(trim(situs_street_name),'')<>'' AND length(situs_zip_code)>=5)
  GROUP BY z, s, h;
CREATE OR REPLACE TABLE s2 AS
  SELECT f.*, try_cast(regexp_extract(f.h,'^(\d+)',1) AS INT) hn,
         coalesce(m.hit,false) in_da19, coalesce(o.in17,false) in_da17, coalesce(o.in21,false) in_da21
  FROM sfr f LEFT JOIN (SELECT z, s, h, true AS hit FROM da19) m USING (z, s, h) LEFT JOIN daoth o USING (z, s, h);
-- tax-only SFR: DA-only address on the same street one character off, or within +-10 numbers (same parity)
CREATE OR REPLACE TABLE near AS
  SELECT t.z, t.s, t.h,
         bool_or(length(u.h)=length(t.h) AND levenshtein(u.h, t.h)=1) one_off,
         bool_or(abs(try_cast(regexp_extract(u.h,'^(\d+)',1) AS INT) - t.hn) <= 10
                 AND (try_cast(regexp_extract(u.h,'^(\d+)',1) AS INT) - t.hn) % 2 = 0) near10
  FROM (SELECT z, s, h, hn FROM s2 WHERE NOT in_da19) t JOIN du u USING (z, s) GROUP BY ALL;
-- nearest P-geocoded DA 2019 address to the parcel point (~50 m grid, 3x3 neighbourhood); tax-only plus a 1% matched calibration sample
CREATE OR REPLACE TABLE dp AS
  SELECT d.z, d.s, d.h, d.lat, d.lon, (u.z IS NOT NULL) da_only, floor(d.lat/0.0005)::BIGINT cy, floor(d.lon/0.0005)::BIGINT cx
  FROM da19 d LEFT JOIN (SELECT z, s, h FROM du) u ON d.z=u.z AND d.s=u.s AND d.h=u.h
  WHERE d.geo='P' AND d.lat IS NOT NULL;
CREATE OR REPLACE TABLE pp AS
  SELECT p.z, p.s, p.h, p.in_da19, p.lat, p.lon, floor(p.lat/0.0005)::BIGINT + dy cy, floor(p.lon/0.0005)::BIGINT + dx cx
  FROM (SELECT * FROM s2 WHERE lat IS NOT NULL AND (NOT in_da19 OR hash(z, s, h) % 100 < 1)) p,
       (SELECT unnest([-1,0,1]) dy), (SELECT unnest([-1,0,1]) dx);
CREATE OR REPLACE TABLE nn AS
  SELECT p.z, p.s, p.h, p.in_da19,
         arg_min(struct_pack(ds := g.s, dh := g.h, da_only := g.da_only),
                 sqrt(power((g.lon - p.lon) * cos(radians(p.lat)) * 111320, 2) + power((g.lat - p.lat) * 110540, 2))) nb,
         min(sqrt(power((g.lon - p.lon) * cos(radians(p.lat)) * 111320, 2) + power((g.lat - p.lat) * 110540, 2))) dist_m
  FROM pp p JOIN dp g USING (cy, cx) GROUP BY ALL;
CREATE OR REPLACE TABLE f AS
  SELECT s2.*, coalesce(n.one_off,false) one_off, coalesce(n.near10,false) near10,
         g.dist_m, (g.nb.ds = s2.s AND g.nb.dh = s2.h) nearest_is_own, coalesce(g.nb.da_only,false) nearest_da_only,
         (g.dist_m <= 15 AND g.nb.da_only) da_only_pt15,
         s2.lat IS NOT NULL AND (NOT s2.in_da19 OR hash(s2.z, s2.s, s2.h) % 100 < 1) geo_sample
  FROM s2 LEFT JOIN near n USING (z, s, h) LEFT JOIN nn g USING (z, s, h);
-- ordered (waterfall) class for tax-only SFR addresses
CREATE OR REPLACE TABLE fc AS
  SELECT *, CASE WHEN in_da19 THEN '0_matched'
                 WHEN one_off OR da_only_pt15 THEN '1_da_has_it_under_other_number'
                 WHEN no_impr THEN '2_no_structure_on_roll'
                 WHEN yb >= 2018 THEN '3_built_2018_plus'
                 WHEN in_da17 OR in_da21 THEN '4_in_da_2017_or_2021'
                 WHEN corp OR out_of_state OR (absentee AND NOT po_box) THEN '5_absentee_or_corporate_owner'
                 WHEN dpv_vacant OR dpv='N' THEN '6_usps_vacant_or_not_deliverable'
                 ELSE '7_other' END cls
  FROM f;
COPY (
  SELECT in_da19, cls, count(*) n,
    avg(one_off::INT) one_off, avg(near10::INT) near10_da_only, avg(da_only_pt15::INT) FILTER (WHERE geo_sample) da_only_pt15,
    avg((dist_m <= 15)::INT) FILTER (WHERE geo_sample) any_da_pt15, median(dist_m) FILTER (WHERE geo_sample) med_dist_da_pt,
    avg(nearest_is_own::INT) FILTER (WHERE geo_sample) nearest_is_own,
    avg(no_impr::INT) no_impr, avg((yb >= 2018)::INT) built_2018p, avg((yb BETWEEN 2010 AND 2017)::INT) built_2010_17, avg((yb IS NULL)::INT) yb_missing,
    avg((in_da17 OR in_da21)::INT) in_da17_or_21, avg(in_da17::INT) in_da17, avg(in_da21::INT) in_da21,
    avg(corp::INT) corp, avg(absentee::INT) absentee, avg(out_of_state::INT) out_of_state, avg(po_box::INT) po_box,
    avg(homestead::INT) homestead, avg((dpv='Y')::INT) dpv_y, avg((dpv='N')::INT) dpv_n, avg((dpv IS NULL)::INT) dpv_null,
    avg(dpv_vacant::INT) dpv_vacant, avg((n_parcels > 1)::INT) multi_parcel
  FROM fc GROUP BY GROUPING SETS ((in_da19), (in_da19, cls)) ORDER BY in_da19 DESC, cls NULLS FIRST
) TO '/home/jr2728/ptax_explore/hh_strat/out/taxonly_sfr_2019_summary.csv' (HEADER);
COPY (SELECT st, in_da19, cls, count(*) n FROM fc GROUP BY ALL ORDER BY ALL) TO '/home/jr2728/ptax_explore/hh_strat/out/taxonly_sfr_2019_state.csv' (HEADER);
COPY (SELECT in_da19, occ AS owner_occupancy_code, count(*) n FROM fc GROUP BY ALL ORDER BY ALL) TO '/home/jr2728/ptax_explore/hh_strat/out/taxonly_sfr_2019_occ.csv' (HEADER);
COPY (SELECT cls, z, s, h, st, occ, absentee, out_of_state, po_box, corp, homestead, dpv, dpv_vacant, yb, no_impr, in_da17, in_da21,
             one_off, near10, round(dist_m) dist_m_nearest_da_p, nearest_da_only
      FROM fc WHERE NOT in_da19 USING SAMPLE 400 ROWS) TO '/home/jr2728/ptax_explore/hh_strat/out/taxonly_sfr_2019_sample.csv' (HEADER);
