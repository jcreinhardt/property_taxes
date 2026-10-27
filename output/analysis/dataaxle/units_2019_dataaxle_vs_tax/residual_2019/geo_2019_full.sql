SET preserve_insertion_order=false;
USE memory;
CREATE OR REPLACE MACRO near_street(a, b) AS a <> b AND least(length(a), length(b)) >= 4 AND (levenshtein(a, b) <= 2 OR jaro_winkler_similarity(a, b) >= 0.92);
CREATE OR REPLACE TABLE da AS
  SELECT lpad(zip::VARCHAR,5,'0') z, upper(trim(street_name)) s, upper(trim(house_num)) h,
         any_value(ge_census_state_2010::INT*1000 + ge_als_county_code_2010::INT) cty, mode(ge_census_level_2010) geo,
         avg(ge_latitude_2010) lat, avg(ge_longitude_2010) lon, count(*) n_fam, mode(lpad(nullif(trim(zip4::VARCHAR),''),4,'0')) z4
  FROM cl.dataaxle_annual 
  WHERE record_year=2019 AND address_type='S' AND coalesce(trim(house_num),'')<>'' AND coalesce(trim(street_name),'')<>'' AND zip IS NOT NULL
  GROUP BY 1,2,3;
CREATE OR REPLACE TABLE txr AS
  SELECT clip, left(situs_zip_code,5) z, upper(trim(situs_street_name)) s, nullif(upper(trim(situs_house_number)),'') h, fips_code fips, situs_state st,
         try_cast(regexp_extract(situs_house_number,'^(\d+)',1) AS INT) hn, try_cast(regexp_extract(situs_house_number_2,'^(\d+)',1) AS INT) hn2,
         coalesce(number_of_buildings,0) nb, coalesce(total_number_of_units_all_buildings,0) nu, property_indicator_code pic,
         parcel_level_latitude lat, parcel_level_longitude lon,
         CASE WHEN length(situs_zip_code)=9 THEN right(situs_zip_code,4) END z4
  FROM cl.tax  WHERE tax_year=2019 AND coalesce(trim(situs_street_name),'')<>'' AND length(situs_zip_code)>=5;
CREATE OR REPLACE TABLE tx AS SELECT DISTINCT z, s, h FROM txr WHERE h IS NOT NULL;
CREATE OR REPLACE TABLE tz AS SELECT DISTINCT z FROM txr;
CREATE OR REPLACE TABLE ts AS SELECT DISTINCT z, s FROM txr WHERE h IS NOT NULL;
CREATE OR REPLACE TABLE tsn AS SELECT DISTINCT z, s FROM txr WHERE h IS NULL;
CREATE OR REPLACE TABLE tcty AS SELECT DISTINCT fips FROM txr WHERE h IS NOT NULL;
CREATE OR REPLACE TABLE du AS SELECT * FROM da ANTI JOIN tx USING (z, s, h);
CREATE OR REPLACE TABLE tu AS SELECT * FROM tx ANTI JOIN (SELECT z, s, h FROM da) USING (z, s, h);
CREATE OR REPLACE TABLE du_c AS
  SELECT u.*,
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
-- points to locate: residual classes 6 and 7b, plus a matched calibration sample; address-level geocodes only
CREATE OR REPLACE TABLE pts AS
  SELECT z, s, h, cls, geo, lat, lon, n_fam, try_cast(regexp_extract(h,'^(\d+)',1) AS INT) hn FROM du_c
  WHERE cls IN ('6_street_in_tax_number_missing','7b_street_not_in_tax') AND geo IN ('P','0') AND lat IS NOT NULL
  UNION ALL
  SELECT z, s, h, '0_matched_sample', geo, lat, lon, n_fam, try_cast(regexp_extract(h,'^(\d+)',1) AS INT) FROM (SELECT * FROM da SEMI JOIN tx USING (z, s, h))
  WHERE geo IN ('P','0') AND lat IS NOT NULL AND hash(z, s, h) % 100 < 1;
-- tax parcels on a ~50 m grid; each point looks in its 3x3 neighbourhood
CREATE OR REPLACE TABLE pg AS SELECT *, floor(lat/0.0005)::BIGINT cy, floor(lon/0.0005)::BIGINT cx FROM txr WHERE lat IS NOT NULL AND lon IS NOT NULL;
CREATE OR REPLACE TABLE pp AS
  SELECT p.*, floor(p.lat/0.0005)::BIGINT + dy cy, floor(p.lon/0.0005)::BIGINT + dx cx
  FROM pts p, (SELECT unnest([-1,0,1]) dy), (SELECT unnest([-1,0,1]) dx);
CREATE OR REPLACE TABLE nn AS
  SELECT p.z, p.s, p.h, p.cls, p.geo, p.n_fam, p.hn,
         arg_min(struct_pack(ps := g.s, ph := g.h, phn := g.hn, nb := g.nb, nu := g.nu, pic := g.pic, pz := g.z),
                 sqrt(power((g.lon - p.lon) * cos(radians(p.lat)) * 111320, 2) + power((g.lat - p.lat) * 110540, 2))) nb_parcel,
         min(sqrt(power((g.lon - p.lon) * cos(radians(p.lat)) * 111320, 2) + power((g.lat - p.lat) * 110540, 2))) dist_m
  FROM pp p JOIN pg g USING (cy, cx) GROUP BY ALL;
CREATE OR REPLACE TABLE nn2 AS
  SELECT n.*, nb_parcel.ps nps, nb_parcel.ph nph, nb_parcel.phn nphn, nb_parcel.nb nnb, nb_parcel.nu nnu, nb_parcel.pic npic,
         (nb_parcel.ps = n.s) same_street, abs(nb_parcel.phn - n.hn) num_gap,
         (n.z, nb_parcel.ps, nb_parcel.ph) IN (SELECT (z, s, h) FROM da) parcel_addr_in_da,
         (nb_parcel.ps = n.s AND nb_parcel.ph = n.h) is_own_parcel
  FROM nn n;
COPY (
  SELECT cls, geo, count(*) n,
    avg((dist_m <= 25)::INT) d25, avg((dist_m <= 50)::INT) d50, avg((dist_m <= 100)::INT) d100, median(dist_m) med_dist,
    avg(is_own_parcel::INT) nearest_is_own,
    avg(same_street::INT) FILTER (WHERE dist_m <= 50) same_street_50,
    avg((same_street AND num_gap <= 10)::INT) FILTER (WHERE dist_m <= 50) same_street_gap10_50,
    avg((nnu > 1 OR nnb > 1)::INT) FILTER (WHERE dist_m <= 50) multi_unit_or_bldg_50,
    avg(parcel_addr_in_da::INT) FILTER (WHERE dist_m <= 50) parcel_addr_in_da_50,
    avg((same_street AND num_gap <= 10 AND parcel_addr_in_da)::INT) FILTER (WHERE dist_m <= 50) multi_number_parcel_50,
    avg((NOT same_street AND parcel_addr_in_da)::INT) FILTER (WHERE dist_m <= 50) cross_street_parcel_50
  FROM nn2 GROUP BY GROUPING SETS ((cls), (cls, geo)) ORDER BY cls, geo NULLS FIRST
) TO '/home/jr2728/ptax_explore/hh_strat/out/geo_2019_nearest_by_class.csv' (HEADER);
COPY (SELECT cls, npic AS property_indicator_code, count(*) n FROM nn2 WHERE dist_m <= 50 GROUP BY ALL ORDER BY ALL) TO '/home/jr2728/ptax_explore/hh_strat/out/geo_2019_nearest_pic.csv' (HEADER);
COPY (SELECT cls, z, h, s, n_fam, round(dist_m) dist_m, nps nearest_parcel_street, nph nearest_parcel_number, nnu nearest_units, npic, parcel_addr_in_da
      FROM nn2 WHERE cls = '7b_street_not_in_tax' USING SAMPLE 300 ROWS) TO '/home/jr2728/ptax_explore/hh_strat/out/geo_2019_street_not_in_tax_sample.csv' (HEADER);
COPY (SELECT cls, z, h, s, n_fam, round(dist_m) dist_m, nps nearest_parcel_street, nph nearest_parcel_number, nnu nearest_units, npic, parcel_addr_in_da
      FROM nn2 WHERE cls = '6_street_in_tax_number_missing' USING SAMPLE 300 ROWS) TO '/home/jr2728/ptax_explore/hh_strat/out/geo_2019_number_missing_sample.csv' (HEADER);
-- ZIP+4: does an unmatched DA address share its ZIP+4 with a tax parcel (same street / any street)?
CREATE OR REPLACE TABLE t4 AS SELECT DISTINCT z, z4, s FROM txr WHERE z4 IS NOT NULL AND h IS NOT NULL;
CREATE OR REPLACE TABLE t4n AS
  SELECT z, z4, s, max(nu) max_units, max(nb) max_bldgs, count(*) n_parcels, min(hn) min_hn, max(coalesce(hn2, hn)) max_hn
  FROM txr WHERE z4 IS NOT NULL AND h IS NOT NULL GROUP BY ALL;
COPY (
  WITH m AS (SELECT da.z, da.s, da.h, da.z4 FROM da SEMI JOIN tx USING (z, s, h) WHERE hash(da.z, da.s, da.h) % 100 < 1),
       mm AS (SELECT '0_matched_sample' cls, m.z4 IS NOT NULL has_z4,
                     EXISTS (SELECT 1 FROM txr t WHERE t.z=m.z AND t.s=m.s AND t.h=m.h AND t.z4=m.z4) z4_same_street,
                     EXISTS (SELECT 1 FROM t4 WHERE t4.z=m.z AND t4.z4=m.z4) z4_any_street,
                     NULL::BOOLEAN z4_multi_unit_parcel
              FROM m),
       rr AS (SELECT u.cls, u.z4 IS NOT NULL has_z4,
                     EXISTS (SELECT 1 FROM t4 WHERE t4.z=u.z AND t4.z4=u.z4 AND t4.s=u.s) z4_same_street,
                     EXISTS (SELECT 1 FROM t4 WHERE t4.z=u.z AND t4.z4=u.z4) z4_any_street,
                     EXISTS (SELECT 1 FROM t4n WHERE t4n.z=u.z AND t4n.z4=u.z4 AND t4n.s=u.s AND (t4n.max_units>1 OR t4n.max_bldgs>1)) z4_multi_unit_parcel
              FROM du_c u)
  SELECT cls, count(*) n, avg(has_z4::INT) share_has_z4, avg(z4_same_street::INT) FILTER (WHERE has_z4) share_z4_same_street,
         avg(z4_any_street::INT) FILTER (WHERE has_z4) share_z4_any_street, avg(z4_multi_unit_parcel::INT) FILTER (WHERE has_z4) share_z4_same_street_multiunit
  FROM (SELECT * FROM mm UNION ALL SELECT * FROM rr) GROUP BY cls ORDER BY cls
) TO '/home/jr2728/ptax_explore/hh_strat/out/geo_2019_zip4_by_class.csv' (HEADER);
-- streets unknown to tax: most common street names (top 60)
COPY (SELECT s, count(*) n FROM du_c WHERE cls='7b_street_not_in_tax' GROUP BY s ORDER BY n DESC LIMIT 60) TO '/home/jr2728/ptax_explore/hh_strat/out/geo_2019_street_not_in_tax_top_names.csv' (HEADER);
