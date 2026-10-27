SET preserve_insertion_order=false;
USE memory;
-- Address key as before (zip5 + street name + house number). 2022 is a truncated file: excluded throughout.
CREATE OR REPLACE MACRO nexty(y) AS CASE WHEN y = 2021 THEN 2023 ELSE y + 1 END;
CREATE OR REPLACE MACRO prevy(y) AS CASE WHEN y = 2023 THEN 2021 ELSE y - 1 END;
CREATE OR REPLACE TABLE ay AS
  SELECT hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num))) ha, record_year::SMALLINT y, count(*) nf
  FROM cl.dataaxle_annual
  WHERE record_year <> 2022 AND address_type='S' AND coalesce(trim(house_num),'')<>'' AND coalesce(trim(street_name),'')<>'' AND zip IS NOT NULL
  GROUP BY ALL;
CREATE OR REPLACE TABLE a AS
  SELECT ha, min(y) fy, max(y) ly, count(*) ny, list(y) ys FROM ay GROUP BY ha;
-- (1) interior missing years per address
COPY (
  SELECT least((ly - fy + 1 - (fy < 2022 AND ly > 2022)::INT) - ny, 5) missing_inside_span, count(*) addresses
  FROM a GROUP BY ALL ORDER BY ALL
) TO '/home/jr2728/ptax_explore/hh_strat/out/gaps_missing_per_address.csv' (HEADER);
-- (2) by year: addresses absent in Y but present before and after; single-year holes (present prev and next year)
COPY (
  SELECT yy.y, count(*) FILTER (WHERE list_contains(ys, prevy(yy.y))) present_prev,
         count(*) FILTER (WHERE fy < yy.y AND ly > yy.y AND NOT list_contains(ys, yy.y)) hole_any,
         count(*) FILTER (WHERE list_contains(ys, prevy(yy.y)) AND list_contains(ys, nexty(yy.y)) AND NOT list_contains(ys, yy.y)) hole_single,
         count(*) FILTER (WHERE list_contains(ys, prevy(yy.y)) AND list_contains(ys, nexty(yy.y)) AND list_contains(ys, yy.y)) continuous
  FROM a, (SELECT unnest([2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021])::SMALLINT y) yy
  GROUP BY yy.y ORDER BY yy.y
) TO '/home/jr2728/ptax_explore/hh_strat/out/gaps_by_year.csv' (HEADER);
-- (3) tax 2019 single-family addresses: DA presence pattern around 2019
CREATE OR REPLACE TABLE t AS
  SELECT DISTINCT hash(left(situs_zip_code,5), upper(trim(situs_street_name)), upper(trim(situs_house_number))) ha
  FROM cl.tax WHERE tax_year=2019 AND property_indicator_code=10 AND coalesce(trim(situs_house_number),'')<>''
    AND coalesce(trim(situs_street_name),'')<>'' AND length(situs_zip_code)>=5;
COPY (
  SELECT CASE WHEN a.ha IS NULL THEN '0_never_in_da'
              WHEN list_contains(ys, 2019) THEN '1_in_da_2019'
              WHEN fy < 2019 AND ly > 2019 THEN '2_hole_in_2019_present_before_and_after'
              WHEN ly < 2019 THEN '3_only_before_2019'
              ELSE '4_only_after_2019' END pattern,
         CASE WHEN a.ha IS NULL THEN NULL ELSE list_contains(ys, 2018) AND list_contains(ys, 2020) END in_2018_and_2020,
         count(*) addresses
  FROM t LEFT JOIN a USING (ha) GROUP BY ALL ORDER BY ALL
) TO '/home/jr2728/ptax_explore/hh_strat/out/gaps_tax_sfr_2019.csv' (HEADER);
-- (4) families at single-year holes vs continuous addresses, 2% address sample
CREATE OR REPLACE TABLE fs AS
  SELECT family_id fid, record_year::SMALLINT y, hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num))) ha, primary_family_ind prim
  FROM cl.dataaxle_annual
  WHERE record_year <> 2022 AND address_type='S' AND coalesce(trim(house_num),'')<>'' AND coalesce(trim(street_name),'')<>'' AND zip IS NOT NULL
    AND hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num))) % 50 = 0;
CREATE OR REPLACE TABLE fid AS SELECT DISTINCT fid FROM fs;
-- full histories of those families (any address type), and each id's first year in the file
CREATE OR REPLACE TABLE fh AS
  SELECT family_id fid, record_year::SMALLINT y,
         CASE WHEN address_type='S' AND coalesce(trim(house_num),'')<>'' AND coalesce(trim(street_name),'')<>'' AND zip IS NOT NULL
              THEN hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num))) END ha
  FROM cl.dataaxle_annual SEMI JOIN fid ON family_id = fid.fid WHERE record_year <> 2022;
CREATE OR REPLACE TABLE fh1 AS SELECT fid, y, any_value(ha) ha FROM fh GROUP BY fid, y;
CREATE OR REPLACE TABLE ffirst AS SELECT fid, min(y) first_y FROM fh1 GROUP BY fid;
-- episodes: address present prev(Y) and next(Y); hole if absent in Y, else continuous
CREATE OR REPLACE TABLE ep AS
  SELECT a.ha, yy.y, NOT list_contains(ys, yy.y) hole
  FROM (SELECT * FROM a WHERE ha % 50 = 0) a, (SELECT unnest([2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021])::SMALLINT y) yy
  WHERE list_contains(ys, prevy(yy.y)) AND list_contains(ys, nexty(yy.y));
-- after-families: at the address in next(Y). Same id as a before-family? new id (first year >= next(Y))? where were they in Y?
COPY (
  SELECT e.hole, e.y, f.prim, count(*) families_after,
         avg((EXISTS (SELECT 1 FROM fs b WHERE b.ha = e.ha AND b.y = prevy(e.y) AND b.fid = f.fid))::INT) same_id_as_before,
         avg((ff.first_y >= nexty(e.y))::INT) brand_new_id,
         avg((h.y IS NULL)::INT) id_absent_from_file_in_Y,
         avg((h.ha = e.ha)::INT) id_at_this_address_in_Y,
         avg((h.ha <> e.ha OR (h.y IS NOT NULL AND h.ha IS NULL))::INT) id_elsewhere_in_Y
  FROM ep e JOIN fs f ON f.ha = e.ha AND f.y = nexty(e.y)
  JOIN ffirst ff ON ff.fid = f.fid
  LEFT JOIN fh1 h ON h.fid = f.fid AND h.y = e.y
  GROUP BY GROUPING SETS ((e.hole, f.prim), (e.hole, e.y, f.prim)) ORDER BY ALL
) TO '/home/jr2728/ptax_explore/hh_strat/out/gaps_families_after.csv' (HEADER);
-- before-families: at the address in prev(Y). In Y: absent from file / elsewhere? In next(Y): back here / elsewhere / absent?
COPY (
  SELECT e.hole, f.prim, count(*) families_before,
         avg((h.y IS NULL)::INT) absent_in_Y, avg((h.ha <> e.ha OR (h.y IS NOT NULL AND h.ha IS NULL))::INT) elsewhere_in_Y,
         avg((n.ha = e.ha)::INT) back_here_next, avg((n.y IS NOT NULL AND (n.ha IS NULL OR n.ha <> e.ha))::INT) elsewhere_next,
         avg((n.y IS NULL)::INT) absent_next
  FROM ep e JOIN fs f ON f.ha = e.ha AND f.y = prevy(e.y)
  LEFT JOIN fh1 h ON h.fid = f.fid AND h.y = e.y
  LEFT JOIN fh1 n ON n.fid = f.fid AND n.y = nexty(e.y)
  GROUP BY ALL ORDER BY ALL
) TO '/home/jr2728/ptax_explore/hh_strat/out/gaps_families_before.csv' (HEADER);
