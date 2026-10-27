SET preserve_insertion_order=false;
USE memory;
-- Surname overlap across single-year address holes. Surnames are md5-hashed inside the query; only rates are exported.
-- 2% address sample (same as gaps_full.sql); 2022 excluded (truncated file).
CREATE OR REPLACE MACRO nexty(y) AS CASE WHEN y = 2021 THEN 2023 ELSE y + 1 END;
CREATE OR REPLACE MACRO prevy(y) AS CASE WHEN y = 2023 THEN 2021 ELSE y - 1 END;
CREATE OR REPLACE MACRO hn(x) AS md5(nullif(upper(trim(x)), ''));
CREATE OR REPLACE TABLE fs AS
  SELECT family_id fid, record_year::SMALLINT y, lpad(zip::VARCHAR,5,'0') z,
         hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num))) ha, primary_family_ind prim,
         list_filter([hn(last_name_1), hn(last_name_2), hn(last_name_3), hn(last_name_4), hn(last_name_5)], x -> x IS NOT NULL) sn,
         list_filter([individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5], x -> x IS NOT NULL) iid
  FROM cl.dataaxle_annual
  WHERE record_year <> 2022 AND address_type='S' AND coalesce(trim(house_num),'')<>'' AND coalesce(trim(street_name),'')<>'' AND zip IS NOT NULL
    AND hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num))) % 50 = 0;
-- first year each family id appears anywhere in DA
CREATE OR REPLACE TABLE fid AS SELECT DISTINCT fid FROM fs;
CREATE OR REPLACE TABLE ffirst AS
  SELECT family_id fid, min(record_year) first_y FROM cl.dataaxle_annual SEMI JOIN fid ON family_id = fid.fid
  WHERE record_year <> 2022 GROUP BY 1;
-- address-year sets of surnames, person ids, family ids
CREATE OR REPLACE TABLE ay AS
  SELECT ha, y, any_value(z) z, list_distinct(flatten(list(sn))) sns, list_distinct(flatten(list(iid))) iids, list(fid) fids
  FROM fs GROUP BY ha, y;
CREATE OR REPLACE TABLE aa AS SELECT ha, list(y) ys FROM ay GROUP BY ha;
CREATE OR REPLACE TABLE ep AS
  SELECT aa.ha, yy.y, NOT list_contains(ys, yy.y) hole
  FROM aa, (SELECT unnest([2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021])::SMALLINT y) yy
  WHERE list_contains(ys, prevy(yy.y)) AND list_contains(ys, nexty(yy.y));
CREATE OR REPLACE TABLE e2 AS
  SELECT e.ha, e.y, e.hole, b.z, b.sns b_sns, b.iids b_iids, b.fids b_fids, n.sns n_sns, n.iids n_iids, n.fids n_fids
  FROM ep e JOIN ay b ON b.ha = e.ha AND b.y = prevy(e.y) JOIN ay n ON n.ha = e.ha AND n.y = nexty(e.y);
-- placebo: before set of this address vs after set of the next sampled address (same ZIP, same year, same hole status)
CREATE OR REPLACE TABLE e3 AS
  SELECT *, lead(n_sns) OVER (PARTITION BY z, y, hole ORDER BY ha) pl_n_sns, lead(b_sns) OVER (PARTITION BY z, y, hole ORDER BY ha) pl_b_sns FROM e2;
-- (1) address level: does any surname / person id / family id overlap between the year before and the year after?
COPY (
  SELECT hole, count(*) episodes,
         avg((len(list_intersect(b_fids, n_fids)) > 0)::INT) any_family_id_overlap,
         avg((len(list_intersect(b_sns, n_sns)) > 0)::INT) FILTER (WHERE len(b_sns) > 0 AND len(n_sns) > 0) any_surname_overlap,
         avg((len(list_intersect(b_iids, n_iids)) > 0)::INT) FILTER (WHERE len(b_iids) > 0 AND len(n_iids) > 0) any_person_id_overlap,
         avg((len(list_intersect(b_sns, n_sns)) > 0)::INT) FILTER (WHERE len(b_sns) > 0 AND len(n_sns) > 0 AND len(list_intersect(b_fids, n_fids)) = 0) surname_overlap_if_no_family_id_overlap,
         avg((len(list_intersect(b_iids, n_iids)) > 0)::INT) FILTER (WHERE len(b_iids) > 0 AND len(n_iids) > 0 AND len(list_intersect(b_fids, n_fids)) = 0) person_id_overlap_if_no_family_id_overlap,
         avg((len(list_intersect(b_sns, pl_n_sns)) > 0)::INT) FILTER (WHERE len(b_sns) > 0 AND len(pl_n_sns) > 0) placebo_surname_overlap_other_address,
         avg(len(b_fids)) families_before, avg(len(n_fids)) families_after
  FROM e3 GROUP BY hole ORDER BY hole
) TO '/home/jr2728/ptax_explore/hh_strat/out/gaps_surnames_address.csv' (HEADER);
-- (2) family level: primary families at the address the year after, by ID type; surname / person id in the before set?
COPY (
  SELECT e.hole,
         CASE WHEN list_contains(e.b_fids, f.fid) THEN '1_same_id' WHEN ff.first_y >= nexty(e.y) THEN '2_brand_new_id' ELSE '3_existing_id_from_elsewhere' END id_type,
         count(*) families,
         avg((len(list_intersect(f.sn, e.b_sns)) > 0)::INT) FILTER (WHERE len(f.sn) > 0 AND len(e.b_sns) > 0) surname_in_before_set,
         avg((len(list_intersect(f.iid, e.b_iids)) > 0)::INT) FILTER (WHERE len(f.iid) > 0 AND len(e.b_iids) > 0) person_id_in_before_set,
         avg((len(list_intersect(f.sn, e.pl_b_sns)) > 0)::INT) FILTER (WHERE len(f.sn) > 0 AND len(e.pl_b_sns) > 0) placebo_surname_in_other_address_before_set
  FROM e3 e JOIN fs f ON f.ha = e.ha AND f.y = nexty(e.y) AND f.prim JOIN ffirst ff ON ff.fid = f.fid
  GROUP BY ALL ORDER BY ALL
) TO '/home/jr2728/ptax_explore/hh_strat/out/gaps_surnames_family.csv' (HEADER);
