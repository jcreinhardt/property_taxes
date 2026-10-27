SET preserve_insertion_order=false;
USE memory;
-- Where do families moving in after a single-year address hole come from? Traced through family_id history.
-- t = first year at the new address (= next(Y)); t-1 = hole year Y; t-2 = prev(Y). 2022 excluded (truncated file);
-- for t = 2023, t-1 is 2021. 2% address sample as in gaps_full.sql.
CREATE OR REPLACE MACRO nexty(y) AS CASE WHEN y = 2021 THEN 2023 ELSE y + 1 END;
CREATE OR REPLACE MACRO prevy(y) AS CASE WHEN y = 2023 THEN 2021 ELSE y - 1 END;
CREATE OR REPLACE TABLE fs AS
  SELECT family_id fid, record_year::SMALLINT y,
         hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num))) ha, primary_family_ind prim,
         length_of_residence lor
  FROM cl.dataaxle_annual
  WHERE record_year <> 2022 AND address_type='S' AND coalesce(trim(house_num),'')<>'' AND coalesce(trim(street_name),'')<>'' AND zip IS NOT NULL
    AND hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num))) % 50 = 0;
CREATE OR REPLACE TABLE aa AS SELECT ha, list(DISTINCT y) ys FROM fs GROUP BY ha;
CREATE OR REPLACE TABLE ep AS
  SELECT aa.ha, yy.y, NOT list_contains(ys, yy.y) hole
  FROM aa, (SELECT unnest([2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021])::SMALLINT y) yy
  WHERE list_contains(ys, prevy(yy.y)) AND list_contains(ys, nexty(yy.y));
-- movers-in: primary families at the address in t that were not there in t-2 (prev(Y))
CREATE OR REPLACE TABLE mi AS
  SELECT e.ha, e.y, e.hole, nexty(e.y) t, f.fid, f.lor
  FROM ep e JOIN fs f ON f.ha = e.ha AND f.y = nexty(e.y) AND f.prim
  ANTI JOIN fs b ON b.ha = e.ha AND b.y = prevy(e.y) AND b.fid = f.fid;
CREATE OR REPLACE TABLE mfid AS SELECT DISTINCT fid FROM mi;
-- full histories of these families, any address type
CREATE OR REPLACE TABLE fh AS
  SELECT family_id fid, record_year::SMALLINT y, any_value(address_type) atype, any_value(lpad(zip::VARCHAR,5,'0')) z, any_value(state) st,
         any_value(ge_census_state_2010::INT*1000 + ge_als_county_code_2010::INT) cty,
         any_value(CASE WHEN address_type='S' AND coalesce(trim(house_num),'')<>'' AND coalesce(trim(street_name),'')<>'' AND zip IS NOT NULL
                        THEN hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num))) END) ha
  FROM cl.dataaxle_annual SEMI JOIN mfid ON family_id = mfid.fid WHERE record_year <> 2022 GROUP BY 1, 2;
CREATE OR REPLACE TABLE dest AS
  SELECT mi.*, d.z dz, d.st dst, d.cty dcty FROM mi JOIN fh d ON d.fid = mi.fid AND d.y = mi.t;
-- last observation before t (any year), and presence in t-1 and t-2
CREATE OR REPLACE TABLE o AS
  SELECT m.ha, m.y, m.hole, m.t, m.fid, m.lor, m.dz, m.dst, m.dcty,
         bool_or(h.y = m.y) in_t1, bool_or(h.y = prevy(m.y)) in_t2, max(h.y) last_y,
         arg_max(struct_pack(atype := h.atype, z := h.z, st := h.st, cty := h.cty, ha := h.ha), h.y) lo
  FROM dest m LEFT JOIN fh h ON h.fid = m.fid AND h.y < m.t
  GROUP BY ALL;
COPY (
  SELECT hole, count(*) movers_in,
         avg((last_y IS NULL)::INT) never_seen_before_t,
         avg(coalesce(in_t1, false)::INT) seen_in_t1, avg(coalesce(in_t2, false)::INT) seen_in_t2,
         avg((coalesce(in_t1, false) OR coalesce(in_t2, false))::INT) seen_in_t1_or_t2,
         avg((last_y IS NOT NULL AND NOT coalesce(in_t1, false) AND NOT coalesce(in_t2, false))::INT) seen_only_earlier,
         -- origin at last observation before t (among those seen before)
         avg((lo.ha = ha)::INT) FILTER (WHERE last_y IS NOT NULL) origin_same_address,
         avg((lo.atype <> 'S')::INT) FILTER (WHERE last_y IS NOT NULL) origin_non_street_address,
         avg((lo.ha <> ha AND lo.z = dz)::INT) FILTER (WHERE last_y IS NOT NULL) origin_same_zip,
         avg((lo.z <> dz AND lo.cty = dcty)::INT) FILTER (WHERE last_y IS NOT NULL) origin_same_county_other_zip,
         avg((lo.cty <> dcty AND lo.st = dst)::INT) FILTER (WHERE last_y IS NOT NULL) origin_same_state_other_county,
         avg((lo.st <> dst)::INT) FILTER (WHERE last_y IS NOT NULL) origin_other_state,
         avg((lor <= 1)::INT) lor_le1_at_t, avg((lor BETWEEN 2 AND 5)::INT) lor_2_5_at_t, avg((lor > 5)::INT) lor_gt5_at_t,
         avg((lor <= 1)::INT) FILTER (WHERE last_y IS NULL) lor_le1_never_seen, avg((lor > 5)::INT) FILTER (WHERE last_y IS NULL) lor_gt5_never_seen
  FROM o GROUP BY hole ORDER BY hole
) TO '/home/jr2728/ptax_explore/hh_strat/out/gaps_origin.csv' (HEADER);
COPY (
  SELECT hole, CASE WHEN last_y IS NULL THEN 'never' ELSE (t - last_y)::VARCHAR END years_since_last_seen, count(*) n
  FROM o GROUP BY ALL ORDER BY ALL
) TO '/home/jr2728/ptax_explore/hh_strat/out/gaps_origin_lastseen.csv' (HEADER);
