-- Primary families at the 1% building sample in t: where are they in t+1? Re-keyed = family_id absent in t+1 but one of its
-- individual_ids appears under another family_id.
SET threads=4;
CREATE OR REPLACE TABLE fp AS SELECT * FROM '/home/jr2728/ptax_explore/samples/da_bld1pct_fampos.parquet' WHERE record_year <> 2022;
CREATE OR REPLACE TABLE p AS
  SELECT family_id fid, record_year::INT y, bkey, length_of_residence lor, owner_renter_status ors,
         list_filter([individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5], x -> x IS NOT NULL) iid
  FROM '/home/jr2728/ptax_explore/samples/da_bld1pct.parquet'
  WHERE primary_family_ind AND record_year BETWEEN 2006 AND 2020;
-- persons in t+1: individual_id -> (family_id, bkey, lor)
CREATE OR REPLACE TABLE pi AS
  SELECT unnest([individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5]) iid, family_id fid, record_year::INT y, bkey, length_of_residence lor, state
  FROM fp;
CREATE OR REPLACE TABLE pi AS SELECT * FROM pi WHERE iid IS NOT NULL;
CREATE OR REPLACE TABLE nx AS
  SELECT p.fid, p.y, p.bkey, p.ors, n.bkey nbkey, n.length_of_residence nlor
  FROM p LEFT JOIN fp n ON n.family_id = p.fid AND n.record_year = p.y + 1;
CREATE OR REPLACE TABLE rk AS
  SELECT p.fid, p.y, bool_or(q.bkey IS NOT DISTINCT FROM p.bkey) rk_here, bool_or(q.bkey IS DISTINCT FROM p.bkey) rk_elsewhere,
         bool_or(q.bkey IS DISTINCT FROM p.bkey AND q.lor <= 1) rk_elsewhere_lor1
  FROM (SELECT fid, y, bkey, unnest(iid) iid FROM p) p JOIN pi q ON q.iid = p.iid AND q.y = p.y + 1 AND q.fid <> p.fid
  GROUP BY p.fid, p.y;
SELECT x.y, count(*) primaries,
  round(100*avg((x.nbkey IS NOT NULL AND x.nbkey = x.bkey)::INT),2) same_id_same_bldg,
  round(100*avg((x.nlor IS NOT NULL AND x.nbkey IS DISTINCT FROM x.bkey)::INT),2) same_id_moved,
  round(100*avg((x.nlor IS NOT NULL AND x.nbkey IS DISTINCT FROM x.bkey AND x.nlor <= 1)::INT),2) same_id_moved_strict,
  round(100*avg((x.nlor IS NULL)::INT),2) id_absent,
  round(100*avg((x.nlor IS NULL AND coalesce(r.rk_here,false))::INT),2) absent_rekeyed_same_bldg,
  round(100*avg((x.nlor IS NULL AND coalesce(r.rk_elsewhere,false) AND NOT coalesce(r.rk_here,false))::INT),2) absent_rekeyed_elsewhere,
  round(100*avg((x.nlor IS NULL AND coalesce(r.rk_elsewhere_lor1,false) AND NOT coalesce(r.rk_here,false))::INT),2) absent_rekeyed_elsewhere_lor1,
  round(100*avg((x.nlor IS NULL AND r.fid IS NULL)::INT),2) absent_not_found
FROM nx x LEFT JOIN rk r ON r.fid = x.fid AND r.y = x.y
GROUP BY ROLLUP (x.y) ORDER BY x.y NULLS LAST;
