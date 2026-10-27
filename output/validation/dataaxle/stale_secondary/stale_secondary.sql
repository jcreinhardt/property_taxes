SET preserve_insertion_order=false;
USE memory;
SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
-- Do moving primary families leave a stale secondary family behind at the old address?
-- 5% sample of buildings (hash of ZIP5 + street + house number), all families there, 2006-2021 (2022 truncated; pairs need t+2).
-- Surnames hashed in-query; only rates exported.
CREATE OR REPLACE MACRO hn(x) AS CASE WHEN coalesce(trim(x), '') <> '' THEN hash(upper(trim(x))) END;
CREATE OR REPLACE MACRO bk(z, s, h) AS hash(lpad(z::VARCHAR,5,'0'), upper(trim(s)), upper(trim(h)));
CREATE OR REPLACE MACRO okaddr(t, h, s, z) AS t = 'S' AND coalesce(trim(h),'') <> '' AND coalesce(trim(s),'') <> '' AND z IS NOT NULL;
CREATE OR REPLACE TABLE s AS
  SELECT family_id fid, record_year::SMALLINT y, bk(zip, street_name, house_num) a, primary_family_ind prim, length_of_residence lor,
         list_filter([hn(last_name_1), hn(last_name_2), hn(last_name_3), hn(last_name_4), hn(last_name_5)], x -> x IS NOT NULL) sn,
         list_filter([individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5], x -> x IS NOT NULL) iid
  FROM cl.dataaxle_annual
  WHERE record_year BETWEEN 2006 AND 2021 AND okaddr(address_type, house_num, street_name, zip) AND bk(zip, street_name, house_num) % 20 = 0;
CREATE OR REPLACE TABLE fids AS SELECT DISTINCT fid FROM s;
-- where are these families in every year (any address; a = NULL if not a street address)
CREATE OR REPLACE TABLE fpos AS
  SELECT family_id fid, record_year::SMALLINT y, CASE WHEN okaddr(address_type, house_num, street_name, zip) THEN bk(zip, street_name, house_num) END a,
         primary_family_ind prim, length_of_residence lor
  FROM cl.dataaxle_annual SEMI JOIN fids ON family_id = fids.fid WHERE record_year BETWEEN 2006 AND 2021;
CREATE OR REPLACE TABLE ffirst AS SELECT fid, min(y) y0 FROM fpos GROUP BY fid;
-- address-year family sets
CREATE OR REPLACE TABLE ay AS SELECT a, y, list(fid) fids FROM s GROUP BY a, y;
-- (1) primary families at A in t: status in t+1, and what is at A in t+1
CREATE OR REPLACE TABLE p AS
  SELECT f.fid, f.a, f.y, f.sn, f.iid,
         CASE WHEN n.fid IS NULL THEN 'vanished' WHEN n.a IS NULL THEN 'moved_nonstreet' WHEN n.a = f.a THEN 'stayed' ELSE 'moved' END status,
         n.lor lor_next
  FROM s f LEFT JOIN fpos n ON n.fid = f.fid AND n.y = f.y + 1
  WHERE f.prim AND f.y BETWEEN 2006 AND 2019;
CREATE OR REPLACE TABLE pn AS
  SELECT p.fid, p.y, p.status, p.lor_next, x.fid xfid, x.prim xprim, x.lor xlor,
         len(list_intersect(x.iid, p.iid)) > 0 x_iid, len(list_intersect(x.sn, p.sn)) > 0 x_sn,
         list_contains(coalesce(b.fids, []), x.fid) x_was_here, ff.y0 = p.y + 1 x_brand_new,
         z.fid IS NOT NULL x_here_t2
  FROM p JOIN s x ON x.a = p.a AND x.y = p.y + 1 AND x.fid <> p.fid
  LEFT JOIN ay b ON b.a = p.a AND b.y = p.y JOIN ffirst ff ON ff.fid = x.fid
  LEFT JOIN (SELECT DISTINCT a, y, fid FROM s) z ON z.a = p.a AND z.y = p.y + 2 AND z.fid = x.fid;
CREATE OR REPLACE TABLE pa AS
  SELECT p.fid, p.y, p.status, p.lor_next,
         coalesce(bool_or(n.x_iid), false) any_iid,
         coalesce(bool_or(n.x_sn AND NOT n.x_was_here), false) new_sn,
         coalesce(bool_or(n.x_sn AND n.x_was_here), false) old_sn,
         coalesce(bool_or((n.x_sn OR n.x_iid) AND NOT n.x_was_here AND NOT n.xprim), false) new_sec_link,
         coalesce(bool_or((n.x_sn OR n.x_iid) AND NOT n.x_was_here AND n.xprim), false) new_prim_link,
         coalesce(bool_or((n.x_sn OR n.x_iid) AND NOT n.x_was_here AND n.x_brand_new), false) new_link_brand_new_id,
         coalesce(bool_or((n.x_sn OR n.x_iid) AND NOT n.x_was_here AND n.x_here_t2), false) new_link_still_t2,
         coalesce(bool_or((n.x_sn OR n.x_iid) AND NOT n.x_was_here AND n.xlor > 1), false) new_link_lor_gt1,
         coalesce(bool_or(NOT n.x_was_here), false) any_new_family
  FROM p LEFT JOIN pn n ON n.fid = p.fid AND n.y = p.y GROUP BY ALL;
COPY (
  SELECT status, y, (lor_next <= 1) lor_reset, count(*) primaries,
         avg(any_iid::INT) old_addr_has_their_person_id, avg(new_sn::INT) old_addr_new_family_their_surname,
         avg(old_sn::INT) old_addr_existing_family_their_surname,
         avg(new_sec_link::INT) new_secondary_linked, avg(new_prim_link::INT) new_primary_linked,
         avg(new_link_brand_new_id::INT) new_linked_brand_new_id, avg(new_link_still_t2::INT) new_linked_still_there_t2,
         avg(new_link_lor_gt1::INT) new_linked_lor_gt1, avg(any_new_family::INT) any_new_family
  FROM pa GROUP BY GROUPING SETS ((status, lor_reset), (status, y)) ORDER BY ALL
) TO '/home/jr2728/ptax_explore/hh_strat/out/stale_by_status.csv' (HEADER);
-- (2) every family new at an address in t+1 (not there in t; address present in t): where does it come from?
CREATE OR REPLACE TABLE nf AS
  SELECT x.fid, x.a, x.y, x.prim, x.lor, x.sn, x.iid, ff.y0 = x.y brand_new,
         z.fid IS NOT NULL still_next
  FROM s x JOIN ay b ON b.a = x.a AND b.y = x.y - 1 JOIN ffirst ff ON ff.fid = x.fid
  LEFT JOIN (SELECT DISTINCT a, y, fid FROM s) z ON z.a = x.a AND z.y = x.y + 1 AND z.fid = x.fid
  WHERE NOT list_contains(b.fids, x.fid) AND x.y BETWEEN 2007 AND 2020;
CREATE OR REPLACE TABLE nf2 AS
  SELECT n.fid, n.y, n.prim, n.lor, n.brand_new, n.still_next,
         coalesce(bool_or(l.status IN ('moved','moved_nonstreet','vanished') AND (len(list_intersect(n.sn, l.sn)) > 0 OR len(list_intersect(n.iid, l.iid)) > 0)), false) link_departed_primary,
         coalesce(bool_or(l.status = 'stayed' AND len(list_intersect(n.sn, l.sn)) > 0), false) link_staying_primary,
         any_value(q.a) a_prev
  FROM nf n LEFT JOIN p l ON l.a = n.a AND l.y = n.y - 1 LEFT JOIN fpos q ON q.fid = n.fid AND q.y = n.y - 1
  GROUP BY n.fid, n.y, n.prim, n.lor, n.brand_new, n.still_next;
COPY (
  SELECT prim, y, CASE WHEN link_departed_primary THEN '1_linked_to_departed_primary' WHEN link_staying_primary THEN '2_shares_surname_with_staying_primary'
                       WHEN brand_new THEN '3_brand_new_id_no_link' WHEN a_prev IS NOT NULL THEN '4_moved_in_from_other_street_address'
                       ELSE '5_existing_id_elsewhere_nonstreet_or_absent' END origin,
         count(*) families, avg((lor <= 1)::INT) lor_le1, avg(still_next::INT) still_there_next_year
  FROM nf2 GROUP BY GROUPING SETS ((prim, origin), (prim, y, origin)) ORDER BY ALL
) TO '/home/jr2728/ptax_explore/hh_strat/out/stale_new_families.csv' (HEADER);
