SET preserve_insertion_order=false; USE memory; SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
-- Do the quarterly files carry new information between quarters? 1% of family_ids (hash(family_id)%100=0), 2019.
CREATE OR REPLACE TABLE q AS
  SELECT file_month fm, family_id, bkey, recency_date, length_of_residence lor, primary_family_ind prim, ln_1, individual_id_1 iid1, zip, state, address_type atype, household_status hs
  FROM read_parquet('/nfs/roberts/scratch/pi_csl53/jr2728/da_quarterly/fy=2019/fm=*/*/*.parquet', hive_partitioning=true)
  WHERE hash(family_id) % 100 = 0;
CREATE OR REPLACE TABLE a AS
  SELECT family_id, bkey, recency_date, length_of_residence lor, primary_family_ind prim, ln_1, individual_id_1 iid1, zip, state, address_type atype, household_status hs
  FROM read_parquet('/home/jr2728/ptax_explore/samples/da_fam1pct.parquet') WHERE record_year = 2019;
.print == rows per quarter and in annual
SELECT fm, count(*) n, count(DISTINCT family_id) fam, min(recency_date) minrec, max(recency_date) maxrec, avg(prim::INT) prim FROM q GROUP BY 1 ORDER BY 1;
SELECT 'annual' fm, count(*) n, count(DISTINCT family_id) fam, min(recency_date), max(recency_date), avg(prim::INT) FROM a;
.print == overlap of family sets: quarter vs annual
SELECT fm, count(*) n_q, count(a.family_id) in_annual, round(avg((a.family_id IS NOT NULL)::INT),4) share_in_annual,
       round(avg((q.bkey IS NOT DISTINCT FROM a.bkey)::INT) FILTER (WHERE a.family_id IS NOT NULL),4) same_bkey,
       round(avg((q.recency_date = a.recency_date)::INT) FILTER (WHERE a.family_id IS NOT NULL),4) same_recency,
       round(avg((q.lor = a.lor)::INT) FILTER (WHERE a.family_id IS NOT NULL),4) same_lor,
       round(avg((q.hs = a.hs)::INT) FILTER (WHERE a.family_id IS NOT NULL),4) same_status,
       round(avg((q.prim = a.prim)::INT) FILTER (WHERE a.family_id IS NOT NULL),4) same_prim
FROM q LEFT JOIN a USING (family_id) GROUP BY 1 ORDER BY 1;
SELECT count(*) annual_rows, count(q.family_id) in_any_quarter FROM a LEFT JOIN (SELECT DISTINCT family_id FROM q) q USING (family_id);
.print == consecutive quarters: what changes for the same family?
WITH p AS (SELECT a.fm fm0, b.fm fm1, a.family_id, a.bkey b0, b.bkey b1, a.recency_date r0, b.recency_date r1, a.lor l0, b.lor l1, a.hs h0, b.hs h1, a.prim p0, b.prim p1
           FROM q a JOIN q b ON a.family_id = b.family_id AND b.fm = a.fm + 3)
SELECT fm0, fm1, count(*) n, round(avg((b0 IS DISTINCT FROM b1)::INT),4) bkey_change, round(avg((r0 <> r1)::INT),4) recency_change,
       round(avg((l0 <> l1)::INT),4) lor_change, round(avg((h0 <> h1)::INT),4) status_change, round(avg((p0 <> p1)::INT),4) prim_change,
       round(avg((b0 IS DISTINCT FROM b1 AND l1 <= 1)::INT),4) move_strict
FROM p GROUP BY 1,2 ORDER BY 1;
.print == family sets between consecutive quarters: new and gone
WITH f AS (SELECT fm, family_id FROM q GROUP BY 1,2)
SELECT a.fm fm0, count(*) n0, count(b.family_id) still_next, round(avg((b.family_id IS NULL)::INT),4) gone_next
FROM f a LEFT JOIN f b ON a.family_id = b.family_id AND b.fm = a.fm + 3 WHERE a.fm < 12 GROUP BY 1 ORDER BY 1;
WITH f AS (SELECT fm, family_id FROM q GROUP BY 1,2)
SELECT b.fm fm1, count(*) n1, round(avg((a.family_id IS NULL)::INT),4) new_vs_prev FROM f b LEFT JOIN f a ON a.family_id = b.family_id AND a.fm = b.fm - 3 WHERE b.fm > 3 GROUP BY 1 ORDER BY 1;
.print == does the Dec file look like the June file minus dropped records? rows in Dec identical to June on all compared fields
SELECT count(*) n_dec, round(avg((j.family_id IS NOT NULL)::INT),4) in_june,
       round(avg((j.family_id IS NOT NULL AND d.bkey IS NOT DISTINCT FROM j.bkey AND d.recency_date = j.recency_date AND d.lor = j.lor)::INT),4) identical_to_june
FROM (SELECT * FROM q WHERE fm = 12) d LEFT JOIN (SELECT * FROM q WHERE fm = 6) j USING (family_id);
.print == recency_date distribution by quarter (share of records with recency in the latest 3 months of the year's cut-off, and > 12 months old)
SELECT fm, round(avg((recency_date >= '201904')::INT),3) rec_since_apr19, round(avg((recency_date < '201807')::INT),3) rec_older_than_12m FROM q GROUP BY 1 ORDER BY 1;
