SET preserve_insertion_order=false;
USE memory;
SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
-- Primary families at the 1% building sample whose family_id is absent in t+1: is person 1 found in t+1 anywhere in Data Axle
-- by hashed (last, first) name and age in [age, age+2] (or [age+10, age+12] as a placebo), under another family_id?
-- Names hashed in-query; only rates exported. 2022 excluded; t+1 <= 2021 (2023 has no ages).
CREATE OR REPLACE MACRO hn(x) AS CASE WHEN coalesce(trim(x), '') <> '' THEN hash(upper(trim(x))) END;
CREATE OR REPLACE TABLE fp AS SELECT family_id, record_year::INT y FROM '/home/jr2728/ptax_explore/samples/da_bld1pct_fampos.parquet' WHERE record_year <> 2022;
CREATE OR REPLACE TABLE tg AS
  SELECT p.family_id fid, p.record_year::INT y, p.bkey, p.state, p.ln_1 ln, p.fn_1 fn, p.age_1 age, p.individual_id_1 iid,
         p.owner_renter_status ors
  FROM '/home/jr2728/ptax_explore/samples/da_bld1pct.parquet' p
  WHERE p.primary_family_ind AND p.record_year BETWEEN 2006 AND 2020 AND p.ln_1 IS NOT NULL AND p.fn_1 IS NOT NULL AND p.age_1 BETWEEN 18 AND 100
    AND NOT EXISTS (SELECT 1 FROM fp n WHERE n.family_id = p.family_id AND n.y = p.record_year + 1);
-- returns later under the same family_id?
CREATE OR REPLACE TABLE tg AS
  SELECT t.*, EXISTS (SELECT 1 FROM fp f WHERE f.family_id = t.fid AND f.y > t.y + 1) returns_later FROM tg t;
CREATE OR REPLACE TABLE nm AS SELECT DISTINCT ln, fn FROM tg;
-- all persons in Data Axle with one of these names, 2007-2021
CREATE OR REPLACE TABLE cand AS
  SELECT * FROM (
    SELECT record_year::INT y, family_id fid, CASE WHEN address_type = 'S' AND coalesce(trim(house_num),'') <> '' AND coalesce(trim(street_name),'') <> '' AND zip IS NOT NULL
                THEN hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num))) END bkey, state, length_of_residence lor,
           unnest([hn(last_name_1), hn(last_name_2), hn(last_name_3), hn(last_name_4), hn(last_name_5)]) ln,
           unnest([hn(first_name_1), hn(first_name_2), hn(first_name_3), hn(first_name_4), hn(first_name_5)]) fn,
           unnest([age_1, age_2, age_3, age_4, age_5]) age,
           unnest([individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5]) iid
    FROM cl.dataaxle_annual WHERE record_year BETWEEN 2006 AND 2021) c
  SEMI JOIN nm USING (ln, fn);
-- national frequency of each (last, first) name pair per year (persons)
CREATE OR REPLACE TABLE fq AS SELECT y, ln, fn, count(*) n FROM cand GROUP BY ALL;
CREATE OR REPLACE TABLE tg AS
  SELECT t.*, coalesce(a.n, 0) n_t, coalesce(b.n, 0) n_t1 FROM tg t LEFT JOIN fq a ON a.y = t.y AND a.ln = t.ln AND a.fn = t.fn
  LEFT JOIN fq b ON b.y = t.y + 1 AND b.ln = t.ln AND b.fn = t.fn;
CREATE OR REPLACE TABLE m AS
  SELECT t.fid, t.y, t.returns_later, t.ors, CASE WHEN t.n_t <= 1 THEN '1_unique' WHEN t.n_t <= 5 THEN '2_2to5' WHEN t.n_t <= 50 THEN '3_6to50' ELSE '4_over50' END freq,
         bool_or(c.age BETWEEN t.age AND t.age + 2) hit, bool_or(c.age BETWEEN t.age + 10 AND t.age + 12) placebo,
         bool_or(c.age BETWEEN t.age AND t.age + 2 AND c.bkey IS DISTINCT FROM t.bkey) hit_elsewhere,
         bool_or(c.age BETWEEN t.age AND t.age + 2 AND c.bkey IS DISTINCT FROM t.bkey AND c.lor <= 1) hit_elsewhere_lor1,
         bool_or(c.age BETWEEN t.age AND t.age + 2 AND c.state IS DISTINCT FROM t.state) hit_other_state,
         bool_or(c.age BETWEEN t.age AND t.age + 2 AND c.iid = t.iid) hit_same_iid,
         count(DISTINCT c.fid) FILTER (WHERE c.age BETWEEN t.age AND t.age + 2) n_hits
  FROM tg t LEFT JOIN cand c ON c.ln = t.ln AND c.fn = t.fn AND c.y = t.y + 1 AND c.fid <> t.fid
  GROUP BY ALL;
COPY (
  SELECT y, freq, returns_later, count(*) absent_primaries,
         avg(coalesce(hit,false)::INT) name_age_match, avg(coalesce(placebo,false)::INT) placebo_match,
         avg(coalesce(hit_elsewhere,false)::INT) match_other_address, avg(coalesce(hit_elsewhere_lor1,false)::INT) match_other_address_lor1,
         avg(coalesce(hit_other_state,false)::INT) match_other_state, avg(coalesce(hit_same_iid,false)::INT) match_same_person_id,
         avg((n_hits = 1)::INT) unique_match
  FROM m GROUP BY GROUPING SETS ((y, returns_later), (returns_later), (freq, returns_later), (freq, returns_later, y)) ORDER BY ALL
) TO '/home/jr2728/ptax_explore/hh_strat/out/name_relink.csv' (HEADER);
-- denominator: all primaries in the sample by year with age and names (for converting to rates per primary family)
COPY (SELECT record_year y, count(*) primaries FROM '/home/jr2728/ptax_explore/samples/da_bld1pct.parquet'
      WHERE primary_family_ind AND record_year BETWEEN 2006 AND 2020 AND ln_1 IS NOT NULL AND fn_1 IS NOT NULL AND age_1 BETWEEN 18 AND 100 GROUP BY 1 ORDER BY 1)
  TO '/home/jr2728/ptax_explore/hh_strat/out/name_relink_denom.csv' (HEADER);
