SET preserve_insertion_order=false;
USE memory;
SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
-- How identifying is (last name, first name, age)? Full Data Axle 2017, all persons 1-5. Names hashed in-query; only counts exported.
CREATE OR REPLACE MACRO hn(x) AS CASE WHEN coalesce(trim(x), '') <> '' THEN hash(upper(trim(x))) END;
CREATE OR REPLACE TABLE p AS
  SELECT * FROM (
    SELECT unnest([hn(last_name_1), hn(last_name_2), hn(last_name_3), hn(last_name_4), hn(last_name_5)]) ln,
           unnest([hn(first_name_1), hn(first_name_2), hn(first_name_3), hn(first_name_4), hn(first_name_5)]) fn,
           unnest([age_1, age_2, age_3, age_4, age_5]) age, state,
           unnest([individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5]) iid
    FROM cl.dataaxle_annual WHERE record_year = 2017)
  WHERE iid IS NOT NULL AND ln IS NOT NULL AND fn IS NOT NULL;
CREATE OR REPLACE TABLE res (key VARCHAR, persons_with_key BIGINT, size_bin VARCHAR, persons BIGINT, combos BIGINT);
INSERT INTO res SELECT 'name', (SELECT count(*) FROM p), b, sum(n), count(*) FROM (
  SELECT count(*) n, CASE WHEN count(*) = 1 THEN '01' WHEN count(*) = 2 THEN '02' WHEN count(*) <= 5 THEN '03-05' WHEN count(*) <= 20 THEN '06-20'
                          WHEN count(*) <= 100 THEN '21-100' WHEN count(*) <= 1000 THEN '101-1000' ELSE '1000+' END b
  FROM p GROUP BY ln, fn) GROUP BY b;
INSERT INTO res SELECT 'name+age', (SELECT count(*) FROM p WHERE age BETWEEN 18 AND 110), b, sum(n), count(*) FROM (
  SELECT count(*) n, CASE WHEN count(*) = 1 THEN '01' WHEN count(*) = 2 THEN '02' WHEN count(*) <= 5 THEN '03-05' WHEN count(*) <= 20 THEN '06-20'
                          WHEN count(*) <= 100 THEN '21-100' WHEN count(*) <= 1000 THEN '101-1000' ELSE '1000+' END b
  FROM p WHERE age BETWEEN 18 AND 110 GROUP BY ln, fn, age) GROUP BY b;
INSERT INTO res SELECT 'name+age+state', (SELECT count(*) FROM p WHERE age BETWEEN 18 AND 110 AND state IS NOT NULL), b, sum(n), count(*) FROM (
  SELECT count(*) n, CASE WHEN count(*) = 1 THEN '01' WHEN count(*) = 2 THEN '02' WHEN count(*) <= 5 THEN '03-05' WHEN count(*) <= 20 THEN '06-20'
                          WHEN count(*) <= 100 THEN '21-100' WHEN count(*) <= 1000 THEN '101-1000' ELSE '1000+' END b
  FROM p WHERE age BETWEEN 18 AND 110 AND state IS NOT NULL GROUP BY ln, fn, age, state) GROUP BY b;
-- name + 3-year age window (what the relink uses): persons whose (name, age in [a-1, a+1]) is shared with nobody else
CREATE OR REPLACE TABLE na AS SELECT ln, fn, age, count(*) n FROM p WHERE age BETWEEN 18 AND 110 GROUP BY ALL;
INSERT INTO res SELECT 'name+age±1', (SELECT sum(n) FROM na), b, sum(n), count(*) FROM (
  SELECT a.n, CASE WHEN w = 1 THEN '01' WHEN w = 2 THEN '02' WHEN w <= 5 THEN '03-05' WHEN w <= 20 THEN '06-20'
                   WHEN w <= 100 THEN '21-100' WHEN w <= 1000 THEN '101-1000' ELSE '1000+' END b
  FROM (SELECT a.ln, a.fn, a.age, a.n, sum(b.n) w FROM na a JOIN na b ON b.ln = a.ln AND b.fn = a.fn AND b.age BETWEEN a.age - 1 AND a.age + 1 GROUP BY ALL) a) GROUP BY b;
-- persons, persons with age, distinct persons by individual_id (duplicate check)
INSERT INTO res SELECT 'totals', count(*), 'persons_with_age', count(*) FILTER (WHERE age BETWEEN 18 AND 110), count(DISTINCT iid) FROM p;
COPY (SELECT * FROM res ORDER BY key, size_bin) TO '/home/jr2728/ptax_explore/hh_strat/out/name_uniq_2017.csv' (HEADER);
