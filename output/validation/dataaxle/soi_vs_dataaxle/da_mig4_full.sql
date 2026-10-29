-- Full data: state-to-state moves among CA, TX, NY, FL (and same-state stayers) for consecutive annual files.
-- Same family_id in both files; loose = state changed; strict = state changed and length_of_residence reset in t+1
-- (<= 1; <= 2 for the two-year pair 2021->23). persons = non-null individual_id_1..5 on the t+1 record.
-- 2022 comes from the quarterly-file replacement (~/ptax_explore/samples/da_annual_2022_from_quarters, state = hive partition), not the truncated table.
USE memory;
CREATE OR REPLACE MACRO np(a,b,c,d,e) AS (a IS NOT NULL)::INT + (b IS NOT NULL)::INT + (c IS NOT NULL)::INT + (d IS NOT NULL)::INT + (e IS NOT NULL)::INT;
CREATE OR REPLACE VIEW da AS
SELECT family_id, record_year, state, primary_family_ind, length_of_residence, individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5
FROM cl.dataaxle_annual WHERE record_year <> 2022
UNION ALL
SELECT family_id::BIGINT, 2022::USMALLINT, state::VARCHAR, primary_family_ind, length_of_residence, individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5
FROM read_parquet('/home/jr2728/ptax_explore/samples/da_annual_2022_from_quarters/state=*/*.parquet', hive_partitioning = true);
CREATE OR REPLACE TABLE res (y0 SMALLINT, y1 SMALLINT, o VARCHAR, d VARCHAR, fam BIGINT, fam_prim BIGINT, fam_strict BIGINT, fam_prim_strict BIGINT, persons BIGINT, persons_strict BIGINT);
INSERT INTO res
WITH a AS (SELECT family_id fid, state st, primary_family_ind prim FROM da WHERE record_year = 2011 AND state IN ('CA','TX','NY','FL')),
     b AS (SELECT family_id fid, state st, length_of_residence lor, np(individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5) np FROM da WHERE record_year = 2012 AND state IN ('CA','TX','NY','FL'))
SELECT 2011, 2012, a.st, b.st, count(*), count(*) FILTER (WHERE a.prim), count(*) FILTER (WHERE b.lor <= 1), count(*) FILTER (WHERE a.prim AND b.lor <= 1), sum(b.np), sum(b.np) FILTER (WHERE b.lor <= 1)
FROM a JOIN b USING (fid) GROUP BY ALL;
COPY (SELECT * FROM res ORDER BY ALL) TO '/home/jr2728/ptax_explore/mig4/da_mig4_full.csv.tmp' (HEADER);
INSERT INTO res
WITH a AS (SELECT family_id fid, state st, primary_family_ind prim FROM da WHERE record_year = 2012 AND state IN ('CA','TX','NY','FL')),
     b AS (SELECT family_id fid, state st, length_of_residence lor, np(individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5) np FROM da WHERE record_year = 2013 AND state IN ('CA','TX','NY','FL'))
SELECT 2012, 2013, a.st, b.st, count(*), count(*) FILTER (WHERE a.prim), count(*) FILTER (WHERE b.lor <= 1), count(*) FILTER (WHERE a.prim AND b.lor <= 1), sum(b.np), sum(b.np) FILTER (WHERE b.lor <= 1)
FROM a JOIN b USING (fid) GROUP BY ALL;
COPY (SELECT * FROM res ORDER BY ALL) TO '/home/jr2728/ptax_explore/mig4/da_mig4_full.csv.tmp' (HEADER);
INSERT INTO res
WITH a AS (SELECT family_id fid, state st, primary_family_ind prim FROM da WHERE record_year = 2013 AND state IN ('CA','TX','NY','FL')),
     b AS (SELECT family_id fid, state st, length_of_residence lor, np(individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5) np FROM da WHERE record_year = 2014 AND state IN ('CA','TX','NY','FL'))
SELECT 2013, 2014, a.st, b.st, count(*), count(*) FILTER (WHERE a.prim), count(*) FILTER (WHERE b.lor <= 1), count(*) FILTER (WHERE a.prim AND b.lor <= 1), sum(b.np), sum(b.np) FILTER (WHERE b.lor <= 1)
FROM a JOIN b USING (fid) GROUP BY ALL;
COPY (SELECT * FROM res ORDER BY ALL) TO '/home/jr2728/ptax_explore/mig4/da_mig4_full.csv.tmp' (HEADER);
INSERT INTO res
WITH a AS (SELECT family_id fid, state st, primary_family_ind prim FROM da WHERE record_year = 2014 AND state IN ('CA','TX','NY','FL')),
     b AS (SELECT family_id fid, state st, length_of_residence lor, np(individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5) np FROM da WHERE record_year = 2015 AND state IN ('CA','TX','NY','FL'))
SELECT 2014, 2015, a.st, b.st, count(*), count(*) FILTER (WHERE a.prim), count(*) FILTER (WHERE b.lor <= 1), count(*) FILTER (WHERE a.prim AND b.lor <= 1), sum(b.np), sum(b.np) FILTER (WHERE b.lor <= 1)
FROM a JOIN b USING (fid) GROUP BY ALL;
COPY (SELECT * FROM res ORDER BY ALL) TO '/home/jr2728/ptax_explore/mig4/da_mig4_full.csv.tmp' (HEADER);
INSERT INTO res
WITH a AS (SELECT family_id fid, state st, primary_family_ind prim FROM da WHERE record_year = 2015 AND state IN ('CA','TX','NY','FL')),
     b AS (SELECT family_id fid, state st, length_of_residence lor, np(individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5) np FROM da WHERE record_year = 2016 AND state IN ('CA','TX','NY','FL'))
SELECT 2015, 2016, a.st, b.st, count(*), count(*) FILTER (WHERE a.prim), count(*) FILTER (WHERE b.lor <= 1), count(*) FILTER (WHERE a.prim AND b.lor <= 1), sum(b.np), sum(b.np) FILTER (WHERE b.lor <= 1)
FROM a JOIN b USING (fid) GROUP BY ALL;
COPY (SELECT * FROM res ORDER BY ALL) TO '/home/jr2728/ptax_explore/mig4/da_mig4_full.csv.tmp' (HEADER);
INSERT INTO res
WITH a AS (SELECT family_id fid, state st, primary_family_ind prim FROM da WHERE record_year = 2016 AND state IN ('CA','TX','NY','FL')),
     b AS (SELECT family_id fid, state st, length_of_residence lor, np(individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5) np FROM da WHERE record_year = 2017 AND state IN ('CA','TX','NY','FL'))
SELECT 2016, 2017, a.st, b.st, count(*), count(*) FILTER (WHERE a.prim), count(*) FILTER (WHERE b.lor <= 1), count(*) FILTER (WHERE a.prim AND b.lor <= 1), sum(b.np), sum(b.np) FILTER (WHERE b.lor <= 1)
FROM a JOIN b USING (fid) GROUP BY ALL;
COPY (SELECT * FROM res ORDER BY ALL) TO '/home/jr2728/ptax_explore/mig4/da_mig4_full.csv.tmp' (HEADER);
INSERT INTO res
WITH a AS (SELECT family_id fid, state st, primary_family_ind prim FROM da WHERE record_year = 2017 AND state IN ('CA','TX','NY','FL')),
     b AS (SELECT family_id fid, state st, length_of_residence lor, np(individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5) np FROM da WHERE record_year = 2018 AND state IN ('CA','TX','NY','FL'))
SELECT 2017, 2018, a.st, b.st, count(*), count(*) FILTER (WHERE a.prim), count(*) FILTER (WHERE b.lor <= 1), count(*) FILTER (WHERE a.prim AND b.lor <= 1), sum(b.np), sum(b.np) FILTER (WHERE b.lor <= 1)
FROM a JOIN b USING (fid) GROUP BY ALL;
COPY (SELECT * FROM res ORDER BY ALL) TO '/home/jr2728/ptax_explore/mig4/da_mig4_full.csv.tmp' (HEADER);
INSERT INTO res
WITH a AS (SELECT family_id fid, state st, primary_family_ind prim FROM da WHERE record_year = 2018 AND state IN ('CA','TX','NY','FL')),
     b AS (SELECT family_id fid, state st, length_of_residence lor, np(individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5) np FROM da WHERE record_year = 2019 AND state IN ('CA','TX','NY','FL'))
SELECT 2018, 2019, a.st, b.st, count(*), count(*) FILTER (WHERE a.prim), count(*) FILTER (WHERE b.lor <= 1), count(*) FILTER (WHERE a.prim AND b.lor <= 1), sum(b.np), sum(b.np) FILTER (WHERE b.lor <= 1)
FROM a JOIN b USING (fid) GROUP BY ALL;
COPY (SELECT * FROM res ORDER BY ALL) TO '/home/jr2728/ptax_explore/mig4/da_mig4_full.csv.tmp' (HEADER);
INSERT INTO res
WITH a AS (SELECT family_id fid, state st, primary_family_ind prim FROM da WHERE record_year = 2019 AND state IN ('CA','TX','NY','FL')),
     b AS (SELECT family_id fid, state st, length_of_residence lor, np(individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5) np FROM da WHERE record_year = 2020 AND state IN ('CA','TX','NY','FL'))
SELECT 2019, 2020, a.st, b.st, count(*), count(*) FILTER (WHERE a.prim), count(*) FILTER (WHERE b.lor <= 1), count(*) FILTER (WHERE a.prim AND b.lor <= 1), sum(b.np), sum(b.np) FILTER (WHERE b.lor <= 1)
FROM a JOIN b USING (fid) GROUP BY ALL;
COPY (SELECT * FROM res ORDER BY ALL) TO '/home/jr2728/ptax_explore/mig4/da_mig4_full.csv.tmp' (HEADER);
INSERT INTO res
WITH a AS (SELECT family_id fid, state st, primary_family_ind prim FROM da WHERE record_year = 2020 AND state IN ('CA','TX','NY','FL')),
     b AS (SELECT family_id fid, state st, length_of_residence lor, np(individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5) np FROM da WHERE record_year = 2021 AND state IN ('CA','TX','NY','FL'))
SELECT 2020, 2021, a.st, b.st, count(*), count(*) FILTER (WHERE a.prim), count(*) FILTER (WHERE b.lor <= 1), count(*) FILTER (WHERE a.prim AND b.lor <= 1), sum(b.np), sum(b.np) FILTER (WHERE b.lor <= 1)
FROM a JOIN b USING (fid) GROUP BY ALL;
COPY (SELECT * FROM res ORDER BY ALL) TO '/home/jr2728/ptax_explore/mig4/da_mig4_full.csv.tmp' (HEADER);
INSERT INTO res
WITH a AS (SELECT family_id fid, state st, primary_family_ind prim FROM da WHERE record_year = 2021 AND state IN ('CA','TX','NY','FL')),
     b AS (SELECT family_id fid, state st, length_of_residence lor, np(individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5) np FROM da WHERE record_year = 2022 AND state IN ('CA','TX','NY','FL'))
SELECT 2021, 2022, a.st, b.st, count(*), count(*) FILTER (WHERE a.prim), count(*) FILTER (WHERE b.lor <= 1), count(*) FILTER (WHERE a.prim AND b.lor <= 1), sum(b.np), sum(b.np) FILTER (WHERE b.lor <= 1)
FROM a JOIN b USING (fid) GROUP BY ALL;
COPY (SELECT * FROM res ORDER BY ALL) TO '/home/jr2728/ptax_explore/mig4/da_mig4_full.csv.tmp' (HEADER);
INSERT INTO res
WITH a AS (SELECT family_id fid, state st, primary_family_ind prim FROM da WHERE record_year = 2022 AND state IN ('CA','TX','NY','FL')),
     b AS (SELECT family_id fid, state st, length_of_residence lor, np(individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5) np FROM da WHERE record_year = 2023 AND state IN ('CA','TX','NY','FL'))
SELECT 2022, 2023, a.st, b.st, count(*), count(*) FILTER (WHERE a.prim), count(*) FILTER (WHERE b.lor <= 1), count(*) FILTER (WHERE a.prim AND b.lor <= 1), sum(b.np), sum(b.np) FILTER (WHERE b.lor <= 1)
FROM a JOIN b USING (fid) GROUP BY ALL;
COPY (SELECT * FROM res ORDER BY ALL) TO '/home/jr2728/ptax_explore/mig4/da_mig4_full.csv.tmp' (HEADER);
INSERT INTO res
WITH a AS (SELECT family_id fid, state st, primary_family_ind prim FROM da WHERE record_year = 2021 AND state IN ('CA','TX','NY','FL')),
     b AS (SELECT family_id fid, state st, length_of_residence lor, np(individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5) np FROM da WHERE record_year = 2023 AND state IN ('CA','TX','NY','FL'))
SELECT 2021, 2023, a.st, b.st, count(*), count(*) FILTER (WHERE a.prim), count(*) FILTER (WHERE b.lor <= 2), count(*) FILTER (WHERE a.prim AND b.lor <= 2), sum(b.np), sum(b.np) FILTER (WHERE b.lor <= 2)
FROM a JOIN b USING (fid) GROUP BY ALL;
COPY (SELECT * FROM res ORDER BY ALL) TO '/home/jr2728/ptax_explore/mig4/da_mig4_full.csv.tmp' (HEADER);
COPY (SELECT * FROM res ORDER BY ALL) TO '/home/jr2728/ptax_explore/mig4/da_mig4_full.csv' (HEADER);
