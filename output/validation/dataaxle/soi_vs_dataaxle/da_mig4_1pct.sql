-- 1% family sample: state-to-state moves among CA, TX, NY, FL (and same-state stayers), consecutive year pairs.
-- Family present in both years (same family_id). loose = state changed; strict = state changed and LOR reset in t+1 (<= 1; <= 2 for 2021->23).
.timer on
USE memory;
CREATE OR REPLACE MACRO np(a,b,c,d,e) AS (a IS NOT NULL)::INT + (b IS NOT NULL)::INT + (c IS NOT NULL)::INT + (d IS NOT NULL)::INT + (e IS NOT NULL)::INT;
CREATE OR REPLACE TABLE memory.main.d AS
SELECT family_id fid, record_year y, state st, primary_family_ind prim, length_of_residence lor,
       np(individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5) np
FROM read_parquet('/home/jr2728/ptax_explore/samples/da_fam1pct.parquet')
WHERE state IN ('CA','TX','NY','FL') AND record_year BETWEEN 2011 AND 2023 AND record_year <> 2022;
COPY (
SELECT a.y y0, b.y y1, a.st o, b.st d, count(*) fam, count(*) FILTER (WHERE a.prim) fam_prim,
       count(*) FILTER (WHERE b.lor <= CASE WHEN a.y = 2021 THEN 2 ELSE 1 END) fam_strict,
       count(*) FILTER (WHERE a.prim AND b.lor <= CASE WHEN a.y = 2021 THEN 2 ELSE 1 END) fam_prim_strict,
       sum(b.np) persons, sum(b.np) FILTER (WHERE b.lor <= CASE WHEN a.y = 2021 THEN 2 ELSE 1 END) persons_strict
FROM memory.main.d a JOIN memory.main.d b ON a.fid = b.fid AND b.y = CASE WHEN a.y = 2021 THEN 2023 ELSE a.y + 1 END
GROUP BY ALL ORDER BY ALL) TO '/home/jr2728/ptax_explore/mig4/da_mig4_1pct.csv' (HEADER);
SELECT count(*) FROM read_csv('/home/jr2728/ptax_explore/mig4/da_mig4_1pct.csv');
