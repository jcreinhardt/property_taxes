-- 1% family sample: origin x destination state counts for consecutive annual files, all states (same family_id in both files).
-- fam = families; fam_strict = state changed and LOR reset at destination (<= 1; <= 2 for 2021->23); same-state rows are stayers (incl. within-state moves).
.timer on
USE memory;
CREATE OR REPLACE TABLE memory.main.d AS
SELECT family_id fid, record_year y, state st, primary_family_ind prim, length_of_residence lor
FROM read_parquet('/home/jr2728/ptax_explore/samples/da_fam1pct.parquet')
WHERE state IS NOT NULL AND record_year BETWEEN 2011 AND 2023 AND record_year <> 2022;
COPY (
SELECT a.y y0, b.y y1, a.st o, b.st d, count(*) fam, count(*) FILTER (WHERE a.prim) fam_prim,
       count(*) FILTER (WHERE a.st <> b.st AND b.lor <= CASE WHEN a.y = 2021 THEN 2 ELSE 1 END) fam_strict,
       count(*) FILTER (WHERE a.prim AND a.st <> b.st AND b.lor <= CASE WHEN a.y = 2021 THEN 2 ELSE 1 END) fam_prim_strict
FROM memory.main.d a JOIN memory.main.d b ON a.fid = b.fid AND b.y = CASE WHEN a.y = 2021 THEN 2023 ELSE a.y + 1 END
GROUP BY ALL ORDER BY ALL) TO '/home/jr2728/ptax_explore/mig4/da_state_pairs_1pct.csv' (HEADER);
SELECT count(*) FROM read_csv('/home/jr2728/ptax_explore/mig4/da_state_pairs_1pct.csv');
