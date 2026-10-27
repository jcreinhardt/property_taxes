SET preserve_insertion_order=false; USE memory; SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
-- Stage 2: unit-year panel and t -> t+1 pair counts for one bkey % 10 bucket (0), same logic as outmig_by_tenure.sql.
-- Unit = building key x unit number, street addresses with exactly one primary family in the year; missing years carry the
-- last record forward. Outputs counts only (summed over buckets and annualised by cut-off months in agg.sql).
-- Template placeholders: 0 bucket, /nfs/roberts/scratch/pi_csl53/jr2728/outmig_full/da_s parquet root from extract.sql, /home/jr2728/ptax_explore/hh_strat/out/outmig_full_parts output folder for the part CSVs.
CREATE OR REPLACE TABLE da AS
  SELECT y, bkey, unit, fid, ors, lor, recency_date, prim FROM read_parquet('/nfs/roberts/scratch/pi_csl53/jr2728/outmig_full/da_s/bucket=0/*.parquet');
-- file cut-offs (max over buckets taken in agg.sql)
CREATE OR REPLACE TABLE cut AS SELECT y, max(recency_date) cutoff FROM da GROUP BY 1;
-- one primary per unit-year
CREATE OR REPLACE TABLE obs AS
  SELECT y, bkey, unit, any_value(fid) fid, any_value(ors) ors, any_value(lor) lor, count(*) n_prim
  FROM da WHERE prim GROUP BY 1, 2, 3 HAVING count(*) = 1;
DROP TABLE da;
-- grid over years for every unit ever seen with one primary, carry the last record forward over missing years
CREATE OR REPLACE TABLE grid AS SELECT u.bkey, u.unit, yy.y FROM (SELECT DISTINCT bkey, unit FROM obs) u CROSS JOIN (SELECT unnest(range(2006, 2024))::INTEGER y) yy;
CREATE OR REPLACE TABLE pan AS
  SELECT g.bkey, g.unit, g.y, o.fid IS NOT NULL observed,
         last_value(o.fid IGNORE NULLS) OVER w fid, last_value(o.ors IGNORE NULLS) OVER w ors, last_value(o.lor IGNORE NULLS) OVER w lor,
         o.lor lor_obs
  FROM grid g LEFT JOIN obs o ON o.bkey = g.bkey AND o.unit = g.unit AND o.y = g.y
  WINDOW w AS (PARTITION BY g.bkey, g.unit ORDER BY g.y ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
DROP TABLE grid; DROP TABLE obs;
CREATE OR REPLACE MACRO tenure(s) AS CASE WHEN s >= 7 THEN 'owner' WHEN s <= 3 THEN 'renter' ELSE 'unknown' END;
-- pairs t -> t+1
CREATE OR REPLACE TABLE pr AS
  SELECT a.y, tenure(a.ors) ten, a.observed obs0, b.observed obs1, a.fid <> b.fid out_move, b.lor_obs <= 1 in_move
  FROM pan a JOIN pan b ON a.bkey = b.bkey AND a.unit = b.unit AND b.y = a.y + 1
  WHERE a.fid IS NOT NULL AND b.fid IS NOT NULL;
-- counts: units = pairs; out_moves = primary family_id changed; in_den = pairs with t+1 observed and LOR known; in_moves = of those, LOR <= 1
COPY (
  SELECT 0 bucket, y, ten, fill, count(*) units, count(*) FILTER (WHERE out_move) out_moves,
         count(in_move) FILTER (WHERE obs1) in_den, count(*) FILTER (WHERE obs1 AND in_move) in_moves
  FROM (SELECT *, 'backfilled' fill FROM pr UNION ALL SELECT *, 'observed_only' FROM pr WHERE obs0 AND obs1)
  GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3, 4
) TO '/home/jr2728/ptax_explore/hh_strat/out/outmig_full_parts/pairs_b0.csv' (HEADER);
COPY (SELECT 0 bucket, y, cutoff FROM cut ORDER BY 2) TO '/home/jr2728/ptax_explore/hh_strat/out/outmig_full_parts/cut_b0.csv' (HEADER);
COPY (SELECT 0 bucket, y, tenure(ors) ten, count(*) units FROM pan WHERE fid IS NOT NULL GROUP BY 1, 2, 3 ORDER BY 2, 3)
  TO '/home/jr2728/ptax_explore/hh_strat/out/outmig_full_parts/shares_b0.csv' (HEADER);
