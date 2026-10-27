SET preserve_insertion_order=false; USE memory; SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
-- Stage 3: sum the bucket counts from bucket.sql and annualise by the files' cut-off months; same layout as outmig_by_tenure.csv.
-- Placeholders: /home/jr2728/ptax_explore/hh_strat/out/outmig_full_parts part-CSV folder, outmig_full output file prefix (outmig_full / outmig_full_test).
CREATE OR REPLACE TABLE p AS SELECT * FROM read_csv('/home/jr2728/ptax_explore/hh_strat/out/outmig_full_parts/pairs_b*.csv', header = true, union_by_name = true);
CREATE OR REPLACE TABLE cut AS SELECT y, max(cutoff::VARCHAR) cutoff FROM read_csv('/home/jr2728/ptax_explore/hh_strat/out/outmig_full_parts/cut_b*.csv', header = true, union_by_name = true, types = {'cutoff': 'VARCHAR'}) GROUP BY 1;
CREATE OR REPLACE TABLE mon AS
  SELECT a.y, b.y y1, (b.cutoff[1:4]::INT * 12 + b.cutoff[5:6]::INT) - (a.cutoff[1:4]::INT * 12 + a.cutoff[5:6]::INT) mon FROM cut a JOIN cut b ON b.y = a.y + 1;
CREATE OR REPLACE TABLE s AS
  SELECT y, ten, fill, sum(units)::BIGINT units, sum(out_moves)::BIGINT out_moves, sum(in_den)::BIGINT in_den, sum(in_moves)::BIGINT in_moves FROM p GROUP BY 1, 2, 3
  UNION ALL
  SELECT y, 'all', fill, sum(units)::BIGINT, sum(out_moves)::BIGINT, sum(in_den)::BIGINT, sum(in_moves)::BIGINT FROM p GROUP BY 1, 2, 3;
SELECT count(DISTINCT bucket) buckets, count(*) part_rows FROM p;
COPY (
  SELECT s.y, s.y + 1 y1, ten, fill, units, m.mon,
         round(100.0 * out_moves / units, 2) out_move_pct, round(100.0 * out_moves / units * 12 / m.mon, 2) out_move_annualised,
         round(100.0 * in_moves / in_den, 2) in_move_pct, round(100.0 * in_moves / in_den * 12 / m.mon, 2) in_move_annualised
  FROM s JOIN mon m ON m.y = s.y ORDER BY fill, ten, y
) TO '/home/jr2728/ptax_explore/hh_strat/out/outmig_full_by_tenure.csv' (HEADER);
COPY (
  SELECT s.y, s.y + 1 y1, ten, fill, units, out_moves, in_den, in_moves FROM s ORDER BY fill, ten, y
) TO '/home/jr2728/ptax_explore/hh_strat/out/outmig_full_counts.csv' (HEADER);
COPY (SELECT y, cutoff FROM cut ORDER BY 1) TO '/home/jr2728/ptax_explore/hh_strat/out/outmig_full_cutoffs.csv' (HEADER);
COPY (SELECT y, ten, sum(units)::BIGINT units FROM read_csv('/home/jr2728/ptax_explore/hh_strat/out/outmig_full_parts/shares_b*.csv', header = true, union_by_name = true) GROUP BY 1, 2 ORDER BY 1, 2)
  TO '/home/jr2728/ptax_explore/hh_strat/out/outmig_full_tenure_shares.csv' (HEADER);
