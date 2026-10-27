SET preserve_insertion_order=false; USE memory; SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
-- Out-migration (primary family changes at a unit) and in-migration (new primary with LOR <= 1) in Data Axle, by owner/renter
-- status, all years 2006-2023, annualised by the files' cut-off mon. 1% building sample + the 2022 replacement built from the
-- quarterly files. Unit = building key x unit number, street addresses with exactly one primary family.
CREATE OR REPLACE TABLE da AS
  SELECT record_year y, bkey, coalesce(upper(trim(unit_num)), '') unit, family_id, owner_renter_status ors, length_of_residence lor, recency_date, primary_family_ind prim
  FROM read_parquet('/home/jr2728/ptax_explore/samples/da_bld1pct.parquet') WHERE address_type = 'S' AND bkey IS NOT NULL AND record_year <> 2022
  UNION ALL
  SELECT record_year, bkey, coalesce(upper(trim(unit_num)), ''), family_id, owner_renter_status, length_of_residence, recency_date, primary_family_ind
  FROM read_parquet('/home/jr2728/ptax_explore/samples/da_annual_2022_from_quarters/*/*.parquet', hive_partitioning = true)
  WHERE address_type = 'S' AND bkey IS NOT NULL AND bkey % 100 = 0;
-- file cut-offs
CREATE OR REPLACE TABLE cut AS SELECT y, max(recency_date) cutoff FROM da GROUP BY 1;
CREATE OR REPLACE TABLE mon AS
  SELECT a.y, b.y y1, (b.cutoff[1:4]::INT * 12 + b.cutoff[5:6]::INT) - (a.cutoff[1:4]::INT * 12 + a.cutoff[5:6]::INT) mon FROM cut a JOIN cut b ON b.y = a.y + 1;
-- one primary per unit-year
CREATE OR REPLACE TABLE obs AS
  SELECT y, bkey, unit, any_value(family_id) fid, any_value(ors) ors, any_value(lor) lor, count(*) n_prim
  FROM da WHERE prim GROUP BY 1, 2, 3 HAVING count(*) = 1;
-- grid over years for every unit ever seen with one primary, carry the last record forward over missing years
CREATE OR REPLACE TABLE grid AS SELECT u.bkey, u.unit, yy.y FROM (SELECT DISTINCT bkey, unit FROM obs) u CROSS JOIN (SELECT unnest(range(2006, 2024)) y) yy;
CREATE OR REPLACE TABLE pan AS
  SELECT g.bkey, g.unit, g.y, o.fid IS NOT NULL observed,
         last_value(o.fid IGNORE NULLS) OVER w fid, last_value(o.ors IGNORE NULLS) OVER w ors, last_value(o.lor IGNORE NULLS) OVER w lor,
         o.lor lor_obs
  FROM grid g LEFT JOIN obs o ON o.bkey = g.bkey AND o.unit = g.unit AND o.y = g.y
  WINDOW w AS (PARTITION BY g.bkey, g.unit ORDER BY g.y ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
CREATE OR REPLACE MACRO tenure(s) AS CASE WHEN s >= 7 THEN 'owner' WHEN s <= 3 THEN 'renter' ELSE 'unknown' END;
-- pairs t -> t+1
CREATE OR REPLACE TABLE pr AS
  SELECT a.y, tenure(a.ors) ten, a.observed obs0, b.observed obs1, a.fid <> b.fid out_move, b.lor_obs <= 1 in_move, m.mon
  FROM pan a JOIN pan b ON a.bkey = b.bkey AND a.unit = b.unit AND b.y = a.y + 1 JOIN mon m ON m.y = a.y
  WHERE a.fid IS NOT NULL AND b.fid IS NOT NULL;
COPY (
  SELECT y, y + 1 y1, ten, fill, count(*) units, any_value(mon) mon,
         round(100 * avg(out_move::INT), 2) out_move_pct, round(100 * avg(out_move::INT) * 12 / any_value(mon), 2) out_move_annualised,
         round(100 * avg(in_move::INT) FILTER (WHERE obs1), 2) in_move_pct, round(100 * avg(in_move::INT) FILTER (WHERE obs1) * 12 / any_value(mon), 2) in_move_annualised
  FROM (SELECT *, 'backfilled' fill FROM pr UNION ALL SELECT *, 'observed_only' FROM pr WHERE obs0 AND obs1)
  GROUP BY 1, 2, 3, 4
  UNION ALL
  SELECT y, y + 1, 'all', fill, count(*), any_value(mon), round(100 * avg(out_move::INT), 2), round(100 * avg(out_move::INT) * 12 / any_value(mon), 2),
         round(100 * avg(in_move::INT) FILTER (WHERE obs1), 2), round(100 * avg(in_move::INT) FILTER (WHERE obs1) * 12 / any_value(mon), 2)
  FROM (SELECT *, 'backfilled' fill FROM pr UNION ALL SELECT *, 'observed_only' FROM pr WHERE obs0 AND obs1)
  GROUP BY 1, 2, 3, 4
  ORDER BY fill, ten, y
) TO '/home/jr2728/ptax_explore/hh_strat/out/outmig_by_tenure.csv' (HEADER);
COPY (SELECT y, cutoff FROM cut ORDER BY 1) TO '/home/jr2728/ptax_explore/hh_strat/out/outmig_cutoffs.csv' (HEADER);
COPY (SELECT y, tenure(ors) ten, count(*) units FROM pan WHERE fid IS NOT NULL GROUP BY 1, 2 ORDER BY 1, 2) TO '/home/jr2728/ptax_explore/hh_strat/out/outmig_tenure_shares.csv' (HEADER);
