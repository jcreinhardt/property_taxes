SET threads=4;
CREATE OR REPLACE TABLE fp AS SELECT family_id, record_year::INT y, bkey FROM '/home/jr2728/ptax_explore/samples/da_bld1pct_fampos.parquet' WHERE record_year <> 2022;
CREATE OR REPLACE TABLE p AS SELECT family_id fid, record_year::INT y, bkey FROM '/home/jr2728/ptax_explore/samples/da_bld1pct.parquet'
  WHERE primary_family_ind AND record_year BETWEEN 2006 AND 2019;
CREATE OR REPLACE TABLE ab AS SELECT p.* FROM p ANTI JOIN fp n ON n.family_id = p.fid AND n.y = p.y + 1;
CREATE OR REPLACE TABLE nxt AS
  SELECT a.fid, a.y, min(f.y) y_back, arg_min(f.bkey, f.y) b_back, a.bkey
  FROM ab a LEFT JOIN fp f ON f.family_id = a.fid AND f.y > a.y + 1 GROUP BY a.fid, a.y, a.bkey;
SELECT count(*) absent_families,
  round(100*avg((y_back IS NULL)::INT),1) never_back,
  round(100*avg((y_back IS NOT NULL AND b_back IS NOT DISTINCT FROM bkey)::INT),1) back_same_bldg,
  round(100*avg((y_back IS NOT NULL AND b_back IS DISTINCT FROM bkey)::INT),1) back_elsewhere,
  round(100*avg((y_back = y + 2)::INT),1) back_after_1yr_gap,
  round(100*avg((y_back IS NULL AND y <= 2016)::INT) / avg((y <= 2016)::INT),1) never_back_if_t_le_2016
FROM nxt;
