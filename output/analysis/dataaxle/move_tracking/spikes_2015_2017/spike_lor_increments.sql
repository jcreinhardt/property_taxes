SET preserve_insertion_order=false; USE memory; SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
SET threads=4; SET memory_limit='26GB';
-- LOR increment of continuing primaries (same family_id, same residential address in t and t+1), 1% building sample. (Re-run of the part of spike_timing.sql that overflowed on UINT16.)
CREATE OR REPLACE TABLE tx AS
  SELECT bkey, arg_max(struct_pack(pic := property_indicator_code,
                 noimpr := coalesce(calculated_improvement_value,0) = 0 AND coalesce(total_living_area_square_feet_all_buildings,0) = 0), tax_year) a
  FROM '/home/jr2728/ptax_explore/samples/tax_bld1pct.parquet' WHERE tax_year BETWEEN 2012 AND 2021 GROUP BY bkey;
CREATE OR REPLACE TABLE res AS SELECT bkey FROM tx WHERE a.pic IN (10, 11, 21, 22) AND NOT a.noimpr;
CREATE OR REPLACE TABLE fam AS
  SELECT bkey, record_year::INT y, family_id fid, primary_family_ind prim, length_of_residence::INT lor
  FROM '/home/jr2728/ptax_explore/samples/da_bld1pct.parquet' WHERE record_year BETWEEN 2012 AND 2021 AND bkey IN (SELECT bkey FROM res);
COPY (
  SELECT a.y, count(*) n, round(100*avg((b.lor - a.lor = -1)::INT),2) d_m1, round(100*avg((b.lor = a.lor)::INT),2) d_0, round(100*avg((b.lor - a.lor = 1)::INT),2) d_1,
         round(100*avg((b.lor - a.lor = 2)::INT),2) d_2, round(100*avg((b.lor - a.lor = 3)::INT),2) d_3, round(100*avg((b.lor - a.lor >= 4)::INT),2) d_4plus,
         round(100*avg((b.lor <= 1)::INT),2) reset_le1, round(100*avg((b.lor - a.lor <= -2 AND b.lor > 1)::INT),2) d_le_m2_not_reset,
         round(12*(1 + avg((b.lor - a.lor = 2)::INT) - avg((b.lor = a.lor)::INT)), 1) implied_interval_months
  FROM fam a JOIN fam b ON b.bkey = a.bkey AND b.fid = a.fid AND b.y = a.y + 1 WHERE a.prim AND b.prim AND a.lor BETWEEN 2 AND 40 GROUP BY a.y ORDER BY a.y
) TO '/home/jr2728/ptax_explore/hh_strat/out/spike_lor_increments.csv' (HEADER);
SELECT * FROM read_csv('/home/jr2728/ptax_explore/hh_strat/out/spike_lor_increments.csv');
