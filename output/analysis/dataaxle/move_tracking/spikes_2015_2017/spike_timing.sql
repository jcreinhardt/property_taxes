SET preserve_insertion_order=false; USE memory; SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
SET threads=4; SET memory_limit='26GB';
-- Test of the snapshot-timing hypothesis: (1) recency_date distribution per file year (latest confirmation dates ~ file cut-off),
-- (2) LOR increments of continuing primaries in detail, (3) arm's-length sales by sale month: share already showing a new primary in the file
-- of the sale year (a later file cut-off captures sales from later months).
CREATE OR REPLACE TABLE tx AS
  SELECT bkey, arg_max(struct_pack(pic := property_indicator_code,
                 noimpr := coalesce(calculated_improvement_value,0) = 0 AND coalesce(total_living_area_square_feet_all_buildings,0) = 0), tax_year) a
  FROM '/home/jr2728/ptax_explore/samples/tax_bld1pct.parquet' WHERE tax_year BETWEEN 2012 AND 2021 GROUP BY bkey;
CREATE OR REPLACE TABLE res AS SELECT bkey FROM tx WHERE a.pic IN (10, 11, 21, 22) AND NOT a.noimpr;
CREATE OR REPLACE TABLE fam AS
  SELECT bkey, record_year::INT y, family_id fid, primary_family_ind prim, length_of_residence lor, recency_date rec
  FROM '/home/jr2728/ptax_explore/samples/da_bld1pct.parquet' WHERE record_year BETWEEN 2012 AND 2021 AND bkey IN (SELECT bkey FROM res);
-- (1) recency dates (YYYYMM) per file year
COPY (
  SELECT y, count(*) n, max(rec) max_rec, quantile_disc(rec, 0.999) p999, quantile_disc(rec, 0.99) p99, quantile_disc(rec, 0.95) p95, quantile_disc(rec, 0.9) p90,
         quantile_disc(rec, 0.75) p75, quantile_disc(rec, 0.5) p50,
         round(100*avg((rec >= (y::VARCHAR || '01'))::INT),1) pct_rec_in_file_year, round(100*avg((rec >= (y::VARCHAR || '07'))::INT),1) pct_rec_jul_or_later
  FROM fam WHERE length(rec) = 6 GROUP BY y ORDER BY y
) TO '/home/jr2728/ptax_explore/hh_strat/out/spike_recency.csv' (HEADER);
-- recency month histogram per file year (months of the file year and the previous year)
COPY (
  SELECT y, rec, count(*) n, round(100.0*count(*)/sum(count(*)) OVER (PARTITION BY y),2) pct FROM fam WHERE length(rec) = 6 AND left(rec,4)::INT BETWEEN y - 1 AND y GROUP BY y, rec ORDER BY y, rec
) TO '/home/jr2728/ptax_explore/hh_strat/out/spike_recency_hist.csv' (HEADER);
-- (2) LOR increment of continuing primaries (same fid, same address, both years)
COPY (
  SELECT a.y, count(*) n, round(100*avg((b.lor::INT - a.lor::INT = -1)::INT),2) d_m1, round(100*avg((b.lor = a.lor)::INT),2) d_0, round(100*avg((b.lor::INT - a.lor::INT = 1)::INT),2) d_1,
         round(100*avg((b.lor::INT - a.lor::INT = 2)::INT),2) d_2, round(100*avg((b.lor::INT - a.lor::INT = 3)::INT),2) d_3, round(100*avg((b.lor::INT - a.lor::INT >= 4)::INT),2) d_4plus,
         round(100*avg((b.lor <= 1)::INT),2) reset_le1, round(100*avg((b.lor::INT - a.lor::INT <= -2 AND b.lor > 1)::INT),2) d_le_m2_not_reset,
         round(12*(1 + avg((b.lor::INT - a.lor::INT = 2)::INT) - avg((b.lor = a.lor)::INT)), 1) implied_interval_months
  FROM fam a JOIN fam b ON b.bkey = a.bkey AND b.fid = a.fid AND b.y = a.y + 1 WHERE a.prim AND b.prim AND a.lor BETWEEN 2 AND 40 GROUP BY a.y ORDER BY a.y
) TO '/home/jr2728/ptax_explore/hh_strat/out/spike_lor_increments.csv' (HEADER);
-- share of primaries with LOR = 1 by file year (new arrivals)
COPY (
  SELECT y, count(*) primaries, round(100*avg((lor = 1)::INT),2) lor1, round(100*avg((lor = 2)::INT),2) lor2 FROM fam WHERE prim GROUP BY y ORDER BY y
) TO '/home/jr2728/ptax_explore/hh_strat/out/spike_lor1_share.csv' (HEADER);
-- (3) sales capture by sale month
CREATE OR REPLACE TABLE obs AS SELECT bkey, y, min(fid) FILTER (WHERE prim) prim_fid FROM fam GROUP BY bkey, y;
CREATE OR REPLACE TABLE grid AS SELECT r.bkey, yy.y FROM res r CROSS JOIN (SELECT unnest(range(2012, 2022)) y) yy;
CREATE OR REPLACE TABLE pan AS
  SELECT g.bkey, g.y, o.prim_fid IS NOT NULL observed, last_value(o.prim_fid IGNORE NULLS) OVER (PARTITION BY g.bkey ORDER BY g.y ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) prim_fid
  FROM grid g LEFT JOIN obs o ON o.bkey = g.bkey AND o.y = g.y;
CREATE OR REPLACE TABLE sales AS
  SELECT DISTINCT bkey, year(sale_derived_date) s, month(sale_derived_date) m FROM '/home/jr2728/ptax_explore/samples/deeds_bld1pct.parquet'
  WHERE primary_category_code = 'A' AND NOT coalesce(interfamily_related_indicator, false) AND sale_amount > 0
    AND year(sale_derived_date) BETWEEN 2013 AND 2020 AND bkey IN (SELECT bkey FROM res);
CREATE OR REPLACE TABLE cap AS
  SELECT s.s, s.m, (p1.prim_fid <> p0.prim_fid) chg_s, (p2.prim_fid <> p0.prim_fid) chg_s1, (p1.prim_fid <> p0.prim_fid OR p2.prim_fid <> p0.prim_fid) chg_either
  FROM sales s JOIN pan p0 ON p0.bkey = s.bkey AND p0.y = s.s - 1 JOIN pan p1 ON p1.bkey = s.bkey AND p1.y = s.s JOIN pan p2 ON p2.bkey = s.bkey AND p2.y = s.s + 1
  WHERE p0.prim_fid IS NOT NULL AND p1.observed AND p2.observed;
COPY (
  SELECT s, m, count(*) sales, round(100*avg(chg_s::INT),1) new_primary_in_file_s, round(100*avg(chg_s1::INT),1) new_primary_by_file_s1
  FROM cap GROUP BY s, m ORDER BY s, m
) TO '/home/jr2728/ptax_explore/hh_strat/out/spike_sale_capture_long.csv' (HEADER);
COPY (
  SELECT m sale_month, round(100*avg(chg_s::INT) FILTER (WHERE s = 2013),1) f2013, round(100*avg(chg_s::INT) FILTER (WHERE s = 2014),1) f2014,
         round(100*avg(chg_s::INT) FILTER (WHERE s = 2015),1) f2015, round(100*avg(chg_s::INT) FILTER (WHERE s = 2016),1) f2016,
         round(100*avg(chg_s::INT) FILTER (WHERE s = 2017),1) f2017, round(100*avg(chg_s::INT) FILTER (WHERE s = 2018),1) f2018,
         round(100*avg(chg_s::INT) FILTER (WHERE s = 2019),1) f2019, round(100*avg(chg_s::INT) FILTER (WHERE s = 2020),1) f2020
  FROM cap GROUP BY m ORDER BY m
) TO '/home/jr2728/ptax_explore/hh_strat/out/spike_sale_capture.csv' (HEADER);
COPY (
  SELECT s sale_year, count(*) sales, round(100*avg(chg_s::INT),1) new_primary_in_file_s, round(100*avg(chg_s1::INT),1) new_primary_by_file_s1, round(100*avg(chg_either::INT),1) either
  FROM cap GROUP BY s ORDER BY s
) TO '/home/jr2728/ptax_explore/hh_strat/out/spike_sale_capture_year.csv' (HEADER);
SELECT * FROM read_csv('/home/jr2728/ptax_explore/hh_strat/out/spike_recency.csv');
SELECT * FROM read_csv('/home/jr2728/ptax_explore/hh_strat/out/spike_lor_increments.csv');
SELECT * FROM read_csv('/home/jr2728/ptax_explore/hh_strat/out/spike_sale_capture.csv');
SELECT * FROM read_csv('/home/jr2728/ptax_explore/hh_strat/out/spike_sale_capture_year.csv');
