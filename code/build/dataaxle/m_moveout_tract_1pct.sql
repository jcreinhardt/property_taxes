-- Address move-outs by census tract and year from the 1% building sample (Bouchet; reads ~/ptax_explore/samples only).
-- Address = building key (bkey); move-out t-1 -> t = primary family_id changes, counted where both files have exactly one
-- primary family at the address (clean measure of dataaxle.md 4.5c). Tract (2010) from the tax roll: fips || left(census_id, 6)
-- of the latest tax record of the bkey, residential (pic 10/11/21/22). Year = destination file t (2013 = 2012 -> 13 ... 2021).
-- Output: ~/ptax_explore/border/moveout_tract_1pct.csv (tract, y, n addresses, n moves). Rates must be annualised by file
-- cut-off months downstream (dataaxle.md 4.5c).
USE memory;
SET preserve_insertion_order=false;
CREATE OR REPLACE TABLE tx AS
SELECT bkey, arg_max(struct_pack(pic := property_indicator_code, tract := lpad(fips_code::VARCHAR, 5, '0') || left(census_id::VARCHAR, 6)), tax_year) a
FROM '/home/jr2728/ptax_explore/samples/tax_bld1pct.parquet' WHERE tax_year BETWEEN 2012 AND 2021 AND length(census_id::VARCHAR) >= 6 GROUP BY bkey;
CREATE OR REPLACE TABLE res AS SELECT bkey, a.tract tract FROM tx WHERE a.pic IN (10, 11, 21, 22);
CREATE OR REPLACE TABLE pf AS
SELECT bkey, record_year::INT y, min(family_id) FILTER (WHERE primary_family_ind) fid, count(DISTINCT family_id) FILTER (WHERE primary_family_ind) np
FROM '/home/jr2728/ptax_explore/samples/da_bld1pct.parquet' WHERE record_year BETWEEN 2012 AND 2021 AND bkey IN (SELECT bkey FROM res) GROUP BY ALL;
COPY (SELECT r.tract, b.y, count(*) n, sum((a.fid <> b.fid)::INT) moves
      FROM pf a JOIN pf b ON b.bkey = a.bkey AND b.y = a.y + 1 JOIN res r ON r.bkey = a.bkey
      WHERE a.np = 1 AND b.np = 1 GROUP BY ALL) TO '/home/jr2728/ptax_explore/border/moveout_tract_1pct.csv' (HEADER);
SELECT y, sum(n) n, round(100 * sum(moves) / sum(n), 2) pct FROM read_csv('/home/jr2728/ptax_explore/border/moveout_tract_1pct.csv') GROUP BY y ORDER BY y;
