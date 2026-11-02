-- Cell exports from altos_unit_month.parquet (written by 01_altos_unit_month.sql, whose cell step failed because a MACRO cannot
-- be created in the read-only attached catalog): ZIP x month, tract x quarter and tract x year cells by type x bedrooms.
--   cd ~/ptax_explore && sbatch $(~/ptax_explore/jobsize.sh 02:00:00) --job-name=altos01b --time=02:00:00 run_sql.sbatch altos_01b_cells.sql
CREATE OR REPLACE VIEW memory.main.um2 AS
SELECT *, CASE WHEN beds IS NULL THEN -1 WHEN beds >= 4 THEN 4 ELSE beds::INT END bc FROM '/home/jr2728/ptax_explore/altos/altos_unit_month.parquet';
SELECT count(*) unit_months, count(DISTINCT unit_id) units, count(*) FILTER (tract IS NOT NULL) with_tract FROM memory.main.um2;
COPY (SELECT zip, y, m, type, bc beds, count(*) n, median(price) p50, avg(ln(price)) mlp, avg(sqft) sqft,
             avg(ln(price / sqft)) FILTER (sqft BETWEEN 200 AND 10000) mlpsf
      FROM memory.main.um2 WHERE zip ~ '^\d{5}$' GROUP BY ALL)
TO '/home/jr2728/ptax_explore/altos/altos_zip_month.parquet' (FORMAT PARQUET);
COPY (SELECT tract, y, quarter(make_date(y, m, 1)) q, type, bc beds, count(*) n, count(DISTINCT unit_id) units,
             median(price) p50, avg(ln(price)) mlp, avg(sqft) sqft, avg(ln(price / sqft)) FILTER (sqft BETWEEN 200 AND 10000) mlpsf
      FROM memory.main.um2 WHERE tract IS NOT NULL GROUP BY ALL)
TO '/home/jr2728/ptax_explore/altos/altos_tract_quarter.parquet' (FORMAT PARQUET);
COPY (WITH uy AS (SELECT unit_id, tract, y, any_value(type) type, any_value(bc) beds, median(price) price, any_value(sqft) sqft
                  FROM memory.main.um2 WHERE tract IS NOT NULL GROUP BY unit_id, tract, y)
      SELECT tract, y, type, beds, count(*) n, median(price) p50, avg(ln(price)) mlp, avg(sqft) sqft,
             avg(ln(price / sqft)) FILTER (sqft BETWEEN 200 AND 10000) mlpsf
      FROM uy GROUP BY ALL)
TO '/home/jr2728/ptax_explore/altos/altos_tract_year.parquet' (FORMAT PARQUET);
SELECT 'done' AS status;
