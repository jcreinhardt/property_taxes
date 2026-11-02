-- Altos Research national rental listings (weekly files 2011-01 .. 2025-12, ~0.9-1.4m unit listings per week) ->
-- (1) unit x month table (median asking rent over the weeks a unit is listed in the month), (2) tract assignment of every
-- distinct listing coordinate (2019 cartographic tracts = 2010 tract codes, DuckDB spatial, ST_Within), (3) ZIP x month
-- and tract x quarter rent cells by dwelling type and bedrooms. Runs on Bouchet as a day batch job:
--   cd ~/ptax_explore && sbatch $(~/ptax_explore/jobsize.sh 08:00:00) --job-name=altos01 --time=08:00:00 run_sql.sbatch altos_01_unit_month.sql
-- Inputs: /nfs/roberts/pi/pi_csl53/corelogic/raw_data/altos/us_national_rentals_classic_YYYY-MM-DD.csv.gz (read only);
-- ~/ptax_explore/geo/cb_2019_us_tract_500k.shp (copied from the laptop tigris cache). Outputs in ~/ptax_explore/altos/:
--   altos_file_stats.parquet   rows, units, price / coordinate coverage per weekly file
--   altos_unit_month.parquet   unit_id x year x month (kept on Bouchet; ~200m rows)
--   altos_zip_month.parquet    zip x year x month x type x beds: n units, median rent, mean log rent, mean sqft
--   altos_tract_quarter.parquet tract x year x quarter x type x beds: the same
--   altos_tract_year.parquet   tract x year x type x beds: the same, plus repeat-listing changes
-- The weekly files are scanned twice (file stats, then the unit x month aggregate) rather than materialised (~750m rows).
-- Rent filter: 200 <= price <= 20000 $/month; coordinates in the lower 48 + DC bounding box.
LOAD spatial;
SET preserve_insertion_order = false;
CREATE OR REPLACE VIEW memory.main.raw AS
SELECT regexp_extract(filename, '(\d{4}-\d{2}-\d{2})', 1)::DATE fd, try_cast(date AS DATE) d,
       unit_id, property_id, left(zip, 5) zip, try_cast(price AS DOUBLE) price, lower(type) type,
       try_cast(beds AS DOUBLE) beds, try_cast(baths AS DOUBLE) baths, try_cast(floor_size AS DOUBLE) sqft,
       try_cast(built_in AS INTEGER) built, try_cast(geo_lat AS DOUBLE) lat, try_cast(geo_long AS DOUBLE) lon, state
FROM read_csv('/nfs/roberts/pi/pi_csl53/corelogic/raw_data/altos/us_national_rentals_classic_*.csv.gz',
              header = true, all_varchar = true, union_by_name = true, filename = true);

COPY (SELECT fd, count(*) n, count(DISTINCT unit_id) units, count(DISTINCT property_id) props,
             avg((price BETWEEN 200 AND 20000)::INT) sh_price_ok, avg((lat IS NOT NULL AND lon IS NOT NULL)::INT) sh_coord,
             avg((zip ~ '^\d{5}$')::INT) sh_zip5, avg((d = fd)::INT) sh_date_eq_file,
             avg((type = 'apartment')::INT) sh_apt, avg((type = 'house')::INT) sh_house, quantile_cont(price, .5) p50
      FROM memory.main.raw GROUP BY fd ORDER BY fd)
TO '/home/jr2728/ptax_explore/altos/altos_file_stats.parquet' (FORMAT PARQUET);

-- unit x month (second pass over the files): median rent over the weeks the unit is listed in that month; attributes = most common (any) value
CREATE OR REPLACE TABLE memory.main.um AS
SELECT unit_id, year(coalesce(d, fd)) y, month(coalesce(d, fd)) m, count(*) nwk, median(price) price,
       any_value(property_id) property_id, any_value(zip) zip, any_value(state) state,
       any_value(CASE WHEN type IN ('apartment', 'house') THEN type ELSE 'other' END) type,
       any_value(beds) beds, any_value(baths) baths, any_value(sqft) sqft, any_value(built) built,
       round(any_value(lat), 5) lat, round(any_value(lon), 5) lon
FROM memory.main.raw
WHERE price BETWEEN 200 AND 20000 AND unit_id IS NOT NULL
GROUP BY unit_id, year(coalesce(d, fd)), month(coalesce(d, fd));
SELECT count(*) unit_months, count(DISTINCT unit_id) units FROM memory.main.um;

-- tract of every distinct coordinate (lower 48 + DC box)
CREATE OR REPLACE TABLE memory.main.pts AS
SELECT DISTINCT lat, lon FROM memory.main.um WHERE lat BETWEEN 24 AND 50 AND lon BETWEEN -125 AND -66;
SELECT count(*) distinct_points FROM memory.main.pts;
CREATE OR REPLACE TABLE memory.main.tr AS
SELECT GEOID tract, geom FROM ST_Read('/home/jr2728/ptax_explore/geo/cb_2019_us_tract_500k.shp');
CREATE OR REPLACE TABLE memory.main.pt_tract AS
SELECT p.lat, p.lon, t.tract
FROM memory.main.pts p JOIN memory.main.tr t ON ST_Within(ST_Point(p.lon, p.lat), t.geom);
SELECT count(*) points_in_tract FROM memory.main.pt_tract;
DROP TABLE memory.main.tr;

CREATE OR REPLACE TABLE memory.main.um2 AS
SELECT u.*, t.tract FROM memory.main.um u LEFT JOIN memory.main.pt_tract t USING (lat, lon);
DROP TABLE memory.main.um;
COPY memory.main.um2 TO '/home/jr2728/ptax_explore/altos/altos_unit_month.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

-- cells: beds capped at 4 (NULL -> -1). NB: run_sql.sbatch USEs the read-only catalog, so the macro must live in memory.main
-- (the first run failed here; the cell exports were then made by 01b_altos_cells.sql from altos_unit_month.parquet).
CREATE OR REPLACE MACRO memory.main.bcat(b) AS CASE WHEN b IS NULL THEN -1 WHEN b >= 4 THEN 4 ELSE b::INT END;
COPY (SELECT zip, y, m, type, memory.main.bcat(beds) beds, count(*) n, median(price) p50, avg(ln(price)) mlp, avg(sqft) sqft,
             avg(ln(price / sqft)) FILTER (sqft BETWEEN 200 AND 10000) mlpsf
      FROM memory.main.um2 WHERE zip ~ '^\d{5}$' GROUP BY ALL)
TO '/home/jr2728/ptax_explore/altos/altos_zip_month.parquet' (FORMAT PARQUET);
COPY (SELECT tract, y, quarter(make_date(y, m, 1)) q, type, memory.main.bcat(beds) beds, count(*) n, count(DISTINCT unit_id) units,
             median(price) p50, avg(ln(price)) mlp, avg(sqft) sqft, avg(ln(price / sqft)) FILTER (sqft BETWEEN 200 AND 10000) mlpsf
      FROM memory.main.um2 WHERE tract IS NOT NULL GROUP BY ALL)
TO '/home/jr2728/ptax_explore/altos/altos_tract_quarter.parquet' (FORMAT PARQUET);
-- tract x year: one observation per unit-year (median of its months), so long-listed units do not dominate
COPY (WITH uy AS (SELECT unit_id, tract, y, any_value(type) type, memory.main.bcat(any_value(beds)) beds, median(price) price, any_value(sqft) sqft
                  FROM memory.main.um2 WHERE tract IS NOT NULL GROUP BY unit_id, tract, y)
      SELECT tract, y, type, beds, count(*) n, median(price) p50, avg(ln(price)) mlp, avg(sqft) sqft,
             avg(ln(price / sqft)) FILTER (sqft BETWEEN 200 AND 10000) mlpsf
      FROM uy GROUP BY ALL)
TO '/home/jr2728/ptax_explore/altos/altos_tract_year.parquet' (FORMAT PARQUET);
-- repeat-listing changes: same unit listed in two different years (log rent change, first to next listing year)
COPY (WITH uy AS (SELECT unit_id, any_value(tract) tract, any_value(zip) zip, y, median(price) price, any_value(type) type
                  FROM memory.main.um2 GROUP BY unit_id, y),
           pr AS (SELECT *, lag(y) OVER w y0, lag(price) OVER w price0 FROM uy WINDOW w AS (PARTITION BY unit_id ORDER BY y))
      SELECT unit_id, tract, zip, type, y0, y, ln(price) - ln(price0) dlp FROM pr WHERE y0 IS NOT NULL)
TO '/home/jr2728/ptax_explore/altos/altos_repeat_unit.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
SELECT 'done' AS status;
