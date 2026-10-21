-- Full roll, step D (SOM): share of parcels on their cell-group line (|bill - (a + r*AV)| <= 1% of bill), then the
-- parcel-level table for the decomposition aggregated to unique (cell, hs, layers) with counts. __DIR__ = data dir.
SET temp_directory='/home/jr2728/ptax_explore/tmp'; SET preserve_insertion_order=false;
.timer on
CREATE TEMP TABLE f AS SELECT * FROM '__DIR__/cell_fits16.parquet';
CREATE TEMP TABLE ev AS
SELECT p.cell, p.hs, f.a, f.slope, count(*) n, avg((abs(p.bill - (f.a + f.slope * p.av)) <= 0.01 * p.bill)::INT) on1,
       median(p.av / nullif(p.mv, 0)) avmv
FROM '__DIR__/parcels16.parquet' p JOIN f ON f.cell = p.cell AND f.hs = p.hs
WHERE p.bill > 0 AND p.av > 0 GROUP BY ALL;
CREATE TEMP TABLE evp AS
SELECT p.cell, f.a, f.slope, count(*) n, avg((abs(p.bill - (f.a + f.slope * p.av)) <= 0.01 * p.bill)::INT) on1
FROM '__DIR__/parcels16.parquet' p JOIN f ON f.cell = p.cell AND f.hs IS NULL WHERE p.bill > 0 AND p.av > 0 GROUP BY ALL;
COPY (SELECT 'group' kind, * FROM ev UNION ALL BY NAME SELECT 'pooled' kind, * FROM evp) TO '__DIR__/cell_eval16.parquet' (FORMAT parquet);
COPY (SELECT p.st, p.fips, p.cell, p.hs, l.g_state, l.g_county, l.g_cousub, l.cousub_fs, l.g_place, l.place_fs, l.g_unsd, l.g_elsd, l.g_scsd,
             (p.lat IS NOT NULL) has_xy, count(*) n
      FROM '__DIR__/parcels16.parquet' p LEFT JOIN '__DIR__/layers16.parquet' l USING (clip) GROUP BY ALL)
  TO '__DIR__/cell_layers16.parquet' (FORMAT parquet);
SELECT (SELECT count(*) FROM '__DIR__/cell_eval16.parquet') eval_rows, (SELECT count(*) FROM '__DIR__/cell_layers16.parquet') layer_rows;
