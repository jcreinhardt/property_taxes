-- Family moves with origin and destination coordinates (Bouchet; reads ~/ptax_explore/samples/da_fam1pct.parquet = all
-- records of 1% of family_ids). One location per family-year (primary record preferred, then the latest recency_date).
-- A move t-1 -> t: same family_id, building key changes, both geocoded, and length_of_residence at t <= 2 (resets;
-- dataaxle.md 4.5c). No names or ids exported. Output ~/ptax_explore/border/family_moves_1pct.parquet
-- (y = destination file, origin/destination lat, lon, state FIPS).
USE memory;
SET preserve_insertion_order=false;
CREATE OR REPLACE TABLE fy AS
SELECT family_id, record_year::INT y,
       arg_max(struct_pack(bkey := bkey, lat := nullif(ge_latitude_2010, 0), lon := nullif(ge_longitude_2010, 0), st := ge_census_state_2010,
                           lor := length_of_residence), (coalesce(primary_family_ind, false)::INT, recency_date)) a
FROM '/home/jr2728/ptax_explore/samples/da_fam1pct.parquet' WHERE record_year BETWEEN 2012 AND 2021 GROUP BY 1, 2;
COPY (SELECT b.y, a.a.lat olat, a.a.lon olon, a.a.st ost, b.a.lat dlat, b.a.lon dlon, b.a.st dst
      FROM fy a JOIN fy b ON b.family_id = a.family_id AND b.y = a.y + 1
      WHERE a.a.bkey IS NOT NULL AND b.a.bkey IS NOT NULL AND a.a.bkey <> b.a.bkey AND coalesce(b.a.lor, 99) <= 2
        AND a.a.lat IS NOT NULL AND b.a.lat IS NOT NULL) TO '/home/jr2728/ptax_explore/border/family_moves_1pct.parquet' (FORMAT parquet);
SELECT y, count(*) moves, round(100 * avg((ost <> dst)::INT), 1) pct_interstate FROM '/home/jr2728/ptax_explore/border/family_moves_1pct.parquet' GROUP BY y ORDER BY y;
SELECT (SELECT count(DISTINCT family_id) FROM fy) families;
