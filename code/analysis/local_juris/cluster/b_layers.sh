#!/bin/bash
# Assign every 2016 parcel point to TIGER/Line 2016 layers with DuckDB spatial (SOM). Usage: b_layers.sh <dir with parcels16.parquet>
# 1. combine per-state shapefiles into one parquet per layer (GEOID, FUNCSTAT, NAME, geometry)
# 2. point-in-polygon joins; output layers16.parquet: clip, g_state, g_county, g_cousub, cousub_fs, g_place, place_fs, g_unsd, g_elsd, g_scsd
set -eu
DIR=$1; T=/home/jr2728/ptax_explore/tiger16/shp; DB=~/.local/bin/duckdb
SQL=$DIR/b_layers_gen.sql
{
echo "SET temp_directory='/home/jr2728/ptax_explore/tmp'; SET preserve_insertion_order=false; SET threads=32; SET memory_limit='130GB'; LOAD spatial;"; echo ".timer on"
for L in state county place cousub unsd elsd scsd; do
  files=$(ls $T/tl_2016_*_${L}/tl_2016_*_${L}.shp 2>/dev/null)
  echo "CREATE TABLE $L AS"
  first=1
  for f in $files; do
    [ $first -eq 1 ] || echo "UNION ALL BY NAME"; first=0
    case $L in state) id=STATEFP;; *) id=GEOID;; esac
    fs=$( [ $L = place ] || [ $L = cousub ] && echo "FUNCSTAT" || echo "NULL::VARCHAR")
    echo "SELECT $id::VARCHAR geoid, $fs fs, NAME::VARCHAR AS nm, geom FROM ST_Read('$f')"
  done
  echo ";"
done
cat <<'Q'
CREATE TABLE pts AS SELECT clip, ST_Point(lon, lat) geom FROM read_parquet('__DIR__/parcels16.parquet') WHERE lat IS NOT NULL AND lon IS NOT NULL;
CREATE TABLE j_state  AS SELECT p.clip, any_value(l.geoid) g FROM pts p JOIN state  l ON ST_Within(p.geom, l.geom) GROUP BY p.clip;
CREATE TABLE j_county AS SELECT p.clip, any_value(l.geoid) g FROM pts p JOIN county l ON ST_Within(p.geom, l.geom) GROUP BY p.clip;
CREATE TABLE j_place  AS SELECT p.clip, any_value(l.geoid) g, any_value(l.fs) fs FROM pts p JOIN place  l ON ST_Within(p.geom, l.geom) GROUP BY p.clip;
CREATE TABLE j_cousub AS SELECT p.clip, any_value(l.geoid) g, any_value(l.fs) fs FROM pts p JOIN cousub l ON ST_Within(p.geom, l.geom) GROUP BY p.clip;
CREATE TABLE j_unsd   AS SELECT p.clip, any_value(l.geoid) g FROM pts p JOIN unsd l ON ST_Within(p.geom, l.geom) GROUP BY p.clip;
CREATE TABLE j_elsd   AS SELECT p.clip, any_value(l.geoid) g FROM pts p JOIN elsd l ON ST_Within(p.geom, l.geom) GROUP BY p.clip;
CREATE TABLE j_scsd   AS SELECT p.clip, any_value(l.geoid) g FROM pts p JOIN scsd l ON ST_Within(p.geom, l.geom) GROUP BY p.clip;
COPY (SELECT p.clip, s.g g_state, c.g g_county, cs.g g_cousub, cs.fs cousub_fs, pl.g g_place, pl.fs place_fs, u.g g_unsd, e.g g_elsd, sc.g g_scsd
      FROM pts p LEFT JOIN j_state s USING (clip) LEFT JOIN j_county c USING (clip) LEFT JOIN j_cousub cs USING (clip)
      LEFT JOIN j_place pl USING (clip) LEFT JOIN j_unsd u USING (clip) LEFT JOIN j_elsd e USING (clip) LEFT JOIN j_scsd sc USING (clip))
  TO '__DIR__/layers16.parquet' (FORMAT parquet);
COPY (SELECT 'place' layer, geoid, fs, nm FROM place UNION ALL SELECT 'cousub', geoid, fs, nm FROM cousub
      UNION ALL SELECT 'unsd', geoid, NULL, nm FROM unsd UNION ALL SELECT 'elsd', geoid, NULL, nm FROM elsd UNION ALL SELECT 'scsd', geoid, NULL, nm FROM scsd)
  TO '__DIR__/layer_names16.parquet' (FORMAT parquet);
SELECT count(*) pts, count(g_state) st, count(g_county) cty, count(g_cousub) cousub, count(g_place) place, count(g_unsd) unsd, count(g_elsd) elsd, count(g_scsd) scsd FROM '__DIR__/layers16.parquet';
Q
} | sed "s#__DIR__#$DIR#g" > $SQL
$DB $DIR/b_layers.duckdb -c ".read $SQL"
rm -f $DIR/b_layers.duckdb $DIR/b_layers.duckdb.wal
