#!/bin/bash
#SBATCH -J tiger16
#SBATCH -p default_queue
#SBATCH --cpus-per-task=2
#SBATCH --mem=4G
#SBATCH --time=02:00:00
#SBATCH -o /home/jr2728/ptax_explore/logs/tiger16_%j.out
# Full-resolution TIGER/Line 2016 boundaries: national state + county; per state place, cousub, unsd, elsd, scsd.
set -u
D=/home/jr2728/ptax_explore/tiger16; mkdir -p $D/zip $D/shp; cd $D/zip
B=https://www2.census.gov/geo/tiger/TIGER2016
get() { [ -s "$2" ] || curl -sf --retry 3 -o "$2" "$1" || { rm -f "$2"; echo "missing $1"; }; }
get $B/STATE/tl_2016_us_state.zip tl_2016_us_state.zip
get $B/COUNTY/tl_2016_us_county.zip tl_2016_us_county.zip
for s in 01 02 04 05 06 08 09 10 11 12 13 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 44 45 46 47 48 49 50 51 53 54 55 56; do
  for L in PLACE COUSUB UNSD ELSD SCSD; do l=$(echo $L | tr A-Z a-z); get $B/$L/tl_2016_${s}_${l}.zip tl_2016_${s}_${l}.zip; done
done
for z in *.zip; do unzip -qo $z -d $D/shp/${z%.zip}; done
ls zip | sed -E 's/tl_2016_(..)_(.*)\.zip/\2/' | sort | uniq -c; du -sh $D
