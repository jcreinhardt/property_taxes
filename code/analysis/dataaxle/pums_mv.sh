#!/bin/bash
# ACS 1-yr PUMS housing files: weighted occupied units by MV (when moved in) x TEN, per year. Raw files deleted after use.
S=$1; OUT=$2
for y in 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017 2018 2019 2021 2022 2023; do
  [ -f $S/mv_$y.csv ] && continue
  d=$S/raw_$y; mkdir -p $d
  curl -s -f -o $d/h.zip https://www2.census.gov/programs-surveys/acs/data/pums/$y/1-Year/csv_hus.zip || { echo "download failed $y"; continue; }
  (cd $d && unzip -q -o h.zip '*.csv')
  T=$(head -1 $(ls $d/*.csv | head -1) | tr ',' '\n' | grep -x -E 'TYPE|TYPEHUGQ' | head -1)
  duckdb -c "COPY (SELECT $y AS y, MV::INT mv, CASE WHEN TEN IN ('1','2') THEN 'own' ELSE 'rent' END ten, sum(WGTP::INT) n
             FROM read_csv('$d/*.csv', all_varchar = true, union_by_name = true)
             WHERE $T = '1' AND MV IS NOT NULL AND MV <> '' GROUP BY ALL) TO '$S/mv_$y.csv' (HEADER)" && echo "done $y"
  rm -rf $d
done
duckdb -c "COPY (SELECT * FROM read_csv('$S/mv_*.csv') ORDER BY ALL) TO '$OUT' (HEADER)"
