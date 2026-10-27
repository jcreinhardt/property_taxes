SET preserve_insertion_order=false;
COPY (WITH a AS (
  SELECT primary_family_ind prim,
         CASE WHEN address_type='S' AND coalesce(trim(house_num),'')<>'' AND coalesce(trim(street_name),'')<>''
              THEN hash(concat_ws('|', zip, upper(house_num), upper(street_pre_dir), upper(street_name), upper(street_suffix), upper(street_post_dir))) END bh,
         nullif(upper(trim(unit_num)), '') u
  FROM cl.dataaxle_annual  WHERE record_year=2006),
b AS (SELECT bh, count(DISTINCT u) nu, bool_or(u IS NULL) hasnull,
             count(*) FILTER (WHERE u IS NULL) null_recs, count(*) FILTER (WHERE u IS NULL AND prim) null_prim
      FROM a WHERE bh IS NOT NULL GROUP BY bh)
SELECT 2006 AS year,
  (SELECT count(*) FROM a) fam_records,
  (SELECT count(*) FILTER (WHERE prim) FROM a) primary_fams,
  (SELECT count(*) FROM a WHERE bh IS NULL) nobldg_records,
  (SELECT count(*) FILTER (WHERE prim) FROM a WHERE bh IS NULL) nobldg_primary,
  count(*) buildings,
  count(*) FILTER (WHERE hasnull AND nu>=1) bldg_null_plus_1unit,
  count(*) FILTER (WHERE hasnull AND nu>=2) bldg_null_plus_2units,
  sum(nu + hasnull::INT) s1_bldg_unit_combos,
  sum(nu + (hasnull AND nu=0)::INT) s2a_combos,
  sum(nu + (hasnull AND nu<2)::INT) s2b_combos,
  sum(null_recs) FILTER (WHERE nu>=1) s2a_dropped_records,
  sum(null_prim) FILTER (WHERE nu>=1) s2a_dropped_primary,
  sum(null_recs) FILTER (WHERE nu>=2) s2b_dropped_records,
  sum(null_prim) FILTER (WHERE nu>=2) s2b_dropped_primary
FROM b) TO '/home/jr2728/ptax_explore/hh_strat/out/hh_2006.csv' (HEADER);
COPY (WITH a AS (
  SELECT primary_family_ind prim,
         CASE WHEN address_type='S' AND coalesce(trim(house_num),'')<>'' AND coalesce(trim(street_name),'')<>''
              THEN hash(concat_ws('|', zip, upper(house_num), upper(street_pre_dir), upper(street_name), upper(street_suffix), upper(street_post_dir))) END bh,
         nullif(upper(trim(unit_num)), '') u
  FROM cl.dataaxle_annual  WHERE record_year=2007),
b AS (SELECT bh, count(DISTINCT u) nu, bool_or(u IS NULL) hasnull,
             count(*) FILTER (WHERE u IS NULL) null_recs, count(*) FILTER (WHERE u IS NULL AND prim) null_prim
      FROM a WHERE bh IS NOT NULL GROUP BY bh)
SELECT 2007 AS year,
  (SELECT count(*) FROM a) fam_records,
  (SELECT count(*) FILTER (WHERE prim) FROM a) primary_fams,
  (SELECT count(*) FROM a WHERE bh IS NULL) nobldg_records,
  (SELECT count(*) FILTER (WHERE prim) FROM a WHERE bh IS NULL) nobldg_primary,
  count(*) buildings,
  count(*) FILTER (WHERE hasnull AND nu>=1) bldg_null_plus_1unit,
  count(*) FILTER (WHERE hasnull AND nu>=2) bldg_null_plus_2units,
  sum(nu + hasnull::INT) s1_bldg_unit_combos,
  sum(nu + (hasnull AND nu=0)::INT) s2a_combos,
  sum(nu + (hasnull AND nu<2)::INT) s2b_combos,
  sum(null_recs) FILTER (WHERE nu>=1) s2a_dropped_records,
  sum(null_prim) FILTER (WHERE nu>=1) s2a_dropped_primary,
  sum(null_recs) FILTER (WHERE nu>=2) s2b_dropped_records,
  sum(null_prim) FILTER (WHERE nu>=2) s2b_dropped_primary
FROM b) TO '/home/jr2728/ptax_explore/hh_strat/out/hh_2007.csv' (HEADER);
COPY (WITH a AS (
  SELECT primary_family_ind prim,
         CASE WHEN address_type='S' AND coalesce(trim(house_num),'')<>'' AND coalesce(trim(street_name),'')<>''
              THEN hash(concat_ws('|', zip, upper(house_num), upper(street_pre_dir), upper(street_name), upper(street_suffix), upper(street_post_dir))) END bh,
         nullif(upper(trim(unit_num)), '') u
  FROM cl.dataaxle_annual  WHERE record_year=2008),
b AS (SELECT bh, count(DISTINCT u) nu, bool_or(u IS NULL) hasnull,
             count(*) FILTER (WHERE u IS NULL) null_recs, count(*) FILTER (WHERE u IS NULL AND prim) null_prim
      FROM a WHERE bh IS NOT NULL GROUP BY bh)
SELECT 2008 AS year,
  (SELECT count(*) FROM a) fam_records,
  (SELECT count(*) FILTER (WHERE prim) FROM a) primary_fams,
  (SELECT count(*) FROM a WHERE bh IS NULL) nobldg_records,
  (SELECT count(*) FILTER (WHERE prim) FROM a WHERE bh IS NULL) nobldg_primary,
  count(*) buildings,
  count(*) FILTER (WHERE hasnull AND nu>=1) bldg_null_plus_1unit,
  count(*) FILTER (WHERE hasnull AND nu>=2) bldg_null_plus_2units,
  sum(nu + hasnull::INT) s1_bldg_unit_combos,
  sum(nu + (hasnull AND nu=0)::INT) s2a_combos,
  sum(nu + (hasnull AND nu<2)::INT) s2b_combos,
  sum(null_recs) FILTER (WHERE nu>=1) s2a_dropped_records,
  sum(null_prim) FILTER (WHERE nu>=1) s2a_dropped_primary,
  sum(null_recs) FILTER (WHERE nu>=2) s2b_dropped_records,
  sum(null_prim) FILTER (WHERE nu>=2) s2b_dropped_primary
FROM b) TO '/home/jr2728/ptax_explore/hh_strat/out/hh_2008.csv' (HEADER);
COPY (WITH a AS (
  SELECT primary_family_ind prim,
         CASE WHEN address_type='S' AND coalesce(trim(house_num),'')<>'' AND coalesce(trim(street_name),'')<>''
              THEN hash(concat_ws('|', zip, upper(house_num), upper(street_pre_dir), upper(street_name), upper(street_suffix), upper(street_post_dir))) END bh,
         nullif(upper(trim(unit_num)), '') u
  FROM cl.dataaxle_annual  WHERE record_year=2009),
b AS (SELECT bh, count(DISTINCT u) nu, bool_or(u IS NULL) hasnull,
             count(*) FILTER (WHERE u IS NULL) null_recs, count(*) FILTER (WHERE u IS NULL AND prim) null_prim
      FROM a WHERE bh IS NOT NULL GROUP BY bh)
SELECT 2009 AS year,
  (SELECT count(*) FROM a) fam_records,
  (SELECT count(*) FILTER (WHERE prim) FROM a) primary_fams,
  (SELECT count(*) FROM a WHERE bh IS NULL) nobldg_records,
  (SELECT count(*) FILTER (WHERE prim) FROM a WHERE bh IS NULL) nobldg_primary,
  count(*) buildings,
  count(*) FILTER (WHERE hasnull AND nu>=1) bldg_null_plus_1unit,
  count(*) FILTER (WHERE hasnull AND nu>=2) bldg_null_plus_2units,
  sum(nu + hasnull::INT) s1_bldg_unit_combos,
  sum(nu + (hasnull AND nu=0)::INT) s2a_combos,
  sum(nu + (hasnull AND nu<2)::INT) s2b_combos,
  sum(null_recs) FILTER (WHERE nu>=1) s2a_dropped_records,
  sum(null_prim) FILTER (WHERE nu>=1) s2a_dropped_primary,
  sum(null_recs) FILTER (WHERE nu>=2) s2b_dropped_records,
  sum(null_prim) FILTER (WHERE nu>=2) s2b_dropped_primary
FROM b) TO '/home/jr2728/ptax_explore/hh_strat/out/hh_2009.csv' (HEADER);
COPY (WITH a AS (
  SELECT primary_family_ind prim,
         CASE WHEN address_type='S' AND coalesce(trim(house_num),'')<>'' AND coalesce(trim(street_name),'')<>''
              THEN hash(concat_ws('|', zip, upper(house_num), upper(street_pre_dir), upper(street_name), upper(street_suffix), upper(street_post_dir))) END bh,
         nullif(upper(trim(unit_num)), '') u
  FROM cl.dataaxle_annual  WHERE record_year=2010),
b AS (SELECT bh, count(DISTINCT u) nu, bool_or(u IS NULL) hasnull,
             count(*) FILTER (WHERE u IS NULL) null_recs, count(*) FILTER (WHERE u IS NULL AND prim) null_prim
      FROM a WHERE bh IS NOT NULL GROUP BY bh)
SELECT 2010 AS year,
  (SELECT count(*) FROM a) fam_records,
  (SELECT count(*) FILTER (WHERE prim) FROM a) primary_fams,
  (SELECT count(*) FROM a WHERE bh IS NULL) nobldg_records,
  (SELECT count(*) FILTER (WHERE prim) FROM a WHERE bh IS NULL) nobldg_primary,
  count(*) buildings,
  count(*) FILTER (WHERE hasnull AND nu>=1) bldg_null_plus_1unit,
  count(*) FILTER (WHERE hasnull AND nu>=2) bldg_null_plus_2units,
  sum(nu + hasnull::INT) s1_bldg_unit_combos,
  sum(nu + (hasnull AND nu=0)::INT) s2a_combos,
  sum(nu + (hasnull AND nu<2)::INT) s2b_combos,
  sum(null_recs) FILTER (WHERE nu>=1) s2a_dropped_records,
  sum(null_prim) FILTER (WHERE nu>=1) s2a_dropped_primary,
  sum(null_recs) FILTER (WHERE nu>=2) s2b_dropped_records,
  sum(null_prim) FILTER (WHERE nu>=2) s2b_dropped_primary
FROM b) TO '/home/jr2728/ptax_explore/hh_strat/out/hh_2010.csv' (HEADER);
COPY (WITH a AS (
  SELECT primary_family_ind prim,
         CASE WHEN address_type='S' AND coalesce(trim(house_num),'')<>'' AND coalesce(trim(street_name),'')<>''
              THEN hash(concat_ws('|', zip, upper(house_num), upper(street_pre_dir), upper(street_name), upper(street_suffix), upper(street_post_dir))) END bh,
         nullif(upper(trim(unit_num)), '') u
  FROM cl.dataaxle_annual  WHERE record_year=2011),
b AS (SELECT bh, count(DISTINCT u) nu, bool_or(u IS NULL) hasnull,
             count(*) FILTER (WHERE u IS NULL) null_recs, count(*) FILTER (WHERE u IS NULL AND prim) null_prim
      FROM a WHERE bh IS NOT NULL GROUP BY bh)
SELECT 2011 AS year,
  (SELECT count(*) FROM a) fam_records,
  (SELECT count(*) FILTER (WHERE prim) FROM a) primary_fams,
  (SELECT count(*) FROM a WHERE bh IS NULL) nobldg_records,
  (SELECT count(*) FILTER (WHERE prim) FROM a WHERE bh IS NULL) nobldg_primary,
  count(*) buildings,
  count(*) FILTER (WHERE hasnull AND nu>=1) bldg_null_plus_1unit,
  count(*) FILTER (WHERE hasnull AND nu>=2) bldg_null_plus_2units,
  sum(nu + hasnull::INT) s1_bldg_unit_combos,
  sum(nu + (hasnull AND nu=0)::INT) s2a_combos,
  sum(nu + (hasnull AND nu<2)::INT) s2b_combos,
  sum(null_recs) FILTER (WHERE nu>=1) s2a_dropped_records,
  sum(null_prim) FILTER (WHERE nu>=1) s2a_dropped_primary,
  sum(null_recs) FILTER (WHERE nu>=2) s2b_dropped_records,
  sum(null_prim) FILTER (WHERE nu>=2) s2b_dropped_primary
FROM b) TO '/home/jr2728/ptax_explore/hh_strat/out/hh_2011.csv' (HEADER);
COPY (WITH a AS (
  SELECT primary_family_ind prim,
         CASE WHEN address_type='S' AND coalesce(trim(house_num),'')<>'' AND coalesce(trim(street_name),'')<>''
              THEN hash(concat_ws('|', zip, upper(house_num), upper(street_pre_dir), upper(street_name), upper(street_suffix), upper(street_post_dir))) END bh,
         nullif(upper(trim(unit_num)), '') u
  FROM cl.dataaxle_annual  WHERE record_year=2012),
b AS (SELECT bh, count(DISTINCT u) nu, bool_or(u IS NULL) hasnull,
             count(*) FILTER (WHERE u IS NULL) null_recs, count(*) FILTER (WHERE u IS NULL AND prim) null_prim
      FROM a WHERE bh IS NOT NULL GROUP BY bh)
SELECT 2012 AS year,
  (SELECT count(*) FROM a) fam_records,
  (SELECT count(*) FILTER (WHERE prim) FROM a) primary_fams,
  (SELECT count(*) FROM a WHERE bh IS NULL) nobldg_records,
  (SELECT count(*) FILTER (WHERE prim) FROM a WHERE bh IS NULL) nobldg_primary,
  count(*) buildings,
  count(*) FILTER (WHERE hasnull AND nu>=1) bldg_null_plus_1unit,
  count(*) FILTER (WHERE hasnull AND nu>=2) bldg_null_plus_2units,
  sum(nu + hasnull::INT) s1_bldg_unit_combos,
  sum(nu + (hasnull AND nu=0)::INT) s2a_combos,
  sum(nu + (hasnull AND nu<2)::INT) s2b_combos,
  sum(null_recs) FILTER (WHERE nu>=1) s2a_dropped_records,
  sum(null_prim) FILTER (WHERE nu>=1) s2a_dropped_primary,
  sum(null_recs) FILTER (WHERE nu>=2) s2b_dropped_records,
  sum(null_prim) FILTER (WHERE nu>=2) s2b_dropped_primary
FROM b) TO '/home/jr2728/ptax_explore/hh_strat/out/hh_2012.csv' (HEADER);
COPY (WITH a AS (
  SELECT primary_family_ind prim,
         CASE WHEN address_type='S' AND coalesce(trim(house_num),'')<>'' AND coalesce(trim(street_name),'')<>''
              THEN hash(concat_ws('|', zip, upper(house_num), upper(street_pre_dir), upper(street_name), upper(street_suffix), upper(street_post_dir))) END bh,
         nullif(upper(trim(unit_num)), '') u
  FROM cl.dataaxle_annual  WHERE record_year=2013),
b AS (SELECT bh, count(DISTINCT u) nu, bool_or(u IS NULL) hasnull,
             count(*) FILTER (WHERE u IS NULL) null_recs, count(*) FILTER (WHERE u IS NULL AND prim) null_prim
      FROM a WHERE bh IS NOT NULL GROUP BY bh)
SELECT 2013 AS year,
  (SELECT count(*) FROM a) fam_records,
  (SELECT count(*) FILTER (WHERE prim) FROM a) primary_fams,
  (SELECT count(*) FROM a WHERE bh IS NULL) nobldg_records,
  (SELECT count(*) FILTER (WHERE prim) FROM a WHERE bh IS NULL) nobldg_primary,
  count(*) buildings,
  count(*) FILTER (WHERE hasnull AND nu>=1) bldg_null_plus_1unit,
  count(*) FILTER (WHERE hasnull AND nu>=2) bldg_null_plus_2units,
  sum(nu + hasnull::INT) s1_bldg_unit_combos,
  sum(nu + (hasnull AND nu=0)::INT) s2a_combos,
  sum(nu + (hasnull AND nu<2)::INT) s2b_combos,
  sum(null_recs) FILTER (WHERE nu>=1) s2a_dropped_records,
  sum(null_prim) FILTER (WHERE nu>=1) s2a_dropped_primary,
  sum(null_recs) FILTER (WHERE nu>=2) s2b_dropped_records,
  sum(null_prim) FILTER (WHERE nu>=2) s2b_dropped_primary
FROM b) TO '/home/jr2728/ptax_explore/hh_strat/out/hh_2013.csv' (HEADER);
