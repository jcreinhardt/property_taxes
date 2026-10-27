SET preserve_insertion_order=false;
COPY (WITH a AS (
  SELECT CASE WHEN address_type='S' AND coalesce(trim(house_num),'')<>'' AND coalesce(trim(street_name),'')<>''
              THEN hash(concat_ws('|', zip, upper(house_num), upper(street_pre_dir), upper(street_name), upper(street_suffix), upper(street_post_dir))) END bh,
         nullif(upper(trim(unit_num)), '') u
  FROM cl.dataaxle_annual WHERE record_year=2006 AND primary_family_ind),
c AS (SELECT bh, u, count(*) np FROM a WHERE bh IS NOT NULL GROUP BY bh, u),
b AS (SELECT bh, count(u) n_units FROM c GROUP BY bh)
SELECT 2006 AS year,
  CASE WHEN c.u IS NOT NULL THEN 'unit' WHEN b.n_units>0 THEN 'no_unit_in_mixed_bldg' ELSE 'no_unit_never_unit_bldg' END combo_type,
  least(c.np, 10) primaries_in_combo,
  count(*) combos, sum(c.np) primaries
FROM c JOIN b USING (bh) GROUP BY ALL
UNION ALL
SELECT 2006, 'no_building_key', NULL, NULL, (SELECT count(*) FROM a WHERE bh IS NULL)
) TO '/home/jr2728/ptax_explore/hh_strat/out/multiprim_2006.csv' (HEADER);
COPY (WITH a AS (
  SELECT CASE WHEN address_type='S' AND coalesce(trim(house_num),'')<>'' AND coalesce(trim(street_name),'')<>''
              THEN hash(concat_ws('|', zip, upper(house_num), upper(street_pre_dir), upper(street_name), upper(street_suffix), upper(street_post_dir))) END bh,
         nullif(upper(trim(unit_num)), '') u
  FROM cl.dataaxle_annual WHERE record_year=2007 AND primary_family_ind),
c AS (SELECT bh, u, count(*) np FROM a WHERE bh IS NOT NULL GROUP BY bh, u),
b AS (SELECT bh, count(u) n_units FROM c GROUP BY bh)
SELECT 2007 AS year,
  CASE WHEN c.u IS NOT NULL THEN 'unit' WHEN b.n_units>0 THEN 'no_unit_in_mixed_bldg' ELSE 'no_unit_never_unit_bldg' END combo_type,
  least(c.np, 10) primaries_in_combo,
  count(*) combos, sum(c.np) primaries
FROM c JOIN b USING (bh) GROUP BY ALL
UNION ALL
SELECT 2007, 'no_building_key', NULL, NULL, (SELECT count(*) FROM a WHERE bh IS NULL)
) TO '/home/jr2728/ptax_explore/hh_strat/out/multiprim_2007.csv' (HEADER);
COPY (WITH a AS (
  SELECT CASE WHEN address_type='S' AND coalesce(trim(house_num),'')<>'' AND coalesce(trim(street_name),'')<>''
              THEN hash(concat_ws('|', zip, upper(house_num), upper(street_pre_dir), upper(street_name), upper(street_suffix), upper(street_post_dir))) END bh,
         nullif(upper(trim(unit_num)), '') u
  FROM cl.dataaxle_annual WHERE record_year=2008 AND primary_family_ind),
c AS (SELECT bh, u, count(*) np FROM a WHERE bh IS NOT NULL GROUP BY bh, u),
b AS (SELECT bh, count(u) n_units FROM c GROUP BY bh)
SELECT 2008 AS year,
  CASE WHEN c.u IS NOT NULL THEN 'unit' WHEN b.n_units>0 THEN 'no_unit_in_mixed_bldg' ELSE 'no_unit_never_unit_bldg' END combo_type,
  least(c.np, 10) primaries_in_combo,
  count(*) combos, sum(c.np) primaries
FROM c JOIN b USING (bh) GROUP BY ALL
UNION ALL
SELECT 2008, 'no_building_key', NULL, NULL, (SELECT count(*) FROM a WHERE bh IS NULL)
) TO '/home/jr2728/ptax_explore/hh_strat/out/multiprim_2008.csv' (HEADER);
COPY (WITH a AS (
  SELECT CASE WHEN address_type='S' AND coalesce(trim(house_num),'')<>'' AND coalesce(trim(street_name),'')<>''
              THEN hash(concat_ws('|', zip, upper(house_num), upper(street_pre_dir), upper(street_name), upper(street_suffix), upper(street_post_dir))) END bh,
         nullif(upper(trim(unit_num)), '') u
  FROM cl.dataaxle_annual WHERE record_year=2009 AND primary_family_ind),
c AS (SELECT bh, u, count(*) np FROM a WHERE bh IS NOT NULL GROUP BY bh, u),
b AS (SELECT bh, count(u) n_units FROM c GROUP BY bh)
SELECT 2009 AS year,
  CASE WHEN c.u IS NOT NULL THEN 'unit' WHEN b.n_units>0 THEN 'no_unit_in_mixed_bldg' ELSE 'no_unit_never_unit_bldg' END combo_type,
  least(c.np, 10) primaries_in_combo,
  count(*) combos, sum(c.np) primaries
FROM c JOIN b USING (bh) GROUP BY ALL
UNION ALL
SELECT 2009, 'no_building_key', NULL, NULL, (SELECT count(*) FROM a WHERE bh IS NULL)
) TO '/home/jr2728/ptax_explore/hh_strat/out/multiprim_2009.csv' (HEADER);
COPY (WITH a AS (
  SELECT CASE WHEN address_type='S' AND coalesce(trim(house_num),'')<>'' AND coalesce(trim(street_name),'')<>''
              THEN hash(concat_ws('|', zip, upper(house_num), upper(street_pre_dir), upper(street_name), upper(street_suffix), upper(street_post_dir))) END bh,
         nullif(upper(trim(unit_num)), '') u
  FROM cl.dataaxle_annual WHERE record_year=2010 AND primary_family_ind),
c AS (SELECT bh, u, count(*) np FROM a WHERE bh IS NOT NULL GROUP BY bh, u),
b AS (SELECT bh, count(u) n_units FROM c GROUP BY bh)
SELECT 2010 AS year,
  CASE WHEN c.u IS NOT NULL THEN 'unit' WHEN b.n_units>0 THEN 'no_unit_in_mixed_bldg' ELSE 'no_unit_never_unit_bldg' END combo_type,
  least(c.np, 10) primaries_in_combo,
  count(*) combos, sum(c.np) primaries
FROM c JOIN b USING (bh) GROUP BY ALL
UNION ALL
SELECT 2010, 'no_building_key', NULL, NULL, (SELECT count(*) FROM a WHERE bh IS NULL)
) TO '/home/jr2728/ptax_explore/hh_strat/out/multiprim_2010.csv' (HEADER);
COPY (WITH a AS (
  SELECT CASE WHEN address_type='S' AND coalesce(trim(house_num),'')<>'' AND coalesce(trim(street_name),'')<>''
              THEN hash(concat_ws('|', zip, upper(house_num), upper(street_pre_dir), upper(street_name), upper(street_suffix), upper(street_post_dir))) END bh,
         nullif(upper(trim(unit_num)), '') u
  FROM cl.dataaxle_annual WHERE record_year=2011 AND primary_family_ind),
c AS (SELECT bh, u, count(*) np FROM a WHERE bh IS NOT NULL GROUP BY bh, u),
b AS (SELECT bh, count(u) n_units FROM c GROUP BY bh)
SELECT 2011 AS year,
  CASE WHEN c.u IS NOT NULL THEN 'unit' WHEN b.n_units>0 THEN 'no_unit_in_mixed_bldg' ELSE 'no_unit_never_unit_bldg' END combo_type,
  least(c.np, 10) primaries_in_combo,
  count(*) combos, sum(c.np) primaries
FROM c JOIN b USING (bh) GROUP BY ALL
UNION ALL
SELECT 2011, 'no_building_key', NULL, NULL, (SELECT count(*) FROM a WHERE bh IS NULL)
) TO '/home/jr2728/ptax_explore/hh_strat/out/multiprim_2011.csv' (HEADER);
COPY (WITH a AS (
  SELECT CASE WHEN address_type='S' AND coalesce(trim(house_num),'')<>'' AND coalesce(trim(street_name),'')<>''
              THEN hash(concat_ws('|', zip, upper(house_num), upper(street_pre_dir), upper(street_name), upper(street_suffix), upper(street_post_dir))) END bh,
         nullif(upper(trim(unit_num)), '') u
  FROM cl.dataaxle_annual WHERE record_year=2012 AND primary_family_ind),
c AS (SELECT bh, u, count(*) np FROM a WHERE bh IS NOT NULL GROUP BY bh, u),
b AS (SELECT bh, count(u) n_units FROM c GROUP BY bh)
SELECT 2012 AS year,
  CASE WHEN c.u IS NOT NULL THEN 'unit' WHEN b.n_units>0 THEN 'no_unit_in_mixed_bldg' ELSE 'no_unit_never_unit_bldg' END combo_type,
  least(c.np, 10) primaries_in_combo,
  count(*) combos, sum(c.np) primaries
FROM c JOIN b USING (bh) GROUP BY ALL
UNION ALL
SELECT 2012, 'no_building_key', NULL, NULL, (SELECT count(*) FROM a WHERE bh IS NULL)
) TO '/home/jr2728/ptax_explore/hh_strat/out/multiprim_2012.csv' (HEADER);
COPY (WITH a AS (
  SELECT CASE WHEN address_type='S' AND coalesce(trim(house_num),'')<>'' AND coalesce(trim(street_name),'')<>''
              THEN hash(concat_ws('|', zip, upper(house_num), upper(street_pre_dir), upper(street_name), upper(street_suffix), upper(street_post_dir))) END bh,
         nullif(upper(trim(unit_num)), '') u
  FROM cl.dataaxle_annual WHERE record_year=2013 AND primary_family_ind),
c AS (SELECT bh, u, count(*) np FROM a WHERE bh IS NOT NULL GROUP BY bh, u),
b AS (SELECT bh, count(u) n_units FROM c GROUP BY bh)
SELECT 2013 AS year,
  CASE WHEN c.u IS NOT NULL THEN 'unit' WHEN b.n_units>0 THEN 'no_unit_in_mixed_bldg' ELSE 'no_unit_never_unit_bldg' END combo_type,
  least(c.np, 10) primaries_in_combo,
  count(*) combos, sum(c.np) primaries
FROM c JOIN b USING (bh) GROUP BY ALL
UNION ALL
SELECT 2013, 'no_building_key', NULL, NULL, (SELECT count(*) FROM a WHERE bh IS NULL)
) TO '/home/jr2728/ptax_explore/hh_strat/out/multiprim_2013.csv' (HEADER);
