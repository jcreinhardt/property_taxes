-- Address-level move-out panel for the 1% building sample (Bouchet; reads ~/ptax_explore/samples only), for the border
-- out-migration comparison (designs/border_twfe/10_moveout_by_side.R). Same definitions as output/validation/dataaxle/move_tracking/moveout_proxies.sql:
-- residential parcels (pic 10/11/21/22 with a structure), Data Axle years 2012-2021 backfilled from the last record;
-- t -> t+1 move-out = primary family_id at the address changes (plus modal primary surname change as a check).
-- Adds parcel coordinates (latest tax record) so sides can be assigned locally. Output: one row per bkey x t.
USE memory;
SET preserve_insertion_order=false;
CREATE OR REPLACE TABLE tx AS
  SELECT bkey, arg_max(struct_pack(pic := property_indicator_code, occ := owner_occupancy_code, corp := coalesce(owner_1_corporate_indicator,false),
                 noimpr := coalesce(calculated_improvement_value,0) = 0 AND coalesce(total_living_area_square_feet_all_buildings,0) = 0,
                 mail_here := left(coalesce(mailing_zip_code,''),5) = left(situs_zip_code,5) AND upper(trim(coalesce(mailing_house_number,''))) = upper(trim(situs_house_number))
                              AND upper(trim(coalesce(mailing_street_name,''))) = upper(trim(situs_street_name)),
                 fips := fips_code, lat := nullif(parcel_level_latitude, 0), lon := nullif(parcel_level_longitude, 0)), tax_year) a
  FROM '/home/jr2728/ptax_explore/samples/tax_bld1pct.parquet' WHERE tax_year BETWEEN 2012 AND 2021 GROUP BY bkey;
CREATE OR REPLACE TABLE sfr AS
  SELECT bkey, a.pic = 10 AND NOT a.corp AND (a.occ = 'O' OR a.mail_here) oo_sfr, a.fips fips, a.lat lat, a.lon lon
  FROM tx WHERE a.pic IN (10, 11, 21, 22) AND NOT a.noimpr AND a.lat IS NOT NULL AND a.lon IS NOT NULL;
CREATE OR REPLACE TABLE per AS
  SELECT bkey, record_year::INT y, family_id fid, primary_family_ind prim, unnest([ln_1, ln_2, ln_3, ln_4, ln_5]) ln
  FROM '/home/jr2728/ptax_explore/samples/da_bld1pct.parquet' WHERE record_year BETWEEN 2012 AND 2021 AND bkey IN (SELECT bkey FROM sfr);
CREATE OR REPLACE TABLE modal AS
  SELECT bkey, y, ln FROM (SELECT bkey, y, ln, row_number() OVER (PARTITION BY bkey, y ORDER BY n DESC, ln) rn
    FROM (SELECT bkey, y, ln, count(*) n FROM per WHERE prim AND ln IS NOT NULL GROUP BY ALL)) WHERE rn = 1;
CREATE OR REPLACE TABLE obs AS
  SELECT p.bkey, p.y, min(p.fid) FILTER (WHERE p.prim) prim_fid, count(DISTINCT p.fid) FILTER (WHERE p.prim) n_prim, any_value(m.ln) ln_prim
  FROM per p LEFT JOIN modal m ON m.bkey = p.bkey AND m.y = p.y GROUP BY p.bkey, p.y;
CREATE OR REPLACE TABLE grid AS SELECT s.bkey, yy.y FROM sfr s CROSS JOIN (SELECT unnest(range(2012, 2022)) y) yy;
CREATE OR REPLACE TABLE pan AS
  SELECT g.bkey, g.y, o.prim_fid IS NOT NULL observed, last_value(o.n_prim IGNORE NULLS) OVER w n_prim,
         last_value(o.prim_fid IGNORE NULLS) OVER w prim_fid, last_value(o.ln_prim IGNORE NULLS) OVER w ln_prim
  FROM grid g LEFT JOIN obs o ON o.bkey = g.bkey AND o.y = g.y
  WINDOW w AS (PARTITION BY g.bkey ORDER BY g.y ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
COPY (
  SELECT a.bkey, s.fips, s.lat, s.lon, s.oo_sfr, a.y::SMALLINT y, a.observed AND b.observed both_obs, (a.n_prim = 1 AND b.n_prim = 1) one_prim,
         a.prim_fid <> b.prim_fid id_chg, a.ln_prim <> b.ln_prim lnp_chg
  FROM pan a JOIN pan b ON b.bkey = a.bkey AND b.y = a.y + 1 JOIN sfr s ON s.bkey = a.bkey
  WHERE a.prim_fid IS NOT NULL AND b.prim_fid IS NOT NULL
) TO '/home/jr2728/ptax_explore/border/moveout_bkey_1pct.parquet.tmp' (FORMAT parquet, COMPRESSION zstd);
SELECT y, count(*) n, round(100 * avg(id_chg::INT), 2) id_chg, round(100 * avg(id_chg::INT) FILTER (WHERE both_obs AND one_prim), 2) id_chg_clean
FROM '/home/jr2728/ptax_explore/border/moveout_bkey_1pct.parquet.tmp' GROUP BY y ORDER BY y;
