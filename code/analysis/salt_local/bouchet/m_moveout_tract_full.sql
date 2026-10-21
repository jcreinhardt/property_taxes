-- Full-data version of the tract move-out measure (subsample -> full-data rule; 1% version: g_moveout_bkey_1pct.sql).
-- Address = building key bkey (ZIP5, street, house number). Tract (2010) from the 2018 tax roll (residential pic 10/11/21/22,
-- fips || left(census_id, 6)). Data Axle annual files 2012-2021, street addresses; per bkey-year the primary family_id and
-- the number of primary families; move-out t-1 -> t where both years have exactly one primary family and the id differs.
-- Output: ~/ptax_explore/border/moveout_tract_full.parquet (tract, y = destination file, n, moves).
SET preserve_insertion_order=false;
CREATE TEMP TABLE res AS
SELECT hash(left(situs_zip_code, 5), upper(trim(situs_street_name)), upper(trim(situs_house_number))) bkey,
       any_value(lpad(fips_code::VARCHAR, 5, '0') || left(census_id::VARCHAR, 6)) tract
FROM cl.tax WHERE tax_year = 2018 AND property_indicator_code IN (10, 11, 21, 22) AND length(census_id::VARCHAR) >= 6
  AND coalesce(trim(situs_house_number), '') <> '' AND coalesce(trim(situs_street_name), '') <> '' AND length(situs_zip_code) >= 5
GROUP BY 1;
CREATE TEMP TABLE pf AS
SELECT hash(lpad(zip::VARCHAR, 5, '0'), upper(trim(street_name)), upper(trim(house_num))) bkey, record_year::INT y,
       min(family_id) FILTER (WHERE primary_family_ind) fid, count(DISTINCT family_id) FILTER (WHERE primary_family_ind) np
FROM cl.dataaxle_annual
WHERE record_year BETWEEN 2012 AND 2021 AND address_type = 'S' AND coalesce(trim(house_num), '') <> '' AND coalesce(trim(street_name), '') <> ''
  AND zip IS NOT NULL
GROUP BY 1, 2;
COPY (SELECT r.tract, b.y, count(*) n, sum((a.fid <> b.fid)::INT) moves
      FROM pf a JOIN pf b ON b.bkey = a.bkey AND b.y = a.y + 1 JOIN res r ON r.bkey = a.bkey
      WHERE a.np = 1 AND b.np = 1 GROUP BY ALL) TO '/home/jr2728/ptax_explore/border/moveout_tract_full.parquet' (FORMAT parquet);
SELECT y, sum(n) n, round(100 * sum(moves) / sum(n), 2) pct FROM '/home/jr2728/ptax_explore/border/moveout_tract_full.parquet' GROUP BY y ORDER BY y;
