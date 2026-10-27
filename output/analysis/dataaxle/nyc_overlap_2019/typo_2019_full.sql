SET preserve_insertion_order=false;
USE memory;
CREATE OR REPLACE TABLE hud AS SELECT zip, max(res_ratio::DOUBLE) > 0 res FROM read_csv('/home/jr2728/ptax_explore/ref/hud_zip_county_032019.csv', all_varchar=true) GROUP BY zip;
CREATE OR REPLACE TABLE nyc AS SELECT zip FROM read_csv('/home/jr2728/ptax_explore/ref/nyc_zips.csv', all_varchar=true);
CREATE OR REPLACE TABLE da AS
  SELECT z, s, h, any_value(st) st FROM (
    SELECT lpad(zip::VARCHAR, 5, '0') z, upper(trim(street_name)) s, upper(trim(house_num)) h, state st
    FROM cl.dataaxle_annual 
    WHERE record_year=2019 AND address_type='S' AND coalesce(trim(house_num),'')<>'' AND coalesce(trim(street_name),'')<>'' AND zip IS NOT NULL)
  GROUP BY ALL;
CREATE OR REPLACE TABLE tx AS
  SELECT z, s, h, any_value(st) st FROM (
    SELECT left(situs_zip_code, 5) z, upper(trim(situs_street_name)) s, upper(trim(situs_house_number)) h, situs_state st
    FROM cl.tax 
    WHERE tax_year=2019 AND coalesce(trim(situs_house_number),'')<>'' AND coalesce(trim(situs_street_name),'')<>'' AND length(situs_zip_code)>=5)
  GROUP BY ALL;
CREATE OR REPLACE TABLE du AS SELECT * FROM da ANTI JOIN tx USING (z, s, h);
CREATE OR REPLACE TABLE tu AS SELECT * FROM tx ANTI JOIN da USING (z, s, h);
-- near-miss flags for each unmatched address against the other side's unmatched addresses
CREATE OR REPLACE MACRO near_street(a, b) AS a <> b AND least(length(a), length(b)) >= 4 AND (levenshtein(a, b) <= 2 OR jaro_winkler_similarity(a, b) >= 0.92);
CREATE OR REPLACE TABLE du_f AS
  SELECT u.*,
    EXISTS (SELECT 1 FROM tu t WHERE t.s = u.s AND t.h = u.h AND t.z <> u.z AND left(t.z, 3) = left(u.z, 3)) zip_mis,
    EXISTS (SELECT 1 FROM tu t WHERE t.z = u.z AND t.h = u.h AND near_street(u.s, t.s)) street_typo,
    EXISTS (SELECT 1 FROM tu t WHERE t.z = u.z AND t.s = u.s AND length(t.h) = length(u.h) AND levenshtein(t.h, u.h) = 1) num_typo
  FROM du u;
CREATE OR REPLACE TABLE tu_f AS
  SELECT u.*,
    EXISTS (SELECT 1 FROM du t WHERE t.s = u.s AND t.h = u.h AND t.z <> u.z AND left(t.z, 3) = left(u.z, 3)) zip_mis,
    EXISTS (SELECT 1 FROM du t WHERE t.z = u.z AND t.h = u.h AND near_street(u.s, t.s)) street_typo,
    EXISTS (SELECT 1 FROM du t WHERE t.z = u.z AND t.s = u.s AND length(t.h) = length(u.h) AND levenshtein(t.h, u.h) = 1) num_typo
  FROM tu u;
COPY (
  SELECT 'dataaxle_unmatched' src, st AS state, h.zip IS NOT NULL zip_in_usps, coalesce(h.res, false) zip_residential, zip_mis, street_typo, num_typo, count(*) n
  FROM du_f LEFT JOIN hud h ON h.zip = du_f.z GROUP BY ALL
  UNION ALL
  SELECT 'tax_unmatched', st, h.zip IS NOT NULL, coalesce(h.res, false), zip_mis, street_typo, num_typo, count(*)
  FROM tu_f LEFT JOIN hud h ON h.zip = tu_f.z GROUP BY ALL
  UNION ALL
  SELECT 'dataaxle_all', st, h.zip IS NOT NULL, coalesce(h.res, false), NULL, NULL, NULL, count(*) FROM da LEFT JOIN hud h ON h.zip = da.z GROUP BY ALL
  UNION ALL
  SELECT 'tax_all', st, h.zip IS NOT NULL, coalesce(h.res, false), NULL, NULL, NULL, count(*) FROM tx LEFT JOIN hud h ON h.zip = tx.z GROUP BY ALL
) TO '/home/jr2728/ptax_explore/hh_strat/out/typo_2019_classes.csv' (HEADER);
COPY (
  WITH a AS (SELECT z, count(*) da_addr FROM da SEMI JOIN nyc ON da.z = nyc.zip GROUP BY z),
       b AS (SELECT z, count(*) tax_addr FROM tx SEMI JOIN nyc ON tx.z = nyc.zip GROUP BY z),
       c AS (SELECT z, count(*) both_addr FROM da JOIN tx USING (z, s, h) SEMI JOIN nyc ON da.z = nyc.zip GROUP BY z),
       d AS (SELECT z, count(*) FILTER (WHERE zip_mis OR street_typo OR num_typo) da_unm_nearmiss FROM du_f SEMI JOIN nyc ON du_f.z = nyc.zip GROUP BY z),
       e AS (SELECT z, count(*) FILTER (WHERE zip_mis OR street_typo OR num_typo) tax_unm_nearmiss FROM tu_f SEMI JOIN nyc ON tu_f.z = nyc.zip GROUP BY z)
  SELECT n.zip, coalesce(da_addr, 0) da_addr, coalesce(tax_addr, 0) tax_addr, coalesce(both_addr, 0) both_addr,
         coalesce(da_unm_nearmiss, 0) da_unm_nearmiss, coalesce(tax_unm_nearmiss, 0) tax_unm_nearmiss
  FROM nyc n LEFT JOIN a ON a.z = n.zip LEFT JOIN b ON b.z = n.zip LEFT JOIN c ON c.z = n.zip
       LEFT JOIN d ON d.z = n.zip LEFT JOIN e ON e.z = n.zip ORDER BY 1
) TO '/home/jr2728/ptax_explore/hh_strat/out/typo_2019_nyc_zip.csv' (HEADER);
COPY (
  SELECT u.z, u.h, u.s AS dataaxle_street, t.s AS tax_street, levenshtein(u.s, t.s) lev, u.z IN (SELECT zip FROM nyc) is_nyc
  FROM du u JOIN tu t ON t.z = u.z AND t.h = u.h AND near_street(u.s, t.s)
  USING SAMPLE 300 ROWS
) TO '/home/jr2728/ptax_explore/hh_strat/out/typo_2019_street_typo_examples.csv' (HEADER);
