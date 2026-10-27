SET preserve_insertion_order=false;
USE memory;
-- Mover rates, consecutive year pairs, full data. Families present in both years (by family_id).
-- loose  = building (ZIP5 + street + house number; NULL if not a street address) or ZIP changed
-- strict = loose AND length_of_residence reset in t+1 (<= 1; <= 2 for the two-year pair 2021->2023)
-- interstate = state changed (loose) / state changed AND LOR reset (strict).
CREATE OR REPLACE MACRO bk(t, z, s, h) AS CASE WHEN t = 'S' AND coalesce(trim(h),'') <> '' AND coalesce(trim(s),'') <> '' AND z IS NOT NULL
                                               THEN hash(lpad(z::VARCHAR,5,'0'), upper(trim(s)), upper(trim(h))) END;
CREATE OR REPLACE TABLE res (y0 SMALLINT, y1 SMALLINT, prim BOOLEAN, n BIGINT, loose BIGINT, strict BIGINT, interstate_loose BIGINT, interstate_strict BIGINT,
                             same_building_lor_reset BIGINT);
INSERT INTO res
WITH a AS (SELECT family_id fid, primary_family_ind prim, bk(address_type, zip, street_name, house_num) b, zip z, state st
           FROM cl.dataaxle_annual WHERE record_year = 2006),
     c AS (SELECT family_id fid, bk(address_type, zip, street_name, house_num) b, zip z, state st, length_of_residence lor
           FROM cl.dataaxle_annual WHERE record_year = 2007),
     j AS (SELECT a.prim, (a.b IS DISTINCT FROM c.b OR a.z IS DISTINCT FROM c.z) mv, a.st IS DISTINCT FROM c.st ist, coalesce(c.lor <= 1, false) rs
           FROM a JOIN c USING (fid))
SELECT 2006, 2007, prim, count(*), count(*) FILTER (WHERE mv), count(*) FILTER (WHERE mv AND rs),
       count(*) FILTER (WHERE ist), count(*) FILTER (WHERE ist AND rs), count(*) FILTER (WHERE NOT mv AND rs)
FROM j GROUP BY prim;
INSERT INTO res
WITH a AS (SELECT family_id fid, primary_family_ind prim, bk(address_type, zip, street_name, house_num) b, zip z, state st
           FROM cl.dataaxle_annual WHERE record_year = 2007),
     c AS (SELECT family_id fid, bk(address_type, zip, street_name, house_num) b, zip z, state st, length_of_residence lor
           FROM cl.dataaxle_annual WHERE record_year = 2008),
     j AS (SELECT a.prim, (a.b IS DISTINCT FROM c.b OR a.z IS DISTINCT FROM c.z) mv, a.st IS DISTINCT FROM c.st ist, coalesce(c.lor <= 1, false) rs
           FROM a JOIN c USING (fid))
SELECT 2007, 2008, prim, count(*), count(*) FILTER (WHERE mv), count(*) FILTER (WHERE mv AND rs),
       count(*) FILTER (WHERE ist), count(*) FILTER (WHERE ist AND rs), count(*) FILTER (WHERE NOT mv AND rs)
FROM j GROUP BY prim;
INSERT INTO res
WITH a AS (SELECT family_id fid, primary_family_ind prim, bk(address_type, zip, street_name, house_num) b, zip z, state st
           FROM cl.dataaxle_annual WHERE record_year = 2008),
     c AS (SELECT family_id fid, bk(address_type, zip, street_name, house_num) b, zip z, state st, length_of_residence lor
           FROM cl.dataaxle_annual WHERE record_year = 2009),
     j AS (SELECT a.prim, (a.b IS DISTINCT FROM c.b OR a.z IS DISTINCT FROM c.z) mv, a.st IS DISTINCT FROM c.st ist, coalesce(c.lor <= 1, false) rs
           FROM a JOIN c USING (fid))
SELECT 2008, 2009, prim, count(*), count(*) FILTER (WHERE mv), count(*) FILTER (WHERE mv AND rs),
       count(*) FILTER (WHERE ist), count(*) FILTER (WHERE ist AND rs), count(*) FILTER (WHERE NOT mv AND rs)
FROM j GROUP BY prim;
INSERT INTO res
WITH a AS (SELECT family_id fid, primary_family_ind prim, bk(address_type, zip, street_name, house_num) b, zip z, state st
           FROM cl.dataaxle_annual WHERE record_year = 2009),
     c AS (SELECT family_id fid, bk(address_type, zip, street_name, house_num) b, zip z, state st, length_of_residence lor
           FROM cl.dataaxle_annual WHERE record_year = 2010),
     j AS (SELECT a.prim, (a.b IS DISTINCT FROM c.b OR a.z IS DISTINCT FROM c.z) mv, a.st IS DISTINCT FROM c.st ist, coalesce(c.lor <= 1, false) rs
           FROM a JOIN c USING (fid))
SELECT 2009, 2010, prim, count(*), count(*) FILTER (WHERE mv), count(*) FILTER (WHERE mv AND rs),
       count(*) FILTER (WHERE ist), count(*) FILTER (WHERE ist AND rs), count(*) FILTER (WHERE NOT mv AND rs)
FROM j GROUP BY prim;
INSERT INTO res
WITH a AS (SELECT family_id fid, primary_family_ind prim, bk(address_type, zip, street_name, house_num) b, zip z, state st
           FROM cl.dataaxle_annual WHERE record_year = 2010),
     c AS (SELECT family_id fid, bk(address_type, zip, street_name, house_num) b, zip z, state st, length_of_residence lor
           FROM cl.dataaxle_annual WHERE record_year = 2011),
     j AS (SELECT a.prim, (a.b IS DISTINCT FROM c.b OR a.z IS DISTINCT FROM c.z) mv, a.st IS DISTINCT FROM c.st ist, coalesce(c.lor <= 1, false) rs
           FROM a JOIN c USING (fid))
SELECT 2010, 2011, prim, count(*), count(*) FILTER (WHERE mv), count(*) FILTER (WHERE mv AND rs),
       count(*) FILTER (WHERE ist), count(*) FILTER (WHERE ist AND rs), count(*) FILTER (WHERE NOT mv AND rs)
FROM j GROUP BY prim;
INSERT INTO res
WITH a AS (SELECT family_id fid, primary_family_ind prim, bk(address_type, zip, street_name, house_num) b, zip z, state st
           FROM cl.dataaxle_annual WHERE record_year = 2011),
     c AS (SELECT family_id fid, bk(address_type, zip, street_name, house_num) b, zip z, state st, length_of_residence lor
           FROM cl.dataaxle_annual WHERE record_year = 2012),
     j AS (SELECT a.prim, (a.b IS DISTINCT FROM c.b OR a.z IS DISTINCT FROM c.z) mv, a.st IS DISTINCT FROM c.st ist, coalesce(c.lor <= 1, false) rs
           FROM a JOIN c USING (fid))
SELECT 2011, 2012, prim, count(*), count(*) FILTER (WHERE mv), count(*) FILTER (WHERE mv AND rs),
       count(*) FILTER (WHERE ist), count(*) FILTER (WHERE ist AND rs), count(*) FILTER (WHERE NOT mv AND rs)
FROM j GROUP BY prim;
INSERT INTO res
WITH a AS (SELECT family_id fid, primary_family_ind prim, bk(address_type, zip, street_name, house_num) b, zip z, state st
           FROM cl.dataaxle_annual WHERE record_year = 2012),
     c AS (SELECT family_id fid, bk(address_type, zip, street_name, house_num) b, zip z, state st, length_of_residence lor
           FROM cl.dataaxle_annual WHERE record_year = 2013),
     j AS (SELECT a.prim, (a.b IS DISTINCT FROM c.b OR a.z IS DISTINCT FROM c.z) mv, a.st IS DISTINCT FROM c.st ist, coalesce(c.lor <= 1, false) rs
           FROM a JOIN c USING (fid))
SELECT 2012, 2013, prim, count(*), count(*) FILTER (WHERE mv), count(*) FILTER (WHERE mv AND rs),
       count(*) FILTER (WHERE ist), count(*) FILTER (WHERE ist AND rs), count(*) FILTER (WHERE NOT mv AND rs)
FROM j GROUP BY prim;
INSERT INTO res
WITH a AS (SELECT family_id fid, primary_family_ind prim, bk(address_type, zip, street_name, house_num) b, zip z, state st
           FROM cl.dataaxle_annual WHERE record_year = 2013),
     c AS (SELECT family_id fid, bk(address_type, zip, street_name, house_num) b, zip z, state st, length_of_residence lor
           FROM cl.dataaxle_annual WHERE record_year = 2014),
     j AS (SELECT a.prim, (a.b IS DISTINCT FROM c.b OR a.z IS DISTINCT FROM c.z) mv, a.st IS DISTINCT FROM c.st ist, coalesce(c.lor <= 1, false) rs
           FROM a JOIN c USING (fid))
SELECT 2013, 2014, prim, count(*), count(*) FILTER (WHERE mv), count(*) FILTER (WHERE mv AND rs),
       count(*) FILTER (WHERE ist), count(*) FILTER (WHERE ist AND rs), count(*) FILTER (WHERE NOT mv AND rs)
FROM j GROUP BY prim;
INSERT INTO res
WITH a AS (SELECT family_id fid, primary_family_ind prim, bk(address_type, zip, street_name, house_num) b, zip z, state st
           FROM cl.dataaxle_annual WHERE record_year = 2014),
     c AS (SELECT family_id fid, bk(address_type, zip, street_name, house_num) b, zip z, state st, length_of_residence lor
           FROM cl.dataaxle_annual WHERE record_year = 2015),
     j AS (SELECT a.prim, (a.b IS DISTINCT FROM c.b OR a.z IS DISTINCT FROM c.z) mv, a.st IS DISTINCT FROM c.st ist, coalesce(c.lor <= 1, false) rs
           FROM a JOIN c USING (fid))
SELECT 2014, 2015, prim, count(*), count(*) FILTER (WHERE mv), count(*) FILTER (WHERE mv AND rs),
       count(*) FILTER (WHERE ist), count(*) FILTER (WHERE ist AND rs), count(*) FILTER (WHERE NOT mv AND rs)
FROM j GROUP BY prim;
INSERT INTO res
WITH a AS (SELECT family_id fid, primary_family_ind prim, bk(address_type, zip, street_name, house_num) b, zip z, state st
           FROM cl.dataaxle_annual WHERE record_year = 2015),
     c AS (SELECT family_id fid, bk(address_type, zip, street_name, house_num) b, zip z, state st, length_of_residence lor
           FROM cl.dataaxle_annual WHERE record_year = 2016),
     j AS (SELECT a.prim, (a.b IS DISTINCT FROM c.b OR a.z IS DISTINCT FROM c.z) mv, a.st IS DISTINCT FROM c.st ist, coalesce(c.lor <= 1, false) rs
           FROM a JOIN c USING (fid))
SELECT 2015, 2016, prim, count(*), count(*) FILTER (WHERE mv), count(*) FILTER (WHERE mv AND rs),
       count(*) FILTER (WHERE ist), count(*) FILTER (WHERE ist AND rs), count(*) FILTER (WHERE NOT mv AND rs)
FROM j GROUP BY prim;
INSERT INTO res
WITH a AS (SELECT family_id fid, primary_family_ind prim, bk(address_type, zip, street_name, house_num) b, zip z, state st
           FROM cl.dataaxle_annual WHERE record_year = 2016),
     c AS (SELECT family_id fid, bk(address_type, zip, street_name, house_num) b, zip z, state st, length_of_residence lor
           FROM cl.dataaxle_annual WHERE record_year = 2017),
     j AS (SELECT a.prim, (a.b IS DISTINCT FROM c.b OR a.z IS DISTINCT FROM c.z) mv, a.st IS DISTINCT FROM c.st ist, coalesce(c.lor <= 1, false) rs
           FROM a JOIN c USING (fid))
SELECT 2016, 2017, prim, count(*), count(*) FILTER (WHERE mv), count(*) FILTER (WHERE mv AND rs),
       count(*) FILTER (WHERE ist), count(*) FILTER (WHERE ist AND rs), count(*) FILTER (WHERE NOT mv AND rs)
FROM j GROUP BY prim;
INSERT INTO res
WITH a AS (SELECT family_id fid, primary_family_ind prim, bk(address_type, zip, street_name, house_num) b, zip z, state st
           FROM cl.dataaxle_annual WHERE record_year = 2017),
     c AS (SELECT family_id fid, bk(address_type, zip, street_name, house_num) b, zip z, state st, length_of_residence lor
           FROM cl.dataaxle_annual WHERE record_year = 2018),
     j AS (SELECT a.prim, (a.b IS DISTINCT FROM c.b OR a.z IS DISTINCT FROM c.z) mv, a.st IS DISTINCT FROM c.st ist, coalesce(c.lor <= 1, false) rs
           FROM a JOIN c USING (fid))
SELECT 2017, 2018, prim, count(*), count(*) FILTER (WHERE mv), count(*) FILTER (WHERE mv AND rs),
       count(*) FILTER (WHERE ist), count(*) FILTER (WHERE ist AND rs), count(*) FILTER (WHERE NOT mv AND rs)
FROM j GROUP BY prim;
INSERT INTO res
WITH a AS (SELECT family_id fid, primary_family_ind prim, bk(address_type, zip, street_name, house_num) b, zip z, state st
           FROM cl.dataaxle_annual WHERE record_year = 2018),
     c AS (SELECT family_id fid, bk(address_type, zip, street_name, house_num) b, zip z, state st, length_of_residence lor
           FROM cl.dataaxle_annual WHERE record_year = 2019),
     j AS (SELECT a.prim, (a.b IS DISTINCT FROM c.b OR a.z IS DISTINCT FROM c.z) mv, a.st IS DISTINCT FROM c.st ist, coalesce(c.lor <= 1, false) rs
           FROM a JOIN c USING (fid))
SELECT 2018, 2019, prim, count(*), count(*) FILTER (WHERE mv), count(*) FILTER (WHERE mv AND rs),
       count(*) FILTER (WHERE ist), count(*) FILTER (WHERE ist AND rs), count(*) FILTER (WHERE NOT mv AND rs)
FROM j GROUP BY prim;
INSERT INTO res
WITH a AS (SELECT family_id fid, primary_family_ind prim, bk(address_type, zip, street_name, house_num) b, zip z, state st
           FROM cl.dataaxle_annual WHERE record_year = 2019),
     c AS (SELECT family_id fid, bk(address_type, zip, street_name, house_num) b, zip z, state st, length_of_residence lor
           FROM cl.dataaxle_annual WHERE record_year = 2020),
     j AS (SELECT a.prim, (a.b IS DISTINCT FROM c.b OR a.z IS DISTINCT FROM c.z) mv, a.st IS DISTINCT FROM c.st ist, coalesce(c.lor <= 1, false) rs
           FROM a JOIN c USING (fid))
SELECT 2019, 2020, prim, count(*), count(*) FILTER (WHERE mv), count(*) FILTER (WHERE mv AND rs),
       count(*) FILTER (WHERE ist), count(*) FILTER (WHERE ist AND rs), count(*) FILTER (WHERE NOT mv AND rs)
FROM j GROUP BY prim;
INSERT INTO res
WITH a AS (SELECT family_id fid, primary_family_ind prim, bk(address_type, zip, street_name, house_num) b, zip z, state st
           FROM cl.dataaxle_annual WHERE record_year = 2020),
     c AS (SELECT family_id fid, bk(address_type, zip, street_name, house_num) b, zip z, state st, length_of_residence lor
           FROM cl.dataaxle_annual WHERE record_year = 2021),
     j AS (SELECT a.prim, (a.b IS DISTINCT FROM c.b OR a.z IS DISTINCT FROM c.z) mv, a.st IS DISTINCT FROM c.st ist, coalesce(c.lor <= 1, false) rs
           FROM a JOIN c USING (fid))
SELECT 2020, 2021, prim, count(*), count(*) FILTER (WHERE mv), count(*) FILTER (WHERE mv AND rs),
       count(*) FILTER (WHERE ist), count(*) FILTER (WHERE ist AND rs), count(*) FILTER (WHERE NOT mv AND rs)
FROM j GROUP BY prim;
INSERT INTO res
WITH a AS (SELECT family_id fid, primary_family_ind prim, bk(address_type, zip, street_name, house_num) b, zip z, state st
           FROM cl.dataaxle_annual WHERE record_year = 2021),
     c AS (SELECT family_id fid, bk(address_type, zip, street_name, house_num) b, zip z, state st, length_of_residence lor
           FROM cl.dataaxle_annual WHERE record_year = 2023),
     j AS (SELECT a.prim, (a.b IS DISTINCT FROM c.b OR a.z IS DISTINCT FROM c.z) mv, a.st IS DISTINCT FROM c.st ist, coalesce(c.lor <= 2, false) rs
           FROM a JOIN c USING (fid))
SELECT 2021, 2023, prim, count(*), count(*) FILTER (WHERE mv), count(*) FILTER (WHERE mv AND rs),
       count(*) FILTER (WHERE ist), count(*) FILTER (WHERE ist AND rs), count(*) FILTER (WHERE NOT mv AND rs)
FROM j GROUP BY prim;
COPY (SELECT * FROM res ORDER BY ALL) TO '/home/jr2728/ptax_explore/hh_strat/out/moves_strict.csv' (HEADER);
