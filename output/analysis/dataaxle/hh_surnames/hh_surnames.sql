SET preserve_insertion_order=false;
USE memory;
-- Pooled households: all family records (primary + secondary) at one building x unit in one year.
-- Distinct surnames among listed adults (persons 1-5 of every family). Surnames hashed in-query; only counts exported.
-- Full data, one year at a time; 2022 excluded (truncated file).
CREATE OR REPLACE MACRO hn(x) AS CASE WHEN coalesce(trim(x), '') <> '' THEN hash(upper(trim(x))) END;
CREATE OR REPLACE TABLE res (y SMALLINT, grp VARCHAR, adults INT, nfam INT, nsn INT, adults_prim INT, nsn_prim INT, children INT, n BIGINT);
INSERT INTO res
WITH f AS (
  SELECT hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num))) bld,
         nullif(upper(trim(unit_num)), '') u, primary_family_ind prim, location_type lt, children_hh_count ch,
         list_filter([hn(last_name_1), hn(last_name_2), hn(last_name_3), hn(last_name_4), hn(last_name_5)], x -> x IS NOT NULL) sn,
         len(list_filter([individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5], x -> x IS NOT NULL)) np
  FROM cl.dataaxle_annual
  WHERE record_year = 2006 AND address_type = 'S' AND coalesce(trim(house_num),'') <> '' AND coalesce(trim(street_name),'') <> '' AND zip IS NOT NULL),
hh AS (
  SELECT u, count(*) nfam, sum(prim::INT) nprim, bool_and(lt = 'S') sf, sum(np) adults, sum(np) FILTER (WHERE prim) adults_prim,
         len(list_distinct(flatten(list(sn)))) nsn, len(list_distinct(flatten(list(sn) FILTER (WHERE prim)))) nsn_prim,
         max(ch) FILTER (WHERE prim) children
  FROM f GROUP BY bld, u)
SELECT 2006, CASE WHEN nprim = 0 THEN 'no_primary' WHEN nprim > 1 THEN 'multi_primary'
                WHEN u IS NULL AND sf THEN 'sf_nounit' WHEN u IS NOT NULL THEN 'unit' ELSE 'other_nounit' END grp,
       least(adults, 10), least(nfam, 6), least(nsn, 6), least(coalesce(adults_prim, 0), 6), least(coalesce(nsn_prim, 0), 4),
       least(coalesce(children, 0), 5), count(*)
FROM hh GROUP BY ALL;
INSERT INTO res
WITH f AS (
  SELECT hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num))) bld,
         nullif(upper(trim(unit_num)), '') u, primary_family_ind prim, location_type lt, children_hh_count ch,
         list_filter([hn(last_name_1), hn(last_name_2), hn(last_name_3), hn(last_name_4), hn(last_name_5)], x -> x IS NOT NULL) sn,
         len(list_filter([individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5], x -> x IS NOT NULL)) np
  FROM cl.dataaxle_annual
  WHERE record_year = 2007 AND address_type = 'S' AND coalesce(trim(house_num),'') <> '' AND coalesce(trim(street_name),'') <> '' AND zip IS NOT NULL),
hh AS (
  SELECT u, count(*) nfam, sum(prim::INT) nprim, bool_and(lt = 'S') sf, sum(np) adults, sum(np) FILTER (WHERE prim) adults_prim,
         len(list_distinct(flatten(list(sn)))) nsn, len(list_distinct(flatten(list(sn) FILTER (WHERE prim)))) nsn_prim,
         max(ch) FILTER (WHERE prim) children
  FROM f GROUP BY bld, u)
SELECT 2007, CASE WHEN nprim = 0 THEN 'no_primary' WHEN nprim > 1 THEN 'multi_primary'
                WHEN u IS NULL AND sf THEN 'sf_nounit' WHEN u IS NOT NULL THEN 'unit' ELSE 'other_nounit' END grp,
       least(adults, 10), least(nfam, 6), least(nsn, 6), least(coalesce(adults_prim, 0), 6), least(coalesce(nsn_prim, 0), 4),
       least(coalesce(children, 0), 5), count(*)
FROM hh GROUP BY ALL;
INSERT INTO res
WITH f AS (
  SELECT hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num))) bld,
         nullif(upper(trim(unit_num)), '') u, primary_family_ind prim, location_type lt, children_hh_count ch,
         list_filter([hn(last_name_1), hn(last_name_2), hn(last_name_3), hn(last_name_4), hn(last_name_5)], x -> x IS NOT NULL) sn,
         len(list_filter([individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5], x -> x IS NOT NULL)) np
  FROM cl.dataaxle_annual
  WHERE record_year = 2008 AND address_type = 'S' AND coalesce(trim(house_num),'') <> '' AND coalesce(trim(street_name),'') <> '' AND zip IS NOT NULL),
hh AS (
  SELECT u, count(*) nfam, sum(prim::INT) nprim, bool_and(lt = 'S') sf, sum(np) adults, sum(np) FILTER (WHERE prim) adults_prim,
         len(list_distinct(flatten(list(sn)))) nsn, len(list_distinct(flatten(list(sn) FILTER (WHERE prim)))) nsn_prim,
         max(ch) FILTER (WHERE prim) children
  FROM f GROUP BY bld, u)
SELECT 2008, CASE WHEN nprim = 0 THEN 'no_primary' WHEN nprim > 1 THEN 'multi_primary'
                WHEN u IS NULL AND sf THEN 'sf_nounit' WHEN u IS NOT NULL THEN 'unit' ELSE 'other_nounit' END grp,
       least(adults, 10), least(nfam, 6), least(nsn, 6), least(coalesce(adults_prim, 0), 6), least(coalesce(nsn_prim, 0), 4),
       least(coalesce(children, 0), 5), count(*)
FROM hh GROUP BY ALL;
INSERT INTO res
WITH f AS (
  SELECT hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num))) bld,
         nullif(upper(trim(unit_num)), '') u, primary_family_ind prim, location_type lt, children_hh_count ch,
         list_filter([hn(last_name_1), hn(last_name_2), hn(last_name_3), hn(last_name_4), hn(last_name_5)], x -> x IS NOT NULL) sn,
         len(list_filter([individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5], x -> x IS NOT NULL)) np
  FROM cl.dataaxle_annual
  WHERE record_year = 2009 AND address_type = 'S' AND coalesce(trim(house_num),'') <> '' AND coalesce(trim(street_name),'') <> '' AND zip IS NOT NULL),
hh AS (
  SELECT u, count(*) nfam, sum(prim::INT) nprim, bool_and(lt = 'S') sf, sum(np) adults, sum(np) FILTER (WHERE prim) adults_prim,
         len(list_distinct(flatten(list(sn)))) nsn, len(list_distinct(flatten(list(sn) FILTER (WHERE prim)))) nsn_prim,
         max(ch) FILTER (WHERE prim) children
  FROM f GROUP BY bld, u)
SELECT 2009, CASE WHEN nprim = 0 THEN 'no_primary' WHEN nprim > 1 THEN 'multi_primary'
                WHEN u IS NULL AND sf THEN 'sf_nounit' WHEN u IS NOT NULL THEN 'unit' ELSE 'other_nounit' END grp,
       least(adults, 10), least(nfam, 6), least(nsn, 6), least(coalesce(adults_prim, 0), 6), least(coalesce(nsn_prim, 0), 4),
       least(coalesce(children, 0), 5), count(*)
FROM hh GROUP BY ALL;
INSERT INTO res
WITH f AS (
  SELECT hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num))) bld,
         nullif(upper(trim(unit_num)), '') u, primary_family_ind prim, location_type lt, children_hh_count ch,
         list_filter([hn(last_name_1), hn(last_name_2), hn(last_name_3), hn(last_name_4), hn(last_name_5)], x -> x IS NOT NULL) sn,
         len(list_filter([individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5], x -> x IS NOT NULL)) np
  FROM cl.dataaxle_annual
  WHERE record_year = 2010 AND address_type = 'S' AND coalesce(trim(house_num),'') <> '' AND coalesce(trim(street_name),'') <> '' AND zip IS NOT NULL),
hh AS (
  SELECT u, count(*) nfam, sum(prim::INT) nprim, bool_and(lt = 'S') sf, sum(np) adults, sum(np) FILTER (WHERE prim) adults_prim,
         len(list_distinct(flatten(list(sn)))) nsn, len(list_distinct(flatten(list(sn) FILTER (WHERE prim)))) nsn_prim,
         max(ch) FILTER (WHERE prim) children
  FROM f GROUP BY bld, u)
SELECT 2010, CASE WHEN nprim = 0 THEN 'no_primary' WHEN nprim > 1 THEN 'multi_primary'
                WHEN u IS NULL AND sf THEN 'sf_nounit' WHEN u IS NOT NULL THEN 'unit' ELSE 'other_nounit' END grp,
       least(adults, 10), least(nfam, 6), least(nsn, 6), least(coalesce(adults_prim, 0), 6), least(coalesce(nsn_prim, 0), 4),
       least(coalesce(children, 0), 5), count(*)
FROM hh GROUP BY ALL;
INSERT INTO res
WITH f AS (
  SELECT hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num))) bld,
         nullif(upper(trim(unit_num)), '') u, primary_family_ind prim, location_type lt, children_hh_count ch,
         list_filter([hn(last_name_1), hn(last_name_2), hn(last_name_3), hn(last_name_4), hn(last_name_5)], x -> x IS NOT NULL) sn,
         len(list_filter([individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5], x -> x IS NOT NULL)) np
  FROM cl.dataaxle_annual
  WHERE record_year = 2011 AND address_type = 'S' AND coalesce(trim(house_num),'') <> '' AND coalesce(trim(street_name),'') <> '' AND zip IS NOT NULL),
hh AS (
  SELECT u, count(*) nfam, sum(prim::INT) nprim, bool_and(lt = 'S') sf, sum(np) adults, sum(np) FILTER (WHERE prim) adults_prim,
         len(list_distinct(flatten(list(sn)))) nsn, len(list_distinct(flatten(list(sn) FILTER (WHERE prim)))) nsn_prim,
         max(ch) FILTER (WHERE prim) children
  FROM f GROUP BY bld, u)
SELECT 2011, CASE WHEN nprim = 0 THEN 'no_primary' WHEN nprim > 1 THEN 'multi_primary'
                WHEN u IS NULL AND sf THEN 'sf_nounit' WHEN u IS NOT NULL THEN 'unit' ELSE 'other_nounit' END grp,
       least(adults, 10), least(nfam, 6), least(nsn, 6), least(coalesce(adults_prim, 0), 6), least(coalesce(nsn_prim, 0), 4),
       least(coalesce(children, 0), 5), count(*)
FROM hh GROUP BY ALL;
INSERT INTO res
WITH f AS (
  SELECT hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num))) bld,
         nullif(upper(trim(unit_num)), '') u, primary_family_ind prim, location_type lt, children_hh_count ch,
         list_filter([hn(last_name_1), hn(last_name_2), hn(last_name_3), hn(last_name_4), hn(last_name_5)], x -> x IS NOT NULL) sn,
         len(list_filter([individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5], x -> x IS NOT NULL)) np
  FROM cl.dataaxle_annual
  WHERE record_year = 2012 AND address_type = 'S' AND coalesce(trim(house_num),'') <> '' AND coalesce(trim(street_name),'') <> '' AND zip IS NOT NULL),
hh AS (
  SELECT u, count(*) nfam, sum(prim::INT) nprim, bool_and(lt = 'S') sf, sum(np) adults, sum(np) FILTER (WHERE prim) adults_prim,
         len(list_distinct(flatten(list(sn)))) nsn, len(list_distinct(flatten(list(sn) FILTER (WHERE prim)))) nsn_prim,
         max(ch) FILTER (WHERE prim) children
  FROM f GROUP BY bld, u)
SELECT 2012, CASE WHEN nprim = 0 THEN 'no_primary' WHEN nprim > 1 THEN 'multi_primary'
                WHEN u IS NULL AND sf THEN 'sf_nounit' WHEN u IS NOT NULL THEN 'unit' ELSE 'other_nounit' END grp,
       least(adults, 10), least(nfam, 6), least(nsn, 6), least(coalesce(adults_prim, 0), 6), least(coalesce(nsn_prim, 0), 4),
       least(coalesce(children, 0), 5), count(*)
FROM hh GROUP BY ALL;
INSERT INTO res
WITH f AS (
  SELECT hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num))) bld,
         nullif(upper(trim(unit_num)), '') u, primary_family_ind prim, location_type lt, children_hh_count ch,
         list_filter([hn(last_name_1), hn(last_name_2), hn(last_name_3), hn(last_name_4), hn(last_name_5)], x -> x IS NOT NULL) sn,
         len(list_filter([individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5], x -> x IS NOT NULL)) np
  FROM cl.dataaxle_annual
  WHERE record_year = 2013 AND address_type = 'S' AND coalesce(trim(house_num),'') <> '' AND coalesce(trim(street_name),'') <> '' AND zip IS NOT NULL),
hh AS (
  SELECT u, count(*) nfam, sum(prim::INT) nprim, bool_and(lt = 'S') sf, sum(np) adults, sum(np) FILTER (WHERE prim) adults_prim,
         len(list_distinct(flatten(list(sn)))) nsn, len(list_distinct(flatten(list(sn) FILTER (WHERE prim)))) nsn_prim,
         max(ch) FILTER (WHERE prim) children
  FROM f GROUP BY bld, u)
SELECT 2013, CASE WHEN nprim = 0 THEN 'no_primary' WHEN nprim > 1 THEN 'multi_primary'
                WHEN u IS NULL AND sf THEN 'sf_nounit' WHEN u IS NOT NULL THEN 'unit' ELSE 'other_nounit' END grp,
       least(adults, 10), least(nfam, 6), least(nsn, 6), least(coalesce(adults_prim, 0), 6), least(coalesce(nsn_prim, 0), 4),
       least(coalesce(children, 0), 5), count(*)
FROM hh GROUP BY ALL;
INSERT INTO res
WITH f AS (
  SELECT hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num))) bld,
         nullif(upper(trim(unit_num)), '') u, primary_family_ind prim, location_type lt, children_hh_count ch,
         list_filter([hn(last_name_1), hn(last_name_2), hn(last_name_3), hn(last_name_4), hn(last_name_5)], x -> x IS NOT NULL) sn,
         len(list_filter([individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5], x -> x IS NOT NULL)) np
  FROM cl.dataaxle_annual
  WHERE record_year = 2014 AND address_type = 'S' AND coalesce(trim(house_num),'') <> '' AND coalesce(trim(street_name),'') <> '' AND zip IS NOT NULL),
hh AS (
  SELECT u, count(*) nfam, sum(prim::INT) nprim, bool_and(lt = 'S') sf, sum(np) adults, sum(np) FILTER (WHERE prim) adults_prim,
         len(list_distinct(flatten(list(sn)))) nsn, len(list_distinct(flatten(list(sn) FILTER (WHERE prim)))) nsn_prim,
         max(ch) FILTER (WHERE prim) children
  FROM f GROUP BY bld, u)
SELECT 2014, CASE WHEN nprim = 0 THEN 'no_primary' WHEN nprim > 1 THEN 'multi_primary'
                WHEN u IS NULL AND sf THEN 'sf_nounit' WHEN u IS NOT NULL THEN 'unit' ELSE 'other_nounit' END grp,
       least(adults, 10), least(nfam, 6), least(nsn, 6), least(coalesce(adults_prim, 0), 6), least(coalesce(nsn_prim, 0), 4),
       least(coalesce(children, 0), 5), count(*)
FROM hh GROUP BY ALL;
INSERT INTO res
WITH f AS (
  SELECT hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num))) bld,
         nullif(upper(trim(unit_num)), '') u, primary_family_ind prim, location_type lt, children_hh_count ch,
         list_filter([hn(last_name_1), hn(last_name_2), hn(last_name_3), hn(last_name_4), hn(last_name_5)], x -> x IS NOT NULL) sn,
         len(list_filter([individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5], x -> x IS NOT NULL)) np
  FROM cl.dataaxle_annual
  WHERE record_year = 2015 AND address_type = 'S' AND coalesce(trim(house_num),'') <> '' AND coalesce(trim(street_name),'') <> '' AND zip IS NOT NULL),
hh AS (
  SELECT u, count(*) nfam, sum(prim::INT) nprim, bool_and(lt = 'S') sf, sum(np) adults, sum(np) FILTER (WHERE prim) adults_prim,
         len(list_distinct(flatten(list(sn)))) nsn, len(list_distinct(flatten(list(sn) FILTER (WHERE prim)))) nsn_prim,
         max(ch) FILTER (WHERE prim) children
  FROM f GROUP BY bld, u)
SELECT 2015, CASE WHEN nprim = 0 THEN 'no_primary' WHEN nprim > 1 THEN 'multi_primary'
                WHEN u IS NULL AND sf THEN 'sf_nounit' WHEN u IS NOT NULL THEN 'unit' ELSE 'other_nounit' END grp,
       least(adults, 10), least(nfam, 6), least(nsn, 6), least(coalesce(adults_prim, 0), 6), least(coalesce(nsn_prim, 0), 4),
       least(coalesce(children, 0), 5), count(*)
FROM hh GROUP BY ALL;
INSERT INTO res
WITH f AS (
  SELECT hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num))) bld,
         nullif(upper(trim(unit_num)), '') u, primary_family_ind prim, location_type lt, children_hh_count ch,
         list_filter([hn(last_name_1), hn(last_name_2), hn(last_name_3), hn(last_name_4), hn(last_name_5)], x -> x IS NOT NULL) sn,
         len(list_filter([individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5], x -> x IS NOT NULL)) np
  FROM cl.dataaxle_annual
  WHERE record_year = 2016 AND address_type = 'S' AND coalesce(trim(house_num),'') <> '' AND coalesce(trim(street_name),'') <> '' AND zip IS NOT NULL),
hh AS (
  SELECT u, count(*) nfam, sum(prim::INT) nprim, bool_and(lt = 'S') sf, sum(np) adults, sum(np) FILTER (WHERE prim) adults_prim,
         len(list_distinct(flatten(list(sn)))) nsn, len(list_distinct(flatten(list(sn) FILTER (WHERE prim)))) nsn_prim,
         max(ch) FILTER (WHERE prim) children
  FROM f GROUP BY bld, u)
SELECT 2016, CASE WHEN nprim = 0 THEN 'no_primary' WHEN nprim > 1 THEN 'multi_primary'
                WHEN u IS NULL AND sf THEN 'sf_nounit' WHEN u IS NOT NULL THEN 'unit' ELSE 'other_nounit' END grp,
       least(adults, 10), least(nfam, 6), least(nsn, 6), least(coalesce(adults_prim, 0), 6), least(coalesce(nsn_prim, 0), 4),
       least(coalesce(children, 0), 5), count(*)
FROM hh GROUP BY ALL;
INSERT INTO res
WITH f AS (
  SELECT hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num))) bld,
         nullif(upper(trim(unit_num)), '') u, primary_family_ind prim, location_type lt, children_hh_count ch,
         list_filter([hn(last_name_1), hn(last_name_2), hn(last_name_3), hn(last_name_4), hn(last_name_5)], x -> x IS NOT NULL) sn,
         len(list_filter([individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5], x -> x IS NOT NULL)) np
  FROM cl.dataaxle_annual
  WHERE record_year = 2017 AND address_type = 'S' AND coalesce(trim(house_num),'') <> '' AND coalesce(trim(street_name),'') <> '' AND zip IS NOT NULL),
hh AS (
  SELECT u, count(*) nfam, sum(prim::INT) nprim, bool_and(lt = 'S') sf, sum(np) adults, sum(np) FILTER (WHERE prim) adults_prim,
         len(list_distinct(flatten(list(sn)))) nsn, len(list_distinct(flatten(list(sn) FILTER (WHERE prim)))) nsn_prim,
         max(ch) FILTER (WHERE prim) children
  FROM f GROUP BY bld, u)
SELECT 2017, CASE WHEN nprim = 0 THEN 'no_primary' WHEN nprim > 1 THEN 'multi_primary'
                WHEN u IS NULL AND sf THEN 'sf_nounit' WHEN u IS NOT NULL THEN 'unit' ELSE 'other_nounit' END grp,
       least(adults, 10), least(nfam, 6), least(nsn, 6), least(coalesce(adults_prim, 0), 6), least(coalesce(nsn_prim, 0), 4),
       least(coalesce(children, 0), 5), count(*)
FROM hh GROUP BY ALL;
INSERT INTO res
WITH f AS (
  SELECT hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num))) bld,
         nullif(upper(trim(unit_num)), '') u, primary_family_ind prim, location_type lt, children_hh_count ch,
         list_filter([hn(last_name_1), hn(last_name_2), hn(last_name_3), hn(last_name_4), hn(last_name_5)], x -> x IS NOT NULL) sn,
         len(list_filter([individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5], x -> x IS NOT NULL)) np
  FROM cl.dataaxle_annual
  WHERE record_year = 2018 AND address_type = 'S' AND coalesce(trim(house_num),'') <> '' AND coalesce(trim(street_name),'') <> '' AND zip IS NOT NULL),
hh AS (
  SELECT u, count(*) nfam, sum(prim::INT) nprim, bool_and(lt = 'S') sf, sum(np) adults, sum(np) FILTER (WHERE prim) adults_prim,
         len(list_distinct(flatten(list(sn)))) nsn, len(list_distinct(flatten(list(sn) FILTER (WHERE prim)))) nsn_prim,
         max(ch) FILTER (WHERE prim) children
  FROM f GROUP BY bld, u)
SELECT 2018, CASE WHEN nprim = 0 THEN 'no_primary' WHEN nprim > 1 THEN 'multi_primary'
                WHEN u IS NULL AND sf THEN 'sf_nounit' WHEN u IS NOT NULL THEN 'unit' ELSE 'other_nounit' END grp,
       least(adults, 10), least(nfam, 6), least(nsn, 6), least(coalesce(adults_prim, 0), 6), least(coalesce(nsn_prim, 0), 4),
       least(coalesce(children, 0), 5), count(*)
FROM hh GROUP BY ALL;
INSERT INTO res
WITH f AS (
  SELECT hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num))) bld,
         nullif(upper(trim(unit_num)), '') u, primary_family_ind prim, location_type lt, children_hh_count ch,
         list_filter([hn(last_name_1), hn(last_name_2), hn(last_name_3), hn(last_name_4), hn(last_name_5)], x -> x IS NOT NULL) sn,
         len(list_filter([individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5], x -> x IS NOT NULL)) np
  FROM cl.dataaxle_annual
  WHERE record_year = 2019 AND address_type = 'S' AND coalesce(trim(house_num),'') <> '' AND coalesce(trim(street_name),'') <> '' AND zip IS NOT NULL),
hh AS (
  SELECT u, count(*) nfam, sum(prim::INT) nprim, bool_and(lt = 'S') sf, sum(np) adults, sum(np) FILTER (WHERE prim) adults_prim,
         len(list_distinct(flatten(list(sn)))) nsn, len(list_distinct(flatten(list(sn) FILTER (WHERE prim)))) nsn_prim,
         max(ch) FILTER (WHERE prim) children
  FROM f GROUP BY bld, u)
SELECT 2019, CASE WHEN nprim = 0 THEN 'no_primary' WHEN nprim > 1 THEN 'multi_primary'
                WHEN u IS NULL AND sf THEN 'sf_nounit' WHEN u IS NOT NULL THEN 'unit' ELSE 'other_nounit' END grp,
       least(adults, 10), least(nfam, 6), least(nsn, 6), least(coalesce(adults_prim, 0), 6), least(coalesce(nsn_prim, 0), 4),
       least(coalesce(children, 0), 5), count(*)
FROM hh GROUP BY ALL;
INSERT INTO res
WITH f AS (
  SELECT hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num))) bld,
         nullif(upper(trim(unit_num)), '') u, primary_family_ind prim, location_type lt, children_hh_count ch,
         list_filter([hn(last_name_1), hn(last_name_2), hn(last_name_3), hn(last_name_4), hn(last_name_5)], x -> x IS NOT NULL) sn,
         len(list_filter([individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5], x -> x IS NOT NULL)) np
  FROM cl.dataaxle_annual
  WHERE record_year = 2020 AND address_type = 'S' AND coalesce(trim(house_num),'') <> '' AND coalesce(trim(street_name),'') <> '' AND zip IS NOT NULL),
hh AS (
  SELECT u, count(*) nfam, sum(prim::INT) nprim, bool_and(lt = 'S') sf, sum(np) adults, sum(np) FILTER (WHERE prim) adults_prim,
         len(list_distinct(flatten(list(sn)))) nsn, len(list_distinct(flatten(list(sn) FILTER (WHERE prim)))) nsn_prim,
         max(ch) FILTER (WHERE prim) children
  FROM f GROUP BY bld, u)
SELECT 2020, CASE WHEN nprim = 0 THEN 'no_primary' WHEN nprim > 1 THEN 'multi_primary'
                WHEN u IS NULL AND sf THEN 'sf_nounit' WHEN u IS NOT NULL THEN 'unit' ELSE 'other_nounit' END grp,
       least(adults, 10), least(nfam, 6), least(nsn, 6), least(coalesce(adults_prim, 0), 6), least(coalesce(nsn_prim, 0), 4),
       least(coalesce(children, 0), 5), count(*)
FROM hh GROUP BY ALL;
INSERT INTO res
WITH f AS (
  SELECT hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num))) bld,
         nullif(upper(trim(unit_num)), '') u, primary_family_ind prim, location_type lt, children_hh_count ch,
         list_filter([hn(last_name_1), hn(last_name_2), hn(last_name_3), hn(last_name_4), hn(last_name_5)], x -> x IS NOT NULL) sn,
         len(list_filter([individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5], x -> x IS NOT NULL)) np
  FROM cl.dataaxle_annual
  WHERE record_year = 2021 AND address_type = 'S' AND coalesce(trim(house_num),'') <> '' AND coalesce(trim(street_name),'') <> '' AND zip IS NOT NULL),
hh AS (
  SELECT u, count(*) nfam, sum(prim::INT) nprim, bool_and(lt = 'S') sf, sum(np) adults, sum(np) FILTER (WHERE prim) adults_prim,
         len(list_distinct(flatten(list(sn)))) nsn, len(list_distinct(flatten(list(sn) FILTER (WHERE prim)))) nsn_prim,
         max(ch) FILTER (WHERE prim) children
  FROM f GROUP BY bld, u)
SELECT 2021, CASE WHEN nprim = 0 THEN 'no_primary' WHEN nprim > 1 THEN 'multi_primary'
                WHEN u IS NULL AND sf THEN 'sf_nounit' WHEN u IS NOT NULL THEN 'unit' ELSE 'other_nounit' END grp,
       least(adults, 10), least(nfam, 6), least(nsn, 6), least(coalesce(adults_prim, 0), 6), least(coalesce(nsn_prim, 0), 4),
       least(coalesce(children, 0), 5), count(*)
FROM hh GROUP BY ALL;
INSERT INTO res
WITH f AS (
  SELECT hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num))) bld,
         nullif(upper(trim(unit_num)), '') u, primary_family_ind prim, location_type lt, children_hh_count ch,
         list_filter([hn(last_name_1), hn(last_name_2), hn(last_name_3), hn(last_name_4), hn(last_name_5)], x -> x IS NOT NULL) sn,
         len(list_filter([individual_id_1, individual_id_2, individual_id_3, individual_id_4, individual_id_5], x -> x IS NOT NULL)) np
  FROM cl.dataaxle_annual
  WHERE record_year = 2023 AND address_type = 'S' AND coalesce(trim(house_num),'') <> '' AND coalesce(trim(street_name),'') <> '' AND zip IS NOT NULL),
hh AS (
  SELECT u, count(*) nfam, sum(prim::INT) nprim, bool_and(lt = 'S') sf, sum(np) adults, sum(np) FILTER (WHERE prim) adults_prim,
         len(list_distinct(flatten(list(sn)))) nsn, len(list_distinct(flatten(list(sn) FILTER (WHERE prim)))) nsn_prim,
         max(ch) FILTER (WHERE prim) children
  FROM f GROUP BY bld, u)
SELECT 2023, CASE WHEN nprim = 0 THEN 'no_primary' WHEN nprim > 1 THEN 'multi_primary'
                WHEN u IS NULL AND sf THEN 'sf_nounit' WHEN u IS NOT NULL THEN 'unit' ELSE 'other_nounit' END grp,
       least(adults, 10), least(nfam, 6), least(nsn, 6), least(coalesce(adults_prim, 0), 6), least(coalesce(nsn_prim, 0), 4),
       least(coalesce(children, 0), 5), count(*)
FROM hh GROUP BY ALL;
COPY (SELECT * FROM res ORDER BY ALL) TO '/home/jr2728/ptax_explore/hh_strat/out/hh_surnames.csv' (HEADER);
