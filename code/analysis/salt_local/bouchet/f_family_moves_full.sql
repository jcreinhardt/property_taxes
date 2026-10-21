-- Full-data cross-border family moves (Bouchet devel). Families whose address is in a ZIP near a state border (border_zips.csv,
-- 8,555 ZIPs of tracts within 20 mi of a border pair) in consecutive annual files; one record per family-year (primary
-- preferred, latest recency); a move = building key changes and length_of_residence <= 2 at t. Year pairs processed one at a
-- time to fit 60 GB. No names or ids exported. Output ~/ptax_explore/border/family_moves_full_YYYY.parquet (destination year).
SET preserve_insertion_order=false;
CREATE TEMP TABLE bz AS SELECT lpad(zip::VARCHAR, 5, '0') zip FROM read_csv('/home/jr2728/ptax_explore/border/border_zips.csv', all_varchar = true);
CREATE OR REPLACE TEMP TABLE f2012 AS
SELECT family_id, arg_max(struct_pack(bkey := hash(lpad(zip::VARCHAR, 5, '0'), upper(trim(street_name)), upper(trim(house_num))),
       lat := nullif(ge_latitude_2010, 0), lon := nullif(ge_longitude_2010, 0), st := ge_census_state_2010, lor := length_of_residence),
       (coalesce(primary_family_ind, false)::INT, recency_date)) a
FROM cl.dataaxle_annual WHERE record_year = 2012 AND address_type = 'S' AND lpad(zip::VARCHAR, 5, '0') IN (SELECT zip FROM bz) GROUP BY family_id;
CREATE OR REPLACE TEMP TABLE f2013 AS
SELECT family_id, arg_max(struct_pack(bkey := hash(lpad(zip::VARCHAR, 5, '0'), upper(trim(street_name)), upper(trim(house_num))),
       lat := nullif(ge_latitude_2010, 0), lon := nullif(ge_longitude_2010, 0), st := ge_census_state_2010, lor := length_of_residence),
       (coalesce(primary_family_ind, false)::INT, recency_date)) a
FROM cl.dataaxle_annual WHERE record_year = 2013 AND address_type = 'S' AND lpad(zip::VARCHAR, 5, '0') IN (SELECT zip FROM bz) GROUP BY family_id;
COPY (SELECT 2013 y, a.a.lat olat, a.a.lon olon, a.a.st ost, b.a.lat dlat, b.a.lon dlon, b.a.st dst FROM f2012 a JOIN f2013 b USING (family_id)
      WHERE a.a.bkey <> b.a.bkey AND coalesce(b.a.lor, 99) <= 2 AND a.a.lat IS NOT NULL AND b.a.lat IS NOT NULL)
  TO '/home/jr2728/ptax_explore/border/family_moves_full_2013.parquet' (FORMAT parquet);
DROP TABLE f2012;
CREATE OR REPLACE TEMP TABLE f2014 AS
SELECT family_id, arg_max(struct_pack(bkey := hash(lpad(zip::VARCHAR, 5, '0'), upper(trim(street_name)), upper(trim(house_num))),
       lat := nullif(ge_latitude_2010, 0), lon := nullif(ge_longitude_2010, 0), st := ge_census_state_2010, lor := length_of_residence),
       (coalesce(primary_family_ind, false)::INT, recency_date)) a
FROM cl.dataaxle_annual WHERE record_year = 2014 AND address_type = 'S' AND lpad(zip::VARCHAR, 5, '0') IN (SELECT zip FROM bz) GROUP BY family_id;
COPY (SELECT 2014 y, a.a.lat olat, a.a.lon olon, a.a.st ost, b.a.lat dlat, b.a.lon dlon, b.a.st dst FROM f2013 a JOIN f2014 b USING (family_id)
      WHERE a.a.bkey <> b.a.bkey AND coalesce(b.a.lor, 99) <= 2 AND a.a.lat IS NOT NULL AND b.a.lat IS NOT NULL)
  TO '/home/jr2728/ptax_explore/border/family_moves_full_2014.parquet' (FORMAT parquet);
DROP TABLE f2013;
CREATE OR REPLACE TEMP TABLE f2015 AS
SELECT family_id, arg_max(struct_pack(bkey := hash(lpad(zip::VARCHAR, 5, '0'), upper(trim(street_name)), upper(trim(house_num))),
       lat := nullif(ge_latitude_2010, 0), lon := nullif(ge_longitude_2010, 0), st := ge_census_state_2010, lor := length_of_residence),
       (coalesce(primary_family_ind, false)::INT, recency_date)) a
FROM cl.dataaxle_annual WHERE record_year = 2015 AND address_type = 'S' AND lpad(zip::VARCHAR, 5, '0') IN (SELECT zip FROM bz) GROUP BY family_id;
COPY (SELECT 2015 y, a.a.lat olat, a.a.lon olon, a.a.st ost, b.a.lat dlat, b.a.lon dlon, b.a.st dst FROM f2014 a JOIN f2015 b USING (family_id)
      WHERE a.a.bkey <> b.a.bkey AND coalesce(b.a.lor, 99) <= 2 AND a.a.lat IS NOT NULL AND b.a.lat IS NOT NULL)
  TO '/home/jr2728/ptax_explore/border/family_moves_full_2015.parquet' (FORMAT parquet);
DROP TABLE f2014;
CREATE OR REPLACE TEMP TABLE f2016 AS
SELECT family_id, arg_max(struct_pack(bkey := hash(lpad(zip::VARCHAR, 5, '0'), upper(trim(street_name)), upper(trim(house_num))),
       lat := nullif(ge_latitude_2010, 0), lon := nullif(ge_longitude_2010, 0), st := ge_census_state_2010, lor := length_of_residence),
       (coalesce(primary_family_ind, false)::INT, recency_date)) a
FROM cl.dataaxle_annual WHERE record_year = 2016 AND address_type = 'S' AND lpad(zip::VARCHAR, 5, '0') IN (SELECT zip FROM bz) GROUP BY family_id;
COPY (SELECT 2016 y, a.a.lat olat, a.a.lon olon, a.a.st ost, b.a.lat dlat, b.a.lon dlon, b.a.st dst FROM f2015 a JOIN f2016 b USING (family_id)
      WHERE a.a.bkey <> b.a.bkey AND coalesce(b.a.lor, 99) <= 2 AND a.a.lat IS NOT NULL AND b.a.lat IS NOT NULL)
  TO '/home/jr2728/ptax_explore/border/family_moves_full_2016.parquet' (FORMAT parquet);
DROP TABLE f2015;
CREATE OR REPLACE TEMP TABLE f2017 AS
SELECT family_id, arg_max(struct_pack(bkey := hash(lpad(zip::VARCHAR, 5, '0'), upper(trim(street_name)), upper(trim(house_num))),
       lat := nullif(ge_latitude_2010, 0), lon := nullif(ge_longitude_2010, 0), st := ge_census_state_2010, lor := length_of_residence),
       (coalesce(primary_family_ind, false)::INT, recency_date)) a
FROM cl.dataaxle_annual WHERE record_year = 2017 AND address_type = 'S' AND lpad(zip::VARCHAR, 5, '0') IN (SELECT zip FROM bz) GROUP BY family_id;
COPY (SELECT 2017 y, a.a.lat olat, a.a.lon olon, a.a.st ost, b.a.lat dlat, b.a.lon dlon, b.a.st dst FROM f2016 a JOIN f2017 b USING (family_id)
      WHERE a.a.bkey <> b.a.bkey AND coalesce(b.a.lor, 99) <= 2 AND a.a.lat IS NOT NULL AND b.a.lat IS NOT NULL)
  TO '/home/jr2728/ptax_explore/border/family_moves_full_2017.parquet' (FORMAT parquet);
DROP TABLE f2016;
CREATE OR REPLACE TEMP TABLE f2018 AS
SELECT family_id, arg_max(struct_pack(bkey := hash(lpad(zip::VARCHAR, 5, '0'), upper(trim(street_name)), upper(trim(house_num))),
       lat := nullif(ge_latitude_2010, 0), lon := nullif(ge_longitude_2010, 0), st := ge_census_state_2010, lor := length_of_residence),
       (coalesce(primary_family_ind, false)::INT, recency_date)) a
FROM cl.dataaxle_annual WHERE record_year = 2018 AND address_type = 'S' AND lpad(zip::VARCHAR, 5, '0') IN (SELECT zip FROM bz) GROUP BY family_id;
COPY (SELECT 2018 y, a.a.lat olat, a.a.lon olon, a.a.st ost, b.a.lat dlat, b.a.lon dlon, b.a.st dst FROM f2017 a JOIN f2018 b USING (family_id)
      WHERE a.a.bkey <> b.a.bkey AND coalesce(b.a.lor, 99) <= 2 AND a.a.lat IS NOT NULL AND b.a.lat IS NOT NULL)
  TO '/home/jr2728/ptax_explore/border/family_moves_full_2018.parquet' (FORMAT parquet);
DROP TABLE f2017;
CREATE OR REPLACE TEMP TABLE f2019 AS
SELECT family_id, arg_max(struct_pack(bkey := hash(lpad(zip::VARCHAR, 5, '0'), upper(trim(street_name)), upper(trim(house_num))),
       lat := nullif(ge_latitude_2010, 0), lon := nullif(ge_longitude_2010, 0), st := ge_census_state_2010, lor := length_of_residence),
       (coalesce(primary_family_ind, false)::INT, recency_date)) a
FROM cl.dataaxle_annual WHERE record_year = 2019 AND address_type = 'S' AND lpad(zip::VARCHAR, 5, '0') IN (SELECT zip FROM bz) GROUP BY family_id;
COPY (SELECT 2019 y, a.a.lat olat, a.a.lon olon, a.a.st ost, b.a.lat dlat, b.a.lon dlon, b.a.st dst FROM f2018 a JOIN f2019 b USING (family_id)
      WHERE a.a.bkey <> b.a.bkey AND coalesce(b.a.lor, 99) <= 2 AND a.a.lat IS NOT NULL AND b.a.lat IS NOT NULL)
  TO '/home/jr2728/ptax_explore/border/family_moves_full_2019.parquet' (FORMAT parquet);
DROP TABLE f2018;
CREATE OR REPLACE TEMP TABLE f2020 AS
SELECT family_id, arg_max(struct_pack(bkey := hash(lpad(zip::VARCHAR, 5, '0'), upper(trim(street_name)), upper(trim(house_num))),
       lat := nullif(ge_latitude_2010, 0), lon := nullif(ge_longitude_2010, 0), st := ge_census_state_2010, lor := length_of_residence),
       (coalesce(primary_family_ind, false)::INT, recency_date)) a
FROM cl.dataaxle_annual WHERE record_year = 2020 AND address_type = 'S' AND lpad(zip::VARCHAR, 5, '0') IN (SELECT zip FROM bz) GROUP BY family_id;
COPY (SELECT 2020 y, a.a.lat olat, a.a.lon olon, a.a.st ost, b.a.lat dlat, b.a.lon dlon, b.a.st dst FROM f2019 a JOIN f2020 b USING (family_id)
      WHERE a.a.bkey <> b.a.bkey AND coalesce(b.a.lor, 99) <= 2 AND a.a.lat IS NOT NULL AND b.a.lat IS NOT NULL)
  TO '/home/jr2728/ptax_explore/border/family_moves_full_2020.parquet' (FORMAT parquet);
DROP TABLE f2019;
CREATE OR REPLACE TEMP TABLE f2021 AS
SELECT family_id, arg_max(struct_pack(bkey := hash(lpad(zip::VARCHAR, 5, '0'), upper(trim(street_name)), upper(trim(house_num))),
       lat := nullif(ge_latitude_2010, 0), lon := nullif(ge_longitude_2010, 0), st := ge_census_state_2010, lor := length_of_residence),
       (coalesce(primary_family_ind, false)::INT, recency_date)) a
FROM cl.dataaxle_annual WHERE record_year = 2021 AND address_type = 'S' AND lpad(zip::VARCHAR, 5, '0') IN (SELECT zip FROM bz) GROUP BY family_id;
COPY (SELECT 2021 y, a.a.lat olat, a.a.lon olon, a.a.st ost, b.a.lat dlat, b.a.lon dlon, b.a.st dst FROM f2020 a JOIN f2021 b USING (family_id)
      WHERE a.a.bkey <> b.a.bkey AND coalesce(b.a.lor, 99) <= 2 AND a.a.lat IS NOT NULL AND b.a.lat IS NOT NULL)
  TO '/home/jr2728/ptax_explore/border/family_moves_full_2021.parquet' (FORMAT parquet);
DROP TABLE f2020;
