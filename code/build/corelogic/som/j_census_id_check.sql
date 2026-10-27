-- What is cl.tax.census_id? Format, length, fill by tax year (0.01% system sample).
SET temp_directory='/home/jr2728/ptax_explore/tmp'; SET threads=4;
ATTACH '/gpfs/project/corelogic_database/corelogic_public.db' AS cl (READ_ONLY);
.timer on
SELECT tax_year, count(*) n, round(100 * avg((census_id IS NOT NULL AND trim(census_id::VARCHAR) <> '')::INT), 1) filled,
       mode(length(census_id::VARCHAR)) len_mode, min(census_id::VARCHAR) ex_min, max(census_id::VARCHAR) ex_max
FROM cl.tax TABLESAMPLE 0.01% (system) WHERE tax_year IN (2016, 2017, 2018, 2019, 2021, 2023) GROUP BY 1 ORDER BY 1;
SELECT lpad(fips_code::VARCHAR, 5, '0') fips, census_id, typeof(census_id) t FROM cl.tax TABLESAMPLE 0.01% (system) WHERE tax_year = 2018 AND census_id IS NOT NULL LIMIT 8;
