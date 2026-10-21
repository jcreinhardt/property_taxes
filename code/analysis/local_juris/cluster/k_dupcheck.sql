SET temp_directory='/home/jr2728/ptax_explore/tmp'; SET threads=4;
ATTACH '/gpfs/project/corelogic_database/corelogic_public.db' AS cl (READ_ONLY);
.timer on
SELECT tax_year, count(*) n, count(DISTINCT clip) nclip FROM cl.tax WHERE tax_year BETWEEN 2012 AND 2024 AND lpad(fips_code::VARCHAR,5,'0') IN ('09009','17031','36119','48201','06037','12086','34013') GROUP BY 1 ORDER BY 1;
SELECT tax_year, count(*) n FROM cl.tax TABLESAMPLE 0.01% (system) WHERE tax_year BETWEEN 2010 AND 2025 GROUP BY 1 ORDER BY 1;
