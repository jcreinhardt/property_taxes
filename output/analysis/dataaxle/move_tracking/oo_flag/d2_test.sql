SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
ATTACH '/nfs/roberts/pi/pi_csl53/corelogic/database/corelogic.db' AS cl (READ_ONLY);
SELECT count(*) FROM (SELECT * FROM cl.tax USING SAMPLE 0.001% (system));
SELECT count(*), count(DISTINCT tax_year) FROM read_parquet('/home/jr2728/ptax_explore/samples/tax_bill_bld1pct.parquet');
