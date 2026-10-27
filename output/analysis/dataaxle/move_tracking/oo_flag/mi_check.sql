SET preserve_insertion_order=false; USE memory; SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
SELECT count(*) n, avg((total_tax_amount>0)::INT) tax_pos, median(total_tax_amount) med_tax, median(assessed_total_value) med_av, median(calculated_total_value) med_mv,
       median(total_tax_amount / nullif(calculated_total_value,0)) med_r, avg((total_tax_amount / nullif(calculated_total_value,0) BETWEEN 0.0005 AND 0.1)::INT) in_range,
       avg((coalesce(calculated_improvement_value,0) > 0)::INT) has_impr
FROM read_parquet('/home/jr2728/ptax_explore/samples/tax_bill_bld1pct.parquet') WHERE situs_state='MI' AND property_indicator_code=10 AND tax_year=2019;
