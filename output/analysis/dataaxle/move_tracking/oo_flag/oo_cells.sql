SET preserve_insertion_order=false; USE memory; SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
-- Why some states are not testable: tax-area coverage, tax amount fill, Michigan.
COPY (SELECT situs_state st, count(*) n, avg((coalesce(tax_area_code, tax_rate_area_code) IS NOT NULL)::INT) has_tac,
             avg((total_tax_amount > 0)::INT) has_tax, avg((assessed_total_value > 0)::INT) has_av, avg((calculated_total_value > 0)::INT) has_mv,
             avg((assessed_total_value = calculated_total_value)::INT) FILTER (WHERE assessed_total_value > 0) av_eq_mv,
             count(DISTINCT fips_code) counties, count(DISTINCT (fips_code, coalesce(tax_area_code, tax_rate_area_code, ''))) cells
      FROM read_parquet('/home/jr2728/ptax_explore/samples/tax_bill_bld1pct.parquet')
      WHERE property_indicator_code = 10 AND tax_year = 2019 GROUP BY 1 ORDER BY 1) TO '/home/jr2728/ptax_explore/hh_strat/out/oo_cells_2019.csv' (HEADER);
