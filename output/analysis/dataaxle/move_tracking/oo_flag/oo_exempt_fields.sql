SET preserve_insertion_order=false; USE memory; SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
COPY (SELECT situs_state st, tax_year y, count(*) n, avg((coalesce(total_tax_exemption_amount,0) > 0)::INT) tot_ex, avg((coalesce(calculated_total_tax_exemption_amount,0) > 0)::INT) calc_ex,
             avg((coalesce(homestead_exempt_indicator,false))::INT) hs
      FROM read_parquet('/home/jr2728/ptax_explore/samples/tax_bill_bld1pct.parquet') t
      WHERE property_indicator_code = 10 AND tax_year IN (2015, 2019) GROUP BY ALL HAVING count(*) >= 1000 ORDER BY st, y)
  TO '/home/jr2728/ptax_explore/hh_strat/out/oo_exempt_fields.csv' (HEADER);
