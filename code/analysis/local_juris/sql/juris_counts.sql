-- County x tax_year x jurisdiction-field cells from the full CoreLogic tax roll (SOM copy, read-only).
-- One scan, GROUPING SETS over the candidate jurisdiction fields; sums of bills and values for rate/revenue work.
-- __SAMPLE__ is replaced by '' (full) or 'USING SAMPLE 0.01% (system)' (timing test).
SET temp_directory='/home/jr2728/ptax_explore/tmp'; SET preserve_insertion_order=false;
ATTACH '/gpfs/project/corelogic_database/corelogic_public.db' AS cl (READ_ONLY);
.timer on
COPY (
  WITH t AS (
    SELECT fips_code fips, situs_state st, tax_year,
           nullif(trim(tax_area_code),'') tac, nullif(trim(tax_rate_area_code),'') trac,
           nullif(trim(municipality_code),'') muni_code, nullif(upper(trim(municipality_name)),'') muni_name,
           nullif(upper(trim(tax_district_municipality_name)),'') tax_muni, nullif(upper(trim(township)),'') township,
           nullif(upper(trim(school_district_name)),'') school_dist,
           property_indicator_code IN ('10','11','21','22') resi,
           total_tax_amount bill, calculated_total_value mv, assessed_total_value av, total_property_tax_rate_percent rate_pct
    FROM cl.tax __SAMPLE__
  )
  SELECT CASE WHEN grouping(tac)=0 THEN 'tac' WHEN grouping(trac)=0 THEN 'trac' WHEN grouping(muni_code)=0 THEN 'muni_code'
              WHEN grouping(muni_name)=0 THEN 'muni_name' WHEN grouping(tax_muni)=0 THEN 'tax_muni'
              WHEN grouping(township)=0 THEN 'township' WHEN grouping(school_dist)=0 THEN 'school_dist' ELSE 'county' END field,
         coalesce(tac, trac, muni_code, muni_name, tax_muni, township, school_dist) val,
         fips, tax_year, any_value(st) st,
         count(*) n, count(*) FILTER (resi) n_resi,
         count(*) FILTER (bill > 0 AND mv > 0) n_bv,
         sum(bill) FILTER (bill > 0 AND mv > 0) sum_bill, sum(mv) FILTER (bill > 0 AND mv > 0) sum_mv,
         sum(bill) FILTER (bill > 0 AND av > 0) sum_bill_av, sum(av) FILTER (bill > 0 AND av > 0) sum_av,
         sum(bill) FILTER (bill > 0 AND mv > 0 AND resi) sum_bill_resi, sum(mv) FILTER (bill > 0 AND mv > 0 AND resi) sum_mv_resi,
         count(rate_pct) FILTER (rate_pct > 0) n_rate, median(rate_pct) FILTER (rate_pct > 0) med_rate_pct
  FROM t
  GROUP BY GROUPING SETS ((fips, tax_year), (fips, tax_year, tac), (fips, tax_year, trac), (fips, tax_year, muni_code),
                          (fips, tax_year, muni_name), (fips, tax_year, tax_muni), (fips, tax_year, township),
                          (fips, tax_year, school_dist))
) TO '/home/jr2728/ptax_explore/juris_cells__TAG__.parquet' (FORMAT parquet);
SELECT field, count(*) cells, count(DISTINCT fips||'|'||coalesce(val,'')) units FROM '/home/jr2728/ptax_explore/juris_cells__TAG__.parquet' GROUP BY 1 ORDER BY 1;
