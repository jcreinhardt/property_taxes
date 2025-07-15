# Taxes
duckdb data/corelogic.db -s "copy (select fips_code // 1000 as state, count(1) as n from tax group by state order by n desc) to '/dev/stdout' with (format csv, header)" \
     | uplot bar -d, -H -t "Counts by state"

duckdb data/corelogic.db -s "copy (select tax_year, count(1) as n from tax group by tax_year order by tax_year) to '/dev/stdout' with (format csv, header)" \
     | uplot bar -d, -H -t "Counts by tax year"

duckdb data/corelogic.db -s "copy(
with sales AS (
 SELECT
 clip,
 sale_amount,
 sale_derived_date
 FROM ownertransfer
 WHERE windsorized = true AND primary_category_code = 'A' AND month(sale_derived_date) != 12
),
tax_after_sale AS (
 SELECT
 s.clip,
 s.sale_amount,
 s.sale_derived_date,
 t.tax_year,
 t.calculated_total_value
 FROM sales s
 JOIN tax t ON s.clip = t.clip
 WHERE t.tax_year = year(s.sale_derived_date) - 1
 AND t.windsorized = true
)
SELECT
 tax_year,
 round(mean(sale_amount)) AS mean_sale_amount,
 round(mean(calculated_total_value)) AS mean_calculated_value
FROM tax_after_sale
GROUP BY tax_year
ORDER BY tax_year
) to '/dev/stdout' with (format csv, header)" \
 | uplot scatter -d, -C -H --xlabel "Tax Year" --ylabel "Amount" -t "Mean sale amount and calculated value by tax year"



