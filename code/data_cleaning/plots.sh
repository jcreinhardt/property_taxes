# Taxes
duckdb data/corelogic.db -s "copy (select fips_code // 1000 as state, count(1) as n from tax group by state order by n desc) to '/dev/stdout' with (format csv, header)" \
     | uplot bar -d, -H -t "Counts by state"

duckdb data/corelogic.db -s "copy (select tax_year, count(1) as n from tax group by tax_year order by tax_year) to '/dev/stdout' with (format csv, header)" \
     | uplot bar -d, -H -t "Counts by tax year"


