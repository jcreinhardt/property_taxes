# ZIP-level SALT exposure (IRS SOI 2016 ZIP x AGI stub), same stub approximation as the county measure: SALT deduction above
# $10k per return. Mapped to 2010 tracts through the modal situs ZIP of the tract's 2016-2019 sales (CoreLogic panel extract).
# Output data/local_juris/salt_exposure_tract_zip2016.csv (tract, zip, exz, exz_z).
suppressPackageStartupMessages({library(data.table); library(duckdb)})
ROOT <- "/Users/jr2728/PhD/projects/property_taxes"
con <- dbConnect(duckdb(), file.path(ROOT, "data/database.db"), read_only = TRUE)
z <- as.data.table(dbGetQuery(con, "SELECT lpad(zipcode::INT::VARCHAR, 5, '0') zip, agi_stub, returns, taxes_paid_returns n, taxes_paid_amount a
  FROM returns_zipcode_agi WHERE year = 2016 AND zipcode NOT IN (0, 99999)")); dbDisconnect(con, shutdown = TRUE)
ez <- z[, .(ret = sum(returns), exz = sum(pmax(0, a / pmax(n, 1) - 10) * n) / sum(returns)), by = zip][ret >= 100]
con <- dbConnect(duckdb())
tz <- as.data.table(dbGetQuery(con, sprintf("SELECT tract, mode(zip) zip, count(*) n FROM read_parquet('%s/data/local_juris/kpanel/sales_201[6-9].parquet')
  WHERE tract IS NOT NULL AND zip IS NOT NULL GROUP BY tract", ROOT))); dbDisconnect(con, shutdown = TRUE)
tz <- merge(tz, ez, by = "zip")[, exz_w := pmin(exz, quantile(exz, .99))][, exz_z := (exz_w - mean(exz_w)) / sd(exz_w)]  # winsorised at p99
fwrite(tz, file.path(ROOT, "data/local_juris/salt_exposure_tract_zip2016.csv"))
cat("ZIPs:", nrow(ez), "| tracts mapped:", nrow(tz), "| exposure mean/sd ($k per return):", round(mean(tz$exz), 3), round(sd(tz$exz), 3), "\n")
