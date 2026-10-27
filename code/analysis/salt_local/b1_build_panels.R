# Build analysis panels from the SOM extract (cluster/k_panel*.sql; files in data/local_juris/kpanel/):
#  tract_year : residential roll (parcels, bills, AV) + arm's-length sales (n, median price, effective rate, assessment ratio)
#  county_year: same aggregates by county; sales-based assessment ratio and regressivity (slope of log(AV/price) on log price;
#               PRD = mean ratio / price-weighted mean ratio); nominal rate = median bill/AV over residential parcels
#  disp_year  : within-county dispersion across tax areas (SD of log median bill/AV, parcel-weighted) and across tracts
#               (SD of tract median effective rates, tracts with >= 10 sales)
# Effective rate = bill / price kept 0.05-10%; ratio = AV / price kept 0.01-5 (AV is assessed, often fractional, value).
suppressPackageStartupMessages({library(data.table); library(duckdb)})
source("code/R/paths.R"); K <- file.path(ROOT, "data/local_juris/kpanel")
con <- dbConnect(duckdb()); q <- function(s) as.data.table(dbGetQuery(con, s))
dbExecute(con, sprintf("CREATE VIEW s AS SELECT *, bill / price eff, av / price ratio FROM read_parquet('%s/sales_*.parquet')
  WHERE NOT coalesce(newcon, false) AND price > 10000", K))
roll_t <- q(sprintf("SELECT tract, fips, y, sum(n) n, sum(n_bill) n_bill, sum(bill) bill, sum(av) av, sum(mv) mv,
  max(med_nom) FILTER (WHERE res) med_nom_res FROM read_parquet('%s/roll_tract_*.parquet') WHERE res AND tract IS NOT NULL GROUP BY ALL", K))
sal_t <- q("SELECT tract, y, count(*) n_sales, median(price) p50, median(eff) FILTER (WHERE eff BETWEEN 0.0005 AND 0.1) eff50,
  median(ratio) FILTER (WHERE ratio BETWEEN 0.01 AND 5) ratio50 FROM s WHERE tract IS NOT NULL GROUP BY ALL")
tract_year <- merge(roll_t, sal_t, by = c("tract", "y"), all.x = TRUE)
roll_c <- q(sprintf("SELECT fips, y, sum(n) FILTER (WHERE res) n_res, sum(bill) FILTER (WHERE res) bill_res, sum(av) FILTER (WHERE res) av_res,
  sum(n) n_all, sum(bill) bill_all FROM read_parquet('%s/roll_tract_*.parquet') GROUP BY ALL", K))
nom_c <- q(sprintf("SELECT fips, y, sum(med_nom * n) / sum(n) nom_w, median(med_nom) nom_area_med, median(med_rate_pct) rate_pct_med,
  sqrt(greatest(sum(n * pow(ln(med_nom), 2)) / sum(n) - pow(sum(n * ln(med_nom)) / sum(n), 2), 0)) sd_log_nom_area, count(*) n_areas
  FROM read_parquet('%s/roll_area_*.parquet') WHERE med_nom > 0 AND med_nom < 1 GROUP BY ALL", K))
sal_c <- q("SELECT fips, y, count(*) n_sales, median(price) p50, median(eff) FILTER (WHERE eff BETWEEN 0.0005 AND 0.1) eff50,
  median(ratio) FILTER (WHERE ratio BETWEEN 0.01 AND 5) ratio50,
  regr_slope(ln(ratio), ln(price)) FILTER (WHERE ratio BETWEEN 0.01 AND 5) regr_slope_ratio,
  avg(ratio) FILTER (WHERE ratio BETWEEN 0.01 AND 5) / (sum(av) FILTER (WHERE ratio BETWEEN 0.01 AND 5) / sum(price) FILTER (WHERE ratio BETWEEN 0.01 AND 5)) prd
  FROM s GROUP BY ALL")
county_year <- Reduce(\(a, b) merge(a, b, by = c("fips", "y"), all = TRUE), list(roll_c, nom_c, sal_c))
disp_t <- tract_year[n_sales >= 10 & !is.na(eff50), .(sd_eff_tract = sd(100 * eff50), iqr_eff_tract = IQR(100 * eff50), n_tracts = .N), by = .(fips, y)]
county_year <- merge(county_year, disp_t, by = c("fips", "y"), all.x = TRUE)
dbDisconnect(con, shutdown = TRUE)
saveRDS(tract_year, file.path(ROOT, "data/local_juris/tract_year.rds")); saveRDS(county_year, file.path(ROOT, "data/local_juris/county_year.rds"))
cat("tract-years:", nrow(tract_year), "| county-years:", nrow(county_year), "\n"); print(county_year[, .(counties = .N, parcels_m = sum(n_res, na.rm = TRUE) / 1e6,
  sales_k = sum(n_sales, na.rm = TRUE) / 1e3), by = y][order(y)])
