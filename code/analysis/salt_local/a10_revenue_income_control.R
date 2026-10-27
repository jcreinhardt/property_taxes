# Does the revenue slowdown in exposed counties survive an income control? Trend-break spec of a1 (school districts by county
# and county governments, CoG; weighted by returns) adding the county share of 2016 returns with AGI >= $200k (SOI stub 8)
# x trend, x post, x post x trend.
suppressPackageStartupMessages({library(data.table); library(duckdb); library(fixest)})
source("code/R/paths.R"); OUT <- file.path(ROOT, "output/analysis/salt_local")
con <- db_open("database.db")
inc <- as.data.table(dbGetQuery(con, "SELECT county::INT county, sum(returns) FILTER (WHERE agi_stub = 8) / sum(returns) sh200 FROM returns_county_agi WHERE year = 2016 GROUP BY 1")); dbDisconnect(con, shutdown = TRUE)
inc[, i_z := (sh200 - mean(sh200, na.rm = TRUE)) / sd(sh200, na.rm = TRUE)]
ex <- fread(file.path(ROOT, "data/local_juris/salt_exposure_county2016.csv"))
con <- db_open("census_finances_test.db")
qf <- function(tb) as.data.table(dbGetQuery(con, sprintf("SELECT county_fips::INT county, state_fips st, year::INT y, property_tax_rev ptax FROM %s", tb)))
fin <- list(school = qf("govt_finances_sd_by_county"), county_govt = qf("govt_finances_county")); dbDisconnect(con, shutdown = TRUE)
fin$county_govt <- fin$county_govt[, if (uniqueN(y) == 11) .SD, by = county]
cat("corr(exposure, share AGI 200k+):", round(cor(merge(ex, inc, by = "county")[, .(ex_z, i_z)], use = "complete")[1, 2], 3), "\n")
res <- rbindlist(lapply(names(fin), \(k) rbindlist(lapply(c(FALSE, TRUE), \(ctrl) {
  d <- merge(merge(fin[[k]], ex[, .(county, ex_z, returns)], by = "county"), inc, by = "county")[ptax > 0][, `:=`(lv = log(ptax), tt = y - 2017, post = as.integer(y >= 2019))]
  f <- if (ctrl) lv ~ ex_z:tt + ex_z:post + ex_z:post:tt + i_z:tt + i_z:post + i_z:post:tt | county + st^y else lv ~ ex_z:tt + ex_z:post + ex_z:post:tt | county + st^y
  as.data.table(coeftable(feols(f, d, cluster = ~county, weights = ~returns)), keep.rownames = "term")[, `:=`(sample = k, income_control = ctrl)] }))))
sink(file.path(OUT, "a10_revenue_income_control.txt"))
cat("Trend break (FY2019+), weighted by returns; exposure and income share in SD units\n")
print(as.data.frame(res[, .(sample, income_control, term, est = round(Estimate, 4), se = round(`Std. Error`, 4))]), row.names = FALSE)
sink(); cat(readLines(file.path(OUT, "a10_revenue_income_control.txt")), sep = "\n")
