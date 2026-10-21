# (a) x border IV at the county level. County "outside-option" share = population share (ACS 2017) of the county's tracts that
# are near (< 10 mi) a state border on its high-tax side (R/26). Revenue event studies (Census of Governments, as a1) with
# exposure x year, outside-option share x year and exposure x share x year; county + state x year FE, SE by county.
# Prediction if mobility across the border constrains exposed jurisdictions: the triple interaction is negative after 2017.
suppressPackageStartupMessages({library(data.table); library(duckdb); library(fixest)})
ROOT <- "/Users/jr2728/PhD/projects/property_taxes"; OUT <- file.path(ROOT, "output/analysis/salt_local")
iv <- unique(readRDS(file.path(ROOT, "data/local_juris/iv_tract_side.rds"))[, .(tract, cty, near, high, pop)])
all_t <- as.data.table(arrow_free <- NULL)
con <- dbConnect(duckdb()); tt <- as.data.table(dbGetQuery(con, sprintf("SELECT tract, cty, pop FROM '%s/data/local_juris/iv_tract.parquet'", ROOT))); dbDisconnect(con, shutdown = TRUE)
tt <- merge(tt, iv[, .(tract, nh = near * high, in_iv = 1L)], by = "tract", all.x = TRUE)[is.na(nh), nh := 0L]
cs <- tt[, .(nh_share = sum(pop * nh, na.rm = TRUE) / sum(pop, na.rm = TRUE)), by = .(county = as.integer(cty))]
ex <- fread(file.path(ROOT, "data/local_juris/salt_exposure_county2016.csv"))
cs <- merge(cs, ex[, .(county, ex_z, returns)], by = "county")
cs[, nh_any := as.integer(nh_share > .25)]
con <- dbConnect(duckdb(), file.path(ROOT, "data/census_finances_test.db"), read_only = TRUE)
qf <- function(tb) as.data.table(dbGetQuery(con, sprintf("SELECT county_fips::INT county, state_fips st, year::INT y, property_tax_rev ptax FROM %s", tb)))
fin <- list(school = qf("govt_finances_sd_by_county"), county_govt = qf("govt_finances_county")); dbDisconnect(con, shutdown = TRUE)
fin$county_govt <- fin$county_govt[, if (uniqueN(y) == 11) .SD, by = county]
res <- rbindlist(lapply(names(fin), \(k) rbindlist(lapply(c(FALSE, TRUE), \(wt) {
  d <- merge(fin[[k]], cs, by = "county")[ptax > 0][, `:=`(lv = log(ptax), post = as.integer(y >= 2019), tt = y - 2017)]
  f1 <- feols(lv ~ i(y, ex_z, ref = 2017) + i(y, nh_any, ref = 2017) + i(y, I(ex_z * nh_any), ref = 2017) | county + st^y, d, cluster = ~county,
              weights = if (wt) ~returns else NULL)
  f2 <- feols(lv ~ ex_z:tt + nh_any:tt + ex_z:nh_any:tt + ex_z:post:tt + nh_any:post:tt + ex_z:nh_any:post:tt | county + st^y, d, cluster = ~county,
              weights = if (wt) ~returns else NULL)
  rbind(as.data.table(coeftable(f1), keep.rownames = "term")[grepl("ex_z \\* nh_any", term)][, spec := "event study: ex_z x border-high county"],
        as.data.table(coeftable(f2), keep.rownames = "term")[, spec := "trend break (FY2019+)"])[, `:=`(sample = k, weighted = wt,
        n_counties = uniqueN(d$county), n_nh = uniqueN(d[nh_any == 1, county]))] }))))
fwrite(res, file.path(OUT, "a2_revenue_outside_option.csv"))
sink(file.path(OUT, "a2_revenue_outside_option.txt"))
cat("County outside-option indicator: > 25% of population in near-border high-side tracts. Counties:", nrow(cs), "| flagged:", sum(cs$nh_any), "\n")
cat("corr(ex_z, nh_share):", round(cor(cs$ex_z, cs$nh_share), 3), "\n")
print(as.data.frame(res[, .(sample, weighted, spec, term, est = round(Estimate, 4), se = round(`Std. Error`, 4), n_counties, n_nh)]), row.names = FALSE)
sink(); cat(readLines(file.path(OUT, "a2_revenue_outside_option.txt")), sep = "\n")
