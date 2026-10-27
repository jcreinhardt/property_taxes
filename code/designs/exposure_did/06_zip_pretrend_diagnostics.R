# Formerly code/analysis/salt_local/c28_zip_pretrend_diagnostics.R (moved in the 2026-10-27 restructuring).
# Why does the ZIP-level median-price design (c27) show a pre-trend that the county design does not?
# Same outcome construction as c27 (ZIP log price = sales-weighted mean of tract log p50, tracts with >= 5 sales every year
# 2013-2024; county log p50, counties with >= 50 sales every year). Per-owner SALT exposure (c26, winsorised p1/p99), per $100.
# Diagnostics, all owner/return-weighted with state x year FE unless noted:
#  (1) ZIP outcome, ZIP exposure (= c27 baseline).
#  (2) ZIP outcome, the ZIP's COUNTY exposure: same exposure variation as the county design, ZIP sample and price measure.
#  (3) County outcome rebuilt from the ZIP sample (sales-weighted mean of its ZIPs), county exposure: county design on the
#      ZIP sample.
#  (4) County design restricted to counties that appear in the ZIP sample.
#  (5) ZIP outcome, ZIP exposure, controlling for the ZIP's 2012 price level (log, demeaned within state; 2012 is outside the
#      estimation window, so the control is not built from the outcome) x year: the post-crisis catch-up of cheaper areas.
#  (6) As (1) with county x year FE (within-county ZIP variation); (7) as (6) plus 2012 price level x year.
#  (8) County design (4b) plus the county's 2012 price level x year.
suppressPackageStartupMessages({library(data.table); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/exposure_did"); invisible(out_dir("designs/exposure_did/figures")); L <- file.path(DATA, "built")
YRS <- 2013:2024
wins <- \(x) { q <- quantile(x, c(.01, .99), na.rm = TRUE); pmin(pmax(x, q[1]), q[2]) }
ec <- fread(file.path(L, "exposure/exposure_components_county.csv"), colClasses = c(geo = "character"))[, .(fips = geo, xc = wins(d_salt_own) / 100, returns16)]
ez <- fread(file.path(L, "exposure/exposure_components_zip.csv"), colClasses = c(geo = "character"))[, .(zip = geo, xz = wins(d_salt_own) / 100, owners, clean)]
ty <- readRDS(file.path(L, "corelogic/tract_year.rds"))[y %in% c(2012, YRS) & n_sales >= 5 & p50 > 0][, nY := .N, by = tract][nY == length(YRS) + 1]
tz <- fread(file.path(L, "exposure/salt_exposure_tract_zip2016.csv"), colClasses = c(zip = "character", tract = "character"))
tz <- tz[order(-n)][, .SD[1], by = tract][, .(tract, zip = sprintf("%05d", as.integer(zip)))]
zp <- merge(ty, tz, by = "tract")[, .(lp = weighted.mean(log(p50), n_sales), ns = sum(n_sales), fips = names(which.max(table(fips)))), by = .(zip, y)]
zp[, fips := fips[1], by = zip][, st := substr(fips, 1, 2)]
zp <- merge(merge(zp, ez, by = "zip"), ec[, .(fips, xc)], by = "fips")
zp[, lp13 := lp[y == 2012], by = zip][, lp13 := lp13 - mean(lp13), by = st]   # named lp13 for brevity; it is the 2012 level
zp <- zp[y %in% YRS]
cy <- readRDS(file.path(L, "corelogic/county_year.rds"))[y %in% c(2012, YRS) & n_sales >= 50 & p50 > 0][, nY := .N, by = fips][nY == length(YRS) + 1]
cy <- merge(cy[, .(fips, y, lp = log(p50), st = substr(fips, 1, 2))], ec, by = "fips")
cy[, lp12 := lp[y == 2012], by = fips][, lp12 := lp12 - mean(lp12), by = st]
cy <- cy[y %in% YRS]
cz <- zp[, .(lp = weighted.mean(lp, ns), st = st[1]), by = .(fips, y)]
cz <- merge(cz, ec, by = "fips")
tid <- \(m, lab, v) as.data.table(coeftable(m), keep.rownames = "term")[grepl(paste0(":", v, "$"), term)][,
  .(spec = lab, y = as.integer(sub("^y::(\\d+):.*", "\\1", term)), b = Estimate, se = `Std. Error`)]
res <- rbind(
  tid(feols(lp ~ i(y, xz, ref = 2017) | zip + st^y, zp, cluster = ~fips, weights = ~owners), "(1) ZIP price, ZIP exposure", "xz"),
  tid(feols(lp ~ i(y, xc, ref = 2017) | zip + st^y, zp, cluster = ~fips, weights = ~owners), "(2) ZIP price, county exposure", "xc"),
  tid(feols(lp ~ i(y, xc, ref = 2017) | fips + st^y, cz, cluster = ~fips, weights = ~returns16), "(3) county price built from ZIP sample, county exposure", "xc"),
  tid(feols(lp ~ i(y, xc, ref = 2017) | fips + st^y, cy[fips %in% zp$fips], cluster = ~fips, weights = ~returns16), "(4) county design, counties in ZIP sample", "xc"),
  tid(feols(lp ~ i(y, xc, ref = 2017) | fips + st^y, cy, cluster = ~fips, weights = ~returns16), "(4b) county design, all balanced counties", "xc"),
  tid(feols(lp ~ i(y, xz, ref = 2017) + i(y, lp13, ref = 2017) | zip + st^y, zp, cluster = ~fips, weights = ~owners), "(5) ZIP, + 2012 price level x year", "xz"),
  tid(feols(lp ~ i(y, xz, ref = 2017) | zip + fips^y, zp, cluster = ~fips, weights = ~owners), "(6) ZIP, county x year FE", "xz"),
  tid(feols(lp ~ i(y, xz, ref = 2017) + i(y, lp13, ref = 2017) | zip + fips^y, zp, cluster = ~fips, weights = ~owners), "(7) ZIP, county x year FE + 2012 price x year", "xz"),
  tid(feols(lp ~ i(y, xc, ref = 2017) + i(y, lp12, ref = 2017) | fips + st^y, cy, cluster = ~fips, weights = ~returns16), "(8) county design + 2012 price level x year", "xc"))
res <- rbind(res, unique(res[, .(spec)])[, `:=`(y = 2017L, b = 0, se = 0)])[order(spec, y)]
fwrite(res, file.path(OUT, "c28_zip_pretrend_diagnostics.csv"))
sink(file.path(OUT, "c28_zip_pretrend_diagnostics.txt"), split = TRUE)
cat("% change in median price per $100/yr lost SALT subsidy per owner (x100), ref 2017. ZIPs:", uniqueN(zp$zip), "| counties in ZIP sample:", uniqueN(zp$fips),
    "| balanced counties (county design):", uniqueN(cy$fips), "\n")
cat("corr(ZIP exposure, 2012 ZIP price level within state):", round(zp[y == 2013, cor(xz, lp13)], 2), "| corr(ZIP exp, county exp):", round(zp[y == 2013, cor(xz, xc, use = "complete")], 2),
    "| corr(county exp, 2012 county price within state):", round(cy[y == 2013, cor(xc, lp12, use = "complete")], 2), "\n\n")
print(dcast(res[, .(spec, y, cell = sprintf("%.2f (%.2f)", 100 * b, 100 * se))], spec ~ y, value.var = "cell"))
sink()
g <- ggplot(res, aes(y, 100 * b)) + geom_hline(yintercept = 0, colour = "grey60") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey40") +
  geom_ribbon(aes(ymin = 100 * (b - 1.96 * se), ymax = 100 * (b + 1.96 * se)), fill = "#2a64a8", alpha = .15) + geom_line(colour = "#2a64a8") + geom_point(colour = "#2a64a8", size = 1) +
  facet_wrap(~ spec, ncol = 2, labeller = label_wrap_gen(55)) + scale_x_continuous(breaks = seq(2013, 2024, 2)) +
  labs(x = NULL, y = "% per $100/yr lost subsidy per owner (vs 2017)", title = "Where does the ZIP-level pre-trend come from?",
       subtitle = "Weighted (ZIP: owners; county: 2016 returns); 95% CI, SE by county") + theme_minimal(base_size = 8)
ggsave(file.path(OUT, "figures/c28_zip_pretrend_diagnostics.png"), g, width = 10, height = 11, dpi = 150)
