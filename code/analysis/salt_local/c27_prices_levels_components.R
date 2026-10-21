# Price event studies with per-owner exposure components (c26) at three levels, plus the no-income-tax-state test.
# Exposure X ($ per owner household per year, TY2016 -> TY2018 lost federal value of SALT deductions): total (d_salt_own), its
# real-estate-tax part (d_prop_own) and its income/sales-tax part (d_nonprop_own). Winsorised at p1/p99 within level.
# Outcome: log median arm's-length sale price (CoreLogic), years 2013-2024, ref 2017.
#   county: county_year p50, counties with >= 50 sales in every year; county + state x year FE; SE by county.
#   state : sales-weighted mean of county log p50 over the same balanced counties; state + year FE (and division x year);
#           SE by state (49 clusters) -- cross-state variation, so state income taxes are part of the comparison.
#   ZIP   : sales-weighted mean of tract log p50 (tract_year) over tracts mapped to the ZIP (a8 tract -> ZIP), tracts with
#           >= 5 sales every year; ZIP + state x year FE (and county x year); SE by county; all ZIPs and SOI-uncensored ZIPs.
#   no-income-tax states (FL, NV, SD, TX, WA, WY; NH and TN taxed only interest/dividends): county design within these
#           states, where SALT is real-estate tax plus sales tax.
# Models: (1) total; (2) d_prop_own and d_nonprop_own jointly. Unweighted and weighted by 2016 returns (owners at ZIP level).
# Capitalisation factor = (coef per $1 of annual lost subsidy per owner) x 2017 mean price / 20 (PV at 5%, perpetual).
suppressPackageStartupMessages({library(data.table); library(fixest); library(ggplot2)})
ROOT <- "/Users/jr2728/PhD/projects/property_taxes"; OUT <- file.path(ROOT, "output/analysis/salt_local"); L <- file.path(ROOT, "data/local_juris")
YRS <- 2013:2024
wins <- \(x) { q <- quantile(x, c(.01, .99), na.rm = TRUE); pmin(pmax(x, q[1]), q[2]) }
ec <- fread(file.path(L, "exposure_components_county.csv"), colClasses = c(geo = "character"))
es <- fread(file.path(L, "exposure_components_state.csv"), colClasses = c(geo = "character"))
ez <- fread(file.path(L, "exposure_components_zip.csv"), colClasses = c(geo = "character", st = "character"))
X <- c("d_salt_own", "d_prop_own", "d_nonprop_own")
for (g in list(ec, es, ez)) for (k in X) set(g, j = k, value = wins(g[[k]]))
# county panel
cy <- readRDS(file.path(L, "county_year.rds"))[y %in% YRS & n_sales >= 50 & p50 > 0][, nY := .N, by = fips][nY == length(YRS)]
cy[, `:=`(lp = log(p50), st = substr(fips, 1, 2))]
cy <- merge(cy, ec, by.x = "fips", by.y = "geo")
# state panel
sp <- cy[, .(lp = weighted.mean(lp, n_sales), p = exp(weighted.mean(lp, n_sales))), by = .(st, y)]
div <- data.table(st = sprintf("%02d", c(9,23,25,33,44,50, 34,36,42, 17,18,26,39,55, 19,20,27,29,31,38,46, 10,11,12,13,24,37,45,51,54, 1,21,28,47, 5,22,40,48, 4,8,16,30,32,35,49,56, 6,41,53)),
                  div = rep(1:9, c(6, 3, 5, 7, 9, 4, 4, 8, 3)))
sp <- merge(merge(sp, es, by.x = "st", by.y = "geo"), div, by = "st")
# ZIP panel
ty <- readRDS(file.path(L, "tract_year.rds"))[y %in% YRS & n_sales >= 5 & p50 > 0][, nY := .N, by = tract][nY == length(YRS)]
tz <- fread(file.path(L, "salt_exposure_tract_zip2016.csv"), colClasses = c(zip = "character", tract = "character"))
tz <- tz[order(-n)][, .SD[1], by = tract][, .(tract, zip = sprintf("%05d", as.integer(zip)))]
zp <- merge(ty, tz, by = "tract")[, .(lp = weighted.mean(log(p50), n_sales), cty = names(which.max(table(fips)))), by = .(zip, y)]
zp[, cty := cty[1], by = zip][, st := substr(cty, 1, 2)]
zp <- merge(zp, ez[, c("geo", X, "clean", "returns16", "owners"), with = FALSE], by.x = "zip", by.y = "geo")
noit <- c("12", "32", "46", "48", "53", "56", "33", "47")
est <- function(d, fe, cl, lab, wt = NULL, ks = list("d_salt_own", c("d_prop_own", "d_nonprop_own")), lev) {
  rbindlist(lapply(ks, \(kk) {
    d <- d[complete.cases(d[, kk, with = FALSE])]
    sdk <- sapply(kk, \(k) sd(unique(d[, c(if ("zip" %in% names(d)) "zip" else if ("fips" %in% names(d)) "fips" else "st", k), with = FALSE])[[k]]))
    f <- as.formula(sprintf("lp ~ %s | %s", paste(sprintf("i(y, %s, ref = 2017)", kk), collapse = " + "), fe))
    m <- feols(f, d, cluster = cl, weights = wt)
    ct <- as.data.table(coeftable(m), keep.rownames = "term")[grepl("^y::", term)]
    ct[, `:=`(y = as.integer(sub("^y::(\\d+):.*", "\\1", term)), measure = sub("^y::\\d+:", "", term))]
    p17 <- if (is.null(wt)) d[y == 2017, mean(exp(lp))] else d[y == 2017, weighted.mean(exp(lp), eval(wt[[2]]))]
    r <- ct[, .(y, measure, b = Estimate, se = `Std. Error`)]
    r <- rbind(r, data.table(y = 2017L, measure = kk, b = 0, se = 0))
    r[, `:=`(sd_x = sdk[measure], model = if (length(kk) == 1) "total" else "property vs non-property", spec = lab, level = lev,
             weighted = !is.null(wt), p17 = p17, n_units = uniqueN(d[[if ("zip" %in% names(d)) "zip" else if ("fips" %in% names(d)) "fips" else "st"]]))]
    r[, factor := b * p17 / 20] })) }
res <- rbind(
  est(cy, "fips + st^y", ~fips, "county: county + state x year", lev = "county"),
  est(cy, "fips + st^y", ~fips, "county: county + state x year", wt = ~returns16, lev = "county"),
  est(cy[st %in% noit], "fips + st^y", ~fips, "county, no-income-tax states only", lev = "county (no income tax)"),
  est(cy[st %in% noit], "fips + st^y", ~fips, "county, no-income-tax states only", wt = ~returns16, lev = "county (no income tax)"),
  est(cy[!st %in% noit], "fips + st^y", ~fips, "county, income-tax states only", wt = ~returns16, lev = "county (income tax)"),
  est(sp, "st + y", ~st, "state: state + year", lev = "state"),
  est(sp, "st + y", ~st, "state: state + year", wt = ~returns16, lev = "state"),
  est(sp, "st + div^y", ~st, "state: state + division x year", wt = ~returns16, lev = "state"),
  est(zp, "zip + st^y", ~cty, "ZIP: zip + state x year, all", wt = ~owners, lev = "ZIP"),
  est(zp[clean == TRUE], "zip + st^y", ~cty, "ZIP: zip + state x year, uncensored", wt = ~owners, lev = "ZIP"),
  est(zp[clean == TRUE], "zip + cty^y", ~cty, "ZIP: zip + county x year, uncensored", wt = ~owners, lev = "ZIP"))
res[, b_sd := b * sd_x]
fwrite(res, file.path(OUT, "c27_prices_levels_components.csv"))
sink(file.path(OUT, "c27_prices_levels_components.txt"), split = TRUE)
cat("Log median price, coefficient per $100 of annual lost federal SALT subsidy per owner household (x100 = % per $100), ref 2017.\n",
    "Capitalisation factor in brackets (price change / PV of the lost subsidy at 5%). Units:",
    paste(res[, .(n = n_units[1]), by = .(spec, weighted)][, sprintf("%s%s: %d", spec, ifelse(weighted, " (wtd)", ""), n)], collapse = "; "), "\n")
for (s in unique(res[, paste(spec, weighted, model)])) {
  x <- res[paste(spec, weighted, model) == s & y %in% c(2013, 2015, 2016, 2018, 2019, 2020, 2021, 2022, 2024)]
  cat("\n==", s, "\n")
  print(dcast(x[, .(measure, y, cell = sprintf("%.2f (%.2f) [%.2f]", 1e4 * b, 1e4 * se, factor))], measure ~ y, value.var = "cell")) }
sink()
pl <- res[model == "total" & (weighted == TRUE | level == "county (no income tax)")]
pl[, lab := paste0(spec, ifelse(weighted, ", weighted", ", unweighted"))]
g <- ggplot(pl, aes(y, 1e4 * b)) + geom_hline(yintercept = 0, colour = "grey60") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey40") +
  geom_ribbon(aes(ymin = 1e4 * (b - 1.96 * se), ymax = 1e4 * (b + 1.96 * se)), fill = "#2a64a8", alpha = .15) + geom_line(colour = "#2a64a8") + geom_point(colour = "#2a64a8", size = 1) +
  facet_wrap(~ lab, ncol = 3, labeller = label_wrap_gen(45)) + scale_x_continuous(breaks = seq(2013, 2024, 2)) +
  labs(x = NULL, y = "% change in median price per $100/yr of lost SALT subsidy per owner (vs 2017)",
       title = "SALT-cap capitalisation at county, state and ZIP level (per-owner exposure, TY2016 -> TY2018)",
       subtitle = "95% CI. County/ZIP: SE by county; state: SE by state.") + theme_minimal(base_size = 8)
ggsave(file.path(OUT, "figures/c27_prices_levels.png"), g, width = 12, height = 9, dpi = 150)
pc <- res[model != "total" & weighted == TRUE][, lab := paste0(spec, " (weighted)")]
g2 <- ggplot(pc, aes(y, 1e4 * b, colour = measure)) + geom_hline(yintercept = 0, colour = "grey60") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey40") +
  geom_pointrange(aes(ymin = 1e4 * (b - 1.96 * se), ymax = 1e4 * (b + 1.96 * se)), position = position_dodge(.45), size = .12) +
  facet_wrap(~ lab, ncol = 3, scales = "free_y", labeller = label_wrap_gen(45)) + scale_x_continuous(breaks = seq(2013, 2024, 2)) +
  scale_colour_manual(NULL, values = c(d_prop_own = "#c0392b", d_nonprop_own = "#2a64a8"), labels = c(d_nonprop_own = "income/sales-tax part", d_prop_own = "real-estate-tax part")) +
  labs(x = NULL, y = "% change in median price per $100/yr per owner (vs 2017)", title = "Real-estate-tax vs income/sales-tax parts of the lost SALT subsidy, estimated jointly") +
  theme_minimal(base_size = 8) + theme(legend.position = "bottom")
ggsave(file.path(OUT, "figures/c27_prices_components.png"), g2, width = 12, height = 9, dpi = 150)
