# Joint event study of the three lost federal subsidies per owner household (c26, TY2016 -> TY2018, $ per year, winsorised
# p1/p99, per $100): (a) real-estate-tax part of the SALT loss (d_prop_own), (b) income/sales-tax part (d_nonprop_own),
# (c) mortgage-interest deduction (d_mid_own). Outcome: log median arm's-length sale price, 2013-2024, ref 2017.
#   lp ~ i(y, prop) + i(y, inc) + i(y, mid) | unit + higher-level x year
# Specs: county (county + state x year; SE by county) unweighted, return-weighted, return-weighted + 2012 county price level x
# year; no-income-tax states (county design within FL NV SD TX WA WY NH TN, weighted); state (state + year, weighted, SE by
# state); ZIP (uncensored ZIPs, zip + state x year, owner-weighted, SE by county) with and without 2012 ZIP price x year.
suppressPackageStartupMessages({library(data.table); library(fixest); library(ggplot2)})
ROOT <- "/Users/jr2728/PhD/projects/property_taxes"; OUT <- file.path(ROOT, "output/analysis/salt_local"); L <- file.path(ROOT, "data/local_juris")
YRS <- 2013:2024; X <- c(prop = "d_prop_own", inc = "d_nonprop_own", mid = "d_mid_own")
wins <- \(x) { q <- quantile(x, c(.01, .99), na.rm = TRUE); pmin(pmax(x, q[1]), q[2]) / 100 }
rdx <- \(f) { e <- fread(file.path(L, f), colClasses = c(geo = "character")); for (k in names(X)) e[, (k) := wins(get(X[k]))]; e }
ec <- rdx("exposure_components_county.csv"); es <- rdx("exposure_components_state.csv"); ez <- rdx("exposure_components_zip.csv")
cy <- readRDS(file.path(L, "county_year.rds"))[y %in% c(2012, YRS) & n_sales >= 50 & p50 > 0][, nY := .N, by = fips][nY == length(YRS) + 1]
cy <- merge(cy[, .(fips, y, lp = log(p50), n_sales, st = substr(fips, 1, 2))], ec[, c("geo", "returns16", names(X)), with = FALSE], by.x = "fips", by.y = "geo")
cy[, lp12 := lp[y == 2012], by = fips][, lp12 := lp12 - mean(lp12), by = st]; cy <- cy[y %in% YRS]
sp <- cy[, .(lp = weighted.mean(lp, n_sales)), by = .(st, y)]
sp <- merge(sp, es[, c("geo", "returns16", names(X)), with = FALSE], by.x = "st", by.y = "geo")
ty <- readRDS(file.path(L, "tract_year.rds"))[y %in% c(2012, YRS) & n_sales >= 5 & p50 > 0][, nY := .N, by = tract][nY == length(YRS) + 1]
tz <- fread(file.path(L, "salt_exposure_tract_zip2016.csv"), colClasses = c(zip = "character", tract = "character"))
tz <- tz[order(-n)][, .SD[1], by = tract][, .(tract, zip = sprintf("%05d", as.integer(zip)))]
zp <- merge(ty, tz, by = "tract")[, .(lp = weighted.mean(log(p50), n_sales), fips = names(which.max(table(fips)))), by = .(zip, y)]
zp[, fips := fips[1], by = zip][, st := substr(fips, 1, 2)]
zp <- merge(zp, ez[clean == TRUE, c("geo", "owners", names(X)), with = FALSE], by.x = "zip", by.y = "geo")
zp[, lp12 := lp[y == 2012], by = zip][, lp12 := lp12 - mean(lp12), by = st]; zp <- zp[y %in% YRS]
noit <- c("12", "32", "46", "48", "53", "56", "33", "47")
rhs <- paste(sprintf("i(y, %s, ref = 2017)", names(X)), collapse = " + ")
est <- function(d, fe, cl, lab, wt = NULL, ctrl = FALSE) {
  f <- as.formula(sprintf("lp ~ %s%s | %s", rhs, if (ctrl) " + i(y, lp12, ref = 2017)" else "", fe))
  m <- feols(f, d[complete.cases(d[, names(X), with = FALSE])], cluster = cl, weights = wt)
  ct <- as.data.table(coeftable(m), keep.rownames = "term")[grepl(paste0(":(", paste(names(X), collapse = "|"), ")$"), term)]
  r <- ct[, .(y = as.integer(sub("^y::(\\d+):.*", "\\1", term)), comp = sub("^y::\\d+:", "", term), b = Estimate, se = `Std. Error`)]
  rbind(r, data.table(y = 2017L, comp = names(X), b = 0, se = 0))[, `:=`(spec = lab, n = nobs(m))] }
res <- rbind(
  est(cy, "fips + st^y", ~fips, "1. County, unweighted"),
  est(cy, "fips + st^y", ~fips, "2. County, return-weighted", ~returns16),
  est(cy, "fips + st^y", ~fips, "3. County, return-weighted, + 2012 price level x year", ~returns16, TRUE),
  est(cy[st %in% noit], "fips + st^y", ~fips, "4. County, no-income-tax states, return-weighted", ~returns16),
  est(sp, "st + y", ~st, "5. State, state + year FE, return-weighted", ~returns16),
  est(zp, "zip + st^y", ~fips, "6. ZIP (uncensored), owner-weighted", ~owners),
  est(zp, "zip + st^y", ~fips, "7. ZIP (uncensored), owner-weighted, + 2012 price level x year", ~owners, TRUE))
res[, comp := factor(comp, names(X), c("(a) property tax (SALT, real-estate part)", "(b) state/local income or sales tax (SALT, rest)", "(c) mortgage interest"))]
fwrite(res, file.path(OUT, "c29_three_components.csv"))
sink(file.path(OUT, "c29_three_components.txt"), split = TRUE)
cat("% change in median price per $100/yr of each lost subsidy per owner household, estimated jointly; ref 2017\n")
for (s in unique(res$spec)) { cat("\n==", s, "| obs", res[spec == s, n[1]], "\n")
  print(dcast(res[spec == s, .(comp, y, cell = sprintf("%.2f (%.2f)", 100 * b, 100 * se))], comp ~ y, value.var = "cell")) }
sink()
g <- ggplot(res, aes(y, 100 * b, colour = comp)) + geom_hline(yintercept = 0, colour = "grey60") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey40") +
  geom_pointrange(aes(ymin = 100 * (b - 1.96 * se), ymax = 100 * (b + 1.96 * se)), position = position_dodge(.55), size = .12, linewidth = .35) +
  geom_line(position = position_dodge(.55), linewidth = .3) +
  facet_wrap(~ spec, ncol = 2, scales = "free_y", labeller = label_wrap_gen(55)) + scale_x_continuous(breaks = seq(2013, 2024, 2)) +
  scale_colour_manual(NULL, values = c("#c0392b", "#2a64a8", "#7f8c8d")) +
  labs(x = NULL, y = "% change in median price per $100/yr lost subsidy per owner (vs 2017)",
       title = "Price response to the three lost federal subsidies, estimated jointly",
       subtitle = "Lost value per owner household, TY2016 -> TY2018. 95% CI; county/ZIP: SE by county, state: SE by state.") +
  theme_minimal(base_size = 8) + theme(legend.position = "bottom")
ggsave(file.path(OUT, "figures/c29_three_components.png"), g, width = 11, height = 12, dpi = 150)
