# Event studies of asking rents (Altos listings, build/altos) on SALT-cap exposure. Outcome: log asking rent at a fixed
# dwelling type x bedroom mix (lr_adj; checks: lr_raw, log median rent of 2-bedroom apartments). Exposure: lost federal subsidy
# per owner household, TY2016 -> TY2018, $ per year (build/exposure/05), winsorised p1/p99, per $100: total (salt) and, jointly,
# the three parts (prop = real-estate-tax part of SALT, inc = income/sales-tax part, mid = mortgage interest), as in 05 for prices.
#   lr ~ i(y, E, ref = 2017) | unit + state x year, weighted by 2016 returns, SE by county.
# Units: ZIP (uncensored SOI ZIPs; zip + state x year) and county (ZIPs' main HUD county; county + state x year), each with and
# without the 2012 rent level x year; county also within the no-income-tax states. Sample: units with >= MIN listings in
# every year 2012-2024 (balanced). Years = calendar year of the listing; the cap first binds for tax year 2018.
# ALTOS_INDEX=repeat: the repeat-listing (same-unit) index of build/altos/03 (lr_rep) as the outcome instead, ZIP specs 1, 2
# and 6 only (ZIPs with >= MIN linked pairs and an index in every year 2013-2024; the 2012 rent level is the fixed-mix index),
# outputs with the suffix _repeat.
suppressPackageStartupMessages({library(data.table); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/exposure_did"); invisible(out_dir("designs/exposure_did/figures")); L <- file.path(DATA, "built")
REP <- identical(Sys.getenv("ALTOS_INDEX"), "repeat"); SFX <- if (REP) "_repeat" else ""
IDX <- if (REP) "repeat-listing (same-unit) log rent index" else "log rent at a fixed type x bedroom mix"
YRS <- 2013:2024; MIN <- 10; X <- c(salt = "d_salt_own", prop = "d_prop_own", inc = "d_nonprop_own", mid = "d_mid_own")
wins <- \(x) { q <- quantile(x, c(.01, .99), na.rm = TRUE); pmin(pmax(x, q[1]), q[2]) / 100 }
rdx <- \(f, w) { e <- fread(file.path(L, f))[, geo := sprintf(paste0("%0", w, "d"), as.integer(geo))]; for (k in names(X)) e[, (k) := wins(get(X[k]))]; e }
ec <- rdx("exposure/exposure_components_county.csv", 5); ez <- rdx("exposure/exposure_components_zip.csv", 5)[clean == TRUE]
prep <- function(d, id, e) {
  d <- d[y %in% c(2012, YRS) & n >= MIN & !is.na(st)][, nY := .N, by = id][nY == length(YRS) + 1]
  d <- merge(d, e[, c("geo", "returns16", names(X)), with = FALSE], by.x = id, by.y = "geo")
  d[, `:=`(lr = lr_adj, lp2 = log(p50_apt2))][, lr12 := lr[y == 2012], by = id][, lr12 := lr12 - mean(lr12), by = st]
  d[y %in% YRS] }
prep_rep <- function(d, lev, e) {
  d <- d[y %in% YRS & n_rep >= MIN & !is.na(lr_rep) & !is.na(st)][, nY := .N, by = zip][nY == length(YRS)]
  d <- merge(d, e[, c("geo", "returns16", names(X)), with = FALSE], by.x = "zip", by.y = "geo")
  merge(d, lev[y == 2012 & n >= MIN, .(zip, lr12 = lr_adj)], by = "zip")[, lr := lr_rep][, lr12 := lr12 - mean(lr12), by = st] }
zfx <- readRDS(file.path(L, "altos/zip_year.rds"))
zp <- if (REP) prep_rep(readRDS(file.path(L, "altos/zip_year_repeat.rds")), zfx, ez) else prep(zfx, "zip", ez)
cy <- if (REP) NULL else prep(readRDS(file.path(L, "altos/county_year.rds")), "fips", ec)
noit <- c("12", "32", "46", "48", "53", "56", "33", "47")
est <- function(d, fe, cl, lab, comps = "salt", dep = "lr", ctrl = FALSE) {
  rhs <- paste(sprintf("i(y, %s, ref = 2017)", comps), collapse = " + ")
  f <- as.formula(sprintf("%s ~ %s%s | %s", dep, rhs, if (ctrl) " + i(y, lr12, ref = 2017)" else "", fe))
  d <- d[is.finite(get(dep))]; m <- feols(f, d, cluster = cl, weights = ~returns16)
  ct <- as.data.table(coeftable(m), keep.rownames = "term")[grepl(paste0(":(", paste(comps, collapse = "|"), ")$"), term)]
  r <- ct[, .(y = as.integer(sub("^y::(\\d+):.*", "\\1", term)), comp = sub("^y::\\d+:", "", term), b = Estimate, se = `Std. Error`)]
  rbind(r, data.table(y = 2017L, comp = comps, b = 0, se = 0))[, `:=`(spec = lab, dep = dep, n = nobs(m), units = uniqueN(d[[1]]))] }
res <- if (REP) rbind(
  est(zp, "zip + st^y", ~fips, "1. ZIP (uncensored SOI ZIPs)"),
  est(zp, "zip + st^y", ~fips, "2. ZIP, + 2012 rent level x year", ctrl = TRUE),
  est(zp, "zip + st^y", ~fips, "6. ZIP, three components jointly", comps = c("prop", "inc", "mid"))) else rbind(
  est(zp, "zip + st^y", ~fips, "1. ZIP (uncensored SOI ZIPs)"),
  est(zp, "zip + st^y", ~fips, "2. ZIP, + 2012 rent level x year", ctrl = TRUE),
  est(cy, "fips + st^y", ~fips, "3. County"),
  est(cy, "fips + st^y", ~fips, "4. County, + 2012 rent level x year", ctrl = TRUE),
  est(cy[st %in% noit], "fips + st^y", ~fips, "5. County, no-income-tax states"),
  est(zp, "zip + st^y", ~fips, "6. ZIP, three components jointly", comps = c("prop", "inc", "mid")),
  est(cy, "fips + st^y", ~fips, "7. County, three components jointly", comps = c("prop", "inc", "mid")),
  est(zp, "zip + st^y", ~fips, "1. ZIP (uncensored SOI ZIPs)", dep = "lr_raw"),
  est(zp, "zip + st^y", ~fips, "1. ZIP (uncensored SOI ZIPs)", dep = "lp2"),
  est(cy, "fips + st^y", ~fips, "3. County", dep = "lr_raw"),
  est(cy, "fips + st^y", ~fips, "3. County", dep = "lp2"))
res[, comp := factor(comp, names(X), c("total lost subsidy", "(a) property tax (SALT, real-estate part)", "(b) state/local income or sales tax (SALT, rest)", "(c) mortgage interest"))]
res[, dep := factor(dep, c("lr", "lr_raw", "lp2"), c(paste0("log rent, ", if (REP) "repeat-listing index" else "fixed type x bedroom mix"), "log rent, raw mean", "log median rent, 2-bed apartments"))]
fwrite(res, file.path(OUT, paste0("d18_rents_altos", SFX, ".csv")))
sink(file.path(OUT, paste0("d18_rents_altos", SFX, ".txt")), split = TRUE)
cat("% change in asking rent per $100/yr of lost federal subsidy per owner household (TY2016 -> TY2018); ref 2017; weighted by 2016 returns; SE by county\n")
if (REP) cat(sprintf("Repeat-listing index (build/altos/03): ZIP panel %d ZIPs, %d counties (>= %d linked pairs every year 2013-2024, 2012 level from the fixed-mix index)\n", uniqueN(zp$zip), uniqueN(zp$fips), MIN)) else
cat(sprintf("ZIP panel: %d ZIPs, %d counties | county panel: %d counties (>= %d listings every year 2012-2024)\n", uniqueN(zp$zip), uniqueN(zp$fips), uniqueN(cy$fips), MIN))
for (k in split(res, by = c("dep", "spec"), drop = TRUE)) { cat("\n==", as.character(k$dep[1]), "|", k$spec[1], "| obs", k$n[1], "| units", k$units[1], "\n")
  print(dcast(k[, .(comp, y, cell = sprintf("%.2f (%.2f)", 100 * b, 100 * se))], comp ~ y, value.var = "cell")) }
sink()
pl <- function(r, ttl, file, h) {
  g <- ggplot(r, aes(y, 100 * b, colour = comp)) + geom_hline(yintercept = 0, colour = "grey60") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey40") +
    geom_pointrange(aes(ymin = 100 * (b - 1.96 * se), ymax = 100 * (b + 1.96 * se)), position = position_dodge(.55), size = .12, linewidth = .35) +
    geom_line(position = position_dodge(.55), linewidth = .3) + facet_wrap(~ spec, ncol = 2, scales = "free_y", labeller = label_wrap_gen(55)) +
    scale_x_continuous(breaks = seq(2013, 2024, 2)) + scale_colour_manual(NULL, values = c("black", "#c0392b", "#2a64a8", "#7f8c8d")) +
    labs(x = NULL, y = "% change in asking rent per $100/yr lost subsidy per owner (vs 2017)", title = ttl,
         subtitle = paste0("Altos listings; ", IDX, ". Weighted by 2016 returns; 95% CI, SE by county.")) +
    theme_minimal(base_size = 8) + theme(legend.position = "bottom")
  ggsave(file.path(OUT, "figures", file), g, width = 11, height = h, dpi = 150) }
pl(res[dep == levels(dep)[1]], paste0("Asking rents and the SALT cap: exposure x year event studies", if (REP) " (repeat-listing index)" else ""), paste0("d18_rents_altos", SFX, ".png"), if (REP) 6 else 12)
if (!REP) pl(res[spec %in% c("1. ZIP (uncensored SOI ZIPs)", "3. County")][, spec := paste(spec, "|", dep)], "Asking rents: rent index variants", "d18_rents_altos_variants.png", 9)
