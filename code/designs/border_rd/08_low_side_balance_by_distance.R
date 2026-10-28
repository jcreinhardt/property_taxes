# Balance in distance to the border on the LOW-tax side of state borders. Are tracts 10 miles from the line like tracts 30
# miles away, even though the whole state is not? For each ranked border pair (iv_tract_side.rds, T = 30), the low-tax side's
# tracts (all distances, i.e. the whole state's tracts whose nearest own-state border is that pair) are binned by distance
# (0-10, 10-20, 20-30, 30-50, 50-100, 100+ mi) and household, housing, school and tax characteristics are compared:
#   (1) bin means net of border-pair FE (Y ~ i(bin, ref = 0-10) | pair, SE by county), in SD units of the variable;
#   (2) linear slope in distance (per 10 mi, SD units) within 0-30 mi vs within the whole state, with pair FE;
#   (3) the joint test of the bin dummies as the window widens (0-20, 0-30, 0-50, 0-100, all), and the largest |difference|
#       between any bin in the window and the 0-10 bin;
#   (4) robustness of (1): pairs weighted equally (the 0-10 bin is a quarter New York City: NJ-NY has 1,707 of its 2,195
#       low-side tracts within 10 mi), and controlling for log population density (TIGER cb 2019 land area).
# Covariates: ACS 2013-17 (income, home value, age, household size, share under 18, households with children, moved in the
# last year, owner share, BA+, white, year built, population; extras cached to data/external/acs17_tract_extra.csv the way
# build/geo/05 caches acs17_tract.csv), CoreLogic 2016 (median sale price, sales per parcel, effective rate of 2017-19 sales),
# Data Axle move-outs 2014-17 (annualised), SOI 2016 ZIP (share of returns with AGI >= $100k, itemiser share, SALT above
# $10k per return), SEDA (achievement, SES). Unweighted tract regressions, as in 02/03 of this folder.
suppressPackageStartupMessages({library(data.table); library(DBI); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_rd"); invisible(out_dir("designs/border_rd/figures"))
# SIDE=high builds the same tract table for the HIGH-tax side (built/geo/high_side_balance_tracts.rds, read by 23) and stops there.
SIDE <- Sys.getenv("SIDE", "low")
BR <- c(0, 10, 20, 30, 50, 100, Inf); LAB <- c("0-10", "10-20", "20-30", "30-50", "50-100", "100+")
d <- readRDS(file.path(DATA, "built/geo/iv_tract_side.rds"))[T == 30 & high == as.integer(SIDE == "high")]
# ACS 2013-17 extras (tidycensus; cached)
acs_f <- file.path(DATA, "external/acs17_tract_extra.csv")
if (!file.exists(acs_f)) {
  suppressPackageStartupMessages({library(tidycensus); library(tigris)})
  v <- c(hhsize = "B25010_001", pop = "B01003_001", u18 = "B09001_001", mob_tot = "B07003_001", same_house = "B07003_004",
         hh_tot = "B11005_001", hh_kids = "B11005_002", rooms = "B25018_001")
  fs <- setdiff(unique(fips_codes$state_code[fips_codes$state_code < "57"]), c("02", "15"))
  a <- rbindlist(lapply(fs, \(s) { for (k in 1:3) { r <- tryCatch(as.data.table(suppressMessages(get_acs("tract", variables = v, state = s, year = 2017, survey = "acs5", output = "wide"))),
      error = \(e) { message(s, " try ", k, ": ", conditionMessage(e)); NULL }); if (!is.null(r)) return(r); Sys.sleep(5) }; NULL }), fill = TRUE)
  a[, NAME := NULL]; fwrite(a, acs_f)
}
a <- fread(acs_f, colClasses = c(GEOID = "character"))
a <- a[, .(tract = GEOID, hhsize = hhsizeE, sh_u18 = u18E / popE, sh_moved = 1 - same_houseE / mob_totE, sh_hh_kids = hh_kidsE / hh_totE)]
d <- merge(d, a, by = "tract", all.x = TRUE)
# CoreLogic 2016 tract-year (kpanel): median sale price, sales per 100 parcels
cl <- readRDS(file.path(DATA, "built/corelogic/tract_year.rds"))[y == 2016 & n >= 50, .(tract, lp50 = log(p50), turnover = 100 * n_sales / n)]
d <- merge(d, cl, by = "tract", all.x = TRUE)
# Data Axle move-outs 2014-17 (destination files), annualised by months between file cut-offs as in designs/border_iv/10
mv <- dq(sprintf("SELECT * FROM '%s/data/built/dataaxle/border_moveout/moveout_tract_full.parquet' WHERE y BETWEEN 2014 AND 2017", ROOT))
cut <- c(`2012` = 3, `2013` = 15, `2014` = 27, `2015` = 40, `2016` = 54, `2017` = 65, `2018` = 79, `2019` = 90, `2020` = 102, `2021` = 114)
mv <- merge(mv, data.table(y = 2013:2021, months = diff(cut)), by = "y")[, .(moveout = 100 * sum(moves) / sum(n) * 12 / mean(months), n_da = sum(n)), by = tract][n_da >= 200]
d <- merge(d, mv[, .(tract, moveout)], by = "tract", all.x = TRUE)
# SOI 2016 by ZIP (modal situs ZIP of the tract's 2016-19 sales, from build/exposure/02): income distribution, itemisers, SALT exposure
tz <- fread(file.path(DATA, "built/exposure/salt_exposure_tract_zip2016.csv"), colClasses = c(tract = "character", zip = "character"))
con <- db_open("database.db")
z <- as.data.table(dbGetQuery(con, "SELECT lpad(zipcode::INT::VARCHAR, 5, '0') zip, agi_stub, returns, returns_w_items FROM returns_zipcode_agi
  WHERE year = 2016 AND zipcode NOT IN (0, 99999)")); dbDisconnect(con, shutdown = TRUE)
z <- z[, .(sh_agi100 = sum(returns[agi_stub >= 5]) / sum(returns), sh_item = sum(returns_w_items) / sum(returns)), by = zip]
d <- merge(d, merge(tz[, .(tract, zip, exz)], z, by = "zip")[, .(tract, sh_agi100, sh_item, exz)], by = "tract", all.x = TRUE)
# population density: ACS population / TIGER cb 2019 tract land area (sq mi)
tr <- as.data.table(foreign::read.dbf(file.path(path.expand("~/Library/Caches/tigris"), "cb_2019_us_tract_500k.dbf"), as.is = TRUE))[, .(tract = GEOID, aland = as.numeric(ALAND))]
d <- merge(d, tr, by = "tract", all.x = TRUE)[, ldens := log(pmax(pop, 1) / (aland / 2589988))]
d[, bin := cut(dist_mi, BR, LAB, right = FALSE)][, w_pair := 1 / .N, by = pair]
saveRDS(d, file.path(DATA, sprintf("built/geo/%s_side_balance_tracts.rds", SIDE)))   # read by 09_low_side_profiles_50mi.R (low), 23 (both)
if (SIDE == "high") { cat("high-side table:", nrow(d), "tracts,", uniqueN(d$pair), "pairs\n"); quit(save = "no") }
v <- c(linc = "log median HH income (ACS)", sh_agi100 = "share of returns AGI >= $100k (SOI ZIP)", age = "median age (ACS)",
       hhsize = "household size (ACS)", sh_u18 = "share under 18 (ACS)", sh_hh_kids = "share of HHs with children (ACS)",
       sh_own = "owner share (ACS)", lhval = "log median home value (ACS)", lp50 = "log median sale price 2016 (CoreLogic)",
       sh_moved = "moved in the last year (ACS)", moveout = "move-out rate 2014-17, %/yr (Data Axle)", turnover = "sales per 100 parcels 2016 (CoreLogic)",
       sh_ba = "share BA+ (ACS)", sh_white = "share white (ACS)", yrbuilt = "median year built (ACS)", lpop = "log population (ACS)",
       sh_item = "itemiser share (SOI ZIP)", exz = "SALT above $10k per return, $k (SOI ZIP)", ach = "district test scores (SEDA)",
       ses = "district SES (SEDA)", med = "effective tax rate, % (CoreLogic)", ldens = "log population density (ACS, TIGER)")
cat("low-tax-side tracts:", nrow(d), "| pairs:", uniqueN(d$pair), "| states:", uniqueN(d$st), "\n"); print(d[, .N, by = bin][order(bin)])
# ---- (1) bin means net of pair FE, relative to 0-10 mi
sds <- sapply(names(v), \(k) sd(d[[k]], na.rm = TRUE)); mns <- sapply(names(v), \(k) d[bin == "0-10", mean(get(k), na.rm = TRUE)])
bins <- rbindlist(lapply(names(v), \(k) { x <- d[is.finite(get(k))]
  m <- feols(as.formula(sprintf("%s ~ i(bin, ref = '0-10') | pair", k)), x, cluster = ~cty)
  ct <- as.data.table(coeftable(m), keep.rownames = "term")[, .(bin = sub("bin::", "", term), b = Estimate, se = `Std. Error`, p = `Pr(>|t|)`)]
  rbind(data.table(bin = "0-10", b = 0, se = 0, p = NA_real_), ct)[, `:=`(var = k, label = v[k], n = nobs(m), raw = sapply(bin, \(bb) x[bin == bb, mean(get(k))]))] }))
bins[, `:=`(b_sd = b / sds[var], se_sd = se / sds[var], bin = factor(bin, LAB))]
# ---- (2) slopes per 10 mi, SD units, pair FE: within 0-30 mi vs whole state (and 0-50, 0-100)
slope <- rbindlist(lapply(names(v), \(k) rbindlist(lapply(c(30, 50, 100, Inf), \(W) { x <- d[is.finite(get(k)) & dist_mi < W]
  m <- feols(as.formula(sprintf("%s ~ dist_mi | pair", k)), x, cluster = ~cty); ct <- coeftable(m)
  data.table(var = k, window = W, slope10_sd = 10 * ct[1, 1] / sds[k], se10_sd = 10 * ct[1, 2] / sds[k], p = ct[1, 4], n = nobs(m)) }))))
# ---- (3) joint tests as the window widens, and the largest |bin - (0-10)| within the window
joint <- rbindlist(lapply(names(v), \(k) rbindlist(lapply(c(20, 30, 50, 100, Inf), \(W) { x <- d[is.finite(get(k)) & dist_mi < W]
  m <- feols(as.formula(sprintf("%s ~ i(bin, ref = '0-10') | pair", k)), x, cluster = ~cty); w <- wald(m, "bin", print = FALSE)
  bb <- bins[var == k & as.numeric(sub("-.*|\\+", "", bin)) < W & bin != "0-10"]
  data.table(var = k, window = W, p_joint = w$p, max_abs_sd = max(abs(bb$b_sd)), n = nobs(m)) }))))
# ---- (4) robustness of the bin differences: equal pair weights; log density as a control
binfit <- function(k, spec) { x <- d[is.finite(get(k)) & is.finite(ldens)]
  fml <- as.formula(sprintf("%s ~ i(bin, ref = '0-10')%s | pair", k, ifelse(spec == "density", " + ldens", "")))
  m <- feols(fml, x, cluster = ~cty, weights = if (spec == "equal pairs") ~w_pair else NULL)
  ct <- as.data.table(coeftable(m), keep.rownames = "term")[grepl("^bin::", term), .(bin = sub("bin::", "", term), b = Estimate, se = `Std. Error`)]
  w30 <- wald(feols(fml, x[dist_mi < 30], cluster = ~cty, weights = if (spec == "equal pairs") ~w_pair else NULL), "bin", print = FALSE)$p
  w50 <- wald(feols(fml, x[dist_mi < 50], cluster = ~cty, weights = if (spec == "equal pairs") ~w_pair else NULL), "bin", print = FALSE)$p
  ct[, `:=`(var = k, spec = spec, b_sd = b / sds[k], se_sd = se / sds[k], p30 = w30, p50 = w50, n = nobs(m))] }
rob <- rbindlist(lapply(c("main", "equal pairs", "density"), \(s) rbindlist(lapply(setdiff(names(v), if (s == "density") "ldens" else NULL), binfit, spec = s))))
fwrite(rob, file.path(OUT, "c21_low_side_balance_robust.csv"))
fwrite(bins, file.path(OUT, "c21_low_side_balance_bins.csv")); fwrite(slope, file.path(OUT, "c21_low_side_balance_slopes.csv")); fwrite(joint, file.path(OUT, "c21_low_side_balance_joint.csv"))
# ---- table
wl <- \(W) ifelse(is.infinite(W), "all", paste0("<", W))
tab <- dcast(bins[bin != "0-10", .(var, bin, cell = sprintf("%.2f (%.2f)", b_sd, se_sd))], var ~ bin, value.var = "cell")
tab <- merge(tab, dcast(slope[, .(var, w = paste0("slope ", wl(window)), cell = sprintf("%.3f (%.3f)", slope10_sd, se10_sd))], var ~ w, value.var = "cell"), by = "var")
tab <- merge(tab, dcast(joint[, .(var, w = paste0("p ", wl(window)), p = round(p_joint, 3))], var ~ w, value.var = "p"), by = "var")
tab <- tab[match(names(v), var)][, `:=`(variable = v[var], mean_0_10 = signif(mns[var], 3), sd = signif(sds[var], 3))]
setcolorder(tab, c("variable", "mean_0_10", "sd", LAB[-1], paste0("slope ", wl(c(30, 50, 100, Inf))), paste0("p ", wl(c(20, 30, 50, 100, Inf)))))
tab[, var := NULL]; fwrite(tab, file.path(OUT, "c21_low_side_balance_table.csv"))
summ <- joint[, .(covariates = .N, rejected_5pct = sum(p_joint < .05), median_max_abs_sd = round(median(max_abs_sd), 2), max_max_abs_sd = round(max(max_abs_sd), 2)), by = .(window = wl(window))]
sl <- slope[, .(median_abs_slope10_sd = round(median(abs(slope10_sd)), 3), slopes_sig_5pct = sum(p < .05)), by = .(window = wl(window))]
sink(file.path(OUT, "c21_low_side_balance.txt"), split = TRUE)
cat("Balance in distance to the border on the LOW-tax side (iv_tract_side, T = 30, high = 0). Tracts:", nrow(d), "| pairs:", uniqueN(d$pair), "| states:", uniqueN(d$st), "\n")
cat("Tracts by bin:", paste(d[, .N, by = bin][order(bin), paste0(bin, ": ", N)], collapse = " | "), "\n")
cat("Pair FE, SE by county, unweighted tracts. Columns 10-20 ... 100+: bin mean minus 0-10 mean, SD units (SE).\n",
    "slope <W: linear slope per 10 mi in SD units within 0-W mi (SE); p <W: joint test of the bin dummies within 0-W mi.\n\n")
print(as.data.frame(tab), row.names = FALSE)
cat("\nHow balance degrades with the window (", length(v), "covariates ): covariates with joint p < .05, median and max over covariates of the\n",
    "largest |bin - (0-10)| in SD units; median |slope per 10 mi| and slopes significant at 5%\n")
print(merge(summ, sl, by = "window", all.x = TRUE)[match(wl(c(20, 30, 50, 100, Inf)), window)])
cat("\nRobustness of the bin differences (SD units, SE): main | pairs weighted equally | + log population density. p30/p50: joint test within 0-30 / 0-50 mi.\n")
rt <- dcast(rob[bin %in% c("10-20", "20-30", "30-50")], var ~ spec + bin, value.var = "b_sd")
rt <- merge(rt, dcast(unique(rob[, .(var, spec, p30, p50)]), var ~ spec, value.var = c("p30", "p50")), by = "var")[match(names(v), var)][!is.na(var)]
sp <- c("main", "equal pairs", "density"); setnames(rt, sub("equal pairs", "eqpair", names(rt)))
rt <- rt[, c("var", paste0(rep(c("main", "eqpair", "density"), each = 3), "_", c("10-20", "20-30", "30-50")), paste0("p30_", c("main", "eqpair", "density")), paste0("p50_", c("main", "eqpair", "density"))), with = FALSE]
rt[, variable := v[var]][, var := NULL]; setcolorder(rt, "variable")
print(as.data.frame(rt[, lapply(.SD, \(x) if (is.numeric(x)) round(x, 2) else x)]), row.names = FALSE)
cat("\nCovariates with joint p < .05 within 0-30 / 0-50 mi by spec (of", uniqueN(rob[spec == "density", var]), "excluding density itself):\n")
print(unique(rob[var != "ldens", .(var, spec, p30, p50)])[, .(rej30 = sum(p30 < .05), rej50 = sum(p50 < .05)), by = spec])
sink()
# ---- figure: bin means relative to 0-10 mi (SD units), with the state-wide and 0-30 mi linear fits
mid <- data.table(bin = factor(LAB, LAB), x = c(5, 15, 25, 40, 75, 125))
pb <- merge(bins, mid, by = "bin")[, label := factor(label, v)]
ln <- merge(slope[window %in% c(30, Inf)], pb[bin == "0-10", .(var, x0 = x)], by = "var")
ln <- ln[, .(x = if (window == 30) c(0, 30) else c(0, 140)), by = .(var, window, slope10_sd, x0)][, y := slope10_sd / 10 * (x - x0 - 0)]
ln[, `:=`(label = factor(v[var], v), fit = ifelse(is.infinite(window), "linear fit, whole state", "linear fit, 0-30 mi"))]
g <- ggplot(pb, aes(x, b_sd)) + geom_hline(yintercept = 0, colour = "grey75") +
  annotate("rect", xmin = 0, xmax = 30, ymin = -Inf, ymax = Inf, alpha = .07, fill = "grey20") +
  geom_line(data = ln, aes(x, y, linetype = fit), colour = "#d1772b", linewidth = .45) +
  geom_pointrange(aes(ymin = b_sd - 1.96 * se_sd, ymax = b_sd + 1.96 * se_sd), colour = "#2a64a8", size = .2, linewidth = .35) +
  facet_wrap(~ label, ncol = 4, labeller = label_wrap_gen(34)) + scale_x_continuous(breaks = c(0, 10, 20, 30, 50, 100, 140), labels = c(0, 10, 20, 30, 50, 100, "100+")) +
  scale_linetype_manual(NULL, values = c("linear fit, 0-30 mi" = "solid", "linear fit, whole state" = "22")) +
  labs(x = "distance to the state border (miles), low-tax side; points at bin midpoints (100+ drawn at 125)",
       y = "bin mean minus 0-10 mi mean, net of border-pair FE (SD units)",
       title = "Low-tax side of state borders: tract characteristics by distance to the border",
       subtitle = sprintf("%d tracts in %d ranked border pairs; pair FE, 95%% CI (SE by county); lines: linear fits within 0-30 mi and over the whole state, through the 0-10 mi bin", nrow(d), uniqueN(d$pair))) +
  theme_minimal(base_size = 8) + theme(legend.position = "bottom", strip.text = element_text(size = 6.5))
ggsave(file.path(OUT, "figures/c21_low_side_balance.png"), g, width = 11, height = 10, dpi = 150)
