# Low-tax side, within 50 mi of the border: the tax rate, outcomes and controls as a function of distance in 5-mile bins,
# for all ranked pairs and for a "rural" subset of pairs. Reads the tract table 08_low_side_balance_by_distance.R writes
# (data/built/geo/low_side_balance_tracts.rds). A pair is dropped from the rural subset when a metropolitan CBSA (TIGER cb
# 2019, LSAD M1) with >= METRO_POP people (ACS 2013-17 tract population summed over iv_tract.parquet) has >= METRO_SHARE of
# that population on the pair's low side within 50 mi (METRO_SHARE, default 0.45 since 2026-10-28: 0.3 was the first choice, 0.5 lets NJ-NY back in), i.e. the metro's core sits inside the window (NJ-NY: New York City;
# NJ-PA: Philadelphia; IL-MO: St. Louis; KS-MO: Kansas City; MN-WI: Minneapolis; ...). METRO_SIDE = low (default) | high |
# either chooses which side's window the share is measured in; outputs get the suffix _high / _either, and _c30 / _c50 etc. when METRO_SHARE != 0.45. Bin means are Y ~ i(bin, ref = 0-5)
# | pair, SE by county, in SD units of the all-pairs sample within 50 mi; lines are Y ~ dist | pair fits within 0-50 mi.
suppressPackageStartupMessages({library(data.table); library(DBI); library(sf); library(tigris); library(fixest); library(ggplot2)})
options(tigris_use_cache = TRUE)
source("code/R/paths.R"); OUT <- out_dir("designs/border_rd"); invisible(out_dir("designs/border_rd/figures"))
METRO_POP <- as.numeric(Sys.getenv("METRO_POP", 5e5)); METRO_SHARE <- as.numeric(Sys.getenv("METRO_SHARE", .45)); MX <- 50; BW <- 5
METRO_SIDE <- Sys.getenv("METRO_SIDE", "low"); SFX <- paste0(if (METRO_SIDE == "low") "" else paste0("_", METRO_SIDE), if (METRO_SHARE == .45) "" else sprintf("_c%.0f", 100 * METRO_SHARE))
d <- readRDS(file.path(DATA, "built/geo/low_side_balance_tracts.rds"))[dist_mi < MX]
d[, bin := cut(dist_mi, seq(0, MX, BW), paste0(seq(0, MX - BW, BW), "-", seq(BW, MX, BW)), right = FALSE)]
# ---- metro pairs: tracts (both sides, all distances) -> metropolitan CBSA -> CBSA population; share of it on the low side within 50 mi
cb <- st_transform(core_based_statistical_areas(cb = TRUE, year = 2019, progress_bar = FALSE)[, c("GEOID", "NAME", "LSAD")], 5070)
t <- dq(sprintf("SELECT tract, lat, lon, pop FROM '%s/data/built/geo/iv_tract.parquet' WHERE lat IS NOT NULL", ROOT))
j <- st_join(st_transform(st_as_sf(t, coords = c("lon", "lat"), crs = 4326), 5070), cb[cb$LSAD == "M1", ], join = st_within, left = TRUE)
tc <- as.data.table(st_drop_geometry(j))[!duplicated(tract) & !is.na(GEOID), .(tract, cbsa = GEOID, cbsa_name = NAME, pop)]
tc[, cbsa_pop := sum(pop, na.rm = TRUE), by = cbsa]
fwrite(merge(tc, t[, .(tract, lat, lon)], by = "tract"), file.path(DATA, "built/geo/tract_cbsa_2019.csv"))   # read by 11_rural_rule_sensitivity.R
hs <- readRDS(file.path(DATA, "built/geo/iv_tract_side.rds"))[T == 30 & high == 1 & dist_mi < MX, .(tract, pair, pop, side = "high")]
m <- merge(rbind(d[, .(tract, pair, pop, side = "low")], hs), tc[, .(tract, cbsa, cbsa_name, cbsa_pop)], by = "tract")
m <- m[, .(share = sum(pop, na.rm = TRUE) / cbsa_pop[1], n = .N), by = .(pair, side, cbsa, cbsa_name, cbsa_pop)]
fwrite(m[cbsa_pop >= METRO_POP][order(-cbsa_pop, pair, side)], file.path(OUT, "c22_metro_shares_by_pair_side.csv"))
m <- m[cbsa_pop >= METRO_POP & share >= METRO_SHARE & (METRO_SIDE == "either" | side == METRO_SIDE)]
metro <- m[order(pair, -cbsa_pop)][, .(metros = paste(sprintf("%s (%.1fm, %.0f%% %s)", sub(",.*", "", cbsa_name), cbsa_pop / 1e6, 100 * share, side), collapse = "; ")), by = pair]
d[, rural := !pair %in% metro$pair]
fwrite(metro, file.path(OUT, paste0("c22_low_side_metro_pairs", SFX, ".csv")))
# ---- variables: the rate, then outcomes, then controls
v <- c(med = "[rate] effective tax rate, % (CoreLogic)",
       lp50 = "[outcome] log median sale price 2016 (CoreLogic)", lhval = "[outcome] log median home value (ACS)",
       moveout = "[outcome] move-out rate 2014-17, %/yr (Data Axle)", sh_moved = "[outcome] moved in the last year (ACS)", turnover = "[outcome] sales per 100 parcels 2016 (CoreLogic)",
       linc = "[control] log median HH income (ACS)", sh_agi100 = "[control] share of returns AGI >= $100k (SOI ZIP)", sh_item = "[control] itemiser share (SOI ZIP)",
       exz = "[control] SALT above $10k per return, $k (SOI ZIP)", age = "[control] median age (ACS)", hhsize = "[control] household size (ACS)",
       sh_u18 = "[control] share under 18 (ACS)", sh_hh_kids = "[control] share of HHs with children (ACS)", sh_own = "[control] owner share (ACS)",
       sh_ba = "[control] share BA+ (ACS)", sh_white = "[control] share white (ACS)", yrbuilt = "[control] median year built (ACS)",
       lpop = "[control] log population (ACS)", ldens = "[control] log population density (ACS, TIGER)",
       ach = "[control] district test scores (SEDA)", ses = "[control] district SES (SEDA)")
sds <- sapply(names(v), \(k) sd(d[[k]], na.rm = TRUE))
smp <- list(`all pairs` = d, `rural pairs` = d[rural == TRUE])
fit <- function(k, s) { x <- smp[[s]][is.finite(get(k))]
  m <- feols(as.formula(sprintf("%s ~ i(bin, ref = '0-5') | pair", k)), x, cluster = ~cty)
  ct <- as.data.table(coeftable(m), keep.rownames = "term")[, .(bin = sub("bin::", "", term), b = Estimate, se = `Std. Error`)]
  ct <- rbind(data.table(bin = "0-5", b = 0, se = 0), ct)[, `:=`(var = k, sample = s, b_sd = b / sds[k], se_sd = se / sds[k], n = nobs(m), pairs = uniqueN(x$pair),
    p_joint = wald(m, "bin", print = FALSE)$p, raw = sapply(bin, \(bb) x[bin == bb, mean(get(k))]))]
  sl <- coeftable(feols(as.formula(sprintf("%s ~ dist_mi | pair", k)), x, cluster = ~cty))
  ct[, `:=`(slope10_sd = 10 * sl[1, 1] / sds[k], slope10_se = 10 * sl[1, 2] / sds[k], slope_p = sl[1, 4])] }
res <- rbindlist(lapply(names(smp), \(s) rbindlist(lapply(names(v), fit, s = s))))
res[, `:=`(label = factor(v[var], v), x = as.numeric(sub("-.*", "", bin)) + BW / 2)]
fwrite(res, file.path(OUT, paste0("c22_low_side_profiles_50mi", SFX, ".csv")))
# ---- summary table: slope per 10 mi (SD), joint p, and the 30-50 minus 0-10 difference (SD), by sample
su <- res[, .(n = n[1], pairs = pairs[1], slope10_sd = slope10_sd[1], slope10_se = slope10_se[1], slope_p = slope_p[1], p_joint = p_joint[1],
              d30_50_vs_0_10 = mean(b_sd[x > 30]) - mean(b_sd[x < 10]), max_abs_sd = max(abs(b_sd))), by = .(var, sample)]
tab <- dcast(su, var ~ sample, value.var = c("slope10_sd", "slope10_se", "slope_p", "p_joint", "d30_50_vs_0_10"))
tab <- tab[match(names(v), var)][, variable := v[var]][, var := NULL]; setcolorder(tab, "variable")
fwrite(tab, file.path(OUT, paste0("c22_low_side_profiles_50mi_table", SFX, ".csv")))
sink(file.path(OUT, paste0("c22_low_side_profiles_50mi", SFX, ".txt")), split = TRUE)
cat("Low-tax side within", MX, "mi, 5-mi bins, pair FE, SE by county, SD units of the all-pairs sample.\n")
cat("Rural subset drops pairs where a metro CBSA >=", METRO_POP / 1e6, "m has >=", 100 * METRO_SHARE, "% of its population on the", METRO_SIDE, "side within 50 mi:\n")
print(metro[order(pair)], nrows = 100)
for (s in names(smp)) cat(sprintf("%s: %d tracts, %d pairs; by bin: %s\n", s, nrow(smp[[s]]), uniqueN(smp[[s]]$pair), paste(smp[[s]][, .N, by = bin][order(bin), N], collapse = " ")))
cat("\nslope: per 10 mi in SD units (SE) within 0-50; p_joint: bins jointly 0; d: mean of the 30-50 mi bins minus mean of the 0-10 mi bins (SD)\n")
pr <- su[, .(variable = v[var], sample, slope = sprintf("%.3f (%.3f)", slope10_sd, slope10_se), p_slope = round(slope_p, 3), p_joint = round(p_joint, 3), d30_50_vs_0_10 = round(d30_50_vs_0_10, 2), n, pairs)]
for (s in names(smp)) { cat("\n==", s, "\n"); print(as.data.frame(pr[sample == s][match(v, variable)][, -"sample"]), row.names = FALSE) }
cat("\nCovariates ([control] + [outcome]) with p_joint < .05 / slope p < .05, by sample:\n")
print(su[var != "med", .(k = .N, joint_rej = sum(p_joint < .05), slope_rej = sum(slope_p < .05), median_abs_slope10 = round(median(abs(slope10_sd)), 3), median_abs_d = round(median(abs(d30_50_vs_0_10)), 2)), by = sample])
sink()
# ---- figure
ln <- unique(res[, .(var, label, sample, slope10_sd)])[, .(x = c(0, MX)), by = .(var, label, sample, slope10_sd)][, y := slope10_sd / 10 * (x - BW / 2)]
g <- ggplot(res, aes(x, b_sd, colour = sample)) + geom_hline(yintercept = 0, colour = "grey75") +
  geom_line(data = ln, aes(x, y, colour = sample), linetype = "22", linewidth = .4) +
  geom_pointrange(aes(ymin = b_sd - 1.96 * se_sd, ymax = b_sd + 1.96 * se_sd), position = position_dodge(width = 2), size = .15, linewidth = .3) +
  facet_wrap(~ label, ncol = 4, labeller = label_wrap_gen(36)) + scale_x_continuous(breaks = seq(0, MX, 10)) +
  scale_colour_manual(NULL, values = c("all pairs" = "#2a64a8", "rural pairs" = "#2e8b57")) +
  labs(x = "distance to the state border (miles), low-tax side, 5-mile bins", y = "bin mean minus 0-5 mi mean, net of border-pair FE (SD units)",
       title = "Low-tax side within 50 miles of the border: rate, outcomes and controls by distance",
       subtitle = sprintf("all pairs: %d tracts, %d pairs; rural pairs (no metro >= %.1fm with >= %.0f%% of its people in the %s-side window): %d tracts, %d pairs. Pair FE, 95%% CI (SE by county); dashed: linear fit 0-50 mi",
                          nrow(d), uniqueN(d$pair), METRO_POP / 1e6, 100 * METRO_SHARE, METRO_SIDE, sum(d$rural), uniqueN(d[rural == TRUE, pair]))) +
  theme_minimal(base_size = 8) + theme(legend.position = "bottom", strip.text = element_text(size = 6.5))
ggsave(file.path(OUT, paste0("figures/c22_low_side_profiles_50mi", SFX, ".png")), g, width = 11, height = 10.5, dpi = 150)
