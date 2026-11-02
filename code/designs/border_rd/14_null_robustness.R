# Are the post-2017 nulls of 13 an artefact of the metro cutoff or of how the instrument is discretised, and does the
# near-border effect move with SALT exposure?  Low-tax side within 50 mi.
#  (a) Grid: samples = all 59 pairs, rural pairs at the 30% / 45% / 70% metro cutoffs (09's rule on the low side);
#      instruments = near within 5 / 10 / 20 mi, linear distance per 10 mi, log distance (sign flipped so "closer" is positive).
#      For each outcome: the event study of 13 (instrument x year, unit FE + pair x year FE, 2017 = 0, SE by county) summarised
#      as mean(b_t, 2018+) - mean(b_t, 2013-16), in SD units of the outcome's 2016 cross-section so instruments are comparable
#      (near dummies: effect of being near; distance: effect of 1 SD of the instrument). Tile figure of the t-statistics.
#  (b) Exposure: near x year x exposure added to the rural-45% and all-pairs event studies, where exposure is (i) the SALT
#      exposure of the HIGH side facing the pair (population-weighted mean over the high side's tracts within 10 mi;
#      standardised across pairs) and (ii) the tract's own exposure (standardised; own exposure x year as a control), for three
#      measures mapped ZIP -> tract through the modal sales ZIP: excess SALT per return (build/exposure/02), and the lost
#      federal subsidy per owner household 2016-18 and its property-tax part (build/exposure/05, clean ZIPs only).
suppressPackageStartupMessages({library(data.table); library(DBI); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_rd"); invisible(out_dir("designs/border_rd/figures")); MX <- 50; REF <- 2017
source("code/designs/border_rd/nb_panels.R")
d0 <- readRDS(file.path(DATA, "built/geo/low_side_balance_tracts.rds"))[dist_mi < MX]
sh <- fread(file.path(OUT, "c22_metro_shares_by_pair_side.csv"))
d0[, `:=`(near5 = as.integer(dist_mi < 5), near10 = as.integer(dist_mi < 10), near20 = as.integer(dist_mi < 20), d10 = -dist_mi / 10, ldist = -log(pmax(dist_mi, .5)))]
zs <- c("near5", "near10", "near20", "d10", "ldist"); zlab <- c(near5 = "near < 5 mi", near10 = "near < 10 mi", near20 = "near < 20 mi", d10 = "-distance / 10 mi", ldist = "-log distance")
samples <- list(`all 59 pairs` = unique(d0$pair), `rural 30%` = setdiff(unique(d0$pair), sh[side == "low" & share >= .3, pair]),
                `rural 45%` = setdiff(unique(d0$pair), sh[side == "low" & share >= .45, pair]), `rural 70%` = setdiff(unique(d0$pair), sh[side == "low" & share >= .7, pair]))
vars <- list(cl = c(lp50 = "log median sale price", lbill = "log tax bill per parcel", eff50 = "effective tax rate", turn = "sales per 100 parcels"),
             ac = c(lhval = "log median home value (ACS)", lpop = "log population (ACS)"), mv = c(moveout = "move-out rate (Data Axle)"),
             f = c(lptax = "log district property tax per pupil", lspend = "log district spending per pupil", lenr = "log district enrolment"), sd = c(lptax = "log county school property tax (CoG)"))
units <- c(cl = "tract", ac = "tract", mv = "tract", f = "leaid", sd = "cty")
pp <- function(r) if (!is.null(r)) r[, .(post_min_pre = mean(b[y > REF]) - mean(b[y < REF & y >= 2013]), se = sqrt(mean(se[y > REF]^2) + mean(se[y < REF & y >= 2013]^2) / sum(y < REF & y >= 2013)), n = n[1])]
# ---- (a) grid
grid <- list()
for (s in names(samples)) { d <- d0[pair %in% samples[[s]]]; P <- nb_panels(d, zs)
  for (tb in names(vars)) for (k in names(vars[[tb]])) { p <- P[[tb]][, yv := get(k)]; sdy <- sd(p[y == 2016, yv], na.rm = TRUE)
    for (z in zs) { sdz <- if (grepl("^near", z)) 1 else sd(p[y == 2016, get(z)], na.rm = TRUE)
      r <- pp(es(p, z, units[tb], "cty")); if (!is.null(r)) grid[[length(grid) + 1]] <- r[, `:=`(sample = s, z = z, outcome = vars[[tb]][k], unit = units[tb], sdy = sdy, sdz = sdz)] } } }
grid <- rbindlist(grid)[, `:=`(b_sd = post_min_pre * sdz / sdy, se_sd = se * sdz / sdy)][, t := b_sd / se_sd]
fwrite(grid, file.path(OUT, "c27_null_robustness_grid.csv"))
# ---- (b) exposure interactions: three measures, each mapped ZIP -> tract through the modal sales ZIP (build/exposure/02)
tz <- fread(file.path(DATA, "built/exposure/salt_exposure_tract_zip2016.csv"), colClasses = c(tract = "character", zip = "character"))[, .(tract, zip, exz)]
cz <- fread(file.path(DATA, "built/exposure/exposure_components_zip.csv"), colClasses = c(geo = "character"))[clean == TRUE, .(zip = geo, d_salt_own, d_prop_own)]
ex <- merge(tz, cz, by = "zip", all.x = TRUE)
meas <- c(exz = "excess SALT per return, $k (2016)", d_salt_own = "lost subsidy per owner HH, $ (2016-18, all SALT)", d_prop_own = "lost subsidy per owner HH, $ (property-tax part)")
hs <- readRDS(file.path(DATA, "built/geo/iv_tract_side.rds"))[T == 30 & high == 1 & dist_mi < 10, .(tract, pair, pop)]
hi <- merge(hs, ex, by = "tract")[, lapply(.SD, \(v) weighted.mean(v, pop, na.rm = TRUE)), by = pair, .SDcols = names(meas)]
setnames(hi, names(meas), paste0("hi_", names(meas)))
d0 <- merge(d0[, setdiff(names(d0), c("exz", intersect(names(d0), c("d_salt_own", "d_prop_own")))), with = FALSE], ex[, c("tract", names(meas)), with = FALSE], by = "tract", all.x = TRUE)
d0 <- merge(d0, hi, by = "pair", all.x = TRUE)[, near := near10]
cat("high-side exposure by pair (within 10 mi), pairs:", nrow(hi), "\n"); print(hi[, lapply(.SD, \(v) round(c(mean = mean(v, na.rm = TRUE), sd = sd(v, na.rm = TRUE), max = max(v, na.rm = TRUE)), 2)), .SDcols = paste0("hi_", names(meas))])
cat("correlation of the three measures across pairs (high side):\n"); print(round(cor(hi[, paste0("hi_", names(meas)), with = FALSE], use = "pairwise"), 2))
inter <- list()
for (mm in names(meas)) {
  zh <- (d0[[paste0("hi_", mm)]] - mean(hi[[paste0("hi_", mm)]], na.rm = TRUE)) / sd(hi[[paste0("hi_", mm)]], na.rm = TRUE)   # standardised across pairs
  zo <- (d0[[mm]] - mean(d0[[mm]], na.rm = TRUE)) / sd(d0[[mm]], na.rm = TRUE)                                                   # standardised across low-side tracts
  d0[, `:=`(ex_h = zh, ex_o = zo, near_xh = near10 * zh, near_xo = near10 * zo)]
  for (s in c("rural 45%", "all 59 pairs")) { d <- d0[pair %in% samples[[s]] & is.finite(ex_h)]; P <- nb_panels(d, c("near", "near_xh", "near_xo", "ex_o"))
    for (tb in names(vars)) for (k in names(vars[[tb]])) { p <- P[[tb]][, yv := get(k)]; sdy <- sd(p[y == 2016, yv], na.rm = TRUE)
      xh <- es(p, "near_xh", units[tb], "cty", extra = sprintf(" + i(y, near, ref = %d)", REF))
      xo <- es(p[is.finite(ex_o) & is.finite(near_xo)], "near_xo", units[tb], "cty", extra = sprintf(" + i(y, near, ref = %d) + i(y, ex_o, ref = %d)", REF, REF))
      for (w in list(list("high side (pair)", xh), list("own tract", xo))) if (!is.null(w[[2]])) inter[[length(inter) + 1]] <- copy(w[[2]])[, `:=`(sample = s, exposure = w[[1]], measure = mm, outcome = vars[[tb]][k], unit = units[tb], sdy = sdy)] } } }
inter <- rbindlist(inter); fwrite(inter, file.path(OUT, "c27_exposure_interaction.csv"))
isum <- inter[, pp(.SD), by = .(sample, exposure, measure, outcome, unit, sdy)][, `:=`(b_sd = post_min_pre / sdy, se_sd = se / sdy)][, t := b_sd / se_sd]
# ---- text
sink(file.path(OUT, "c27_null_robustness.txt"), split = TRUE)
cat("(a) Post-2017 change in the instrument x year coefficients (mean 2018+ minus mean 2013-16), SD units of the outcome's 2016 cross-section\n",
    "   (near dummies: being near; distance: per SD of the instrument, closer = positive). Cells: estimate (t). Unit FE + pair x year FE, SE by county.\n")
for (s in names(samples)) { cat("\n== sample:", s, "| pairs:", length(samples[[s]]), "\n")
  w <- dcast(grid[sample == s, .(outcome, z = factor(z, zs), cell = sprintf("%.2f (%.1f)", b_sd, t))], outcome ~ z, value.var = "cell")
  print(as.data.frame(w[match(unlist(vars), outcome)]), row.names = FALSE) }
cat("\nShare of (outcome x sample x instrument) cells with |t| > 1.96:", round(mean(abs(grid$t) > 1.96), 3), "of", nrow(grid), "| by sample:\n"); print(grid[, .(cells = .N, sig = sum(abs(t) > 1.96), median_abs_b_sd = round(median(abs(b_sd)), 3)), by = sample])
cat("by instrument:\n"); print(grid[, .(cells = .N, sig = sum(abs(t) > 1.96), median_abs_b_sd = round(median(abs(b_sd)), 3)), by = z])
cat("\n(b) near x year x exposure: post - pre change of the interaction (SD of outcome per SD of exposure), cells: estimate (t)\n")
for (mm in names(meas)) { cat("\n== exposure measure:", meas[mm], "\n")
  w <- dcast(isum[measure == mm, .(outcome, col = paste(sample, "|", exposure), cell = sprintf("%.3f (%.1f)", b_sd, t))], outcome ~ col, value.var = "cell")
  print(as.data.frame(w[match(unlist(vars), outcome)]), row.names = FALSE) }
cat("\ncells with |t| > 1.96 by measure x sample x exposure (of", length(unlist(vars)), "outcomes):\n"); print(isum[, .(sig = sum(abs(t) > 1.96), median_abs = round(median(abs(b_sd)), 3)), by = .(measure, sample, exposure)])
sink()
# ---- figures
grid[, `:=`(outcome = factor(outcome, rev(unlist(vars))), z = factor(zlab[z], zlab), sample = factor(sample, names(samples)))]
g <- ggplot(grid, aes(z, outcome, fill = pmax(pmin(t, 3), -3))) + geom_tile(colour = "white") + geom_text(aes(label = sprintf("%.2f", b_sd)), size = 2.3) +
  facet_wrap(~ sample, nrow = 1) + scale_fill_gradient2("t-stat", low = "#c0392b", mid = "white", high = "#2a64a8", limits = c(-3, 3)) +
  labs(x = NULL, y = NULL, title = "Post-2017 change in the near-border effect, by sample and instrument form (low-tax side within 50 mi)",
       subtitle = "cell: mean(b_t, 2018+) - mean(b_t, 2013-16) in SD units of the outcome (near: being near; distance: per SD, closer = +); colour: t-statistic. Unit FE + pair x year FE") +
  theme_minimal(base_size = 8) + theme(axis.text.x = element_text(angle = 30, hjust = 1), legend.position = "right")
ggsave(file.path(OUT, "figures/c27_null_robustness_grid.png"), g, width = 12, height = 5, dpi = 150)
for (ee in c("high side (pair)", "own tract")) {
  pl <- inter[sample == "rural 45%" & exposure == ee & measure != "exz"][, `:=`(outcome = factor(outcome, unlist(vars)), measure = factor(meas[measure], meas))]   # figure: the two lost-subsidy measures only
  g2 <- ggplot(pl, aes(y, b / sdy, colour = measure)) + geom_hline(yintercept = 0, colour = "grey70") + geom_vline(xintercept = REF + .5, linetype = "22", colour = "grey50") +
    geom_pointrange(aes(ymin = (b - 1.96 * se) / sdy, ymax = (b + 1.96 * se) / sdy), position = position_dodge(width = .6), size = .2, linewidth = .3) + geom_line(position = position_dodge(width = .6), linewidth = .3) +
    facet_wrap(~ outcome, scales = "free_y", ncol = 4, labeller = label_wrap_gen(30)) + scale_x_continuous(breaks = seq(2012, 2024, 2)) +
    scale_colour_manual(NULL, values = c("#2a64a8", "#2e8b57"), drop = TRUE) +
    labs(x = NULL, y = "near x year x exposure (SD of outcome per SD of exposure), 2017 = 0; 95% CI, SE by county",
         title = sprintf("Does the near-border effect move with SALT exposure of the %s after 2017?  Rural 45%% pairs, low-tax side within 50 mi", ee),
         subtitle = "near x year included; own exposure x year included with the own-tract interaction; pair x year FE absorb the high side's exposure x year. ZIP measures at the tract's modal sales ZIP, censored ZIPs dropped") +
    theme_minimal(base_size = 8) + theme(legend.position = "bottom", strip.text = element_text(size = 6.5))
  ggsave(file.path(OUT, sprintf("figures/c27_exposure_interaction_%s.png", ifelse(ee == "own tract", "own", "high"))), g2, width = 11, height = 7, dpi = 150) }
