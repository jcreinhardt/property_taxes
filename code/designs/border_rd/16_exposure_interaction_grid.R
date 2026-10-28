# The near x exposure x year interaction of 15 (own-tract lost subsidy per owner household) across the instrument cutoffs
# (near < 5 / 10 / 20 mi), the samples (all 59 pairs; rural pairs at the 30 / 45 / 70% metro cutoffs) and two forms of
# exposure: continuous (standardised across the low-side tracts within 50 mi) and binary (above the median lost subsidy of
# those tracts). y = sum_t b_t near x 1[t] + sum_t g_t near x exposure x 1[t] + sum_t d_t exposure x 1[t] + unit FE + pair x
# year FE, 2017 = 0, SE by county; districts / counties with population-weighted instruments (nb_panels). Reported: the
# post - pre change of g_t (mean 2018+ minus mean 2013-16) in SD units of the outcome's 2016 cross-section, per SD of exposure
# (continuous) or for high vs low exposure (binary). Tile figure of the t-statistics.
suppressPackageStartupMessages({library(data.table); library(DBI); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_rd"); invisible(out_dir("designs/border_rd/figures")); MX <- 50; REF <- 2017
source("code/designs/border_rd/nb_panels.R")
d0 <- readRDS(file.path(DATA, "built/geo/low_side_balance_tracts.rds"))[dist_mi < MX]
sh <- fread(file.path(OUT, "c22_metro_shares_by_pair_side.csv"))
tz <- fread(file.path(DATA, "built/exposure/salt_exposure_tract_zip2016.csv"), colClasses = c(tract = "character", zip = "character"))[, .(tract, zip)]
cz <- fread(file.path(DATA, "built/exposure/exposure_components_zip.csv"), colClasses = c(geo = "character"))[clean == TRUE, .(zip = geo, d_salt_own)]
d0 <- merge(d0, merge(tz, cz, by = "zip")[, .(tract, d_salt_own)], by = "tract")
MED <- median(d0$d_salt_own)
d0[, `:=`(ex_c = (d_salt_own - mean(d_salt_own)) / sd(d_salt_own), ex_b = as.integer(d_salt_own > MED))]
for (c in c(5, 10, 20)) { nz <- paste0("near", c); d0[, (nz) := as.integer(dist_mi < c)]; d0[, (paste0(nz, "_xc")) := get(nz) * ex_c]; d0[, (paste0(nz, "_xb")) := get(nz) * ex_b] }
samples <- list(`all 59 pairs` = unique(d0$pair), `rural 30%` = setdiff(unique(d0$pair), sh[side == "low" & share >= .3, pair]),
                `rural 45%` = setdiff(unique(d0$pair), sh[side == "low" & share >= .45, pair]), `rural 70%` = setdiff(unique(d0$pair), sh[side == "low" & share >= .7, pair]))
cat("low-side tracts with exposure:", nrow(d0), "| median lost subsidy per owner HH: $", round(MED), "| mean", round(mean(d0$d_salt_own)), "SD", round(sd(d0$d_salt_own)), "\n")
for (s in names(samples)) cat(sprintf("%s: %d tracts, high-exposure share %.2f, within 10 mi %.2f\n", s, d0[pair %in% samples[[s]], .N], d0[pair %in% samples[[s]], mean(ex_b)], d0[pair %in% samples[[s]] & near10 == 1, mean(ex_b)]))
vars <- list(cl = c(lp50 = "log median sale price", lbill = "log tax bill per parcel", eff50 = "effective tax rate", turn = "sales per 100 parcels"),
             ac = c(lhval = "log median home value (ACS)", lpop = "log population (ACS)"), mv = c(moveout = "move-out rate (Data Axle)"),
             f = c(lptax = "log district property tax per pupil", lspend = "log district spending per pupil", lenr = "log district enrolment"), sd = c(lptax = "log county school property tax (CoG)"))
units <- c(cl = "tract", ac = "tract", mv = "tract", f = "leaid", sd = "cty")
zs <- c(paste0("near", c(5, 10, 20)), paste0("near", c(5, 10, 20), "_xc"), paste0("near", c(5, 10, 20), "_xb"), "ex_c", "ex_b")
pp <- function(r) r[, .(post_min_pre = mean(b[y > REF]) - mean(b[y < REF & y >= 2013]), se = sqrt(mean(se[y > REF]^2) + mean(se[y < REF & y >= 2013]^2) / sum(y < REF & y >= 2013)), n = n[1])]
res <- list()
for (s in names(samples)) { d <- d0[pair %in% samples[[s]]]; P <- nb_panels(d, zs)
  for (tb in names(vars)) for (k in names(vars[[tb]])) { p <- P[[tb]][, yv := get(k)]; sdy <- sd(p[y == 2016, yv], na.rm = TRUE)
    for (c in c(5, 10, 20)) for (fm in c("c", "b")) { nz <- paste0("near", c); xz <- paste0(nz, "_x", fm); ez <- paste0("ex_", fm)
      r <- es(p, xz, units[tb], "cty", extra = sprintf(" + i(y, %s, ref = %d) + i(y, %s, ref = %d)", nz, REF, ez, REF))
      if (!is.null(r)) res[[length(res) + 1]] <- pp(r)[, `:=`(sample = s, cutoff = c, form = ifelse(fm == "c", "continuous (per SD)", "binary (above median)"), outcome = vars[[tb]][k], unit = units[tb], sdy = sdy)] } } }
res <- rbindlist(res)[, `:=`(b_sd = post_min_pre / sdy, se_sd = se / sdy)][, t := b_sd / se_sd]
fwrite(res, file.path(OUT, "c29_exposure_interaction_grid.csv"))
sink(file.path(OUT, "c29_exposure_interaction_grid.txt"), split = TRUE)
cat("near x exposure x year: post - pre change (mean 2018+ minus 2013-16) in SD units of the outcome, per SD of exposure (continuous) or high vs low (binary, cut at the\n",
    "median lost subsidy per owner HH, $", round(MED), "). Unit FE + pair x year FE, near x year and exposure x year included, SE by county. Cells: estimate (t).\n")
for (fm in unique(res$form)) for (s in names(samples)) { cat("\n==", fm, "|", s, "\n")
  w <- dcast(res[form == fm & sample == s, .(outcome, cutoff = paste0("near<", cutoff), cell = sprintf("%.3f (%.1f)", b_sd, t))], outcome ~ cutoff, value.var = "cell")
  print(as.data.frame(w[match(unlist(vars), outcome)][, c("outcome", "near<5", "near<10", "near<20")]), row.names = FALSE) }
cat("\ncells with |t| > 1.96 (of", length(unlist(vars)), "outcomes) and median |estimate|:\n"); print(res[, .(sig = sum(abs(t) > 1.96), median_abs = round(median(abs(b_sd)), 3)), by = .(form, sample, cutoff)][order(form, sample, cutoff)], nrows = 30)
cat("\nby outcome: sign consistency across the 24 cells (share positive) and cells with |t| > 1.96\n"); print(res[, .(share_pos = round(mean(b_sd > 0), 2), sig = sum(abs(t) > 1.96), median_b = round(median(b_sd), 3)), by = outcome])
sink()
res[, `:=`(outcome = factor(outcome, rev(unlist(vars))), cutoff = factor(paste0("near < ", cutoff, " mi"), paste0("near < ", c(5, 10, 20), " mi")), sample = factor(sample, names(samples)))]
g <- ggplot(res, aes(cutoff, outcome, fill = pmax(pmin(t, 3), -3))) + geom_tile(colour = "white") + geom_text(aes(label = sprintf("%.2f", b_sd)), size = 2.2) +
  facet_grid(form ~ sample) + scale_fill_gradient2("t-stat", low = "#c0392b", mid = "white", high = "#2a64a8", limits = c(-3, 3)) +
  labs(x = NULL, y = NULL, title = "Post-2017 change in the near x exposure interaction, by cutoff, sample and exposure form (low-tax side within 50 mi)",
       subtitle = sprintf("cell: mean(g_t, 2018+) - mean(g_t, 2013-16), SD units of the outcome, per SD of own-tract lost subsidy (continuous) or high vs low (binary, median $%.0f); colour: t. Tract/unit FE + pair x year FE", MED)) +
  theme_minimal(base_size = 8) + theme(axis.text.x = element_text(angle = 30, hjust = 1))
ggsave(file.path(OUT, "figures/c29_exposure_interaction_grid.png"), g, width = 12, height = 7, dpi = 150)
