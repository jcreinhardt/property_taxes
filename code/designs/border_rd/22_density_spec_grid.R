# Zooming in on design B of 21 (distance to the border with log density x year as a control, all pairs): the exposure
# interaction and the main term under alternative instruments, exposure forms and samples. Every spec:
#   y = sum_t b_t Z x 1[t] + sum_t g_t Z x E x 1[t] + sum_t d_t E x 1[t] + sum_t l_t log density x 1[t] + tract/unit FE + pair x year FE
# (pair x side x year FE in the both-sides sample), 2017 = 0, SE by county.
#   Z: z = -distance / 10 mi (continuous); near < 5 / 10 / 20 mi.   E: own lost subsidy per owner HH, continuous (SD) or 1(above median).
#   samples: low side, all 59 pairs | both sides, all 129 pair-sides (no metro exclusion) | low side, tracts < 5,000 per sq mi |
#            low side, tracts < 1,000 per sq mi | low side, rural 45% pairs.
# Reported: post - pre (mean 2018+ minus 2013-16) in SD units of the outcome for g_t (per SD of E, or high vs low) and b_t
# (per 10 mi closer, or near); tile figures by sample; event-study lines for the interaction in four reference specs.
suppressPackageStartupMessages({library(data.table); library(DBI); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_rd"); invisible(out_dir("designs/border_rd/figures")); MX <- 50; REF <- 2017
source("code/designs/border_rd/nb_panels.R")
iv <- readRDS(file.path(DATA, "built/geo/iv_tract_side.rds"))[T == 30 & dist_mi < MX]
iv[, side := ifelse(high == 1, "high", "low")][, ps := paste(pair, side)]
tr <- as.data.table(foreign::read.dbf(file.path(path.expand("~/Library/Caches/tigris"), "cb_2019_us_tract_500k.dbf"), as.is = TRUE))[, .(tract = GEOID, aland = as.numeric(ALAND))]
iv <- merge(iv, tr, by = "tract", all.x = TRUE)[, dens := pmax(pop, 1) / (aland / 2589988)][, ldens := log(dens)]
tz <- fread(file.path(DATA, "built/exposure/salt_exposure_tract_zip2016.csv"), colClasses = c(tract = "character", zip = "character"))[, .(tract, zip)]
cz <- fread(file.path(DATA, "built/exposure/exposure_components_zip.csv"), colClasses = c(geo = "character"))[returns16 >= 100 & is.finite(d_salt_own), .(zip = geo, d_salt_own)]
iv <- merge(iv, merge(tz, cz, by = "zip")[, .(tract, d_salt_own)], by = "tract")[is.finite(ldens)]
MED <- median(iv[side == "low", d_salt_own])
iv[, `:=`(ex_c = (d_salt_own - mean(d_salt_own)) / sd(d_salt_own), ex_b = as.integer(d_salt_own > MED), z = -dist_mi / 10, near5 = as.integer(dist_mi < 5), near10 = as.integer(dist_mi < 10), near20 = as.integer(dist_mi < 20))]
Zs <- c(z = "-distance / 10 mi", near5 = "near < 5 mi", near10 = "near < 10 mi", near20 = "near < 20 mi"); Es <- c(c = "continuous (per SD)", b = "binary (above median)")
for (zz in names(Zs)) for (ee in names(Es)) iv[, (paste0(zz, "_x", ee)) := get(zz) * get(paste0("ex_", ee))]
metro_low <- fread(file.path(OUT, "c22_low_side_metro_pairs.csv"))$pair
samples <- list(`low side, all pairs` = iv[side == "low"], `both sides, all pair-sides` = copy(iv)[, pair := ps],
                `low side, density < 5,000/sq mi` = iv[side == "low" & dens < 5000], `low side, density < 1,000/sq mi` = iv[side == "low" & dens < 1000], `low side, rural 45%` = iv[side == "low" & !pair %in% metro_low])
cat("median lost subsidy (low side): $", round(MED), "\n"); for (s in names(samples)) cat(sprintf("%s: %d tracts, %d pairs(-sides), %d near10, high-exposure share %.2f\n", s, nrow(samples[[s]]), uniqueN(samples[[s]]$pair), sum(samples[[s]]$near10), mean(samples[[s]]$ex_b)))
vars <- list(cl = c(lp50 = "log median sale price", lbill = "log tax bill per parcel", eff50 = "effective tax rate", turn = "sales per 100 parcels"),
             ac = c(lhval = "log median home value (ACS)", lpop = "log population (ACS)"), mv = c(moveout = "move-out rate (Data Axle)"),
             f = c(lptax = "log district property tax per pupil", lspend = "log district spending per pupil", lenr = "log district enrolment"), sd = c(lptax = "log county school property tax (CoG)"))
units <- c(cl = "tract", ac = "tract", mv = "tract", f = "leaid", sd = "cty")
zs <- c(names(Zs), paste0(rep(names(Zs), each = 2), "_x", names(Es)), "ex_c", "ex_b", "ldens")
pp <- function(r) r[, .(post_min_pre = mean(b[y > REF]) - mean(b[y < REF & y >= 2013]), se = sqrt(mean(se[y > REF]^2) + mean(se[y < REF & y >= 2013]^2) / sum(y < REF & y >= 2013)), n = n[1])]
both <- function(p, zz, ee, fe_unit) { x <- p[is.finite(yv)]
  m <- feols(as.formula(sprintf("yv ~ i(y, %s, ref = %d) + i(y, %s_x%s, ref = %d) + i(y, ex_%s, ref = %d) + i(y, ldens, ref = %d) | %s + pair^y", zz, REF, zz, ee, REF, ee, REF, REF, fe_unit)), x, cluster = ~cty)
  ct <- as.data.table(coeftable(m), keep.rownames = "term")[grepl(sprintf("^y::\\d+:(%s|%s_x%s)$", zz, zz, ee), term)]
  ct[, `:=`(y = as.integer(sub("y::(\\d+):.*", "\\1", term)), coef = ifelse(grepl("_x", term), "interaction", "main"), n = nobs(m))][, .(y, coef, b = Estimate, se = `Std. Error`, n)] }
res <- list(); ser <- list()
for (s in names(samples)) { P <- nb_panels(samples[[s]], zs)
  for (tb in names(vars)) for (k in names(vars[[tb]])) { p <- P[[tb]][, yv := get(k)]; sdy <- sd(p[y == 2016, yv], na.rm = TRUE)
    for (zz in names(Zs)) for (ee in names(Es)) { r <- both(p, zz, ee, units[tb]); r <- rbind(r, data.table(y = REF, coef = c("main", "interaction"), b = 0, se = 0, n = r$n[1]))
      r[, `:=`(sample = s, Z = Zs[zz], E = Es[ee], outcome = vars[[tb]][k], b_sd = b / sdy, se_sd = se / sdy)]; ser[[length(ser) + 1]] <- r
      res[[length(res) + 1]] <- r[, pp(.SD), by = .(sample, Z, E, outcome, coef)][, `:=`(sdy = sdy)] } } }
res <- rbindlist(res)[, `:=`(b_sd = post_min_pre / sdy, se_sd = se / sdy)][, t := b_sd / se_sd]; ser <- rbindlist(ser)
fwrite(res, file.path(OUT, "c35_density_spec_grid.csv")); fwrite(ser, file.path(OUT, "c35_density_spec_grid_series.csv"))
sink(file.path(OUT, "c35_density_spec_grid.txt"), split = TRUE)
cat("Design B grid: Z x year + Z x E x year + E x year + log density x year | unit FE + pair x year FE (pair x side x year for both sides), SE by county.\n",
    "post - pre in SD units of the outcome: interaction per SD of exposure (continuous) or high vs low (binary); main term per 10 mi closer or near. Cells: estimate (t).\n")
for (cc in c("interaction", "main")) for (s in names(samples)) { cat("\n==", cc, "|", s, "\n")
  w <- dcast(res[coef == cc & sample == s, .(outcome, col = paste(Z, "|", substr(E, 1, 6)), cell = sprintf("%.3f (%.1f)", b_sd, t))], outcome ~ col, value.var = "cell")
  print(as.data.frame(w[match(unlist(vars), outcome)]), row.names = FALSE) }
cat("\nsummary: cells with |t| > 1.96 (of 11 outcomes) and median |estimate|\n"); print(res[, .(sig = sum(abs(t) > 1.96), median_abs = round(median(abs(b_sd)), 3)), by = .(coef, sample, Z, E)][order(coef, sample, Z, E)], nrows = 100)
cat("\nsign consistency of the interaction across the 40 specs, by outcome (share positive; cells with |t| > 1.96):\n"); print(res[coef == "interaction", .(share_pos = round(mean(b_sd > 0), 2), sig = sum(abs(t) > 1.96), median_b = round(median(b_sd), 3)), by = outcome])
sink()
# ---- tiles: interaction, per sample
tl <- copy(res)[, `:=`(outcome = factor(outcome, rev(unlist(vars))), col = factor(paste(Z, "|", substr(E, 1, 6)), unique(paste(rep(Zs, each = 2), "|", substr(Es, 1, 6)))), sample = factor(sample, names(samples)))]
for (cc in c("interaction", "main")) { g <- ggplot(tl[coef == cc], aes(col, outcome, fill = pmax(pmin(t, 3), -3))) + geom_tile(colour = "white") + geom_text(aes(label = sprintf("%.2f", b_sd)), size = 2) +
    facet_wrap(~ sample, ncol = 5) + scale_fill_gradient2("t-stat", low = "#c0392b", mid = "white", high = "#2a64a8", limits = c(-3, 3)) +
    labs(x = NULL, y = NULL, title = sprintf("Design B grid, %s term: post-2017 change by instrument, exposure form and sample", cc),
         subtitle = ifelse(cc == "interaction", "cells: Z x exposure x year, post - pre, SD units per SD of exposure (contin.) or high vs low (binary); colour: t. Density x year, unit FE, pair x year FE throughout", "cells: Z x year at mean / low exposure, post - pre, SD units per 10 mi closer or near; colour: t")) +
    theme_minimal(base_size = 7.5) + theme(axis.text.x = element_text(angle = 40, hjust = 1))
  ggsave(file.path(OUT, sprintf("figures/c35_density_spec_grid_%s.png", cc)), g, width = 16, height = 5.5, dpi = 150) }
# ---- event studies for the interaction, four reference specs
ref <- list(c("low side, all pairs", "-distance / 10 mi", "continuous (per SD)"), c("both sides, all pair-sides", "-distance / 10 mi", "continuous (per SD)"), c("low side, all pairs", "near < 10 mi", "binary (above median)"), c("low side, density < 1,000/sq mi", "-distance / 10 mi", "continuous (per SD)"))
pl <- rbindlist(lapply(ref, \(r) ser[coef == "interaction" & sample == r[1] & Z == r[2] & E == r[3]][, spec := paste0(r[1], "; ", r[2], "; ", substr(r[3], 1, 6))]))
pl[, `:=`(outcome = factor(outcome, unlist(vars)), spec = factor(spec, unique(spec)))]
g <- ggplot(pl, aes(y, b_sd, colour = spec)) + geom_hline(yintercept = 0, colour = "grey70") + geom_vline(xintercept = REF + .5, linetype = "22", colour = "grey50") +
  geom_pointrange(aes(ymin = b_sd - 1.96 * se_sd, ymax = b_sd + 1.96 * se_sd), position = position_dodge(width = .6), size = .18, linewidth = .3) + geom_line(position = position_dodge(width = .6), linewidth = .3) +
  facet_wrap(~ outcome, scales = "free_y", ncol = 4, labeller = label_wrap_gen(30)) + scale_x_continuous(breaks = seq(2012, 2024, 2)) + scale_colour_manual(NULL, values = c("#2a64a8", "#c0392b", "#2e8b57", "#8c6d31")) +
  labs(x = NULL, y = "Z x exposure x year, SD units of the outcome, 2017 = 0; 95% CI, SE by county", title = "Design B: the exposure interaction in four reference specifications", subtitle = "density x year control, unit FE, pair x year FE") +
  theme_minimal(base_size = 8) + theme(legend.position = "bottom", strip.text = element_text(size = 6.5)) + guides(colour = guide_legend(nrow = 2))
ggsave(file.path(OUT, "figures/c35_density_spec_grid_es.png"), g, width = 11, height = 7.5, dpi = 150)
