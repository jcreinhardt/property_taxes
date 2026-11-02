# Two more border designs with the tract panels of 13 (nb_panels), outcomes of 14, exposure = own lost subsidy per owner HH.
#  A. BOTH SIDES: tracts within 50 mi on either side of the ranked pairs; near = 1(distance < 10 mi) vs 10-50 mi inland on the
#     same side. y = sum_t b_t near x 1[t] + tract FE + pair x side x year FE (the comparison is near vs far within a pair-side),
#     2017 = 0, SE by county; also near x high-side x year (does the near effect differ on the high side) and near x exposure x
#     year. Pair-sides with a metro core are dropped (a metro >= 0.5m with >= 45% of its population within 50 mi on that side,
#     from 09's shares file), so the high side of a pair can stay while its low side is dropped and vice versa. Balance of
#     near vs far on 8 covariates by side is reported.
#  B. ONE SIDE, CONTINUOUS DISTANCE, DENSITY CONTROL: low side within 50 mi, ALL 59 pairs (metro pairs included), z = -distance
#     / 10 mi, y = sum_t b_t z x 1[t] + sum_t l_t log density x 1[t] + tract FE + pair x year FE; with and without the density
#     control, and on the rural 45% subset for comparison; plus z x exposure x year. Districts / counties: population-weighted
#     instruments and density, units with >= 50% of their population in the sample.
suppressPackageStartupMessages({library(data.table); library(DBI); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_rd"); invisible(out_dir("designs/border_rd/figures")); MX <- 50; REF <- 2017
source("code/designs/border_rd/nb_panels.R")
iv <- readRDS(file.path(DATA, "built/geo/iv_tract_side.rds"))[T == 30 & dist_mi < MX]
iv[, side := ifelse(high == 1, "high", "low")][, ps := paste(pair, side)]
tr <- as.data.table(foreign::read.dbf(file.path(path.expand("~/Library/Caches/tigris"), "cb_2019_us_tract_500k.dbf"), as.is = TRUE))[, .(tract = GEOID, aland = as.numeric(ALAND))]
iv <- merge(iv, tr, by = "tract", all.x = TRUE)[, ldens := log(pmax(pop, 1) / (aland / 2589988))]
tz <- fread(file.path(DATA, "built/exposure/salt_exposure_tract_zip2016.csv"), colClasses = c(tract = "character", zip = "character"))[, .(tract, zip)]
cz <- fread(file.path(DATA, "built/exposure/exposure_components_zip.csv"), colClasses = c(geo = "character"))[returns16 >= 100 & is.finite(d_salt_own), .(zip = geo, d_salt_own)]
iv <- merge(iv, merge(tz, cz, by = "zip")[, .(tract, d_salt_own)], by = "tract", all.x = TRUE)
sh <- fread(file.path(OUT, "c22_metro_shares_by_pair_side.csv")); metro_ps <- sh[cbsa_pop >= 5e5 & share >= .45, unique(paste(pair, side))]
iv[, `:=`(near = as.integer(dist_mi < 10), z = -dist_mi / 10, hi = high)]
vars <- list(cl = c(lp50 = "log median sale price", lbill = "log tax bill per parcel", eff50 = "effective tax rate", turn = "sales per 100 parcels"),
             ac = c(lhval = "log median home value (ACS)", lpop = "log population (ACS)"), mv = c(moveout = "move-out rate (Data Axle)"),
             f = c(lptax = "log district property tax per pupil", lspend = "log district spending per pupil", lenr = "log district enrolment"), sd = c(lptax = "log county school property tax (CoG)"))
units <- c(cl = "tract", ac = "tract", mv = "tract", f = "leaid", sd = "cty")
pp <- function(r) if (!is.null(r)) r[, .(post_min_pre = mean(b[y > REF]) - mean(b[y < REF & y >= 2013]), se = sqrt(mean(se[y > REF]^2) + mean(se[y < REF & y >= 2013]^2) / sum(y < REF & y >= 2013)), n = n[1])]
sdys <- function(P) sapply(names(vars), \(tb) sapply(names(vars[[tb]]), \(k) sd(P[[tb]][y == 2016][[k]], na.rm = TRUE)), simplify = FALSE)
runset <- function(P, specs) { out <- list(); ser <- list(); S <- sdys(P)
  for (tb in names(vars)) for (k in names(vars[[tb]])) { p <- P[[tb]][, yv := get(k)]; sdy <- S[[tb]][k]
    for (nm in names(specs)) { sp <- specs[[nm]]; r <- es(p[is.finite(get(sp$z))], sp$z, units[tb], "cty", extra = sp$extra); if (is.null(r)) next
      ser[[length(ser) + 1]] <- copy(r)[, `:=`(spec = nm, outcome = vars[[tb]][k], b_sd = b / sdy, se_sd = se / sdy)]
      out[[length(out) + 1]] <- pp(r)[, `:=`(spec = nm, outcome = vars[[tb]][k], b_sd = post_min_pre / sdy, se_sd = se / sdy)] } }
  list(res = rbindlist(out)[, t := b_sd / se_sd], ser = rbindlist(ser)) }
ptab <- function(res, specs) { w <- dcast(res[, .(outcome, spec = factor(spec, names(specs)), cell = sprintf("%.3f (%.1f)", b_sd, t))], outcome ~ spec, value.var = "cell"); print(as.data.frame(w[match(unlist(vars), outcome)]), row.names = FALSE) }
# ================= A. both sides =================
dA <- iv[!ps %in% metro_ps]; dA[, ex := (d_salt_own - mean(d_salt_own, na.rm = TRUE)) / sd(d_salt_own, na.rm = TRUE)][, `:=`(near_hi = near * hi, near_x = near * ex, pair0 = pair, pair = ps)]
cat("A. pair-sides:", uniqueN(dA$ps), "of", uniqueN(iv$ps), "| tracts:", nrow(dA), "(low", dA[side == "low", .N], ", high", dA[side == "high", .N], ") | near:", sum(dA$near), "| pairs with both sides kept:", dA[, uniqueN(side), by = pair0][V1 == 2, .N], "\n")
cat("dropped pair-sides:", paste(sort(metro_ps[metro_ps %in% unique(iv$ps)]), collapse = ", "), "\n")
# balance: near vs far within pair-side, by side, 8 covariates (SD units)
bv <- c(linc = "log income", lhval = "log home value", sh_own = "owner share", sh_ba = "share BA+", sh_white = "share white", age = "median age", yrbuilt = "year built", ldens = "log density")
dA[, `:=`(linc = log(inc), lhval = log(hval))]
bal <- rbindlist(lapply(c("low", "high"), \(s) rbindlist(lapply(names(bv), \(k) { x <- dA[side == s & is.finite(get(k))]; ct <- coeftable(feols(as.formula(sprintf("I(%s / sd(%s)) ~ near | pair", k, k)), x, cluster = ~cty))
  data.table(side = s, covariate = bv[k], b = ct[1, 1], se = ct[1, 2]) }))))
PA <- nb_panels(dA, c("near", "near_hi", "near_x", "ex", "hi"))
specsA <- list(`near x year (both sides)` = list(z = "near", extra = ""), `near x high-side x year (+ near x year)` = list(z = "near_hi", extra = sprintf(" + i(y, near, ref = %d)", REF)),
               `near x exposure x year` = list(z = "near_x", extra = sprintf(" + i(y, near, ref = %d) + i(y, ex, ref = %d)", REF, REF)))
A <- runset(PA, specsA)
fwrite(A$res, file.path(OUT, "c34_both_sides.csv")); fwrite(A$ser, file.path(OUT, "c34_both_sides_series.csv")); fwrite(bal, file.path(OUT, "c34_both_sides_balance.csv"))
# ================= B. low side, continuous distance, density control =================
dB <- iv[side == "low"]; dB[, ex := (d_salt_own - mean(d_salt_own, na.rm = TRUE)) / sd(d_salt_own, na.rm = TRUE)][, z_x := z * ex]
metro_low <- fread(file.path(OUT, "c22_low_side_metro_pairs.csv"))$pair
PB <- nb_panels(dB, c("z", "z_x", "ex", "ldens")); PBr <- nb_panels(dB[!pair %in% metro_low], c("z", "z_x", "ex", "ldens"))
cat("B. low side all pairs:", nrow(dB), "tracts,", uniqueN(dB$pair), "pairs | rural 45%:", dB[!pair %in% metro_low, .N], "tracts\n")
specsB <- list(`all pairs, no control` = list(z = "z", extra = ""), `all pairs, + density x year` = list(z = "z", extra = sprintf(" + i(y, ldens, ref = %d)", REF)),
               `all pairs, z x exposure, + density` = list(z = "z_x", extra = sprintf(" + i(y, z, ref = %d) + i(y, ex, ref = %d) + i(y, ldens, ref = %d)", REF, REF, REF)))
specsBr <- list(`rural 45%, no control` = list(z = "z", extra = ""), `rural 45%, + density x year` = list(z = "z", extra = sprintf(" + i(y, ldens, ref = %d)", REF)),
                `rural 45%, z x exposure, + density` = list(z = "z_x", extra = sprintf(" + i(y, z, ref = %d) + i(y, ex, ref = %d) + i(y, ldens, ref = %d)", REF, REF, REF)))
B <- runset(PB, specsB); Br <- runset(PBr, specsBr); Bres <- rbind(B$res, Br$res); Bser <- rbind(B$ser, Br$ser)
fwrite(Bres, file.path(OUT, "c34_density.csv")); fwrite(Bser, file.path(OUT, "c34_density_series.csv"))
# ================= extended local-government outcomes (the set of 18) for both designs =================
ppx <- function(r) { pre <- if (any(r$y >= 2013 & r$y < REF)) r$y >= 2013 & r$y < REF else r$y < REF
  r[, .(post_min_pre = mean(b[y > REF]) - mean(b[pre]), se = sqrt(mean(se[y > REF]^2) + mean(se[pre]^2) / sum(pre)), n = n[1])] }
runext <- function(E, specs, design) { out <- list()
  for (tb in names(E$panels)) for (k in names(E$vars[[tb]])) { p <- E$panels[[tb]][, yv := get(k)]; if (sum(is.finite(p$yv)) < 200 || uniqueN(p[is.finite(yv), y]) < 3) next
    sdy <- sd(p[y == 2016, yv], na.rm = TRUE); if (!is.finite(sdy) || sdy == 0) sdy <- sd(p$yv, na.rm = TRUE)
    for (nm in names(specs)) { sp <- specs[[nm]]; r <- es(p[is.finite(get(sp$z))], sp$z, E$units[tb], "cty", extra = sp$extra); if (is.null(r)) next
      out[[length(out) + 1]] <- ppx(r)[, `:=`(design = design, spec = nm, source = E$src[tb], outcome = E$vars[[tb]][k], b_sd = post_min_pre / sdy, se_sd = se / sdy)] } }
  rbindlist(out)[, t := b_sd / se_sd] }
EA <- nb_extra_panels(PA$dis, PA$cnt); XA <- runext(EA, specsA, "A")
EB <- nb_extra_panels(PB$dis, PB$cnt); XB <- runext(EB, specsB[c(1, 3)], "B"); EBr <- nb_extra_panels(PBr$dis, PBr$cnt); XBr <- runext(EBr, specsBr[c(2, 3)], "B")
X <- rbind(XA, XB, XBr); fwrite(X, file.path(OUT, "c34_extended_outcomes.csv"))
tiles <- function(x, specs, ttl, fn) { x <- copy(x)[, `:=`(outcome = factor(outcome, rev(unique(unlist(EA$vars)))), spec = factor(spec, names(specs)), source = factor(source, EA$src))]
  g <- ggplot(x, aes(spec, outcome, fill = pmax(pmin(t, 3), -3))) + geom_tile(colour = "white") + geom_text(aes(label = sprintf("%.2f", b_sd)), size = 2.1) +
    facet_wrap(~ source, scales = "free_y", ncol = 2) + scale_fill_gradient2("t-stat", low = "#c0392b", mid = "white", high = "#2a64a8", limits = c(-3, 3)) +
    labs(x = NULL, y = NULL, title = ttl, subtitle = "cells: post - pre in SD units of the outcome; colour: t (SE by county)") + theme_minimal(base_size = 7.5) + theme(axis.text.x = element_text(angle = 20, hjust = 1))
  ggsave(file.path(OUT, fn), g, width = 13, height = 12, dpi = 150) }
tiles(XA, specsA, "A. Both sides of the border: extended local-government outcomes", "figures/c34_both_sides_extended_tiles.png")
tiles(rbind(XB, XBr), c(specsB[c(1, 3)], specsBr[c(2, 3)]), "B. Low side, continuous distance with density control: extended local-government outcomes", "figures/c34_density_extended_tiles.png")
# ================= text =================

sink(file.path(OUT, "c34_both_sides_and_density.txt"), split = TRUE)
cat("A. BOTH SIDES. Tracts within 50 mi on either side, pair-sides with a metro core dropped:", uniqueN(dA$ps), "pair-sides,", nrow(dA), "tracts (low", dA[side == "low", .N], ", high", dA[side == "high", .N], "), near:", sum(dA$near), "\n",
    "   tract (unit) FE + pair x side x year FE, SE by county. post - pre in SD units of the outcome (per SD of exposure for the interaction). Cells: estimate (t).\n")
ptab(A$res, specsA)
cat("\n   balance of near vs far within pair-side, by side (near coefficient, SD units, pair-side FE; SE):\n")
print(as.data.frame(dcast(bal[, .(covariate, side, cell = sprintf("%.2f (%.2f)", b, se))], covariate ~ side, value.var = "cell")), row.names = FALSE)
cat("\nB. LOW SIDE, CONTINUOUS DISTANCE (z = -distance / 10 mi, closer = +), density x year control. Tract FE + pair x year FE. post - pre in SD units per 10 mi closer (per SD of exposure for z x exposure).\n")
ptab(Bres, c(specsB, specsBr))
cat("\nExtended local-government outcomes (18's set): cells with |t| > 1.96 and median |estimate| by design x spec x source\n")
print(X[, .(k = .N, sig = sum(abs(t) > 1.96), median_abs = round(median(abs(b_sd)), 3)), by = .(design, spec, source)][order(design, spec, source)], nrows = 60)
cat("\nextended cells with |t| > 2.5:\n"); print(X[abs(t) > 2.5, .(design, spec, source = substr(source, 1, 20), outcome, b_sd = round(b_sd, 3), t = round(t, 1))][order(design, spec)], nrows = 60)
cat("\ncore outcomes: cells with |t| > 1.96 (of 11):\n"); print(rbind(A$res[, .(design = "A", spec, sig = sum(abs(t) > 1.96), median_abs = round(median(abs(b_sd)), 3)), by = spec][, -1], Bres[, .(design = "B", spec, sig = sum(abs(t) > 1.96), median_abs = round(median(abs(b_sd)), 3)), by = spec][, -1]))
sink()
# ================= figures =================
fig <- function(ser, specs, ttl, sub, fn, cols) { pl <- copy(ser)[, `:=`(outcome = factor(outcome, unlist(vars)), spec = factor(spec, names(specs)))]
  g <- ggplot(pl, aes(y, b_sd, colour = spec)) + geom_hline(yintercept = 0, colour = "grey70") + geom_vline(xintercept = REF + .5, linetype = "22", colour = "grey50") +
    geom_pointrange(aes(ymin = b_sd - 1.96 * se_sd, ymax = b_sd + 1.96 * se_sd), position = position_dodge(width = .6), size = .18, linewidth = .3) + geom_line(position = position_dodge(width = .6), linewidth = .3) +
    facet_wrap(~ outcome, scales = "free_y", ncol = 4, labeller = label_wrap_gen(30)) + scale_x_continuous(breaks = seq(2012, 2024, 2)) + scale_colour_manual(NULL, values = cols) +
    labs(x = NULL, y = "SD units of the outcome, 2017 = 0; 95% CI, SE by county", title = ttl, subtitle = sub) + theme_minimal(base_size = 8) + theme(legend.position = "bottom", strip.text = element_text(size = 6.5)) + guides(colour = guide_legend(nrow = 2))
  ggsave(file.path(OUT, fn), g, width = 11, height = 7.5, dpi = 150) }
fig(A$ser, specsA, "A. Both sides of the border: near (< 10 mi) vs inland, within pair-side", sprintf("%d pair-sides without a metro core, %d tracts; tract/unit FE + pair x side x year FE; exposure = own lost subsidy per owner HH (SD)", uniqueN(dA$ps), nrow(dA)), "figures/c34_both_sides_es.png", c("#2a64a8", "#c0392b", "#8c6d31"))
fig(Bser, c(specsB, specsBr), "B. Low side, continuous distance (per 10 mi closer), with and without log density x year", sprintf("all 59 pairs (%d tracts) vs rural 45%% (%d tracts); tract/unit FE + pair x year FE", nrow(dB), dB[!pair %in% metro_low, .N]), "figures/c34_density_es.png", c("#555555", "#2a64a8", "#8c6d31", "#aaaaaa", "#2e8b57", "#d1772b"))
