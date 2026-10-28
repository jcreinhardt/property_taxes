# Balance of pre-treatment covariates for the density-controlled design (21 B / 22): does conditioning on log population
# density first make distance to the border look like a valid instrument on the low-tax side (SIDE=high: high-tax side) within 50 mi?
# Each standardised covariate (SD units over the low side, all pairs, within 50 mi) is regressed with pair FE, SE by county:
#   main term:    cov ~ Z | pair                          (raw, as in 12)
#                 cov ~ Z + log density | pair             (conditional on density)
#   interaction:  cov ~ Z + Z x E + E + log density | pair (the term that carries the result in 22; E = own lost subsidy per
#                 owner HH, standardised; reported: Z x E, plus E itself in the table)
# Z: z = -distance / 10 mi (continuous) and near = 1(< 10 mi). Samples as in 22 minus both sides (the balance table is low side
# only): all pairs | density < 5,000 / sq mi | density < 1,000 / sq mi | rural 45% pairs. Holm-adjusted p across the controls.
# DENS_SPEC=bins replaces the linear log density term by within-sample density-decile dummies (outputs suffixed _densbins).
# SIDE=high runs the same on the high-tax side (table from 08 with SIDE=high; rural subset keyed on high-side metro shares; outputs suffixed _high).
suppressPackageStartupMessages({library(data.table); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_rd"); invisible(out_dir("designs/border_rd/figures")); MX <- 50; DS <- Sys.getenv("DENS_SPEC", "linear"); SIDE <- Sys.getenv("SIDE", "low"); SFX <- paste0(ifelse(SIDE == "high", "_high", ""), ifelse(DS == "bins", "_densbins", "")); SL <- sprintf("%s-tax side", SIDE)
d <- readRDS(file.path(DATA, sprintf("built/geo/%s_side_balance_tracts.rds", SIDE)))[dist_mi < MX & is.finite(ldens)]
tz <- fread(file.path(DATA, "built/exposure/salt_exposure_tract_zip2016.csv"), colClasses = c(tract = "character", zip = "character"))[, .(tract, zip)]
cz <- fread(file.path(DATA, "built/exposure/exposure_components_zip.csv"), colClasses = c(geo = "character"))[returns16 >= 100 & is.finite(d_salt_own), .(zip = geo, d_salt_own)]
d <- merge(d, merge(tz, cz, by = "zip")[, .(tract, d_salt_own)], by = "tract", all.x = TRUE)
d[, `:=`(dens = exp(ldens), z = -dist_mi / 10, near = as.integer(dist_mi < 10), ex = (d_salt_own - mean(d_salt_own, na.rm = TRUE)) / sd(d_salt_own, na.rm = TRUE))]
metro_low <- fread(file.path(OUT, "c22_metro_shares_by_pair_side.csv"))[side == SIDE & share >= .45, unique(pair)]
smp <- list(`all pairs` = d, `density < 5,000/sq mi` = d[dens < 5000], `density < 1,000/sq mi` = d[dens < 1000], `rural 45%` = d[!pair %in% metro_low])
v <- c(med = "effective tax rate, % (CoreLogic)",
       lp50 = "log median sale price 2016 (CoreLogic)", lhval = "log median home value (ACS)", moveout = "move-out rate 2014-17 (Data Axle)",
       sh_moved = "moved in the last year (ACS)", turnover = "sales per 100 parcels 2016 (CoreLogic)",
       d_salt_own = "own lost SALT subsidy per owner HH (SOI ZIP)", exz = "SALT above $10k per return (SOI ZIP)",
       linc = "log median HH income (ACS)", sh_agi100 = "share of returns AGI >= $100k (SOI ZIP)", sh_item = "itemiser share (SOI ZIP)",
       age = "median age (ACS)", hhsize = "household size (ACS)", sh_u18 = "share under 18 (ACS)",
       sh_hh_kids = "share of HHs with children (ACS)", sh_own = "owner share (ACS)", sh_ba = "share BA+ (ACS)", sh_white = "share white (ACS)",
       yrbuilt = "median year built (ACS)", lpop = "log population (ACS)", ldens = "log population density (ACS, TIGER)",
       ach = "district test scores (SEDA)", ses = "district SES (SEDA)")
grp <- c(med = "first stage: rate", lp50 = "outcomes", lhval = "outcomes", moveout = "outcomes", sh_moved = "outcomes", turnover = "outcomes", d_salt_own = "exposure", exz = "exposure")
grp <- c(grp, setNames(rep("controls", length(v) - length(grp)), setdiff(names(v), names(grp))))
sds <- sapply(names(v), \(k) sd(d[[k]], na.rm = TRUE))
DT <- ifelse(DS == "bins", "i(dbin)", "ldens"); specs <- list(raw = "%s", dens = paste("%s +", DT), inter = paste("%s + %s:ex + ex +", DT))
for (s in names(smp)) smp[[s]][, dbin := cut(ldens, quantile(ldens, 0:10 / 10), include.lowest = TRUE, labels = FALSE)]
run <- function(k, s, zz, sp) { if (k == "ldens" && sp != "raw") return(NULL)
  x <- smp[[s]][is.finite(get(k))][, y := get(k) / sds[k]]; if (sp == "inter") x <- x[is.finite(ex)]
  m <- feols(as.formula(sprintf("y ~ %s | pair", gsub("%s", zz, specs[[sp]], fixed = TRUE))), x, cluster = ~cty); ct <- as.data.table(coeftable(m), keep.rownames = "term")
  ct[, .(var = k, sample = s, instrument = zz, spec = sp, term, b = Estimate, se = `Std. Error`, p = `Pr(>|t|)`, n = nobs(m), pairs = uniqueN(x$pair))] }
res <- list(); for (s in names(smp)) for (zz in c("z", "near")) for (sp in names(specs)) for (k in names(v)) res[[length(res) + 1]] <- run(k, s, zz, sp)
res <- rbindlist(res)[, term := fcase(term == instrument, "Z", term == "ex", "E", grepl("^(ldens|dbin)", term), "density", default = "Z x E")]
res[grp[var] == "controls", p_holm := p.adjust(p, "holm"), by = .(sample, instrument, spec, term)]
res[, `:=`(label = factor(v[var], rev(v)), group = factor(grp[var], c("first stage: rate", "outcomes", "exposure", "controls")), sample = factor(sample, names(smp)),
           instrument = factor(instrument, c("z", "near"), c("per 10 mi closer", "near < 10 mi")))]
fwrite(res, file.path(OUT, paste0("c36_balance_conditional_on_density", SFX, ".csv")))
sink(file.path(OUT, paste0("c36_balance_conditional_on_density", SFX, ".txt")), split = TRUE)
cat("Balance of distance to the border on the", SL, "within 50 mi, raw vs conditional on log density, and for the Z x exposure interaction (E, density controlled).\n",
    "Coefficients in SD units of the covariate (SD over all pairs), pair FE, SE by county; p_holm across the", sum(grp == "controls"), "controls.\n")
for (s in names(smp)) cat(sprintf("%s: %d tracts, %d pairs, %d with exposure\n", s, nrow(smp[[s]]), uniqueN(smp[[s]]$pair), sum(is.finite(smp[[s]]$ex))))
cat("\nSummary over the controls: count with |b| > 0.1 SD / p < .05 / Holm p < .05 (of", sum(grp == "controls"), "), and median |b|\n")
sm <- res[group == "controls" & term %in% c("Z", "Z x E"), .(gt_0.1 = sum(abs(b) > .1), p05 = sum(p < .05), holm05 = sum(p_holm < .05), median_abs = round(median(abs(b)), 3), k = .N), by = .(instrument, term, spec, sample)][order(instrument, term, spec, sample)]
print(as.data.frame(sm), row.names = FALSE)
for (zz in levels(res$instrument)) for (s in names(smp)) { cat("\n==", zz, "|", s, "  (cells: b (se); columns raw Z, Z given density, Z x E given E and density, E given the rest)\n")
  w <- dcast(res[instrument == zz & sample == s & ((spec == "raw" & term == "Z") | (spec == "dens" & term == "Z") | (spec == "inter" & term %in% c("Z x E", "E"))),
                 .(var, col = fcase(spec == "raw", "Z_raw", spec == "dens", "Z_dens", term == "Z x E", "ZxE", default = "E"), cell = sprintf("%.3f (%.3f)%s", b, se, fifelse(p < .05, "*", "")))], var ~ col, value.var = "cell")
  w <- w[match(names(v), var)][, variable := v[var]][, group := grp[var]][, var := NULL]; setcolorder(w, c("group", "variable", "Z_raw", "Z_dens", "ZxE", "E")); print(as.data.frame(w), row.names = FALSE) }
sink()
# ---- figure 1: main term, raw vs conditional on density, by sample
p1 <- res[term == "Z" & spec %in% c("raw", "dens")][, spec := factor(spec, c("raw", "dens"), c("pair FE only", "pair FE + log density"))]
g <- ggplot(p1, aes(b, label, colour = spec, shape = instrument)) + geom_vline(xintercept = 0, colour = "grey50") +
  geom_pointrange(aes(xmin = b - 1.96 * se, xmax = b + 1.96 * se), position = position_dodge(width = .7), size = .2, linewidth = .35) +
  facet_grid(group ~ sample, scales = "free", space = "free_y") + scale_colour_manual(NULL, values = c("grey55", "#2a64a8")) + scale_shape_manual(NULL, values = c(16, 17)) +
  labs(x = "coefficient on the instrument, SD units of the covariate; 95% CI, SE by county", y = NULL,
       title = paste0("Balance of distance to the border on the ", SL, " within 50 mi: raw vs conditional on population density (", ifelse(DS == "bins", "deciles", "log, linear"), ")"),
       subtitle = sprintf("samples: %s", paste(sprintf("%s %d tracts / %d pairs", names(smp), sapply(smp, nrow), sapply(smp, \(x) uniqueN(x$pair))), collapse = "; "))) +
  theme_minimal(base_size = 8) + theme(legend.position = "bottom", strip.text.y = element_text(angle = 0))
ggsave(file.path(OUT, paste0("figures/c36_balance_conditional_on_density", SFX, ".png")), g, width = 13, height = 8.5, dpi = 150)
# ---- figure 2: the interaction Z x E (and E) with E and density controlled
p2 <- res[spec == "inter" & term %in% c("Z x E", "E")][, term := factor(term, c("Z x E", "E"), c("Z x exposure (per SD of E)", "exposure E (per SD)"))]
g <- ggplot(p2, aes(b, label, colour = term, shape = instrument)) + geom_vline(xintercept = 0, colour = "grey50") +
  geom_pointrange(aes(xmin = b - 1.96 * se, xmax = b + 1.96 * se), position = position_dodge(width = .7), size = .2, linewidth = .35) +
  facet_grid(group ~ sample, scales = "free", space = "free_y") + scale_colour_manual(NULL, values = c("#c0392b", "#8c6d31")) + scale_shape_manual(NULL, values = c(16, 17)) +
  labs(x = "coefficient, SD units of the covariate; 95% CI, SE by county; Z, E and log density in the regression, pair FE", y = NULL,
       title = paste0("Balance of the Z x exposure interaction on the ", SL, " within 50 mi, conditional on exposure and density (", ifelse(DS == "bins", "deciles", "log, linear"), ")"),
       subtitle = "Z x E is the term that carries the post-2017 result in the density grid (22); E = own lost SALT subsidy per owner household, standardised") +
  theme_minimal(base_size = 8) + theme(legend.position = "bottom", strip.text.y = element_text(angle = 0))
ggsave(file.path(OUT, paste0("figures/c36_balance_interaction", SFX, ".png")), g, width = 13, height = 8.5, dpi = 150)
