# Balance coefficient plot for distance to the border as an instrument, low-tax side within 50 mi, rural subset at the
# METRO_SHARE cutoff (default 0.45; pairs dropped when a metro >= 0.5m has >= that share of its population on the low side
# within 50 mi; outputs suffixed _c45, _c50, ...). At 50% the NJ-NY pair is back in (New York City: 48%), so the same
# regressions are also shown without NJ-NY (identical to the main series when the cutoff already drops it).
# Each standardised covariate (SD units over the all-pairs sample within 50 mi) is regressed on the instrument with pair FE,
# SE clustered by county, in two forms: near = 1(distance < 10 mi) vs 10-50 mi, and linear distance per 10 mi. The rate
# (first stage) and the outcomes are shown with the controls, grouped. Holm-adjusted p-values across the controls.
suppressPackageStartupMessages({library(data.table); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_rd"); invisible(out_dir("designs/border_rd/figures")); MX <- 50; CUT <- as.numeric(Sys.getenv("METRO_SHARE", .45)); SFX <- sprintf("_c%.0f", 100 * CUT)
d <- readRDS(file.path(DATA, "built/geo/low_side_balance_tracts.rds"))[dist_mi < MX]
sh <- fread(file.path(OUT, "c22_metro_shares_by_pair_side.csv"))
drop <- sh[side == "low" & share >= CUT, unique(pair)]
v <- c(med = "effective tax rate, % (CoreLogic)",
       lp50 = "log median sale price 2016 (CoreLogic)", lhval = "log median home value (ACS)", moveout = "move-out rate 2014-17 (Data Axle)",
       sh_moved = "moved in the last year (ACS)", turnover = "sales per 100 parcels 2016 (CoreLogic)",
       linc = "log median HH income (ACS)", sh_agi100 = "share of returns AGI >= $100k (SOI ZIP)", sh_item = "itemiser share (SOI ZIP)",
       exz = "SALT above $10k per return (SOI ZIP)", age = "median age (ACS)", hhsize = "household size (ACS)", sh_u18 = "share under 18 (ACS)",
       sh_hh_kids = "share of HHs with children (ACS)", sh_own = "owner share (ACS)", sh_ba = "share BA+ (ACS)", sh_white = "share white (ACS)",
       yrbuilt = "median year built (ACS)", lpop = "log population (ACS)", ldens = "log population density (ACS, TIGER)",
       ach = "district test scores (SEDA)", ses = "district SES (SEDA)")
grp <- c(med = "first stage: rate", lp50 = "outcomes", lhval = "outcomes", moveout = "outcomes", sh_moved = "outcomes", turnover = "outcomes")
grp <- c(grp, setNames(rep("controls", length(v) - length(grp)), setdiff(names(v), names(grp))))
sds <- sapply(names(v), \(k) sd(d[[k]], na.rm = TRUE))
d[, near := as.integer(dist_mi < 10)][, d10 := dist_mi / 10]
smp <- setNames(list(d[!pair %in% drop], d[!pair %in% c(drop, "NJ-NY")]), c(sprintf("rural %.0f%% (%d pairs%s)", 100 * CUT, uniqueN(d[!pair %in% drop, pair]), ifelse("NJ-NY" %in% drop, "", ", NJ-NY in")), sprintf("rural %.0f%% without NJ-NY", 100 * CUT)))
run <- function(k, s, z) { x <- smp[[s]][is.finite(get(k))][, y := get(k) / sds[k]]
  m <- feols(as.formula(sprintf("y ~ %s | pair", z)), x, cluster = ~cty); ct <- coeftable(m)
  data.table(var = k, sample = s, instrument = z, b = ct[1, 1], se = ct[1, 2], p = ct[1, 4], n = nobs(m), pairs = uniqueN(x$pair)) }
res <- rbindlist(lapply(names(smp), \(s) rbindlist(lapply(c("near", "d10"), \(z) rbindlist(lapply(names(v), run, s = s, z = z))))))
res[grp[var] == "controls", p_holm := p.adjust(p, "holm"), by = .(sample, instrument)]
res[, `:=`(label = factor(v[var], rev(v)), group = factor(grp[var], c("first stage: rate", "outcomes", "controls")),
           instrument = factor(instrument, c("near", "d10"), c("near the border: < 10 mi vs 10-50 mi", "linear: per 10 miles from the border")))]
fwrite(res, file.path(OUT, paste0("c25_balance_coefplot", SFX, ".csv")))
sink(file.path(OUT, paste0("c25_balance_coefplot", SFX, ".txt")), split = TRUE)
cat("Distance to the border as an instrument on the low-tax side within 50 mi; rural subset at the", 100 * CUT, "% cutoff. Pairs dropped:", paste(sort(drop), collapse = " "), "\n")
for (s in names(smp)) cat(sprintf("%s: %d tracts, %d pairs\n", s, nrow(smp[[s]]), uniqueN(smp[[s]]$pair)))
cat("Coefficients in SD units of the covariate (SE by county); p_holm across the", sum(grp == "controls"), "controls.\n")
for (s in names(smp)) { cat("\n==", s, "\n")
  w <- dcast(res[sample == s, .(var, instrument = ifelse(grepl("^near", instrument), "near", "per10mi"), cell = sprintf("%.3f (%.3f)", b, se), p_holm = round(p_holm, 3))], var ~ instrument, value.var = c("cell", "p_holm"))
  w <- w[match(names(v), var)][, variable := v[var]][, group := grp[var]][, var := NULL]; setcolorder(w, c("group", "variable", "cell_near", "p_holm_near", "cell_per10mi", "p_holm_per10mi"))
  print(as.data.frame(w), row.names = FALSE) }
cat("\nControls with |coef| > 0.1 SD / p < .05 / p_holm < .05:\n")
print(res[group == "controls", .(gt_0.1 = sum(abs(b) > .1), p05 = sum(p < .05), holm05 = sum(p_holm < .05), k = .N), by = .(sample, instrument)])
sink()
g <- ggplot(res, aes(b, label, colour = sample, shape = group)) + geom_vline(xintercept = 0, colour = "grey50") +
  geom_pointrange(aes(xmin = b - 1.96 * se, xmax = b + 1.96 * se), position = position_dodge(width = .6), size = .25, linewidth = .4) +
  facet_grid(group ~ instrument, scales = "free", space = "free_y") +
  scale_colour_manual(NULL, values = c("#2e8b57", "#8c6d31")) + scale_shape_manual(NULL, values = c(17, 15, 16), guide = "none") +
  labs(x = "coefficient on the instrument, SD units of the covariate; 95% CI, SE by county; pair FE", y = NULL,
       title = sprintf("Balance of distance to the border as an instrument: low-tax side within 50 miles, rural subset at the %.0f%% cutoff", 100 * CUT),
       subtitle = sprintf("%d tracts in %d pairs (without NJ-NY: %d tracts, %d pairs). Pairs dropped when a metro >= 0.5m has >= %.0f%% of its people on the low side within 50 mi",
                          nrow(smp[[1]]), uniqueN(smp[[1]]$pair), nrow(smp[[2]]), uniqueN(smp[[2]]$pair), 100 * CUT)) +
  theme_minimal(base_size = 8.5) + theme(legend.position = "bottom", strip.text.y = element_text(angle = 0))
ggsave(file.path(OUT, paste0("figures/c25_balance_coefplot", SFX, ".png")), g, width = 10, height = 8, dpi = 150)
