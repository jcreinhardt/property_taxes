# What the density-selected border design (29/31) gives without the pair x year fixed effects. Core 11 outcomes, by side, two
# models x two FE sets:
#   reduced form:  y = sum_t b_t Z 1[t] + sum_t l_t log density 1[t] + unit FE + {pair x year | year} FE          (29's design A)
#   interaction:   y = sum_t b_t Z 1[t] + sum_t g_t Z x E 1[t] + sum_t d_t E 1[t] + sum_t l_t log density 1[t] + unit FE + {pair x year | year} FE   (31)
# Z = -distance / 10 mi, E = own lost SALT subsidy per owner HH (standardised), 2017 = 0, SE by county, SD units of the outcome.
# With year FE only, the near-vs-inland comparison is pooled across all 68 borders of the side instead of within each border-year,
# so state-level shocks that differ across borders (incl. the state's own tax changes) load on Z whenever near tracts are
# distributed differently across pairs than inland tracts. Reported: yearly series and post - pre (2018+ minus 2012-16) for the
# low side, the high side and high minus low (difference of the side coefficients). Writes c46_fe_variants_{series,post_pre}.csv,
# c46_fe_variants.txt and figures/c46_fe_variants_{rf_main,int_main,int_interaction}.png.
suppressPackageStartupMessages({library(data.table); library(DBI); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_rd"); invisible(out_dir("designs/border_rd/figures")); REF <- 2017
source("code/designs/border_rd/nb_panels.R")
tz <- fread(file.path(DATA, "built/exposure/salt_exposure_tract_zip2016.csv"), colClasses = c(tract = "character", zip = "character"))[, .(tract, zip)]
cz <- fread(file.path(DATA, "built/exposure/exposure_components_zip.csv"), colClasses = c(geo = "character"))[returns16 >= 100 & is.finite(d_salt_own), .(zip = geo, d_salt_own)]
d <- fread(file.path(OUT, "c41_sample_competition.csv"), colClasses = c(tract = "character", pair = "character"))
d <- merge(d, rbindlist(lapply(c("low", "high"), \(s) readRDS(file.path(DATA, sprintf("built/geo/%s_side_balance_tracts.rds", s)))[, .(tract, leaid, cty, pop)])), by = "tract")
d <- merge(d, merge(tz, cz, by = "zip")[, .(tract, d_salt_own)], by = "tract")[, z := -dist_mi / 10]
d[, ex := (d_salt_own - mean(d_salt_own)) / sd(d_salt_own)][, zx := z * ex]
samples <- list(`low-tax side` = d[side == "low"], `high-tax side` = d[side == "high"])
core <- list(cl = c(lp50 = "log median sale price", lbill = "log tax bill per parcel", eff50 = "effective tax rate", turn = "sales per 100 parcels"), ac = c(lhval = "log median home value (ACS)", lpop = "log population (ACS)"), mv = c(moveout = "move-out rate (Data Axle)"),
             f = c(lptax = "log district property tax per pupil", lspend = "log district spending per pupil", lenr = "log district enrolment"), sd = c(lptax = "log county school property tax (CoG)"))
cunits <- c(cl = "tract", ac = "tract", mv = "tract", f = "leaid", sd = "cty")
fes <- c(`pair x year` = "pair^y", `year only` = "y")
forms <- c(rf = "yv ~ i(y, z, ref = %d) + i(y, ldens, ref = %d) | %s + %s", int = "yv ~ i(y, z, ref = %d) + i(y, zx, ref = %d) + i(y, ex, ref = %d) + i(y, ldens, ref = %d) | %s + %s")
est <- function(p, model, fe_unit, fe) { x <- p[is.finite(yv)]; if (model == "int") x <- x[is.finite(ex)]
  f <- if (model == "rf") sprintf(forms[model], REF, REF, fe_unit, fe) else sprintf(forms[model], REF, REF, REF, REF, fe_unit, fe)
  m <- feols(as.formula(f), x, cluster = ~cty); ct <- as.data.table(coeftable(m), keep.rownames = "term")[grepl("^y::\\d+:(z|zx)$", term)]
  ct[, `:=`(y = as.integer(sub("y::(\\d+):.*", "\\1", term)), coef = ifelse(grepl(":zx$", term), "interaction", "main"))][, .(y, coef, b = Estimate, se = `Std. Error`)][, n := nobs(m)] }
ser <- list()
for (s in names(samples)) { P <- nb_panels(samples[[s]], c("z", "zx", "ex", "ldens"))
  for (tb in names(core)) for (k in names(core[[tb]])) { p <- copy(P[[tb]])[, yv := get(k)]; sdy <- sd(p[y == 2016, yv], na.rm = TRUE)
    for (model in names(forms)) for (fe in names(fes)) { r <- est(p, model, cunits[tb], fes[fe]); if (is.null(r)) next
      r <- rbind(r, data.table(y = REF, coef = unique(r$coef), b = 0, se = 0, n = r$n[1]))[, `:=`(sample = s, model = model, fe = fe, outcome = core[[tb]][k], b_sd = b / sdy, se_sd = se / sdy)]
      ser[[length(ser) + 1]] <- r } } }
ser <- rbindlist(ser)[, side := fifelse(sample == "low-tax side", "low", "high")]
w <- dcast(ser, model + coef + fe + outcome + y ~ side, value.var = c("b_sd", "se_sd"))[is.finite(b_sd_low) & is.finite(b_sd_high)]
w[, `:=`(b_sd_diff = b_sd_high - b_sd_low, se_sd_diff = sqrt(se_sd_high^2 + se_sd_low^2))]
S <- melt(w, id.vars = c("model", "coef", "fe", "outcome", "y"), measure.vars = list(b = c("b_sd_low", "b_sd_high", "b_sd_diff"), se = c("se_sd_low", "se_sd_high", "se_sd_diff")), variable.name = "series")
S[, series := factor(series, 1:3, c("low-tax side", "high-tax side", "high minus low"))][, fe := factor(fe, names(fes))]
fwrite(S, file.path(OUT, "c46_fe_variants_series.csv"))
pp <- S[, { pre <- .SD[y < REF & y >= 2012]; post <- .SD[y > REF]; .(post_min_pre = mean(post$b) - mean(pre$b), se = sqrt(mean(post$se^2) + mean(pre$se^2) / nrow(pre))) }, by = .(model, coef, fe, outcome, series)][, t := post_min_pre / se]
fwrite(pp, file.path(OUT, "c46_fe_variants_post_pre.csv"))
sink(file.path(OUT, "c46_fe_variants.txt"), split = TRUE)
cat("Density-selected border sample, core outcomes: pair x year FE vs year FE only (unit FE always). post - pre (2018+ minus 2012-16), SD units per 10 mi closer (main) / per 10 mi per SD of E (interaction); cells: estimate (t).\n")
for (mc in list(c("rf", "main"), c("int", "main"), c("int", "interaction"))) { cat("\n==", ifelse(mc[1] == "rf", "reduced form", "interaction model"), "|", mc[2], "term\n")
  x <- pp[model == mc[1] & coef == mc[2]][, cell := sprintf("%+.3f (%.1f)", post_min_pre, t)]
  print(as.data.frame(dcast(x, outcome ~ series + fe, value.var = "cell")), row.names = FALSE) }
cat("\nCounts of |t| > 1.96 (11 outcomes) and median |post - pre|:\n"); print(as.data.frame(pp[, .(sig = sum(abs(t) > 1.96), median_abs = round(median(abs(post_min_pre)), 3)), by = .(model, coef, fe, series)][order(model, coef, series, fe)]), row.names = FALSE)
sink()
plot_es <- function(x, title, file, unit) { x <- copy(x)[, outcome := factor(outcome, unique(outcome))]
  g <- ggplot(x, aes(y, b, colour = series, linetype = fe, alpha = fe)) + geom_hline(yintercept = 0, colour = "grey50") + geom_vline(xintercept = REF + .5, linetype = 3, colour = "grey50") +
    geom_ribbon(data = x[fe == "year only"], aes(ymin = b - 1.96 * se, ymax = b + 1.96 * se, fill = series), alpha = .1, colour = NA, show.legend = FALSE) + geom_line(linewidth = .45) + geom_point(size = .8) +
    facet_wrap(~outcome, scales = "free_y", ncol = 4) + scale_colour_manual(NULL, values = c("#2a64a8", "#c0392b", "black")) + scale_fill_manual(NULL, values = c("#2a64a8", "#c0392b", "black")) +
    scale_linetype_manual(NULL, values = c(`pair x year` = "22", `year only` = "solid")) + scale_alpha_manual(NULL, values = c(`pair x year` = .55, `year only` = 1), guide = "none") +
    labs(x = NULL, y = paste0("SD units of the outcome ", unit, "; 2017 = 0; 95% CI (year-only FE), SE by county"), title = title,
         subtitle = "density-selected sample; dashed = unit FE + pair x year FE (as 29/31), solid with band = unit FE + year FE only; high minus low = difference of the side coefficients") +
    theme_minimal(base_size = 8) + theme(legend.position = "bottom")
  ggsave(file.path(OUT, "figures", file), g, width = 13, height = 7.5, dpi = 150) }
plot_es(S[model == "rf"], "Reduced form Z x year: pair x year FE vs year FE only", "c46_fe_variants_rf_main.png", "per 10 mi closer")
plot_es(S[model == "int" & coef == "main"], "Interaction model, main term Z x year at mean exposure: pair x year FE vs year FE only", "c46_fe_variants_int_main.png", "per 10 mi closer")
plot_es(S[model == "int" & coef == "interaction"], "Interaction model, Z x exposure x year: pair x year FE vs year FE only", "c46_fe_variants_int_interaction.png", "per 10 mi closer per SD of SALT exposure")
