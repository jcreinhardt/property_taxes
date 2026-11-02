# Event studies of the exposure interaction with a continuous instrument: z = -distance to the border / 10 mi (closer =
# positive), low-tax side within 50 mi, rural pairs at the 45% cutoff (09's default). Exposure = the tract's own lost federal
# subsidy per owner household 2016-18 (build/exposure/05, modal sales ZIP, clean ZIPs), standardised across the low-side tracts.
#   y = sum_t b_t z x 1[t] + sum_t g_t z x exposure x 1[t] + sum_t d_t exposure x 1[t] + unit FE + pair x year FE, 2017 = 0,
# SE by county; districts / counties with population-weighted z and exposure (nb_panels). Coefficients in SD units of the
# outcome's 2016 cross-section, per 10 mi closer (b_t) and per 10 mi closer per SD of exposure (g_t). Also the near < 10 mi
# version of the same model for reference (dashed), from 15 / 16.
suppressPackageStartupMessages({library(data.table); library(DBI); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_rd"); invisible(out_dir("designs/border_rd/figures")); MX <- 50; REF <- 2017
source("code/designs/border_rd/nb_panels.R")
d <- readRDS(file.path(DATA, "built/geo/low_side_balance_tracts.rds"))[dist_mi < MX]
metro <- fread(file.path(OUT, "c22_low_side_metro_pairs.csv"))$pair; d <- d[!pair %in% metro]
tz <- fread(file.path(DATA, "built/exposure/salt_exposure_tract_zip2016.csv"), colClasses = c(tract = "character", zip = "character"))[, .(tract, zip)]
cz <- fread(file.path(DATA, "built/exposure/exposure_components_zip.csv"), colClasses = c(geo = "character"))[clean == TRUE, .(zip = geo, d_salt_own)]
d <- merge(d, merge(tz, cz, by = "zip")[, .(tract, d_salt_own)], by = "tract")
d[, ex := (d_salt_own - mean(d_salt_own)) / sd(d_salt_own)][, `:=`(z = -dist_mi / 10, near = as.integer(dist_mi < 10))][, `:=`(z_x = z * ex, near_x = near * ex)]
cat("rural 45% low-side tracts with exposure:", nrow(d), "| pairs:", uniqueN(d$pair), "| mean distance", round(mean(d$dist_mi), 1), "mi\n")
P <- nb_panels(d, c("z", "z_x", "near", "near_x", "ex"))
vars <- list(cl = c(lp50 = "log median sale price", lbill = "log tax bill per parcel", eff50 = "effective tax rate", turn = "sales per 100 parcels"),
             ac = c(lhval = "log median home value (ACS)", lpop = "log population (ACS)"), mv = c(moveout = "move-out rate (Data Axle)"),
             f = c(lptax = "log district property tax per pupil", lspend = "log district spending per pupil", lenr = "log district enrolment"), sd = c(lptax = "log county school property tax (CoG)"))
units <- c(cl = "tract", ac = "tract", mv = "tract", f = "leaid", sd = "cty")
grab <- function(m, pat) as.data.table(coeftable(m), keep.rownames = "term")[grepl(pat, term), .(term, b = Estimate, se = `Std. Error`)]
res <- list()
for (tb in names(vars)) for (k in names(vars[[tb]])) { p <- P[[tb]][, yv := get(k)]; x <- p[is.finite(yv) & is.finite(ex)]; sdy <- sd(x[y == 2016, yv], na.rm = TRUE)
  for (iz in c("z", "near")) { xz <- paste0(iz, "_x")
    m <- feols(as.formula(sprintf("yv ~ i(y, %s, ref = %d) + i(y, %s, ref = %d) + i(y, ex, ref = %d) | %s + pair^y", iz, REF, xz, REF, REF, units[tb])), x, cluster = ~cty)
    r <- grab(m, sprintf("^y::\\d+:(%s|%s)$", iz, xz))[, `:=`(y = as.integer(sub("y::(\\d+):.*", "\\1", term)), coef = ifelse(grepl(paste0(":", iz, "$"), term), "instrument x year", "instrument x exposure x year"))]
    r <- rbind(r, data.table(term = "", b = 0, se = 0, y = REF, coef = c("instrument x year", "instrument x exposure x year")))
    res[[length(res) + 1]] <- r[, `:=`(b = b / sdy, se = se / sdy, instrument = ifelse(iz == "z", "-distance / 10 mi (continuous)", "near < 10 mi (dummy)"), outcome = vars[[tb]][k], unit = units[tb], n = nobs(m))] } }
res <- rbindlist(res); fwrite(res, file.path(OUT, "c30_exposure_event_study_continuous.csv"))
su <- res[, .(pre = mean(b[y >= 2013 & y < REF]), post = mean(b[y > REF]), se_post = sqrt(mean(se[y > REF]^2))), by = .(instrument, coef, outcome)][, diff := post - pre]
sink(file.path(OUT, "c30_exposure_event_study_continuous.txt"), split = TRUE)
cat("Continuous instrument z = -distance / 10 mi (closer = +), rural 45% low side within 50 mi (", nrow(d), "tracts,", uniqueN(d$pair), "pairs); exposure = own lost subsidy per owner HH (SD).\n",
    "Unit FE + pair x year FE, 2017 = 0, SE by county. SD units of the outcome per 10 mi closer (x year) and per 10 mi closer per SD of exposure (x exposure x year).\n",
    "Mean 2013-16 / mean 2018+ (difference; RMS post SE). Near < 10 mi dummy version for reference.\n")
for (iz in unique(su$instrument)) { cat("\n==", iz, "\n")
  w <- dcast(su[instrument == iz, .(outcome, coef, cell = sprintf("%.3f / %.3f (d %.3f, se %.3f)", pre, post, diff, se_post))], outcome ~ coef, value.var = "cell")
  print(as.data.frame(w[match(unlist(vars), outcome)][, c("outcome", "instrument x year", "instrument x exposure x year")]), row.names = FALSE) }
cat("\nYearly coefficients, continuous instrument x exposure x year:\n")
for (o in unlist(vars)) { r <- res[instrument == "-distance / 10 mi (continuous)" & coef == "instrument x exposure x year" & outcome == o][order(y)]; cat("\n==", o, "\n")
  print(as.data.frame(rbind(round(dcast(r, . ~ y, value.var = "b")[, -1], 3), round(dcast(r, . ~ y, value.var = "se")[, -1], 3))), row.names = c("b", "se")) }
sink()
pl <- copy(res)[, `:=`(outcome = factor(outcome, unlist(vars)), coef = factor(coef, c("instrument x exposure x year", "instrument x year")))]
for (cc in levels(pl$coef)) { g <- ggplot(pl[coef == cc], aes(y, b, colour = instrument, linetype = instrument)) + geom_hline(yintercept = 0, colour = "grey70") + geom_vline(xintercept = REF + .5, linetype = "22", colour = "grey50") +
    geom_pointrange(aes(ymin = b - 1.96 * se, ymax = b + 1.96 * se), position = position_dodge(width = .5), size = .2, linewidth = .3) + geom_line(position = position_dodge(width = .5), linewidth = .35) +
    facet_wrap(~ outcome, scales = "free_y", ncol = 4, labeller = label_wrap_gen(30)) + scale_x_continuous(breaks = seq(2012, 2024, 2)) +
    scale_colour_manual(NULL, values = c("#2a64a8", "grey55")) + scale_linetype_manual(NULL, values = c("solid", "22")) +
    labs(x = NULL, y = sprintf("%s: SD units of the outcome%s, 2017 = 0; 95%% CI, SE by county", cc, ifelse(grepl("exposure", cc), " per 10 mi closer per SD of exposure (dummy: near, per SD)", " per 10 mi closer (dummy: near)")),
         title = sprintf("%s.  Rural 45%% pairs, low-tax side within 50 mi, continuous distance instrument", ifelse(grepl("exposure", cc), "Exposure loading on distance to the border, year by year", "Distance-to-border effect at mean exposure, year by year")),
         subtitle = "exposure = own lost federal subsidy per owner household 2016-18 (SD); tract/unit FE + pair x year FE; instrument x year and exposure x year included. Grey dashed: near < 10 mi dummy instead of distance") +
    theme_minimal(base_size = 8) + theme(legend.position = "bottom", strip.text = element_text(size = 6.5))
  ggsave(file.path(OUT, sprintf("figures/c30_exposure_event_study_continuous_%s.png", ifelse(grepl("exposure", cc), "interaction", "main"))), g, width = 11, height = 7.5, dpi = 150) }
