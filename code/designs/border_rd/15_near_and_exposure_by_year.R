# Both coefficients of the interacted model, year by year: the near-border effect at mean exposure (near x year) and the
# loading on SALT exposure (near x exposure x year), low-tax side within 50 mi, rural pairs (09's default rule). Exposure =
# the lost federal subsidy per owner household 2016-18 (build/exposure/05, ZIP -> tract via the modal sales ZIP, clean ZIPs),
# as the tract's own value (standardised across low-side tracts; exposure x year as a control) or the high side's value for
# the pair (population-weighted within 10 mi, standardised across pairs; absorbed by pair x year FE in the DiD).
# Two versions of each coefficient:
#   levels: a separate cross-section per year, y = b_t near + g_t near x exposure (+ d_t exposure) | pair, no unit FE, no
#           reference year, i.e. the near-far level difference that year and how it varies with exposure;
#   DiD:    y = sum_t b_t near x 1[t] + sum_t g_t near x exposure x 1[t] (+ exposure x 1[t]) + unit FE + pair x year FE, 2017 = 0.
# Districts / counties: population share within 10 mi and population-weighted exposure, units with >= 50% of their population
# in the sample. SE by county; coefficients in SD units of the outcome's 2016 cross-section (per SD of exposure for g_t).
suppressPackageStartupMessages({library(data.table); library(DBI); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_rd"); invisible(out_dir("designs/border_rd/figures")); MX <- 50; REF <- 2017
source("code/designs/border_rd/nb_panels.R")
d <- readRDS(file.path(DATA, "built/geo/low_side_balance_tracts.rds"))[dist_mi < MX]
metro <- fread(file.path(OUT, "c22_low_side_metro_pairs.csv"))$pair; d <- d[!pair %in% metro][, near := as.integer(dist_mi < 10)]
tz <- fread(file.path(DATA, "built/exposure/salt_exposure_tract_zip2016.csv"), colClasses = c(tract = "character", zip = "character"))[, .(tract, zip)]
cz <- fread(file.path(DATA, "built/exposure/exposure_components_zip.csv"), colClasses = c(geo = "character"))[clean == TRUE, .(zip = geo, d_salt_own)]
ex <- merge(tz, cz, by = "zip")
hs <- merge(readRDS(file.path(DATA, "built/geo/iv_tract_side.rds"))[T == 30 & high == 1 & dist_mi < 10, .(tract, pair, pop)], ex, by = "tract")
hi <- hs[, .(hi_ex = weighted.mean(d_salt_own, pop, na.rm = TRUE)), by = pair]; hi[, hi_z := (hi_ex - mean(hi_ex)) / sd(hi_ex)]
d <- merge(merge(d, ex[, .(tract, d_salt_own)], by = "tract", all.x = TRUE), hi[, .(pair, hi_z)], by = "pair", all.x = TRUE)
d[, ex_o := (d_salt_own - mean(d_salt_own, na.rm = TRUE)) / sd(d_salt_own, na.rm = TRUE)][, `:=`(near_xo = near * ex_o, near_xh = near * hi_z)]
cat("tracts:", nrow(d), "| with own exposure:", sum(is.finite(d$ex_o)), "| with high-side exposure:", sum(is.finite(d$hi_z)), "| pairs:", uniqueN(d$pair), "\n")
P <- nb_panels(d, c("near", "near_xo", "near_xh", "ex_o"))
vars <- list(cl = c(lp50 = "log median sale price", lbill = "log tax bill per parcel", eff50 = "effective tax rate", turn = "sales per 100 parcels"),
             ac = c(lhval = "log median home value (ACS)", lpop = "log population (ACS)"), mv = c(moveout = "move-out rate (Data Axle)"),
             f = c(lptax = "log district property tax per pupil", lspend = "log district spending per pupil", lenr = "log district enrolment"), sd = c(lptax = "log county school property tax (CoG)"))
units <- c(cl = "tract", ac = "tract", mv = "tract", f = "leaid", sd = "cty")
grab <- function(m, pat) as.data.table(coeftable(m), keep.rownames = "term")[grepl(pat, term), .(term, b = Estimate, se = `Std. Error`)]
run <- function(p, xz, ctrl, fe_unit) { x <- p[is.finite(yv) & is.finite(get(xz))]; sdy <- sd(x[y == 2016, yv], na.rm = TRUE)
  lev <- rbindlist(lapply(sort(unique(x$y)), \(t) { xt <- x[y == t]; if (nrow(xt) < 30 || uniqueN(xt$pair) < 2) return(NULL)
    m <- feols(as.formula(sprintf("yv ~ near + %s%s | pair", xz, ctrl)), xt, cluster = ~cty)
    grab(m, sprintf("^(near|%s)$", xz))[, `:=`(y = t, coef = ifelse(term == "near", "near x year", "near x exposure x year"))] }))[, version := "levels: cross-section per year, pair FE"]
  m <- feols(as.formula(sprintf("yv ~ i(y, near, ref = %d) + i(y, %s, ref = %d)%s | %s + pair^y", REF, xz, REF, if (ctrl == "") "" else sprintf(" + i(y, ex_o, ref = %d)", REF), fe_unit)), x, cluster = ~cty)
  did <- grab(m, sprintf("^y::\\d+:(near|%s)$", xz))[, `:=`(y = as.integer(sub("y::(\\d+):.*", "\\1", term)), coef = ifelse(grepl(":near$", term), "near x year", "near x exposure x year"))]
  did <- rbind(did, data.table(term = "", b = 0, se = 0, y = REF, coef = c("near x year", "near x exposure x year")))[, version := "DiD: unit FE + pair x year FE, 2017 = 0"]
  rbind(lev, did)[, `:=`(b = b / sdy, se = se / sdy, n = nrow(x))] }
res <- list()
for (e in c("own tract", "high side of the pair")) for (tb in names(vars)) for (k in names(vars[[tb]])) { p <- P[[tb]][, yv := get(k)]
  r <- run(p, if (e == "own tract") "near_xo" else "near_xh", if (e == "own tract") " + ex_o" else "", units[tb])
  res[[length(res) + 1]] <- r[, `:=`(exposure = e, outcome = vars[[tb]][k], unit = units[tb])] }
res <- rbindlist(res)
fwrite(res, file.path(OUT, "c28_near_and_exposure_by_year.csv"))
su <- res[, .(pre = mean(b[y >= 2013 & y < REF]), post = mean(b[y > REF]), se_post = sqrt(mean(se[y > REF]^2))), by = .(exposure, coef, version, outcome)][, diff := post - pre]
sink(file.path(OUT, "c28_near_and_exposure_by_year.txt"), split = TRUE)
cat("near x year and near x exposure x year, low side within 50 mi, rural pairs (", uniqueN(d$pair), "pairs,", nrow(d), "tracts). Exposure: lost subsidy per owner HH (SD units).\n",
    "Coefficients in SD units of the outcome (per SD of exposure for the interaction). Mean 2013-16, mean 2018+, difference; SE = RMS of post SEs.\n")
for (e in unique(su$exposure)) for (v in unique(su$version)) { cat("\n== exposure:", e, "|", v, "\n")
  w <- dcast(su[exposure == e & version == v, .(outcome, coef, cell = sprintf("%.3f / %.3f (d %.3f, se %.3f)", pre, post, diff, se_post))], outcome ~ coef, value.var = "cell")
  print(as.data.frame(w[match(unlist(vars), outcome)]), row.names = FALSE) }
sink()
for (e in unique(res$exposure)) { pl <- res[exposure == e][, `:=`(outcome = factor(outcome, unlist(vars)), coef = factor(coef, c("near x year", "near x exposure x year")))]
  g <- ggplot(pl, aes(y, b, colour = version)) + geom_hline(yintercept = 0, colour = "grey70") + geom_vline(xintercept = REF + .5, linetype = "22", colour = "grey50") +
    geom_pointrange(aes(ymin = b - 1.96 * se, ymax = b + 1.96 * se), position = position_dodge(width = .6), size = .18, linewidth = .3) + geom_line(position = position_dodge(width = .6), linewidth = .3) +
    facet_wrap(~ outcome + coef, scales = "free_y", ncol = 4, labeller = label_wrap_gen(28)) + scale_x_continuous(breaks = seq(2012, 2024, 4)) +
    scale_colour_manual(NULL, values = c("#2a64a8", "#c0392b")) +
    labs(x = NULL, y = "SD units of the outcome (interaction: per SD of exposure); 95% CI, SE by county",
         title = sprintf("Near-border effect at mean exposure, and its loading on SALT exposure (%s), year by year.  Rural pairs, low-tax side within 50 mi", e),
         subtitle = "exposure = lost federal subsidy per owner household 2016-18 (SOI ZIP at the tract's modal sales ZIP). Blue: separate cross-section each year, pair FE only. Red: unit FE + pair x year FE, 2017 = 0") +
    theme_minimal(base_size = 7.5) + theme(legend.position = "bottom", strip.text = element_text(size = 6))
  ggsave(file.path(OUT, sprintf("figures/c28_near_and_exposure_by_year_%s.png", ifelse(e == "own tract", "own", "high"))), g, width = 11, height = 12, dpi = 150) }
