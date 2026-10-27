# Formerly code/analysis/salt_local/c16_border_rd_by_year.R (moved in the 2026-10-27 restructuring).
# Border RD by year, 2013-2024: rdrobust at signed distance 0 (high-tax minus low-tax side) separately for each year.
# Sides and distances fixed (local_juris/R/26: sides ranked on 2017-19 sale medians). Outcome demeaned by border pair within
# year (spec A of c15). Local linear, triangular kernel, fixed bandwidth h = 15 mi in every year (about the MSE-optimal h in
# c15), so year-to-year changes are not driven by bandwidth choice; robust bias-corrected CI, SE clustered by county.
# Tract-year outcomes (CoreLogic full roll, data/built/corelogic/tract_year.rds): median effective rate of arm's-length sales,
# log median nominal rate on the residential roll (bill / AV; levels not comparable across fractional-assessment states, hence logs and pair demeaning), log mean bill per residential parcel, log median sale price, log median
# assessment ratio, sales per parcel. District-year (F-33, fiscal year = y): log current spending per pupil, log property
# tax per pupil (> 0), state share of state + local revenue. Covariates (c18): SEDA 4.1 district-year achievement (grades 3-8,
# cohort scale), SES and FRL share (2009-2019 only); ACS 5-year tract estimates by release year (year y = survey years y-4..y;
# releases from 2020 mapped from 2020 to 2010 tracts by land-area shares).
suppressPackageStartupMessages({library(data.table); library(rdrobust); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_rd"); invisible(out_dir("designs/border_rd/figures")); H <- 15; MX <- 100; YRS <- 2013:2024
s <- unique(readRDS(file.path(DATA, "built/geo/iv_tract_side.rds"))[T == 30, .(tract, leaid, pair, high, dist_mi, cty)])
s[, sd_mi := ifelse(high == 1, dist_mi, -dist_mi)]; s <- s[abs(sd_mi) <= MX]
ty <- readRDS(file.path(DATA, "built/corelogic/tract_year.rds"))[y %in% YRS & tract %in% s$tract]
ty <- ty[, .(tract, y, eff = 100 * eff50, lnom = log(med_nom_res), lbill = log(bill / n_bill), lp50 = log(p50), lratio = log(ratio50),
             turn = n_sales / n)]
ty[!is.finite(lnom), lnom := NA][!is.finite(lbill), lbill := NA][!is.finite(lratio), lratio := NA][!is.finite(lp50), lp50 := NA]
fin <- readRDS(file.path(DATA, "built/census_finances/district_finance.rds"))[enr > 0]
fin[, lenr_g := log(enr) - log(enr[y == 2017][1]), by = leaid]   # enrolment growth relative to FY2017 (NA if no 2017 record)
fin <- fin[y %in% YRS, .(leaid, y, lspend = log(spend / enr), lptax = fifelse(ptax > 0, log(ptax / enr), NA_real_), aid_sh = strev / (strev + loc), lenr_g)]
d <- merge(CJ(tract = s$tract, y = YRS), s, by = "tract")
d <- merge(d, ty, by = c("tract", "y"), all.x = TRUE)
d <- merge(d, fin, by = c("leaid", "y"), all.x = TRUE)
d <- merge(d, readRDS(file.path(DATA, "built/acs/seda_district_year.rds"))[, .(leaid, y, ach, ses, perfrl)], by = c("leaid", "y"), all.x = TRUE)
d <- merge(d, readRDS(file.path(DATA, "built/acs/acs_tract_year.rds")), by = c("tract", "y"), all.x = TRUE)
v <- c(eff = "effective tax rate, sales (pp)", lnom = "log nominal rate (bill / AV), roll", lbill = "log mean bill per parcel", lp50 = "log median sale price",
       lratio = "log median assessment ratio", turn = "sales per parcel", lspend = "district log spending per pupil",
       lptax = "district log property tax per pupil", aid_sh = "district state-aid share",
       lenr_g = "district log enrolment, change since FY2017",
       ach = "district test scores (SEDA, SD)", ses = "district SES (SEDA)", perfrl = "district share free/reduced lunch (SEDA)",
       linc = "tract log median HH income (ACS 5-yr)", lhval = "tract log median home value (ACS 5-yr)", sh_ba = "tract share BA+ (ACS 5-yr)",
       sh_white = "tract share white (ACS 5-yr)", sh_own = "tract owner share (ACS 5-yr)", yrbuilt = "tract median year built (ACS 5-yr)",
       age = "tract median age (ACS 5-yr)", lpop = "tract log population (ACS 5-yr)")
run <- function(k, yy) {
  x <- d[y == yy & is.finite(get(k))]
  if (nrow(x) < 500) return(NULL)
  yv <- x[[k]] - ave(x[[k]], x$pair)
  r <- tryCatch(rdrobust(yv, x$sd_mi, c = 0, h = H, cluster = x$cty, vce = "hc1"), error = \(e) NULL)
  if (is.null(r)) return(NULL)
  data.table(var = k, label = v[k], y = yy, est = r$coef[1], lo = r$ci[3, 1], hi = r$ci[3, 2], p = r$pv[3], n_eff = sum(r$N_h)) }
res <- rbindlist(lapply(names(v), \(k) rbindlist(lapply(YRS, \(yy) run(k, yy)))))
fwrite(res, file.path(OUT, "c16_border_rd_by_year.csv"))
sink(file.path(OUT, "c16_border_rd_by_year.txt"), split = TRUE)
cat("Border RD by year (h =", H, "mi, pair-demeaned, robust bias-corrected 95% CI, SE by county). Cells: estimate [CI]\n")
print(dcast(res[, .(label, y, cell = sprintf("%.3f [%.3f, %.3f]", est, lo, hi))], y ~ label, value.var = "cell"))
cat("\nEffective n within the bandwidth:\n"); print(dcast(res, y ~ var, value.var = "n_eff"))
sink()
res[, label := factor(label, v)]
g <- ggplot(res, aes(y, est)) + geom_hline(yintercept = 0, colour = "grey60") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey40") +
  geom_ribbon(aes(ymin = lo, ymax = hi), fill = "#2a64a8", alpha = .15) + geom_line(colour = "#2a64a8") + geom_point(colour = "#2a64a8", size = 1) +
  facet_wrap(~ label, scales = "free_y", ncol = 4) + scale_x_continuous(breaks = seq(2013, 2024, 2)) +
  labs(x = "tax year (F-33: fiscal year; SEDA: school year ending; ACS: last year of the 5-year window)", y = "RD estimate at the border: high-tax minus low-tax side",
       title = "Border discontinuities by year (rdrobust, local linear, h = 15 mi, pair-demeaned)",
       subtitle = "Robust bias-corrected 95% CI, SE by county. Dashed line: SALT cap enacted Dec 2017 (first affected tax year 2018).") +
  theme_minimal(base_size = 9)
ggsave(file.path(OUT, "figures/c16_border_rd_by_year.png"), g, width = 13, height = 13, dpi = 150)

# ---- Difference in discontinuities: RD of the within-tract change Y_y - Y_2017 (same tract, same bandwidth), so 2017 = 0
# by construction and the CI accounts for the covariance between the two years' discontinuities. District outcomes are
# differenced within the tract's district. Pair-demeaned within year as above.
for (k in names(v)) d[, (paste0(k, "_d")) := get(k) - get(k)[y == 2017][1], by = tract]
rund <- function(k, yy) {
  kd <- paste0(k, "_d"); x <- d[y == yy & is.finite(get(kd))]
  if (nrow(x) < 500) return(NULL)
  yv <- x[[kd]] - ave(x[[kd]], x$pair)
  r <- tryCatch(rdrobust(yv, x$sd_mi, c = 0, h = H, cluster = x$cty, vce = "hc1"), error = \(e) NULL)
  if (is.null(r)) return(NULL)
  data.table(var = k, label = v[k], y = yy, est = r$coef[1], lo = r$ci[3, 1], hi = r$ci[3, 2], p = r$pv[3], n_eff = sum(r$N_h)) }
rd2 <- rbindlist(lapply(names(v), \(k) { z <- rbindlist(lapply(setdiff(YRS, 2017), \(yy) rund(k, yy)))
  if (nrow(z)) rbind(z, data.table(var = k, label = v[k], y = 2017L, est = 0, lo = 0, hi = 0, p = NA_real_, n_eff = NA_integer_)) }))[order(var, y)]
fwrite(rd2, file.path(OUT, "c16_border_diff_in_disc.csv"))
sink(file.path(OUT, "c16_border_diff_in_disc.txt"), split = TRUE)
cat("Difference in discontinuities: RD (h =", H, "mi) of the within-tract change since 2017. Cells: estimate [robust 95% CI]\n")
print(dcast(rd2[, .(var, y, cell = sprintf("%.3f [%.3f, %.3f]", est, lo, hi))], y ~ var, value.var = "cell"))
sink()
rd2[, label := factor(label, v)]
g2 <- ggplot(rd2, aes(y, est)) + geom_hline(yintercept = 0, colour = "grey60") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey40") +
  geom_ribbon(aes(ymin = lo, ymax = hi), fill = "#2a64a8", alpha = .15) + geom_line(colour = "#2a64a8") + geom_point(colour = "#2a64a8", size = 1) +
  facet_wrap(~ label, scales = "free_y", ncol = 4) + scale_x_continuous(breaks = seq(2013, 2024, 2)) +
  labs(x = "tax year (F-33: fiscal year; SEDA: school year ending; ACS: last year of the 5-year window)",
       y = "change in the border discontinuity since 2017 (high-tax minus low-tax side)",
       title = "Difference in discontinuities at state borders, relative to 2017 (rdrobust, h = 15 mi, pair-demeaned)",
       subtitle = "RD of the within-tract change Y(year) - Y(2017); robust bias-corrected 95% CI, SE by county. Dashed line: SALT cap (first affected tax year 2018).") +
  theme_minimal(base_size = 9)
ggsave(file.path(OUT, "figures/c16_border_diff_in_disc.png"), g2, width = 13, height = 13, dpi = 150)
