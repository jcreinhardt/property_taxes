# Formerly code/analysis/salt_local/c20_enrolment_rd.R (moved in the 2026-10-27 restructuring).
# Border RD on pupils per district, by year (FY2012-2023), high-tax minus low-tax side. Unit = school district.
# District running variable: its tracts' (local_juris/R/26) majority border pair and side; signed distance = median
# distance of its tracts to that border, negative on the low-tax side. Districts within 100 mi; outcome demeaned by pair
# within year; rdrobust local linear, triangular kernel, SE clustered by county. Bandwidth fixed across years at the
# MSE-optimal value for log enrolment in FY2017. Two outcomes per year:
#   levels: log fall enrolment (F-33 V33);
#   difference in discontinuities: within-district change log enr(y) - log enr(2017), so 2017 = 0.
# Districts with enrolment > 0 in all 12 years; unweighted (each district one observation).
suppressPackageStartupMessages({library(data.table); library(rdrobust); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_rd"); invisible(out_dir("designs/border_rd/figures")); MX <- 100
t <- unique(readRDS(file.path(DATA, "built/geo/iv_tract_side.rds"))[!is.na(leaid), .(tract, leaid, pair, high, dist_mi, cty)])
mode1 <- \(x) names(which.max(table(x)))
dv <- t[, .(pair = mode1(pair), n_tr = .N), by = leaid]
t <- merge(t, dv[, .(leaid, pmaj = pair)], by = "leaid")[pair == pmaj]
dv <- t[, .(pair = pair[1], high = as.integer(mode1(high)), dist = median(dist_mi), cty = mode1(cty), n_tr = .N), by = leaid]
dv[, sd_mi := ifelse(high == 1, dist, -dist)]
p <- readRDS(file.path(DATA, "built/census_finances/district_finance.rds"))[enr > 0]
p[, bal := uniqueN(y) == 12, by = leaid]
p <- p[bal == TRUE][, `:=`(lenr = log(enr), d17 = log(enr) - log(enr[y == 2017])), by = leaid]
d <- merge(p[, .(leaid, y, enr, lenr, d17)], dv, by = "leaid")[abs(sd_mi) <= MX]
x17 <- d[y == 2017]
H <- rdbwselect(x17$lenr - ave(x17$lenr, x17$pair), x17$sd_mi, c = 0, cluster = x17$cty, vce = "hc1")$bws[1, 1]
run <- function(k, yy) {
  x <- d[y == yy & is.finite(get(k))]; yv <- x[[k]] - ave(x[[k]], x$pair)
  r <- tryCatch(rdrobust(yv, x$sd_mi, c = 0, h = H, cluster = x$cty, vce = "hc1"), error = \(e) NULL)
  if (is.null(r)) return(NULL)
  data.table(y = yy, est = r$coef[1], se = r$se[1], lo = r$ci[3, 1], hi = r$ci[3, 2], p = r$pv[3], n_eff = sum(r$N_h)) }
res <- rbind(rbindlist(lapply(2012:2023, \(yy) run("lenr", yy)))[, spec := "levels: log enrolment"],
             rbind(rbindlist(lapply(setdiff(2012:2023, 2017), \(yy) run("d17", yy))), data.table(y = 2017L, est = 0, se = 0, lo = 0, hi = 0, p = NA, n_eff = NA))[
               , spec := "difference in discontinuities: change in log enrolment since FY2017"])[order(spec, y)]
fwrite(res, file.path(OUT, "c20_enrolment_rd.csv"))
sink(file.path(OUT, "c20_enrolment_rd.txt"), split = TRUE)
cat("Border RD on log district enrolment, high-tax minus low-tax side. Districts within", MX, "mi:", uniqueN(d$leaid),
    "| high side:", uniqueN(d[high == 1, leaid]), "| bandwidth (MSE-optimal, FY2017):", round(H, 1), "mi | pairs:", uniqueN(d$pair), "\n")
cat("Coefficients x 100 (log points); conventional SE; robust bias-corrected 95% CI and p\n")
print(res[, .(spec, y, est = round(100 * est, 2), se = round(100 * se, 2), ci = sprintf("[%.2f, %.2f]", 100 * lo, 100 * hi), p = round(p, 3), n_eff)])
sink()
g <- ggplot(res, aes(y, 100 * est)) + geom_hline(yintercept = 0, colour = "grey60") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey40") +
  geom_ribbon(aes(ymin = 100 * lo, ymax = 100 * hi), fill = "#2a64a8", alpha = .15) + geom_line(colour = "#2a64a8") + geom_point(colour = "#2a64a8", size = 1.2) +
  facet_wrap(~ spec, scales = "free_y", ncol = 1) + scale_x_continuous(breaks = 2012:2023) +
  labs(x = "fiscal year (fall enrolment of the school year)", y = "high-tax minus low-tax side at the border, log points x 100",
       title = "Border RD on pupils per district, by year",
       subtitle = sprintf("District-level rdrobust, local linear, h = %.1f mi (MSE-optimal in FY2017), pair-demeaned; robust bias-corrected 95%% CI, SE by county", H)) +
  theme_minimal(base_size = 9)
ggsave(file.path(OUT, "figures/c20_enrolment_rd.png"), g, width = 8, height = 7, dpi = 150)
