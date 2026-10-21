# Border RD by year, 2013-2024: rdrobust at signed distance 0 (high-tax minus low-tax side) separately for each year.
# Sides and distances fixed (local_juris/R/26: sides ranked on 2017-19 sale medians). Outcome demeaned by border pair within
# year (spec A of c15). Local linear, triangular kernel, fixed bandwidth h = 15 mi in every year (about the MSE-optimal h in
# c15), so year-to-year changes are not driven by bandwidth choice; robust bias-corrected CI, SE clustered by county.
# Tract-year outcomes (CoreLogic full roll, data/local_juris/tract_year.rds): median effective rate of arm's-length sales,
# log median nominal rate on the residential roll (bill / AV; levels not comparable across fractional-assessment states, hence logs and pair demeaning), log mean bill per residential parcel, log median sale price, log median
# assessment ratio, sales per parcel. District-year (F-33, fiscal year = y): log current spending per pupil, log property
# tax per pupil (> 0), state share of state + local revenue.
suppressPackageStartupMessages({library(data.table); library(rdrobust); library(ggplot2)})
ROOT <- "/Users/jr2728/PhD/projects/property_taxes"; OUT <- file.path(ROOT, "output/analysis/salt_local"); H <- 15; MX <- 100; YRS <- 2013:2024
s <- unique(readRDS(file.path(ROOT, "data/local_juris/iv_tract_side.rds"))[T == 30, .(tract, leaid, pair, high, dist_mi, cty)])
s[, sd_mi := ifelse(high == 1, dist_mi, -dist_mi)]; s <- s[abs(sd_mi) <= MX]
ty <- readRDS(file.path(ROOT, "data/local_juris/tract_year.rds"))[y %in% YRS & tract %in% s$tract]
ty <- ty[, .(tract, y, eff = 100 * eff50, lnom = log(med_nom_res), lbill = log(bill / n_bill), lp50 = log(p50), lratio = log(ratio50),
             turn = n_sales / n)]
ty[!is.finite(lnom), lnom := NA][!is.finite(lbill), lbill := NA][!is.finite(lratio), lratio := NA][!is.finite(lp50), lp50 := NA]
fin <- readRDS(file.path(ROOT, "data/local_juris/district_finance.rds"))[y %in% YRS & enr > 0,
  .(leaid, y, lspend = log(spend / enr), lptax = fifelse(ptax > 0, log(ptax / enr), NA_real_), aid_sh = strev / (strev + loc))]
d <- merge(CJ(tract = s$tract, y = YRS), s, by = "tract")
d <- merge(d, ty, by = c("tract", "y"), all.x = TRUE)
d <- merge(d, fin, by = c("leaid", "y"), all.x = TRUE)
v <- c(eff = "effective tax rate, sales (pp)", lnom = "log nominal rate (bill / AV), roll", lbill = "log mean bill per parcel", lp50 = "log median sale price",
       lratio = "log median assessment ratio", turn = "sales per parcel", lspend = "district log spending per pupil",
       lptax = "district log property tax per pupil", aid_sh = "district state-aid share")
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
  facet_wrap(~ label, scales = "free_y", ncol = 3) + scale_x_continuous(breaks = seq(2013, 2024, 2)) +
  labs(x = "tax year (F-33: fiscal year)", y = "RD estimate at the border: high-tax minus low-tax side",
       title = "Border discontinuities by year (rdrobust, local linear, h = 15 mi, pair-demeaned)",
       subtitle = "Robust bias-corrected 95% CI, SE by county. Dashed line: SALT cap enacted Dec 2017 (first affected tax year 2018).") +
  theme_minimal(base_size = 9)
ggsave(file.path(OUT, "figures/c16_border_rd_by_year.png"), g, width = 11, height = 8, dpi = 150)
