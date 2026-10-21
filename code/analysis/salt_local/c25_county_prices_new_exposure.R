# County price event study (b2 spec) with alternative exposure measures side by side: log median arm's-length sale price ~
# i(y, X, ref 2017) | county + state x year, SE by county; county-years with >= 50 sales, 2013-2024; unweighted and weighted
# by 2016 returns. X = old exposure (a1: stub-average SALT above $10k per return, TY2016) or the c24 measures (lost federal
# value per 2016 return, TY2016 -> TY2018): d_salt, cap_bite, d_mid, d_both. Each X in SD units (SD across counties).
# Capitalisation: per $1 of annual lost federal subsidy per return (raw $ measures), the implied price change in $ at the
# 2017 mean median price, divided by the PV of $1/yr at 5% (= $20): factor = coef_per_$ x P / 20.
suppressPackageStartupMessages({library(data.table); library(fixest); library(ggplot2)})
ROOT <- "/Users/jr2728/PhD/projects/property_taxes"; OUT <- file.path(ROOT, "output/analysis/salt_local")
cy <- readRDS(file.path(ROOT, "data/local_juris/county_year.rds"))[y >= 2013 & y <= 2024 & n_sales >= 50 & p50 > 0]
cy[, `:=`(l_p50 = log(p50), st = substr(fips, 1, 2))]
old <- fread(file.path(ROOT, "data/local_juris/salt_exposure_county2016.csv"))[, .(fips = sprintf("%05d", county), old = 1000 * excess_pp, returns)]
new <- fread(file.path(ROOT, "data/local_juris/salt_exposure_county_delta.csv"), colClasses = c(fips = "character"))[, .(fips, d_salt, cap_bite, d_mid, d_both)]
cy <- merge(merge(cy, old, by = "fips"), new, by = "fips")
ms <- c(old = "old: SALT above $10k per return (deduction $)", d_salt = "d_salt: lost value of SALT deductions",
        cap_bite = "cap_bite: value of SALT disallowed by the cap", d_mid = "d_mid: lost value of mortgage interest",
        d_both = "d_both: d_salt + d_mid")
u <- unique(cy[, c("fips", names(ms)), with = FALSE])
sds <- sapply(names(ms), \(k) sd(u[[k]], na.rm = TRUE))
for (k in names(ms)) cy[, (paste0(k, "_z")) := get(k) / sds[k]]
est <- function(k, wt, extra = NULL) {
  f <- as.formula(sprintf("l_p50 ~ i(y, %s_z, ref = 2017)%s | fips + st^y", k, if (is.null(extra)) "" else paste0(" + i(y, ", extra, "_z, ref = 2017)")))
  m <- feols(f, cy, cluster = ~fips, weights = if (wt) ~returns else NULL)
  ct <- as.data.table(coeftable(m), keep.rownames = "term")[grepl(paste0(":", k, "_z$"), term)]
  rbind(data.table(y = 2017L, b = 0, se = 0), ct[, .(y = as.integer(sub("y::(\\d+):.*", "\\1", term)), b = Estimate, se = `Std. Error`)])[,
    `:=`(measure = k, weighted = wt, joint = !is.null(extra))] }
res <- rbind(rbindlist(lapply(names(ms), \(k) rbindlist(lapply(c(FALSE, TRUE), \(w) est(k, w))))),
             rbindlist(lapply(c(FALSE, TRUE), \(w) rbind(est("d_salt", w, "d_mid"), est("d_mid", w, "d_salt")))))
res[, sd_dollars := sds[measure]]
p17 <- c(`FALSE` = cy[y == 2017, mean(p50)], `TRUE` = cy[y == 2017, weighted.mean(p50, returns)])
res[, factor := fifelse(measure == "old", NA_real_, b / sd_dollars * p17[as.character(weighted)] / 20)]
fwrite(res, file.path(OUT, "c25_county_prices_new_exposure.csv"))
sink(file.path(OUT, "c25_county_prices_new_exposure.txt"), split = TRUE)
cat("Log median sale price per SD of each exposure (x100), ref 2017; county + state x year FE; SE by county. Counties:", uniqueN(cy$fips), "\n")
cat("SD of each measure ($ per 2016 return):", paste(names(sds), round(sds), sep = " = ", collapse = ", "), "\n")
cat("2017 mean median price: unweighted", round(p17[1]), "| return-weighted", round(p17[2]), "\n")
for (w in c(FALSE, TRUE)) for (j in c(FALSE, TRUE)) { cat("\n== weighted:", w, "| d_salt and d_mid jointly:", j, "\n")
  x <- res[weighted == w & joint == j]
  print(dcast(x[, .(y, measure, cell = sprintf("%.2f (%.2f)", 100 * b, 100 * se))], y ~ measure, value.var = "cell")) }
cat("\n== Implied capitalisation factor (price change / PV of the lost subsidy at 5%), single-measure models:\n")
print(dcast(res[joint == FALSE & measure != "old" & y %in% c(2018, 2019, 2021, 2024), .(measure, weighted, y, f = round(factor, 2))], measure + weighted ~ y, value.var = "f"))
sink()
pl <- res[joint == FALSE][, label := factor(ms[measure], ms)]
g <- ggplot(pl, aes(y, 100 * b, colour = weighted)) + geom_hline(yintercept = 0, colour = "grey60") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey40") +
  geom_pointrange(aes(ymin = 100 * (b - 1.96 * se), ymax = 100 * (b + 1.96 * se)), position = position_dodge(.45), size = .15) +
  facet_wrap(~ label, ncol = 2) + scale_x_continuous(breaks = seq(2013, 2024, 2)) +
  scale_colour_manual(NULL, values = c(`FALSE` = "black", `TRUE` = "#e34a33"), labels = c("unweighted", "weighted by 2016 returns")) +
  labs(x = NULL, y = "log median sale price x 100, per SD of exposure (vs 2017)",
       title = "County median sale prices and alternative SALT-cap exposure measures",
       subtitle = "County + state x year FE; 95% CI, SE by county; counties with >= 50 sales per year. Same y-axis across panels.") +
  theme_minimal(base_size = 9) + theme(legend.position = "bottom")
ggsave(file.path(OUT, "figures/c25_county_prices_new_exposure.png"), g, width = 10, height = 10, dpi = 150)
