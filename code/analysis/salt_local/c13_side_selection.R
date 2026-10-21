# Is the high-tax side of a state border selected? Level comparison across the line, within border pair:
# (1) Y ~ high | pair for tracts within 5 and 10 mi of the border; (2) local-linear boundary discontinuity within 10 mi:
#     Y ~ high + high x signed distance + signed distance (slopes by pair) | pair. SE clustered by county.
# Coefficients also in SD units of Y (SD over all instrument tracts). Sides as in local_juris/R/26 (high = higher median
# effective rate within 20 mi). Spending and district property tax per pupil from F-33 FY2016 (district of the tract).
suppressPackageStartupMessages({library(data.table); library(fixest); library(ggplot2)})
ROOT <- "/Users/jr2728/PhD/projects/property_taxes"; OUT <- file.path(ROOT, "output/analysis/salt_local")
d <- readRDS(file.path(ROOT, "data/local_juris/iv_tract_side.rds"))[T == 30]
f <- readRDS(file.path(ROOT, "data/local_juris/district_finance.rds"))[y == 2016 & enr > 0, .(leaid, lspend = log(spend / enr))]
d <- merge(d, f, by = "leaid", all.x = TRUE)
d[, sd_mi := ifelse(high == 1, dist_mi, -dist_mi)]
v <- c(med = "effective tax rate (%)", ach = "test scores (SEDA, SD)", ses = "district SES (SEDA)", perfrl = "share free/reduced lunch",
       lspend = "log spending per pupil", linc = "log median HH income", lhval = "log median home value", sh_ba = "share BA+",
       sh_white = "share white", sh_own = "owner share", yrbuilt = "median year built", age = "median age", lpop = "log population")
sds <- sapply(names(v), \(k) sd(d[[k]], na.rm = TRUE))
res <- rbindlist(lapply(names(v), \(k) rbindlist(list(
  { m <- feols(as.formula(sprintf("%s ~ high | pair", k)), d[dist_mi < 5], cluster = ~cty); data.table(spec = "< 5 mi, side means", b = coef(m)["high"], se = se(m)["high"], n = nobs(m)) },
  { m <- feols(as.formula(sprintf("%s ~ high | pair", k)), d[dist_mi < 10], cluster = ~cty); data.table(spec = "< 10 mi, side means", b = coef(m)["high"], se = se(m)["high"], n = nobs(m)) },
  { m <- feols(as.formula(sprintf("%s ~ high + high:sd_mi | pair[sd_mi]", k)), d[dist_mi < 10], cluster = ~cty); data.table(spec = "< 10 mi, local linear (jump at 0)", b = coef(m)["high"], se = se(m)["high"], n = nobs(m)) }
))[, `:=`(var = k, label = v[k], b_sd = b / sds[k], se_sd = se / sds[k])]))
fwrite(res, file.path(OUT, "c13_side_selection.csv"))
sink(file.path(OUT, "c13_side_selection.txt"), split = TRUE)
cat("High-tax side minus low-side, within border pair; SE by county. Columns: raw (SE) | SD units\n")
print(dcast(res[, .(label, spec, cell = sprintf("%.3f (%.3f) | %.2f SD", b, se, b_sd))], label ~ spec, value.var = "cell")[match(v, label)])
sink()
res[, label := factor(label, rev(v))]
g <- ggplot(res, aes(b_sd, label, colour = spec)) + geom_vline(xintercept = 0, colour = "grey50") +
  geom_pointrange(aes(xmin = b_sd - 1.96 * se_sd, xmax = b_sd + 1.96 * se_sd), position = position_dodge(width = .6), size = .25) +
  scale_colour_manual(NULL, values = c("#2a64a8", "#7aa6d8", "#d1772b")) +
  labs(x = "high-tax side minus low-tax side (SD units of the variable), within border pair; 95% CI, SE by county", y = NULL,
       title = "Is the high-tax side of a state border selected?", subtitle = "Tracts near the border, pair fixed effects. Top row: the tax-rate gap itself.") +
  theme_minimal(base_size = 9) + theme(legend.position = "bottom")
ggsave(file.path(OUT, "figures/c13_side_selection.png"), g, width = 8, height = 6, dpi = 150)
