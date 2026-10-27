# Border RD on covariates: running variable = signed distance to the tract's state border (- = low-tax side, + = high-tax
# side), cutoff 0, tracts within 100 mi. rdrobust (Calonico-Cattaneo-Titiunik): local linear, triangular kernel,
# MSE-optimal common bandwidth, robust bias-corrected inference, SE clustered by county. Border-pair heterogeneity two ways:
#   (A) outcome demeaned by border pair (pair means over all tracts within 100 mi);
#   (B) pair dummies as additive covariates in rdrobust (Calonico, Cattaneo, Farrell & Titiunik 2019).
# Estimates also in SD units of the variable. Holm adjustment across the 11 non-rate covariates within each spec.
suppressPackageStartupMessages({library(data.table); library(rdrobust); library(ggplot2)})
source("code/R/paths.R"); OUT <- file.path(ROOT, "output/analysis/salt_local"); MX <- 100
d <- readRDS(file.path(ROOT, "data/local_juris/iv_tract_side.rds"))[T == 30]
f <- readRDS(file.path(ROOT, "data/local_juris/district_finance.rds"))[y == 2016 & enr > 0, .(leaid, lspend = log(spend / enr))]
d <- merge(d, f, by = "leaid", all.x = TRUE)
d[, sd_mi := ifelse(high == 1, dist_mi, -dist_mi)]
d <- d[abs(sd_mi) <= MX]
v <- c(med = "effective tax rate (%)", ach = "district test scores (SEDA, SD)", ses = "district SES (SEDA)", perfrl = "district share free/reduced lunch",
       lspend = "district log spending per pupil", linc = "tract log median HH income", lhval = "tract log median home value", sh_ba = "tract share BA+",
       sh_white = "tract share white", sh_own = "tract owner share", yrbuilt = "tract median year built", lpop = "tract log population")
run <- function(k, spec) {
  x <- d[is.finite(get(k))]
  y <- x[[k]]
  if (spec == "A: pair-demeaned") { y <- y - ave(y, x$pair); r <- rdrobust(y, x$sd_mi, c = 0, cluster = x$cty, vce = "hc1") }
  else { Z <- model.matrix(~ pair, x)[, -1, drop = FALSE]; r <- rdrobust(y, x$sd_mi, c = 0, covs = Z, cluster = x$cty, vce = "hc1") }
  data.table(var = k, label = v[k], spec = spec, est = r$coef[1], se_conv = r$se[1], ci_lo = r$ci[3, 1], ci_hi = r$ci[3, 2], p_rob = r$pv[3],
             h = r$bws[1, 1], n_eff = sum(r$N_h), sdy = sd(x[[k]])) }
res <- rbindlist(lapply(c("A: pair-demeaned", "B: pair dummies as covariates"), \(s) rbindlist(lapply(names(v), run, spec = s))))
res[var != "med", p_holm := p.adjust(p_rob, "holm"), by = spec]
res[, `:=`(est_sd = est / sdy, lo_sd = ci_lo / sdy, hi_sd = ci_hi / sdy)]
fwrite(res, file.path(OUT, "c15_border_rd.csv"))
sink(file.path(OUT, "c15_border_rd.txt"), split = TRUE)
cat("Border RD at signed distance 0 (high-tax minus low-tax side). rdrobust local linear, triangular, MSE-optimal h,\n",
    "robust bias-corrected 95% CI and p, SE clustered by county. Tracts within", MX, "mi:", nrow(d), "\n\n")
for (s in unique(res$spec)) { cat("==", s, "\n")
  print(res[spec == s, .(variable = label, est = round(est, 3), `robust 95% CI` = sprintf("[%.3f, %.3f]", ci_lo, ci_hi), p = round(p_rob, 3),
    p_holm = round(p_holm, 3), est_SD = round(est_sd, 2), h_mi = round(h, 1), n_eff)]); cat("\n") }
sink()
res[, label := factor(label, rev(v))]
g <- ggplot(res, aes(est_sd, label, colour = spec)) + geom_vline(xintercept = 0, colour = "grey50") +
  geom_pointrange(aes(xmin = lo_sd, xmax = hi_sd), position = position_dodge(width = .55), size = .25) +
  scale_colour_manual(NULL, values = c("#2a64a8", "#d1772b")) +
  labs(x = "RD estimate at the border, high-tax minus low-tax side (SD units); robust bias-corrected 95% CI, SE by county", y = NULL,
       title = "Border RD on covariates (rdrobust, local linear, MSE-optimal bandwidth)",
       subtitle = "Top row: the tax-rate discontinuity itself. Bandwidths and Holm-adjusted p-values in c15_border_rd.txt.") +
  theme_minimal(base_size = 9) + theme(legend.position = "bottom")
ggsave(file.path(OUT, "figures/c15_border_rd.png"), g, width = 8, height = 6, dpi = 150)
