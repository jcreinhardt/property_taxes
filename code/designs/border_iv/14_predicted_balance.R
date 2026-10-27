# Formerly code/analysis/salt_local/c5_iv_predicted_balance.R (moved in the 2026-10-27 restructuring).
# Predicted-endogenous balance test for the border instrument. Endogenous variables: x_cheap (a >= 0.5 pp cheaper cross-state
# tract within 30 min) and tax_gap (own rate - p10 of reachable tracts, pp). Step 1: regress each on the (supposedly exogenous)
# controls with pair FE, all tracts; the prediction is the covariate index X'b (FE excluded). Two control sets: without and
# with log median home value (which may itself respond to the outside option). Step 2: regress the predicted value on the
# instrument: (a) directional near x high (pair x side + pair x distance-bin FE), (b) high side only and low side only (near vs
# far, pair FE). Compare with the actual first stage. Step 3: binned plot of predicted and actual vs signed distance.
suppressPackageStartupMessages({library(data.table); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_iv"); invisible(out_dir("designs/border_iv/figures"))
d <- readRDS(file.path(DATA, "built/geo/iv_tract_side.rds"))[T == 30]
d[, sd_mi := ifelse(high == 1, dist_mi, -dist_mi)]
X0 <- c("ach", "ses", "linc", "sh_ba", "sh_white", "sh_own", "yrbuilt", "lpop")
sets <- list(`controls` = X0, `controls + log home value` = c(X0, "lhval"))
res <- list(); bins <- list()
for (en in c("x_cheap", "tax_gap")) for (sn in names(sets)) {
  X <- sets[[sn]]; dd <- d[complete.cases(d[, c(en, X), with = FALSE])]
  m1 <- feols(as.formula(sprintf("%s ~ %s | pair", en, paste(X, collapse = " + "))), dd)
  dd[, phat := as.vector(as.matrix(dd[, X, with = FALSE]) %*% coef(m1)[X])]
  r2w <- fitstat(m1, "wr2")[[1]]
  sdy <- sd(dd[[en]])
  run <- function(f, x, lab) { m <- feols(f, x, cluster = ~cty); data.table(spec = lab, est = coef(m)[1], se = se(m)[1]) }
  out <- rbind(
    run(phat ~ near:high | pair^high + pair^dbin, dd, "predicted: near x high (directional)"),
    run(as.formula(sprintf("%s ~ near:high | pair^high + pair^dbin", en)), dd, "actual: near x high (directional)"),
    run(phat ~ near | pair, dd[high == 1], "predicted: near vs far, high side only"),
    run(as.formula(sprintf("%s ~ near | pair", en)), dd[high == 1], "actual: near vs far, high side only"),
    run(phat ~ near | pair, dd[high == 0], "predicted: near vs far, low side only"),
    run(as.formula(sprintf("%s ~ near | pair", en)), dd[high == 0], "actual: near vs far, low side only"))
  out[, `:=`(endog = en, controls = sn, within_r2 = r2w, n = nrow(dd), est_sd = est / sdy, se_sd = se / sdy)]
  res[[length(res) + 1]] <- out
  w <- dd[abs(sd_mi) <= 50][, bin := floor(sd_mi / 5) * 5 + 2.5]
  w[, `:=`(pr = phat - mean(phat), ac = get(en) - mean(get(en))), by = pair]
  bins[[length(bins) + 1]] <- rbind(w[, .(m = mean(pr), se = sd(pr) / sqrt(.N), what = "predicted from controls"), by = bin],
                                    w[, .(m = mean(ac), se = sd(ac) / sqrt(.N), what = "actual"), by = bin])[, `:=`(endog = en, controls = sn)]
}
res <- rbindlist(res); bins <- rbindlist(bins)
fwrite(res, file.path(OUT, "c5_iv_predicted_balance.csv"))
sink(file.path(OUT, "c5_iv_predicted_balance.txt"))
cat("Predicted-endogenous balance test. Step 1 within-R2 of the endogenous variable on controls (pair FE):\n")
print(unique(res[, .(endog, controls, within_r2 = round(within_r2, 3), n)]))
cat("\nStep 2: coefficients (SE), level and SD units of the endogenous variable; SE by county\n")
print(res[, .(endog, controls, spec, est = round(est, 4), se = round(se, 4), est_sd = round(est_sd, 3), se_sd = round(se_sd, 3))], nrows = 100)
sink(); cat(readLines(file.path(OUT, "c5_iv_predicted_balance.txt")), sep = "\n")
bins[, panel := sprintf("%s | %s", endog, controls)]
g <- ggplot(bins, aes(bin, m, colour = what)) + geom_hline(yintercept = 0, colour = "grey70") + geom_vline(xintercept = 0) +
  annotate("rect", xmin = -10, xmax = 10, ymin = -Inf, ymax = Inf, alpha = .08, fill = "red") +
  geom_pointrange(aes(ymin = m - 1.96 * se, ymax = m + 1.96 * se), size = .15) + geom_line(aes(group = interaction(what, bin > 0)), linewidth = .3) +
  facet_wrap(~ panel, scales = "free_y", ncol = 2) + scale_x_continuous(breaks = seq(-50, 50, 10)) +
  scale_colour_manual(values = c(actual = "#b2182b", `predicted from controls` = "#2166ac")) +
  labs(x = "signed distance to nearest state border (miles): low-tax side < 0 < high-tax side", y = "demeaned within border pair", colour = NULL,
       title = "Predicted vs actual access to a cheaper cross-border tract, by signed distance",
       subtitle = "prediction = controls' index from a pair-FE regression of the endogenous variable on SEDA achievement/SES and ACS covariates; 5-mi bins, 95% CI") +
  theme_minimal(base_size = 9) + theme(legend.position = "bottom")
ggsave(file.path(OUT, "figures/c5_iv_predicted_balance.png"), g, width = 11, height = 7.5, dpi = 150)
