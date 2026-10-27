# Nonparametric version of the predicted-endogenous balance test (c5). Target: endogenous variable demeaned within border
# pair (the variation the pair-FE first stage uses). Features: the controls, both raw and demeaned within pair; no location
# or distance information. Models: linear (OLS), GAM (mgcv, thin-plate smooth per control), random forest (ranger, 500
# trees). All predictions cross-fitted: 5 folds by county (a tract is never predicted by a model trained on its own county).
# Out-of-sample R2 on the demeaned target, then step 2 as in c5 (prediction on near x high etc., SE by county).
suppressPackageStartupMessages({library(data.table); library(fixest); library(ranger); library(mgcv); library(ggplot2)})
source("code/R/paths.R"); OUT <- file.path(ROOT, "output/analysis/salt_local"); set.seed(7)
d0 <- readRDS(file.path(ROOT, "data/local_juris/iv_tract_side.rds"))[T == 30]
d0[, sd_mi := ifelse(high == 1, dist_mi, -dist_mi)]
X0 <- c("ach", "ses", "linc", "sh_ba", "sh_white", "sh_own", "yrbuilt", "lpop")
sets <- list(controls = X0, `controls + log home value` = c(X0, "lhval"))
r2 <- function(y, p) 1 - sum((y - p)^2) / sum((y - mean(y))^2)
res <- list(); fitq <- list(); bins <- list()
for (sn in names(sets)) for (en in c("x_cheap", "tax_gap")) {
  X <- sets[[sn]]; d <- d0[complete.cases(d0[, c(en, X), with = FALSE])]
  d[, yt := get(en) - mean(get(en)), by = pair]
  for (v in X) d[, paste0(v, "_w") := get(v) - mean(get(v)), by = pair]
  feats <- c(X, paste0(X, "_w"))
  cts <- unique(d$cty); fold <- setNames(sample(rep(1:5, length.out = length(cts))), cts); d[, f := fold[cty]]
  for (mname in c("linear", "gam", "forest")) d[, paste0("p_", mname) := NA_real_]
  for (k in 1:5) {
    tr <- d[f != k]; te <- d$f == k
    lm1 <- lm(as.formula(paste("yt ~", paste(feats, collapse = " + "))), tr)
    d[te, p_linear := predict(lm1, d[te])]
    gm <- bam(as.formula(paste("yt ~", paste(sprintf("s(%s, k = 8)", feats), collapse = " + "))), data = tr, discrete = TRUE, nthreads = 8)
    d[te, p_gam := predict(gm, d[te])]
    rf <- ranger(x = tr[, ..feats], y = tr$yt, num.trees = 500, min.node.size = 20, num.threads = 8)
    d[te, p_forest := predict(rf, d[te, ..feats])$predictions]
  }
  sdy <- sd(d[[en]])
  for (mname in c("linear", "gam", "forest")) {
    pv <- d[[paste0("p_", mname)]]
    fitq[[length(fitq) + 1]] <- data.table(endog = en, controls = sn, model = mname, oos_r2 = r2(d$yt, pv), n = nrow(d))
    d[, phat := pv]
    run <- function(f, x, lab) { m <- feols(f, x, cluster = ~cty); data.table(spec = lab, est = coef(m)[1], se = se(m)[1]) }
    out <- rbind(run(phat ~ near:high | pair^high + pair^dbin, d, "near x high (directional)"),
                 run(phat ~ near | pair, d[high == 1], "near vs far, high side only"),
                 run(phat ~ near | pair, d[high == 0], "near vs far, low side only"))
    act <- rbind(run(as.formula(sprintf("%s ~ near:high | pair^high + pair^dbin", en)), d, "near x high (directional)"),
                 run(as.formula(sprintf("%s ~ near | pair", en)), d[high == 1], "near vs far, high side only"),
                 run(as.formula(sprintf("%s ~ near | pair", en)), d[high == 0], "near vs far, low side only"))
    out[, `:=`(actual = act$est, share_of_actual = est / act$est, endog = en, controls = sn, model = mname, est_sd = est / sdy, se_sd = se / sdy)]
    res[[length(res) + 1]] <- out
    w <- d[abs(sd_mi) <= 50][, bin := floor(sd_mi / 5) * 5 + 2.5][, pr := phat - mean(phat), by = pair]
    bins[[length(bins) + 1]] <- w[, .(m = mean(pr), se = sd(pr) / sqrt(.N)), by = bin][, `:=`(endog = en, controls = sn, model = mname)]
  }
  if (en == "tax_gap" && sn == "controls") { rfa <- ranger(x = d[, ..feats], y = d$yt, num.trees = 500, min.node.size = 20, importance = "permutation", num.threads = 8)
    imp <- sort(rfa$variable.importance, decreasing = TRUE) }
}
res <- rbindlist(res); fitq <- rbindlist(fitq); bins <- rbindlist(bins)
fwrite(res, file.path(OUT, "c6_iv_predicted_balance_np.csv")); fwrite(fitq, file.path(OUT, "c6_iv_predicted_fit.csv"))
sink(file.path(OUT, "c6_iv_predicted_balance_np.txt"))
cat("Out-of-sample R2 (5-fold, folds by county) of the within-pair endogenous variable on controls:\n")
print(dcast(fitq, endog + controls ~ model, value.var = "oos_r2")[, lapply(.SD, \(v) if (is.numeric(v)) round(v, 3) else v)])
cat("\nStep 2: predicted endogenous on the instrument (SE by county); actual first stage and predicted/actual ratio\n")
print(res[, .(endog, controls, model, spec, est = round(est, 4), se = round(se, 4), actual = round(actual, 3), share = round(share_of_actual, 3))], nrows = 200)
cat("\nRandom-forest permutation importance (tax_gap, controls):\n"); print(round(imp, 5))
sink(); cat(readLines(file.path(OUT, "c6_iv_predicted_balance_np.txt")), sep = "\n")
bins[, panel := sprintf("%s | %s", endog, controls)]
g <- ggplot(bins, aes(bin, m, colour = model)) + geom_hline(yintercept = 0, colour = "grey70") + geom_vline(xintercept = 0) +
  annotate("rect", xmin = -10, xmax = 10, ymin = -Inf, ymax = Inf, alpha = .08, fill = "red") +
  geom_pointrange(aes(ymin = m - 1.96 * se, ymax = m + 1.96 * se), size = .12, position = position_dodge(1.5)) +
  facet_wrap(~ panel, scales = "free_y", ncol = 2) + scale_x_continuous(breaks = seq(-50, 50, 10)) +
  labs(x = "signed distance to nearest state border (miles): low-tax side < 0 < high-tax side", y = "cross-fitted prediction, demeaned within pair", colour = NULL,
       title = "Access to a cheaper cross-border tract predicted from controls (cross-fitted linear, GAM, random forest)",
       subtitle = "5-mi bins, 95% CI; compare with the actual jump of +0.4 (x_cheap) / +0.3 pp (tax_gap) at 0-5 mi on the high side") +
  theme_minimal(base_size = 9) + theme(legend.position = "bottom")
ggsave(file.path(OUT, "figures/c6_iv_predicted_balance_np.png"), g, width = 11, height = 7.5, dpi = 150)
