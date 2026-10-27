# s3_core.R — Strategy 3: bill conditional on home value (core design). Pre-specified in decision_log.md (2026-10-16).
source("code/archive/2026-10-household-salt-cap/R/00_setup.R"); source("code/archive/2026-10-household-salt-cap/R/01b_load.R")
d <- load_panel(2012:2019, "oo")[rate_ok == TRUE & !is.na(valuebin)]
cat("S3 sample:", nrow(d), "parcel-years,", uniqueN(d$clip), "parcels\n")
FE_BASE <- "clip + vb^year + county^year"
FE_AUG  <- "clip + vb^year + county^year + agi_bin^year + mortint_dec^year"
res <- list()
for (tr in c("rate16_pp", "above16")) for (oc in OUTCOMES_ALL) {
  id <- paste0("S3_", ifelse(tr == "rate16_pp", "rate", "above"))
  r <- run_es(d, oc, tr, FE_BASE, ~juris, "S3", id, label = sprintf("S3 base: %s on %s", oc, tr)); res[[paste(id, oc)]] <- r
  ra <- run_es(d, oc, tr, FE_AUG, ~juris, "S3", paste0(id, "_aug"), label = sprintf("S3 augmented: %s on %s", oc, tr)); res[[paste(id, "aug", oc)]] <- ra
  rc <- run_es(d, oc, tr, FE_BASE, ~county, "S3", paste0(id, "_cl_county"), fig = FALSE); 
  # ledger: pre-trends (base), control stability (aug vs base)
  s <- r$summary; sa <- ra$summary
  pre_v <- if (is.na(s$pre_p)) "inconclusive" else if (s$pre_p >= 0.05) "pass" else "fail"
  add_ledger("S3", sprintf("Parallel trends given value bin [%s, %s]", oc, tr), "Joint Wald test of 2012-2015 coefficients",
             sprintf("F=%.2f p=%.3f (df %d); post avg %.4g (SE %.4g)", s$pre_F, s$pre_p, s$pre_df, s$post_coef, s$post_se), pre_v)
  chg <- (sa$post_coef - s$post_coef) / abs(s$post_coef)
  add_ledger("S3", sprintf("Value bins absorb other TCJA provisions [%s, %s]", oc, tr), "Add imputed-AGI bin x year and mortint16 decile x year",
             sprintf("beta %.4g -> %.4g (%.0f%% change)", s$post_coef, sa$post_coef, 100 * chg),
             if (is.na(chg)) "inconclusive" else if (abs(chg) < 0.25) "pass" else "fail")
  cat(sprintf("%-12s %-10s %-9s post=%.4g (%.4g) preF p=%.3f | aug=%.4g\n", oc, tr, id, s$post_coef, s$post_se, s$pre_p, sa$post_coef))
}
# repeat-sales robustness for log_price (parcel FE)
for (tr in c("rate16_pp", "above16")) {
  id <- paste0("S3_", ifelse(tr == "rate16_pp", "rate", "above"), "_rs")
  rs <- tryCatch(run_es(d, "log_price", tr, FE_BASE, ~juris, "S3", id, price_fe = TRUE, fig = FALSE), error = function(e) NULL)
  if (!is.null(rs)) cat("repeat sales", tr, rs$summary$post_coef, rs$summary$post_se, rs$N, "\n")
}
# Diagnostic 4: value16 measured well — subsample with 2014-2016 sales, value bins from the sale price vs from value16
dp <- d[!is.na(price1416) & !is.na(valuebin_price)]
dp[, vb_price := paste(cbsa_use, valuebin_price)]
for (tr in c("rate16_pp", "above16")) for (oc in c("sale", "exit", "refi")) {
  id <- paste0("S3_", ifelse(tr == "rate16_pp", "rate", "above"))
  r1 <- run_es(dp, oc, tr, FE_BASE, ~juris, "S3", paste0(id, "_pricesub"), fig = FALSE)
  r2 <- run_es(dp, oc, tr, "clip + vb_price^year + county^year", ~juris, "S3", paste0(id, "_price"), fig = FALSE)
  add_ledger("S3", sprintf("value16 measured well [%s, %s]", oc, tr), "Replace value16 bins by 2014-16 sale-price bins (same subsample)",
             sprintf("beta value16 %.4g (%.4g) vs price %.4g (%.4g), N=%d", r1$summary$post_coef, r1$summary$post_se, r2$summary$post_coef, r2$summary$post_se, r2$N),
             if (abs(r1$summary$post_coef - r2$summary$post_coef) < 2 * r2$summary$post_se) "pass" else "fail")
}
# Diagnostic 1b: covariate balance across rate16 terciles within value bins
b <- unique(d[, .(clip, vb, rate16, year_built, living_area, lot_sqft, lor16, agi_imp, age16, mortint16, bill16)])
b[, terc := as.integer(cut(frank(rate16, ties.method = "average") / .N, c(0, 1/3, 2/3, 1), labels = FALSE)), by = vb]
bal <- rbindlist(lapply(c("year_built", "living_area", "lot_sqft", "lor16", "agi_imp", "age16", "mortint16"), function(v) {
  bb <- b[!is.na(get(v)) & !is.na(terc)]; sdv <- sd(bb[[v]])
  m <- feols(as.formula(sprintf("%s ~ i(terc, ref = 1) | vb", v)), bb, cluster = ~vb)
  data.table(covariate = v, diff_T3_T1_sd = coef(m)["terc::3"] / sdv, se_sd = se(m)["terc::3"] / sdv, n = nrow(bb))
}))
fwrite(bal, file.path(OUT_S, "table_s3_balance.csv")); print(bal)
add_ledger("S3", "Given value, millage unrelated to post-2018 trends (balance)", "2016 covariates by rate16 tercile within CBSA x value bin (T3-T1, SD units)",
           paste(sprintf("%s=%.2f", bal$covariate, bal$diff_T3_T1_sd), collapse = "; "), if (all(abs(bal$diff_T3_T1_sd) < 0.1)) "pass" else "fail")
add_ledger("S3", "Local public goods differences do not change after 2018", "none", "untestable; noted", "untestable")
# Extended window 2020-2024 (COVID-confounded), sale and price only
de <- load_panel(2012:2024, "oo")[rate_ok == TRUE & !is.na(valuebin)]
for (tr in c("rate16_pp", "above16")) for (oc in c("sale", "log_price", "refi")) {
  id <- paste0("S3_", ifelse(tr == "rate16_pp", "rate", "above"), "_ext")
  run_es(de, oc, tr, FE_BASE, ~juris, "S3", id, years_post = 2020:2024, label = sprintf("S3 extended window (COVID-confounded): %s on %s", oc, tr))
}
cat("S3 done\n")
