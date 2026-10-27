# s2_hh_did.R — Strategy 2: household DiD on the 2016 bill (county x year FE only). Pre-specified 2026-10-16.
source("code/analysis/salt_cap/R/00_setup.R"); source("code/analysis/salt_cap/R/01b_load.R")
d <- load_panel(2012:2019, "oo")[rate_ok == TRUE]
cat("S2 sample:", nrow(d), "\n")
for (tr in c("above16", "lbill16")) for (oc in OUTCOMES_ALL) {
  id <- paste0("S2_", ifelse(tr == "above16", "above", "logbill"))
  r  <- run_es(d, oc, tr, "clip + county^year", ~juris, "S2", id, label = sprintf("S2: %s on %s", oc, tr))
  rv <- run_es(d, oc, tr, "clip + county^year", ~juris, "S2", paste0(id, "_val"), controls = "i(year, lv16, ref = 2016)", fig = FALSE)
  run_es(d, oc, tr, "clip + county^year", ~county, "S2", paste0(id, "_cl_county"), fig = FALSE)
  s <- r$summary; sv <- rv$summary; chg <- 100 * (sv$post_coef - s$post_coef) / abs(s$post_coef)
  add_ledger("S2", sprintf("Parallel trends within county [%s, %s]", oc, tr), "Joint Wald test of 2012-2015 coefficients",
             sprintf("F=%.2f p=%.3f; post %.4g (%.4g)", s$pre_F, s$pre_p, s$post_coef, s$post_se), if (is.na(s$pre_p)) "inconclusive" else if (s$pre_p >= 0.05) "pass" else "fail")
  add_ledger("S2", sprintf("Bill does not proxy for wealth [%s, %s]", oc, tr), "Add log(value16) x year; % change in beta",
             sprintf("beta %.4g -> %.4g (%.0f%%)", s$post_coef, sv$post_coef, chg), if (is.na(chg)) "inconclusive" else if (abs(chg) < 25) "pass" else "fail")
  cat(sprintf("%-12s %-8s post=%.4g (%.4g) p_pre=%.3f  with value x year: %.4g (%.0f%%)\n", oc, tr, s$post_coef, s$post_se, s$pre_p, sv$post_coef, chg))
}
cat("S2 done\n")
