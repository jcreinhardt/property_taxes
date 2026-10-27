# s3_alt_incumbents.R — the single labelled alternative to Strategy 3 (decision log 2026-10-16): base spec on incumbents only
# (no arm's-length purchase recorded 2013–2016). Run with SALT_PART=S3alt.
source("code/analysis/salt_cap/R/00_setup.R"); source("code/analysis/salt_cap/R/01b_load.R")
d <- load_panel(2012:2019, "oo")[rate_ok == TRUE & !is.na(valuebin) & (is.na(buy_year) | buy_year < 2013)]
cat("S3 incumbents:", nrow(d), "parcel-years,", uniqueN(d$clip), "parcels\n")
FE_BASE <- "clip + vb^year + county^year"
for (tr in c("rate16_pp", "above16")) for (oc in c("sale", "exit_prim", "refi", "to_rental")) {   # log_price dropped: incumbents have no 2013-16 sales by construction
  id <- paste0("S3_", ifelse(tr == "rate16_pp", "rate", "above"), "_inc")
  r <- run_es(d, oc, tr, FE_BASE, ~juris, "S3", id, label = sprintf("S3 labelled alternative (incumbents, bought before 2013): %s on %s", oc, tr))
  s <- r$summary
  add_ledger("S3", sprintf("Parallel trends, incumbents-only alternative [%s, %s]", oc, tr), "Joint Wald test of 2012-2015 coefficients", pre_text(s), verdict_pre(s$pre_p, s$pre_mean_abs, s$post_coef))
  cat(sprintf("%-10s %-9s post=%.4g (%.4g) p_pre=%.3f mean|pre|=%.3g N=%d\n", oc, tr, s$post_coef, s$post_se, s$pre_p, s$pre_mean_abs, r$N))
}
cat("S3 alternative done\n")
