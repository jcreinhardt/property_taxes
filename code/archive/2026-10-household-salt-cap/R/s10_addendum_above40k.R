# s10_addendum_above40k.R — the above24_40k treatment named in the decision log for Strategy 10 (omitted from the s10 loop). SALT_PART=S10b.
source("code/archive/2026-10-household-salt-cap/R/00_setup.R"); source("code/archive/2026-10-household-salt-cap/R/01b_load.R")
d <- load_panel(2012:2025, "oo")[rate_ok == TRUE & !is.na(valuebin)]
b24 <- d[year == 2024 & !is.na(bill_t), .(clip, bill24 = bill_t)]
d <- merge(d, b24, by = "clip"); d[, above24_40k := as.integer(bill24 > 40000)]
cat("parcels with bill24 > 40k:", uniqueN(d[above24_40k == 1, clip]), "\n")
for (oc in c("sale", "log_price", "refi")) {
  r <- tryCatch(run_es(d, oc, "above24_40k", "clip + vb^year + county^year", ~juris, "S10", "S10_above24_40k", years_post = 2025, label = sprintf("S10 reversal: %s on bill24 > $40k", oc)), error = function(e) {cat("error", conditionMessage(e), "\n"); NULL})
  if (!is.null(r)) cat(sprintf("%-10s 2025 effect=%.4g (%.4g) p_pre=%.3f N=%d\n", oc, r$summary$post_coef, r$summary$post_se, r$summary$pre_p, r$N))
}
cat("S10 addendum done\n")
