# s10_reversal.R — Strategy 10: 2025 reversal ($40k cap, 30% phase-down above $500k MAGI). Parameters verified in decision_log.md.
source("code/analysis/salt_cap/R/00_setup.R"); source("code/analysis/salt_cap/R/01b_load.R")
library(rdrobust)
# full post-year check: deeds coverage in 2025 vs 2024
cov <- dq(sprintf("SELECT y, sum(sale) sales FROM read_parquet('%s') WHERE y IN (2023, 2024, 2025, 2026) GROUP BY 1 ORDER BY 1", pq("analysis_panel"))); print(cov)
ok <- cov[y == 2025, sales] >= 0.8 * cov[y == 2024, sales]
add_ledger("S10", "At least one full post-year of outcomes exists", "2025 arm's-length sales vs 2024 in the sample", sprintf("2024: %d, 2025: %d", cov[y == 2024, sales], cov[y == 2025, sales]), if (ok) "pass" else "fail")
if (!ok) { cat("S10: 2025 incomplete; pre-analysis plan only\n"); quit(save = "no") }
d <- load_panel(2012:2025, "oo")[rate_ok == TRUE & !is.na(valuebin)]
b24 <- d[year == 2024 & !is.na(bill_t), .(clip, bill24 = bill_t)]
d <- merge(d, b24, by = "clip", all.x = TRUE)
d[, above24_40k := as.integer(bill24 > 40000)]; d[, mid24 := as.integer(bill24 > 10000 & bill24 <= 40000)]   # newly deductible range
d[, hi_agi := as.integer(agi_imp >= 500000)]
FE <- "clip + vb^year + county^year"
for (tr in c("rate16_pp", "above16", "mid24")) for (oc in c("sale", "log_price", "refi", "cashout")) {
  r <- tryCatch(run_es(d, oc, tr, FE, ~juris, "S10", paste0("S10_", tr), years_post = 2025, label = sprintf("S10 reversal (post = 2025): %s on %s", oc, tr), fig = tr != "above16"), error = function(e) NULL)
  if (!is.null(r)) cat(sprintf("%-10s %-10s 2025 effect=%.4g (%.4g) p_pre=%.3f N=%d\n", oc, tr, r$summary$post_coef, r$summary$post_se, r$summary$pre_p, r$N))
}
add_ledger("S10", "Responses symmetric (or asymmetry is the object)", "Compare S10_rate16_pp 2025 coefficient with S3 2018-19 coefficient", "see estimates S3_rate vs S10_rate16_pp", "untestable")
add_ledger("S10", "No other 2025 shock correlated with exposure", "none", "untestable (rates, insurance costs rose with value in 2023-25)", "untestable")
# S7 at $40k in no-income-tax states, 2025 vs 2022-2024
NOTAX <- c("TX", "FL", "WA", "NV", "SD", "WY")
k <- d[st %in% NOTAX & !is.na(bill24) & bill24 > 20000 & bill24 < 60000 & year %in% 2022:2025]
k[, per := fifelse(year == 2025, "post", "pre")]
cat("S10 kink sample at 40k:", uniqueN(k$clip), "parcels\n")
for (oc in c("sale", "log_price")) {
  res <- rbindlist(lapply(c("pre", "post"), function(p) { x <- k[per == p & !is.na(get(oc))]; if (oc == "log_price") x <- x[sale == 1]; if (nrow(x) < 300) return(NULL)
    r <- tryCatch(rdrobust(x[[oc]], x$bill24, c = 40000, deriv = 1, cluster = x$juris), error = function(e) NULL); if (is.null(r)) NULL else data.table(per = p, est = r$coef[1], se = r$se[3], n = sum(r$N_h)) }))
  if (nrow(res) == 2) add_estimate("S10", oc, 1000 * (res[per == "post", est] - res[per == "pre", est]), 1000 * sqrt(sum(res$se^2)), sum(res$n), NA, "S10_kink40k")
  print(res)
}
# MAGI phase-down threshold: imputed AGI >= 500k vs 400-500k within the above-40k group (descriptive)
ph <- d[year %in% 2023:2025 & bill24 > 10000 & agi_imp >= 400000, .(sale = mean(sale), n = .N), by = .(year, hi_agi)][order(hi_agi, year)]
fwrite(ph, file.path(OUT_S, "table_s10_phase_down.csv")); print(ph)
cat("S10 done\n")
