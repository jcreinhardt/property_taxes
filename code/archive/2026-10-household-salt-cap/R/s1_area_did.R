# s1_area_did.R — Strategy 1: ZIP-level exposure DiD (descriptive benchmark). Pre-specified 2026-10-16.
source("code/archive/2026-10-household-salt-cap/R/00_setup.R"); source("code/archive/2026-10-household-salt-cap/R/01b_load.R")
# --- exposure E_z: 2016 share of returns with SALT > 10k, log-normal within AGI bin (mean from SOI, log-SD from ACS imputation)
soi <- dq(sprintf("SELECT zipcode, agi_stub, returns n1, taxes_paid_returns n_salt, taxes_paid_amount a_salt, agi_for_itemizers FROM read_parquet('%s') WHERE year = 2016", SOI_PQ))
acs <- dq(sprintf("SELECT inctot, salt FROM read_parquet('%s')", ACS_PQ))
acs[, agi_stub := cut(inctot, c(-Inf, 25e3, 50e3, 75e3, 100e3, 200e3, Inf), labels = FALSE)]
sig <- acs[, .(sigma = sd(log(salt))), by = agi_stub]
soi <- merge(soi, sig, by = "agi_stub")
soi[, mean_salt := 1000 * a_salt / pmax(n_salt, 1)]
soi[, p_above := fifelse(n_salt > 0, 1 - pnorm((log(10000) - (log(mean_salt) - sigma^2 / 2)) / sigma), 0)]
Ez <- soi[, .(E_z = sum(p_above * n_salt) / sum(n1), n_returns = sum(n1), mean_agi = sum(agi_for_itemizers) / sum(n1)), by = .(zip5 = sprintf("%05d", zipcode))]
fwrite(sig, file.path(OUT_S, "table_s1_sigma_by_bin.csv"))
# --- ZIP x year outcomes from the base sample
d <- load_panel(2012:2019, "oo")
z <- d[, .(exit_rate = mean(exit, na.rm = TRUE), sale_rate = mean(sale), med_log_price = median(log_price, na.rm = TRUE), n = .N,
           county = as.integer(names(which.max(table(county)))), st = st[1]), by = .(zip5, year)]
z <- merge(z, Ez, by = "zip5")
z <- z[n >= 20]
cat("S1: ZIP-years", nrow(z), " ZIPs", uniqueN(z$zip5), "\n")
z[, lagi := log(mean_agi)]
for (oc in c("exit_rate", "sale_rate", "med_log_price")) {
  r  <- run_es(z, oc, "E_z", "zip5 + st^year", ~county, "S1", "S1_base", weights = ~n, label = sprintf("S1: ZIP %s on exposure E_z", oc))
  ra <- run_es(z, oc, "E_z", "zip5 + st^year", ~county, "S1", "S1_agi", weights = ~n, controls = "i(year, lagi, ref = 2016)", fig = FALSE)
  s <- r$summary; sa <- ra$summary
  add_ledger("S1", sprintf("Parallel trends high vs low exposure ZIPs [%s]", oc), "Joint Wald test of 2012-2015 coefficients",
             sprintf("F=%.2f p=%.3f; post %.4g (%.4g)", s$pre_F, s$pre_p, s$post_coef, s$post_se), if (is.na(s$pre_p)) "inconclusive" else if (s$pre_p >= 0.05) "pass" else "fail")
  add_ledger("S1", sprintf("No other TCJA provision correlated with E_z [%s]", oc), "Add ZIP mean AGI x year",
             sprintf("beta %.4g -> %.4g (%.0f%%)", s$post_coef, sa$post_coef, 100 * (sa$post_coef - s$post_coef) / abs(s$post_coef)), "untestable")
  cat(sprintf("%-14s post=%.4g (%.4g) p_pre=%.3f | with AGI x year %.4g\n", oc, s$post_coef, s$post_se, s$pre_p, sa$post_coef))
}
add_ledger("S1", "No spillovers across ZIPs (SUTVA)", "none", "untestable; movers may relocate to nearby ZIPs", "untestable")
fwrite(Ez, file.path(OUT_S, "table_s1_exposure_by_zip.csv"))
cat("S1 done\n")
