# s13_capitalization.R — Section 13: capitalisation and incumbents vs new buyers. Uses S3 price estimates.
source("code/archive/2026-10-household-salt-cap/R/00_setup.R"); source("code/archive/2026-10-household-salt-cap/R/01b_load.R")
est <- fread(file.path(OUT, "estimates.csv"))
pr <- est[strategy == "S3" & outcome == "log_price" & spec_id %in% c("S3_rate", "S3_above")]
# implied PV: an owner with bill B and marginal tax rate tau loses tau*min(B, B-10k)... use the S6 mean dprice when available; here the
# raw conversion: beta (log price per pp of rate16) vs the PV of the lost deduction = (rate16 * V * tau_eff) / r. With tau_eff = 0.24 (typical
# marginal rate of affected itemisers) and r in {4%, 7%}, a 1 pp higher rate raises the annual after-tax cost by 0.01 * V * tau_eff, i.e.
# a log-price change of -0.01 * tau_eff / r = -0.06 (4%) to -0.034 (7%) per pp if fully capitalised for a household losing the deduction.
pv <- data.table(r = c(0.04, 0.05, 0.07), full_cap_per_pp = -0.01 * 0.24 / c(0.04, 0.05, 0.07))
pv[, beta_rate := pr[spec_id == "S3_rate", coef][1]]; pv[, se_rate := pr[spec_id == "S3_rate", se][1]]
pv[, share_capitalised := beta_rate / full_cap_per_pp]
fwrite(pv, file.path(OUT_S, "table_s13_pv_comparison.csv")); print(pv)
# incumbents vs new buyers: owners who bought <= 2015 vs parcels bought 2018+ (post-cap buyers); outcome sale/refi event study after purchase
d <- load_panel(2012:2024, "oo")[rate_ok == TRUE & !is.na(valuebin)]
d[, inc := fifelse(!is.na(buy_year) & buy_year <= 2015, "incumbent", fifelse(is.na(buy_year), "unknown", "other"))]
# post-2018 buyers: parcels with an arm's-length sale in 2018-2019; follow 2020-2024 outcomes vs incumbents
s1819 <- d[year %in% 2018:2019 & sale == 1, unique(clip)]
d[, grp := fifelse(clip %in% s1819, "bought_2018_19", fifelse(inc == "incumbent", "incumbent", "other"))]
dd <- d[grp != "other" & year %in% c(2012:2016, 2020:2024)]
dd[, grp_b := as.integer(grp == "bought_2018_19")]; dd[, rate_b := rate16_pp * grp_b]
for (oc in c("sale", "refi")) {
  x <- dd[!is.na(get(oc))]
  m <- feols(as.formula(sprintf("%s ~ i(year, rate16_pp, ref = 2016) + i(year, rate_b, ref = 2016) | clip + vb^grp_b^year + county^grp_b^year", oc)), x, cluster = ~juris, notes = FALSE, mem.clean = TRUE)
  nm <- names(coef(m)); yr <- as.integer(sub(".*year::(\\d{4}).*", "\\1", nm))
  for (g in c("rate16_pp", "rate_b")) { w <- as.numeric(grepl(paste0(":", g, "$"), nm) & yr %in% 2020:2024) / 5
    add_estimate("S13", oc, sum(w * coef(m)), sqrt(as.numeric(t(w) %*% vcov(m) %*% w)), nobs(m), uniqueN(x$juris), paste0("S13_", if (g == "rate16_pp") "incumbent" else "buyer_diff")) }
}
add_ledger("S13", "Post-2018 buyers paid a capitalised price (descriptive; buyers select into bills)", "Incumbent vs 2018-19 buyer response 2020-24", "see S13 rows", "untestable")
cat("S13 done\n")
