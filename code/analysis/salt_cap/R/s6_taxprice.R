# s6_taxprice.R — Strategy 6: simulated change in the after-tax price of property tax (TAXSIM 35 via usincometaxes). Pre-specified 2026-10-16.
source("code/analysis/salt_cap/R/00_setup.R"); source("code/analysis/salt_cap/R/01b_load.R")
library(usincometaxes)
P <- dq(sprintf("SELECT clip, st, zip5, agi_imp, agi_bin, bill16, mortint16, married, coowner, base_oo FROM read_parquet('%s') WHERE base_oo AND agi_imp IS NOT NULL", pq("analysis_parcels")))
P <- P[!st %in% c("GU", "VI", "PR")]
P[, mstat := fifelse(married, "married, jointly", "single")]
# TAXSIM inputs: wages = imputed AGI, property tax = bill16, mortgage interest = mortint16; state income tax computed by TAXSIM from state + income.
mk <- function(year, proptax) data.frame(taxsimid = seq_len(nrow(P)), year = year, state = P$st, mstat = P$mstat, pwages = round(P$agi_imp), proptax = round(proptax),
                                         mortgage = round(P$mortint16), depx = 0, page = 45, sage = ifelse(P$mstat == "single", 0, 45))
run_taxsim <- function(df) {
  out <- vector("list", ceiling(nrow(df) / 50000))
  for (i in seq_along(out)) { idx <- ((i - 1) * 50000 + 1):min(i * 50000, nrow(df))
    out[[i]] <- as.data.table(taxsim_calculate_taxes(df[idx, ], marginal_tax_rates = "Wages", return_all_information = TRUE)); cat("  chunk", i, "\n") }
  rbindlist(out)
}
cache <- file.path(PANEL_DIR, "taxsim_s6.parquet")
if (!file.exists(cache)) {
  res <- list()
  for (law in c(2017, 2018)) for (dlt in c(0, 100)) {
    cat("TAXSIM law", law, "delta", dlt, "\n")
    r <- run_taxsim(mk(law, P$bill16 + dlt))
    nm <- names(r); itmcol <- c(grep("^v17_itemized", nm, value = TRUE), grep("itemized", nm, value = TRUE))[1]   # v17 = federal itemised deductions, 0 if standard deduction taken
    amtcol <- c(grep("amt_liab", nm, value = TRUE), grep("amt", nm, value = TRUE))[1]                               # v27 = AMT liability (plain grep("amt") would pick v26_amt_income)
    res[[paste(law, dlt)]] <- r[, .(taxsimid, law = law, dlt = dlt, fiitax, siitax, frate, itemized = if (is.na(itmcol)) NA_real_ else get(itmcol), amt = if (is.na(amtcol)) NA_real_ else get(amtcol))]
  }
  tx <- rbindlist(res)
  con <- dbConnect(duckdb::duckdb()); duckdb_register(con, "tx", tx); dbExecute(con, sprintf("COPY tx TO '%s' (FORMAT parquet)", cache)); dbDisconnect(con, shutdown = TRUE)
} else tx <- dq(sprintf("SELECT * FROM read_parquet('%s')", cache))
# after-tax price of $1 of property tax = 1 + d(fed+state tax)/d(proptax) (finite difference of $100); dprice = price(2018 law) - price(2017 law)
w <- dcast(tx, taxsimid ~ law + dlt, value.var = c("fiitax", "siitax"))
w[, price17 := 1 + ((fiitax_2017_100 + siitax_2017_100) - (fiitax_2017_0 + siitax_2017_0)) / 100]
w[, price18 := 1 + ((fiitax_2018_100 + siitax_2018_100) - (fiitax_2018_0 + siitax_2018_0)) / 100]
w[, dprice := price18 - price17]
itm <- dcast(tx[dlt == 0], taxsimid ~ law, value.var = c("itemized", "amt"))
P <- cbind(P, w[, .(price17, price18, dprice)], itm[, .(item17 = itemized_2017 > 0, item18 = itemized_2018 > 0, amt17 = amt_2017 > 0, amt18 = amt_2018 > 0)])
cat("dprice: mean", mean(P$dprice, na.rm = TRUE), " share > 0.01:", mean(P$dprice > 0.01, na.rm = TRUE), "\n")
fwrite(P[, .(n = .N, mean_dprice = mean(dprice, na.rm = TRUE), p_dprice_pos = mean(dprice > 0.01, na.rm = TRUE), item17 = mean(item17, na.rm = TRUE), item18 = mean(item18, na.rm = TRUE), amt17 = mean(amt17, na.rm = TRUE)), by = agi_bin][order(agi_bin)],
       file.path(OUT_S, "table_s6_dprice_by_agi.csv"))
# ---- validation vs SOI (ZIP and ZIP x AGI bin itemisation shares, 2016 and 2018)
soi <- dq(sprintf("SELECT year, zipcode, agi_stub, returns n1, returns_w_items n_item FROM read_parquet('%s') WHERE year IN (2016, 2018)", SOI_PQ)); soi[, zip5 := sprintf("%05d", zipcode)]
val <- merge(P[!is.na(item17), .(pred17 = mean(item17), pred18 = mean(item18), n = .N), by = .(zip5, agi_bin)],
             dcast(soi[, .(share = n_item / n1), by = .(zip5, agi_stub, year)], zip5 + agi_stub ~ year, value.var = "share"), by.x = c("zip5", "agi_bin"), by.y = c("zip5", "agi_stub"))
setnames(val, c("2016", "2018"), c("soi16", "soi18")); val <- val[n >= 10]
vz <- merge(P[!is.na(item17), .(pred17 = mean(item17), pred18 = mean(item18), n = .N), by = zip5],
            soi[, .(soi16 = sum(n_item[year == 2016]) / sum(n1[year == 2016]), soi18 = sum(n_item[year == 2018]) / sum(n1[year == 2018])), by = zip5], by = "zip5")[n >= 20]
# SOI cells suppressed (returns = 0) or absent in one year give NaN/NA shares: pairwise-complete correlations and means
corr <- data.table(level = c("zip", "zip x agi bin"), cor16 = c(cor(vz$pred17, vz$soi16, use = "complete.obs"), cor(val$pred17, val$soi16, use = "complete.obs")),
                   cor18 = c(cor(vz$pred18, vz$soi18, use = "complete.obs"), cor(val$pred18, val$soi18, use = "complete.obs")),
                   mean_err16 = c(mean(vz$pred17 - vz$soi16, na.rm = TRUE), mean(val$pred17 - val$soi16, na.rm = TRUE)), mean_err18 = c(mean(vz$pred18 - vz$soi18, na.rm = TRUE), mean(val$pred18 - val$soi18, na.rm = TRUE)),
                   n_cells16 = c(sum(is.finite(vz$soi16)), sum(is.finite(val$soi16))), n_cells18 = c(sum(is.finite(vz$soi18)), sum(is.finite(val$soi18))))
fwrite(corr, file.path(OUT_S, "table_s6_validation.csv")); print(corr)
weak <- is.na(corr$cor16[1]) || corr$cor16[1] < 0.6
add_ledger("S6", "TAXSIM simulation reproduces itemisation (validation vs SOI)", "ZIP itemisation share correlation 2016/2018, mean error (homeowners vs all filers)",
           sprintf("cor16=%.2f cor18=%.2f, mean err %.3f/%.3f", corr$cor16[1], corr$cor18[1], corr$mean_err16[1], corr$mean_err18[1]), if (weak) "fail" else "pass")
# ---- estimation: S3 spec with dprice x year, flexible controls for every TAXSIM input x year
d <- load_panel(2012:2019, "oo")[rate_ok == TRUE & !is.na(valuebin)]
d <- merge(d, P[, .(clip, dprice, price17, price18)], by = "clip")
d <- d[!is.na(dprice)]
d[, bill_dec := as.integer(cut(bill16, unique(quantile(bill16, 0:10 / 10)), include.lowest = TRUE, labels = FALSE))]
d[, bill_v20 := as.integer(cut(bill16, unique(quantile(bill16, 0:20 / 20)), include.lowest = TRUE, labels = FALSE))]
d[, agi_v20 := as.integer(cut(agi_imp, unique(quantile(agi_imp, 0:20 / 20)), include.lowest = TRUE, labels = FALSE))]
d[, mort_v20 := fifelse(mortint16 <= 0, 0L, as.integer(cut(mortint16, unique(quantile(mortint16[mortint16 > 0], 0:20 / 20)), include.lowest = TRUE, labels = FALSE)))]
FE <- list(S6_base = "clip + vb^year + county^year + agi_bin^year + mortint_dec^year + married^year + bill_dec^year",
           S6_coarse = "clip + vb^year + county^year + agi_bin^year + married^year",
           S6_fine = "clip + vb^year + county^year + agi_v20^year + mort_v20^year + married^year + bill_v20^year + st^agi_bin^year")
for (oc in c("sale", "log_price", "exit_prim", "refi")) for (id in names(FE)) {
  r <- run_es(d, oc, "dprice", FE[[id]], ~juris, "S6", id, label = sprintf("S6: %s on simulated tax-price change (%s)", oc, id), fig = id == "S6_base")
  cat(sprintf("%-10s %-9s post=%.4g (%.4g) p_pre=%.3f\n", oc, id, r$summary$post_coef, r$summary$post_se, r$summary$pre_p))
  if (id == "S6_base") add_ledger("S6", sprintf("Given controls, dprice affects outcomes only through the tax price [%s]", oc), "Pre-trends of dprice x year",
                                  sprintf("F=%.2f p=%.3f; post %.4g (%.4g)", r$summary$pre_F, r$summary$pre_p, r$summary$post_coef, r$summary$post_se), verdict_pre(r$summary$pre_p, r$summary$pre_mean_abs, r$summary$post_coef))
}
est <- fread(if (nzchar(Sys.getenv("SALT_PART"))) part_file("estimates") else file.path(OUT, "estimates.csv"))[strategy == "S6" & spec_id %in% names(FE)]   # own part file under SALT_PART (merged estimates.csv may be stale)
for (oc in unique(est$outcome)) { e <- est[outcome == oc]; add_ledger("S6", sprintf("Functional form of controls [%s]", oc), "beta under coarse / base / fine controls",
  paste(sprintf("%s=%.4g", e$spec_id, e$coef), collapse = "; "), if (diff(range(e$coef)) < 2 * max(e$se)) "pass" else "fail") }
add_ledger("S6", "Imputation error unrelated to potential outcomes", "none", "untestable", "untestable")
# ---- decomposition: cap vs larger standard deduction. TAXSIM has no "2018 without cap" switch; the cap effect is isolated by running 2018 law with
# proptax capped at the household's own binding amount: price(2018, no cap) is approximated as 1 - mtr18 x 1[itemises under 2018 law with uncapped SALT],
# where "itemises with uncapped SALT" = (2017-law itemised deductions > 2018 standard deduction) and mtr18 = federal+state marginal rate under 2018 law.
w2 <- dcast(tx[dlt == 0], taxsimid ~ law, value.var = c("frate", "itemized"))
w2[, std18 := fifelse(P$mstat == "married, jointly", 24000, 12000)]
w2[, price18_nocap := 1 - (frate_2018 / 100) * (itemized_2017 > std18)]
P[, price18_nocap := w2$price18_nocap]
P[, d_std := price18_nocap - price17]; P[, d_cap := price18 - price18_nocap]
dec <- P[, .(mean_dprice = mean(dprice, na.rm = TRUE), from_std_deduction = mean(d_std, na.rm = TRUE), from_cap = mean(d_cap, na.rm = TRUE)), by = agi_bin][order(agi_bin)]
fwrite(dec, file.path(OUT_S, "table_s6_decomposition.csv")); print(dec)
d <- merge(d, P[, .(clip, d_std, d_cap)], by = "clip")
for (oc in c("sale", "log_price")) {
  dd <- d[!is.na(get(oc)) & !is.na(d_cap)]; if (oc == "log_price") dd <- dd[sale == 1]
  fe <- FE$S6_base; if (oc == "log_price") fe <- sub("clip \\+ ", "", fe)
  ctrl <- if (oc == "log_price") paste0(" + ", HEDONIC) else ""
  m <- feols(as.formula(sprintf("%s ~ i(year, d_cap, ref = 2016) + i(year, d_std, ref = 2016)%s | %s", oc, ctrl, fe)), dd, cluster = ~juris, notes = FALSE, mem.clean = TRUE)
  nm <- names(coef(m)); yr <- as.integer(sub(".*year::(\\d{4}).*", "\\1", nm))
  for (g in c("d_cap", "d_std")) { w <- as.numeric(grepl(paste0(":", g, "$"), nm) & yr %in% 2018:2019) / 2
    add_estimate("S6", oc, sum(w * coef(m)), sqrt(as.numeric(t(w) %*% vcov(m) %*% w)), nobs(m), uniqueN(dd$juris), paste0("S6_", g)) }
}
cat("S6 done\n")
