# s6_decomp_fix.R — corrected Strategy 6 decomposition (decision log 2026-10-16): the no-cap 2018 counterfactual keeps TAXSIM's state
# component (finite difference of siitax) and removes only the federal cap: price18_nocap = 1 + d(siitax18)/100 - mtr_fed18 x 1[itemises with uncapped SALT].
source("code/analysis/salt_cap/R/00_setup.R"); source("code/analysis/salt_cap/R/01b_load.R")
P <- dq(sprintf("SELECT clip, st, agi_bin, bill16, mortint16, married FROM read_parquet('%s') WHERE base_oo AND agi_imp IS NOT NULL", pq("analysis_parcels")))[!st %in% c("GU", "VI", "PR")]
tx <- dq(sprintf("SELECT * FROM read_parquet('%s')", file.path(PANEL_DIR, "taxsim_s6.parquet")))
stopifnot(nrow(P) == max(tx$taxsimid))
w <- dcast(tx, taxsimid ~ law + dlt, value.var = c("fiitax", "siitax", "frate", "itemized"))
w[, price17 := 1 + ((fiitax_2017_100 + siitax_2017_100) - (fiitax_2017_0 + siitax_2017_0)) / 100]
w[, price18 := 1 + ((fiitax_2018_100 + siitax_2018_100) - (fiitax_2018_0 + siitax_2018_0)) / 100]
w[, state18 := (siitax_2018_100 - siitax_2018_0) / 100]
w[, std18 := fifelse(P$married, 24000, 12000)]
w[, price18_nocap := 1 + state18 - (frate_2018_0 / 100) * (itemized_2017_0 > std18)]
w[, `:=`(dprice = price18 - price17, d_std = price18_nocap - price17, d_cap = price18 - price18_nocap)]
P <- cbind(P, w[, .(dprice, d_std, d_cap)])
dec <- P[, .(n = .N, mean_dprice = mean(dprice, na.rm = TRUE), from_std_deduction = mean(d_std, na.rm = TRUE), from_cap = mean(d_cap, na.rm = TRUE)), by = agi_bin][order(agi_bin)]
fwrite(dec, file.path(OUT_S, "table_s6_decomposition.csv")); print(dec)
d <- load_panel(2012:2019, "oo")[rate_ok == TRUE & !is.na(valuebin)]
d <- merge(d, P[, .(clip, d_std, d_cap)], by = "clip")[!is.na(d_cap)]
d[, bill_dec := as.integer(cut(bill16, unique(quantile(bill16, 0:10 / 10)), include.lowest = TRUE, labels = FALSE))]
FE <- "clip + vb^year + county^year + agi_bin^year + mortint_dec^year + married^year + bill_dec^year"
for (oc in c("sale", "log_price")) {
  dd <- d[!is.na(get(oc))]; if (oc == "log_price") dd <- dd[sale == 1]
  fe <- if (oc == "log_price") sub("clip \\+ ", "", FE) else FE; ctrl <- if (oc == "log_price") paste0(" + ", HEDONIC) else ""
  m <- feols(as.formula(sprintf("%s ~ i(year, d_cap, ref = 2016) + i(year, d_std, ref = 2016)%s | %s", oc, ctrl, fe)), dd, cluster = ~juris, notes = FALSE, mem.clean = TRUE)
  nm <- names(coef(m)); yr <- suppressWarnings(as.integer(sub(".*year::(\\d{4}).*", "\\1", nm)))
  for (g in c("d_cap", "d_std")) { wv <- as.numeric(grepl(paste0(":", g, "$"), nm) & yr %in% 2018:2019) / 2
    add_estimate("S6", oc, sum(wv * coef(m)), sqrt(as.numeric(t(wv) %*% vcov(m) %*% wv)), nobs(m), uniqueN(dd$juris), paste0("S6_", g))
    cat(oc, g, sum(wv * coef(m)), sqrt(as.numeric(t(wv) %*% vcov(m) %*% wv)), "\n") }
}
cat("S6 decomposition fix done\n")
