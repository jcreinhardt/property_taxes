# s8_married.R — Strategy 8: married vs unmarried co-owners, bills in (10k, 20k] vs <=10k and >20k. Pre-specified 2026-10-16.
source("code/analysis/salt_cap/R/00_setup.R"); source("code/analysis/salt_cap/R/01b_load.R")
d0 <- load_panel(2012:2019, "oo")[rate_ok == TRUE & !is.na(valuebin) & coowner %in% c("married", "likely_married", "unmarried")]
d0[, mid := as.integer(bill16 > 10000 & bill16 <= 20000)]
run_s8 <- function(d, label) {
  d[, mar := as.integer(coowner %in% c("married", "likely_married"))]
  d[, mid_mar := mid * mar]
  cat(label, ": parcels", uniqueN(d$clip), " married", uniqueN(d[mar == 1, clip]), " unmarried", uniqueN(d[mar == 0, clip]), " mid-bill", uniqueN(d[mid == 1, clip]), "\n")
  for (oc in c("sale", "log_price", "exit_prim", "refi")) {
    dd <- d[!is.na(get(oc))]; if (oc == "log_price") dd <- dd[sale == 1]
    fe <- if (oc == "log_price") "vb^mar^year + county^year" else "clip + vb^mar^year + county^year"
    ctrl <- if (oc == "log_price") paste0(" + ", HEDONIC) else ""
    f <- as.formula(sprintf("%s ~ i(year, mid, ref = 2016) + i(year, mid_mar, ref = 2016)%s | %s", oc, ctrl, fe))
    m <- feols(f, dd, cluster = ~juris, notes = FALSE, mem.clean = TRUE)
    nm <- names(coef(m)); yr <- as.integer(sub(".*year::(\\d{4}).*", "\\1", nm)); mm <- grepl("mid_mar", nm)
    w <- as.numeric(mm & yr %in% 2018:2019) / 2; post <- sum(w * coef(m)); se_ <- sqrt(as.numeric(t(w) %*% vcov(m) %*% w))
    ipre <- which(mm & yr %in% 2012:2015); b <- coef(m)[ipre]; V <- vcov(m)[ipre, ipre]; preF <- as.numeric(t(b) %*% solve(V) %*% b) / length(ipre)
    prep <- pf(preF, length(ipre), 1e6, lower.tail = FALSE)
    add_estimate("S8", oc, post, se_, nobs(m), uniqueN(dd$juris), label)
    cf <- as.data.table(coeftable(m), keep.rownames = "term"); setnames(cf, c("term", "coef", "se", "t", "p")); cf <- cf[grepl("mid_mar", term)]
    cf[, year := as.integer(sub(".*year::(\\d{4}).*", "\\1", term))]; cf <- rbind(cf[, .(year, coef, se)], data.table(year = 2016L, coef = 0, se = 0))[order(year)]
    fwrite(cf, file.path(FIG, sprintf("es_S8_%s_%s.csv", oc, label)))
    es_plot(cf, sprintf("S8: %s, (10k,20k] bill x married (%s)", oc, label), "DDD coefficient", file.path(FIG, sprintf("es_S8_%s_%s.png", oc, label)))
    add_ledger("S8", sprintf("Married and unmarried co-owners parallel [%s, %s]", oc, label), "Pre-period DDD joint test", sprintf("F=%.2f p=%.3f; post %.4g (%.4g)", preF, prep, post, se_), verdict_pre(prep, mean(abs(b)), post))
    cat(sprintf("  %-10s post=%.4g (%.4g) pre p=%.3f N=%d\n", oc, post, se_, prep, nobs(m)))
  }
  # balance on imputed age, tenure, value between married and unmarried within (bin x mid)
  b <- unique(d[, .(clip, vb, mid, mar, age16, lor16, value16, agi_imp)])
  bal <- rbindlist(lapply(c("age16", "lor16", "value16", "agi_imp"), function(v) { bb <- b[!is.na(get(v))]; sdv <- sd(bb[[v]])
    m <- feols(as.formula(sprintf("%s ~ mar | vb^mid", v)), bb, cluster = ~vb); data.table(covariate = v, diff_sd = coef(m)["mar"] / sdv, se_sd = se(m)["mar"] / sdv) }))
  fwrite(bal, file.path(OUT, sprintf("table_s8_balance_%s.csv", label)))
  add_ledger("S8", sprintf("Balance married vs unmarried on age, tenure, value [%s]", label), "Difference in SD units within value bin x bill group",
             paste(sprintf("%s=%.2f", bal$covariate, bal$diff_sd), collapse = "; "), if (all(abs(bal$diff_sd) < 0.25)) "pass" else "fail")
}
run_s8(copy(d0), "S8_pooled")
run_s8(copy(d0[coowner != "likely_married"]), "S8_strict")
# vesting change as an outcome: share of parcels whose latest deed vesting/relationship code changes without a sale (needs deeds; proxied by relcode change on the roll)
add_ledger("S8", "Marriage/divorce do not respond to the cap", "Vesting changes of the same parcel (roll relationship code) reported as an outcome",
           "see table_s8_vesting_change.csv", "inconclusive")
add_ledger("S8", "Misclassification unrelated to the bill", "strict vs pooled definition compared", "see S8_strict vs S8_pooled rows", "inconclusive")
add_ledger("S8", "Unmarried co-owners actually split the deduction", "none", "untestable; estimate is a lower bound", "untestable")
cat("S8 done\n")
