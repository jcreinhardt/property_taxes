# s4_investor_ddd.R — Strategy 4: owner-occupied vs investor-owned triple difference. Pre-specified 2026-10-16.
source("code/archive/2026-10-household-salt-cap/R/00_setup.R"); source("code/archive/2026-10-household-salt-cap/R/01b_load.R")
d <- load_panel(2012:2019, "both")[rate_ok == TRUE & !is.na(valuebin)]
cat("S4 sample:", nrow(d), " owner parcels", uniqueN(d[owner == 1, clip]), " investor parcels", uniqueN(d[owner == 0, clip]), "\n")
# DDD event study: Y ~ T x year x Owner + T x year + (Owner x year absorbed in FE) | clip + vb^owner^year + county^owner^year
run_ddd <- function(dd, oc, tr, spec_id, fig = TRUE) {
  dd <- dd[!is.na(get(oc))]; if (oc == "log_price") dd <- dd[sale == 1]
  fe <- if (oc == "log_price") "vb^owner^year + county^owner^year" else "clip + vb^owner^year + county^owner^year"
  ctrl <- if (oc == "log_price") paste0(" + ", HEDONIC, " + i(owner, lv16)") else ""
  f <- as.formula(sprintf("%s ~ i(year, %s, ref = 2016) + i(year, %s_own, ref = 2016)%s | %s", oc, tr, tr, ctrl, fe))
  m <- feols(f, dd, cluster = ~juris, notes = FALSE, mem.clean = TRUE)
  # owner x T x year coefficients
  cf <- as.data.table(coeftable(m), keep.rownames = "term"); setnames(cf, c("term", "coef", "se", "t", "p"))
  cf <- cf[grepl("_own", term)]; cf[, year := as.integer(sub(".*year::(\\d{4}).*", "\\1", term))]
  cf <- rbind(cf[, .(year, coef, se)], data.table(year = 2016L, coef = 0, se = 0))[order(year)]
  nm <- names(coef(m)); yr <- as.integer(sub(".*year::(\\d{4}).*", "\\1", nm)); own <- grepl("_own", nm)
  w <- as.numeric(own & yr %in% 2018:2019) / 2; post <- sum(w * coef(m)); post_se <- sqrt(as.numeric(t(w) %*% vcov(m) %*% w))
  ipre <- which(own & yr %in% 2012:2015); b <- coef(m)[ipre]; V <- vcov(m)[ipre, ipre]
  preF <- as.numeric(t(b) %*% solve(V) %*% b) / length(ipre); prep <- pf(preF, length(ipre), 1e6, lower.tail = FALSE)
  add_estimate("S4", oc, post, post_se, nobs(m), uniqueN(dd$juris), spec_id)
  cf[, `:=`(strategy = "S4", outcome = oc, spec_id = spec_id, treat = paste0(tr, " x owner"))]
  fwrite(cf, file.path(FIG, sprintf("es_S4_%s_%s.csv", oc, spec_id)))
  if (fig) es_plot(cf, sprintf("S4 triple difference: %s, %s x owner-occupied", oc, tr), "DDD coefficient", file.path(FIG, sprintf("es_S4_%s_%s.png", oc, spec_id)))
  list(post = post, se = post_se, preF = preF, prep = prep, pre_mean_abs = mean(abs(b)), N = nobs(m))
}
d[, rate16_pp_own := rate16_pp * owner]; d[, above16_own := above16 * owner]
for (tr in c("rate16_pp", "above16")) for (oc in c("sale", "log_price", "refi", "to_rental")) {
  id <- paste0("S4_", ifelse(tr == "rate16_pp", "rate", "above"))
  r <- run_ddd(d, oc, tr, id)
  add_ledger("S4", sprintf("Bill gradient would have changed alike for owners and investors [%s, %s]", oc, tr), "Pre-period DDD event study (2012-2015 joint test)",
             sprintf("F=%.2f p=%.3f; post DDD %.4g (%.4g)", r$preF, r$prep, r$post, r$se), verdict_pre(r$prep, r$pre_mean_abs, r$post))
  cat(sprintf("%-10s %-9s DDD post=%.4g (%.4g) pre p=%.3f N=%d\n", oc, tr, r$post, r$se, r$prep, r$N))
  # classification robustness: owners where all three signals agree; investors with corporate owner or mailing elsewhere (already)
  da <- d[owner == 0 | (hs16 & mh16 & nm16)]
  ra <- run_ddd(da, oc, tr, paste0(id, "_agree"), fig = FALSE)
  add_ledger("S4", sprintf("Occupancy misclassification unrelated to bill [%s, %s]", oc, tr), "Owners restricted to parcels where all three occupancy signals agree",
             sprintf("DDD %.4g (%.4g) vs base %.4g (%.4g)", ra$post, ra$se, r$post, r$se), if (abs(ra$post - r$post) < 2 * ra$se) "pass" else "fail")
}
add_ledger("S4", "Classification does not respond to treatment", "Occupancy fixed at 2016; to_rental reported as an outcome", "see to_rental rows", "pass")
cat("S4 done\n")
