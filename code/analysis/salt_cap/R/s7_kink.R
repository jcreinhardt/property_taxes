# s7_kink.R — Strategy 7: difference in kinks at a $10k bill in states without an income tax. Pre-specified 2026-10-16.
source("code/analysis/salt_cap/R/00_setup.R"); source("code/analysis/salt_cap/R/01b_load.R")
library(rdrobust)
NOTAX <- c("TX", "FL", "WA", "NV", "SD", "WY"); ROB <- c("TN", "NH")
d <- load_panel(2014:2019, "oo")[rate_ok == TRUE]
d[, per := fifelse(year %in% 2018:2019, "post", fifelse(year %in% 2014:2016, "pre", NA_character_))]
d <- d[!is.na(per)]
# kink estimate: change in slope of Y wrt bill16 at 10k (rdrobust deriv = 1), by period; difference post - pre with SEs combined
kink <- function(dd, oc, h = NULL, c0 = 10000) {
  dd <- dd[!is.na(get(oc)) & bill16 > c0 - 8000 & bill16 < c0 + 8000]
  if (oc == "log_price") dd <- dd[sale == 1]
  out <- lapply(c("pre", "post"), function(p) {
    x <- dd[per == p]; if (nrow(x) < 500) return(NULL)
    r <- tryCatch(if (is.null(h)) rdrobust(x[[oc]], x$bill16, c = c0, deriv = 1, cluster = x$juris) else rdrobust(x[[oc]], x$bill16, c = c0, deriv = 1, h = h, cluster = x$juris), error = function(e) NULL)
    if (is.null(r)) NULL else data.table(per = p, est = r$coef[1], se = r$se[3], h = r$bws[1, 1], n = sum(r$N_h))
  })
  rbindlist(out)
}
run_s7 <- function(dd, label, oc, hmult = NULL) {
  k0 <- kink(dd, oc); if (nrow(k0) < 2) return(NULL)
  h <- if (is.null(hmult)) NULL else hmult * k0[per == "post", h]
  k <- if (is.null(hmult)) k0 else kink(dd, oc, h = h)
  if (nrow(k) < 2) return(NULL)
  dif <- k[per == "post", est] - k[per == "pre", est]; se <- sqrt(sum(k$se^2))
  id <- paste0("S7_", label, if (is.null(hmult)) "" else if (hmult == 0.5) "_h05" else "_h2")
  add_estimate("S7", oc, dif * 1000, se * 1000, sum(k$n), NA, id)        # scaled: change in slope per $1,000 of bill
  k[, `:=`(outcome = oc, spec_id = id)]
  k
}
res <- list()
for (smp in list(list("notax", d[st %in% NOTAX]), list("notax_itm", d[st %in% NOTAX & mortint16 >= 6000]), list("notax_tnnh", d[st %in% c(NOTAX, ROB)]), list("all", d))) {
  for (oc in c("sale", "log_price", "exit_prim", "refi")) for (hm in list(NULL, 0.5, 2)) {
    k <- run_s7(smp[[2]], smp[[1]], oc, hm); if (!is.null(k)) { k[, sample_def := smp[[1]]]; res[[length(res) + 1]] <- k }
  }
}
res <- rbindlist(res); fwrite(res, file.path(OUT_S, "table_s7_kinks.csv")); print(res[spec_id %in% c("S7_notax", "S7_notax_itm")])
# diagnostics: pre-period kink ~ 0
pre <- res[per == "pre" & spec_id == "S7_notax"]
add_ledger("S7", "No other kink at $10k", "Pre-period (2014-16) kink in each outcome, no-income-tax states",
           paste(sprintf("%s: %.3g (%.3g)", pre$outcome, 1000 * pre$est, 1000 * pre$se), collapse = "; "), if (all(abs(pre$est) < 2 * pre$se)) "pass" else "fail")
# covariate kinks
b <- unique(d[st %in% NOTAX, .(clip, bill16, juris, year_built, living_area, value16, agi_imp, lor16)])
ck <- rbindlist(lapply(c("year_built", "living_area", "value16", "agi_imp", "lor16"), function(v) { x <- b[!is.na(get(v)) & bill16 > 2000 & bill16 < 18000]
  r <- tryCatch(rdrobust(x[[v]], x$bill16, c = 10000, deriv = 1, cluster = x$juris), error = function(e) NULL)
  if (is.null(r)) NULL else data.table(covariate = v, kink = r$coef[1] * 1000 / sd(x[[v]]), se = r$se[3] * 1000 / sd(x[[v]])) }))
fwrite(ck, file.path(OUT_S, "table_s7_covariate_kinks.csv"))
add_ledger("S7", "Outcomes otherwise smooth in the bill", "Covariate kinks at $10k (slope change per $1k, SD units)",
           paste(sprintf("%s=%.3f(%.3f)", ck$covariate, ck$kink, ck$se), collapse = "; "), if (all(abs(ck$kink) < 2 * ck$se)) "pass" else "fail")
# bunching / density: histogram of bill at 10k in 2016 vs 2019 (bill_t from the roll), no-income-tax states
bt <- load_panel(2016:2019, "oo", extra = "AND a.bill IS NOT NULL")[st %in% NOTAX & year %in% c(2016, 2019) & bill_t > 7000 & bill_t < 13000]
bt[, bin := floor(bill_t / 100) * 100]
dens <- bt[, .N, by = .(year, bin)][order(year, bin)]; dens[, share := N / sum(N), by = year]
fwrite(dens, file.path(OUT_S, "table_s7_density.csv"))
g <- ggplot(dens, aes(bin, share, colour = factor(year))) + geom_line() + geom_vline(xintercept = 10000, linetype = 2) +
  labs(x = "property tax bill ($100 bins)", y = "share of parcels", colour = "tax year", title = "S7: bill density around $10k, no-income-tax states") + theme_minimal()
ggsave(file.path(FIG, "s7_density_10k.png"), g, width = 7, height = 4, dpi = 150, bg = "white")
# bunching statistic: excess mass in [9000, 10000) relative to a linear fit on [7000,9000) U [10000,13000)
bun <- dens[, { fit <- lm(share ~ bin, .SD[bin < 9000 | bin >= 10000]); pred <- predict(fit, .SD[bin >= 9000 & bin < 10000]); .(excess = sum(share[bin >= 9000 & bin < 10000]) - sum(pred)) }, by = year]
fwrite(bun, file.path(OUT_S, "table_s7_bunching.csv")); print(bun)
add_estimate("S7", "bunching_below_10k", bun[year == 2019, excess] - bun[year == 2016, excess], NA, nrow(bt), NA, "S7_bunch_2019_vs_2016")
add_ledger("S7", "No manipulation of the bill around $10k", "Excess mass in [$9k,$10k) 2019 vs 2016",
           sprintf("excess 2016 %.4f, 2019 %.4f", bun[year == 2016, excess], bun[year == 2019, excess]), if (abs(bun[year == 2019, excess] - bun[year == 2016, excess]) < 0.005) "pass" else "fail")
# sales-tax blur: shift threshold down by the IRS optional sales tax table amount (approx. by AGI: 2018 table, 2 exemptions, TX/FL/WA/NV average)
stx <- function(agi) fifelse(agi < 40e3, 500, fifelse(agi < 80e3, 800, fifelse(agi < 120e3, 1000, fifelse(agi < 200e3, 1250, 1600))))
ds <- d[st %in% NOTAX]; ds[, bill16_adj := bill16 + stx(agi_imp)]   # SALT = bill + sales tax; kink in bill at 10k - sales tax
for (oc in c("sale", "log_price")) { x <- copy(ds); x[, bill16 := bill16_adj]; k <- run_s7(x, "notax_salesadj", oc); if (!is.null(k)) print(k) }
add_ledger("S7", "Sales-tax deduction does not blur the threshold", "Threshold shifted by IRS optional sales-tax table amount", "see S7_notax_salesadj rows", "inconclusive")
cat("S7 done\n")
