# Do cross-border rate gaps line up with state school-finance regimes? For each ranked border pair (sides from
# local_juris/R/26): high-side minus low-side median effective rate (tracts < 20 mi) against the high-minus-low difference in
# the state's K-12 finance (F-33 FY2016, all districts in the state): state-aid share of state+local revenue, property tax per
# pupil, total spending per pupil. Pair-level OLS, HC1 SE; weights = tracts within 20 mi.
suppressPackageStartupMessages({library(data.table); library(fixest)})
source("code/R/paths.R"); OUT <- file.path(ROOT, "output/analysis/salt_local")
f <- readRDS(file.path(ROOT, "data/local_juris/district_finance.rds"))[y == 2016]
f[, st := substr(leaid, 1, 2)]
s <- f[, .(aid_sh = sum(strev, na.rm = TRUE) / sum(strev + loc, na.rm = TRUE), ptax_pp = sum(ptax, na.rm = TRUE) / sum(enr, na.rm = TRUE),
           spend_pp = sum(spend, na.rm = TRUE) / sum(enr, na.rm = TRUE)), by = st]
d <- unique(readRDS(file.path(ROOT, "data/local_juris/iv_tract_side.rds"))[dist_mi < 20, .(tract, st, pair, high, side_med)])
p <- d[, .(side_med = side_med[1], n = .N), by = .(pair, high, st)]
p <- merge(p, s, by = "st")
w <- dcast(p, pair ~ high, value.var = c("side_med", "aid_sh", "ptax_pp", "spend_pp", "n", "st"))
w[, `:=`(gap = side_med_1 - side_med_0, d_aid = 100 * (aid_sh_1 - aid_sh_0), d_ptax = ptax_pp_1 - ptax_pp_0, d_spend = spend_pp_1 - spend_pp_0, wt = n_0 + n_1)]
w <- w[is.finite(gap) & is.finite(d_aid)]
sink(file.path(OUT, "c8_pair_gap_school_finance.txt"))
cat("Pairs:", nrow(w), "| mean rate gap high - low (pp):", round(weighted.mean(w$gap, w$wt), 3), "\n")
cat("Share of pairs where the high-rate side has the lower state-aid share:", round(mean(w$d_aid < 0), 2), "\n")
cat("Share where the high-rate side raises more property tax per pupil:", round(mean(w$d_ptax > 0), 2), "\n\n")
m <- list(aid = feols(gap ~ d_aid, w, weights = ~wt, vcov = "hc1"), ptax = feols(gap ~ d_ptax, w, weights = ~wt, vcov = "hc1"),
          both = feols(gap ~ d_aid + d_spend, w, weights = ~wt, vcov = "hc1"))
etable(m, fitstat = ~ n + r2) |> print()
cat("\nUnweighted R2, gap ~ d_aid:", round(summary(lm(gap ~ d_aid, w))$r.squared, 3), "\n")
print(w[order(-gap), .(pair, low = st_0, high = st_1, gap = round(gap, 2), d_aid = round(d_aid, 1), d_ptax = round(d_ptax, 2))][1:15])
sink(); cat(readLines(file.path(OUT, "c8_pair_gap_school_finance.txt")), sep = "\n")
