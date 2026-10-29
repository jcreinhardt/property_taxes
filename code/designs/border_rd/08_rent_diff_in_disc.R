# Difference in discontinuities of asking rents at state borders, scaled by the cross-border difference in SALT-cap exposure.
# Tract-year rent index (build/altos: lr_adj, log rent at a fixed type x bedroom mix; tracts with >= MIN listings) on 2010
# tracts; nearest ranked border pair, side and distance from data/built/geo/iv_tract_side.rds (T = 30). Exposure E_i = lost
# federal subsidy per owner household (build/exposure/05, d_salt_own, $100/yr, winsorised p1/p99; ZIP where uncensored, county
# otherwise). Per pair, the high-EXPOSURE side (hx) is the side with the larger return-weighted mean E within 20 mi of the
# line; dE_p = the difference between the sides (>= 0, $100). Running variable: signed distance, positive on the hx side.
# Outcome: within-tract change in log rent since 2017 (dlr), so 2017 = 0 by construction.
# (1) Pooled local-linear diff-in-disc, year by year:  dlr_it = a_pt + s_p d_i + s'_p d_i hx_i + g_t hx_i + b_t hx_i dE_p,
#     triangular kernel on |d| <= H, SE by county. b_t: change in the rent discontinuity per $100/yr of exposure gap.
# (2) The series: per pair and year, rdrobust (h = H) of dlr on signed distance -> tau_pt; then tau_pt on dE_p across pairs
#     (WLS, 1 / se^2) by year and for the post period (2018-24 pooled per pair); scatter of the post-period tau_p on dE_p.
# (3) Reference: the unscaled diff-in-disc on the high-TAX side (high) as in border_rd/04 for prices, pair-demeaned.
# ALTOS_INDEX=repeat: the repeat-listing (same-unit) tract index of build/altos/03 (lr_rep, tract-years with >= MIN linked
# pairs) as the outcome instead; outputs with the suffix _repeat.
suppressPackageStartupMessages({library(data.table); library(fixest); library(rdrobust); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_rd"); invisible(out_dir("designs/border_rd/figures")); L <- file.path(DATA, "built")
REP <- identical(Sys.getenv("ALTOS_INDEX"), "repeat"); P <- paste0("d08_rent_diff_in_disc", if (REP) "_repeat" else ""); IDX <- if (REP) "repeat-listing rent index" else "rent index"
H <- 15; MX <- 100; MIN <- 5; YRS <- 2013:2024; SIDE_MI <- 20
wins <- \(x) { q <- quantile(x, c(.01, .99), na.rm = TRUE); pmin(pmax(x, q[1]), q[2]) / 100 }
pad <- \(x, w) { x <- as.character(x); paste0(strrep("0", pmax(0, w - nchar(x))), x) }
s <- unique(readRDS(file.path(L, "geo/iv_tract_side.rds"))[T == 30, .(tract, pair, high, dist_mi, cty)])[dist_mi <= MX]
ez <- fread(file.path(L, "exposure/exposure_components_zip.csv"))[clean == TRUE, .(zip = pad(geo, 5), ez = wins(d_salt_own), ret_z = returns16)]
ec <- fread(file.path(L, "exposure/exposure_components_county.csv"))[, .(cty = pad(geo, 5), ecty = wins(d_salt_own), ret_c = returns16)]
tz <- fread(file.path(L, "exposure/salt_exposure_tract_zip2016.csv"), colClasses = c(tract = "character", zip = "character"))[, .(tract = pad(tract, 11), zip = pad(zip, 5), np = n)]
tz[, sh := np / sum(np), by = zip]
s <- merge(s, tz, by = "tract", all.x = TRUE); s <- merge(s, ez, by = "zip", all.x = TRUE); s <- merge(s, ec, by = "cty", all.x = TRUE)
s[, nt_c := .N, by = cty][, `:=`(E = fcoalesce(ez, ecty), w = fcoalesce(ret_z * sh, ret_c / nt_c), src = fifelse(!is.na(ez), "zip", "county"))]
s <- s[!is.na(E)]
side <- s[dist_mi <= SIDE_MI, .(E_side = weighted.mean(E, w), nt = .N), by = .(pair, high)][, k := .N, by = pair][k == 2 & nt >= 10]
side[, hx := as.integer(E_side == max(E_side)), by = pair][, dE := max(E_side) - min(E_side), by = pair]
s <- merge(s, side[, .(pair, high, hx, dE, E_side)], by = c("pair", "high"))
s[, `:=`(sd = fifelse(hx == 1, dist_mi, -dist_mi), sd_tax = fifelse(high == 1, dist_mi, -dist_mi))][, `:=`(sd_hx = sd * hx, hx_dE = hx * dE)]
tr <- if (REP) readRDS(file.path(L, "altos/tract_year_repeat.rds"))[n_rep >= MIN & !is.na(lr_rep) & y %in% YRS, .(tract, y, lr = lr_rep, n = n_rep)] else
  readRDS(file.path(L, "altos/tract_year.rds"))[n >= MIN & y %in% YRS, .(tract, y, lr = lr_adj, n)]
d <- merge(tr, s, by = "tract"); d[, dlr := lr - lr[y == 2017][1], by = tract]; d <- d[is.finite(dlr)]
# exposure discontinuity at the line (first stage of the scaling): E on signed distance, pair-demeaned, and per pair
x0 <- unique(d[, .(tract, pair, sd, E, w, cty)]); x0[, Ed := E - ave(E, pair)]
r0 <- rdrobust(x0$Ed, x0$sd, c = 0, h = H, cluster = x0$cty, vce = "hc1")
x0[, bin := cut(sd, seq(-30, 30, 5), right = FALSE)]
prof <- x0[!is.na(bin), .(E = weighted.mean(Ed, w), tracts = .N), by = bin][order(bin)]   # profile of pair-demeaned exposure across the line
# (1) pooled local-linear diff-in-disc by year
pool <- function(yy, h = H) {
  x <- d[y == yy & abs(sd) <= h][, kw := 1 - abs(sd) / h]
  m <- feols(dlr ~ hx + hx_dE | pair[sd, sd_hx], x, weights = ~kw, cluster = ~cty)
  ct <- as.data.table(coeftable(m), keep.rownames = "term")
  ct[, .(y = yy, h = h, term, b = Estimate, se = `Std. Error`, p = `Pr(>|t|)`, n = nobs(m), pairs = uniqueN(x$pair))] }
p1 <- rbindlist(lapply(c(10, H, 25), \(h) rbindlist(lapply(setdiff(YRS, 2017), pool, h = h))))
p1 <- rbind(p1, CJ(y = 2017L, h = c(10, H, 25), term = c("hx", "hx_dE"), b = 0, se = 0, p = NA_real_, n = NA_integer_, pairs = NA_integer_))[order(h, term, y)]
# (2) per pair and year rdrobust, then the cross-pair series
rdp <- function(x) { r <- tryCatch(rdrobust(x$dlr, x$sd, c = 0, h = H, cluster = x$cty, vce = "hc1"), error = \(e) NULL)
  if (is.null(r)) r <- tryCatch(rdrobust(x$dlr, x$sd, c = 0, h = H, vce = "hc1"), error = \(e) NULL)
  if (is.null(r)) return(NULL); data.table(tau = r$coef[1], se = r$se[3], n_eff = sum(r$N_h)) }
d[, ok := sum(abs(sd) <= H & sd > 0) >= 20 & sum(abs(sd) <= H & sd < 0) >= 20, by = .(pair, y)]
taus <- d[ok == TRUE & y != 2017, rdp(.SD), by = .(pair, y, dE)][is.finite(se) & se > 0]
ser <- rbindlist(lapply(setdiff(YRS, 2017), \(yy) { x <- taus[y == yy]; if (nrow(x) < 8) return(NULL)
  m <- feols(tau ~ dE, x, weights = ~ 1 / se^2, vcov = "hetero"); ct <- coeftable(m)
  data.table(y = yy, b = ct["dE", 1], se = ct["dE", 2], a = ct["(Intercept)", 1], a_se = ct["(Intercept)", 2], pairs = nrow(x)) }))
post <- taus[y >= 2018, .(tau = weighted.mean(tau, 1 / se^2), se = sqrt(1 / sum(1 / se^2)), years = .N), by = .(pair, dE)]
pre <- taus[y <= 2016, .(tau = weighted.mean(tau, 1 / se^2), se = sqrt(1 / sum(1 / se^2)), years = .N), by = .(pair, dE)]
mpost <- feols(tau ~ dE, post, weights = ~ 1 / se^2, vcov = "hetero"); mpre <- feols(tau ~ dE, pre, weights = ~ 1 / se^2, vcov = "hetero")
# (3) unscaled diff-in-disc on the high-tax side, pair-demeaned (as 04)
ref <- rbindlist(lapply(setdiff(YRS, 2017), \(yy) { x <- d[y == yy]; yv <- x$dlr - ave(x$dlr, x$pair)
  r <- rdrobust(yv, x$sd_tax, c = 0, h = H, cluster = x$cty, vce = "hc1")
  data.table(y = yy, est = r$coef[1], lo = r$ci[3, 1], hi = r$ci[3, 2], p = r$pv[3], n_eff = sum(r$N_h)) }))
fwrite(p1, file.path(OUT, paste0(P, "_pooled.csv"))); fwrite(ser, file.path(OUT, paste0(P, "_series.csv")))
fwrite(taus, file.path(OUT, paste0(P, "_pair_taus.csv"))); fwrite(ref, file.path(OUT, paste0(P, "_taxside.csv")))
sink(file.path(OUT, paste0(P, ".txt")), split = TRUE)
cat(sprintf("Tracts with rents (>= %d listings / linked pairs) within %d mi of a ranked pair: %d in %d pairs; exposure from ZIP for %.0f%% of tracts\n",
            MIN, MX, uniqueN(d$tract), uniqueN(d$pair), 100 * mean(unique(d[, .(tract, src)])$src == "zip")))
cat(sprintf("Exposure gap between sides (dE, $100/yr per owner, 20-mi means): median %.2f, IQR %.2f-%.2f, max %.2f; high-exposure side = high-tax side in %.0f%% of pairs\n",
            median(side[hx == 1, dE]), quantile(side[hx == 1, dE], .25), quantile(side[hx == 1, dE], .75), max(side$dE), 100 * mean(side[hx == 1, high] == 1)))
cat(sprintf("RD of exposure at the line (pair-demeaned, h = %d): %.3f [%.3f, %.3f] $100/yr\n", H, r0$coef[1], r0$ci[3, 1], r0$ci[3, 2]))
cat("Exposure profile across the line (pair-demeaned, return-weighted mean E by signed distance bin; positive = high-exposure side):\n")
print(as.data.frame(prof[, .(bin, tracts, E = round(E, 2))]), row.names = FALSE)
cat("\n(1) Pooled local-linear diff-in-disc: change in the rent discontinuity (log points) since 2017 per $100/yr exposure gap (hx_dE) and the unscaled part (hx)\n")
for (hh in c(10, H, 25)) { cat("\n-- h =", hh, "mi\n"); print(dcast(p1[h == hh, .(term, y, cell = sprintf("%.3f (%.3f)", b, se))], term ~ y, value.var = "cell")) }
cat("\n(2) Series: per-pair rdrobust diff-in-disc tau_pt (h = 15) regressed on dE_p across pairs (WLS 1/se^2, robust SE)\n")
print(ser[, .(y, pairs, slope = sprintf("%.3f (%.3f)", b, se), intercept = sprintf("%.3f (%.3f)", a, a_se))])
cat(sprintf("\npost 2018-24 pooled per pair: slope %.3f (%.3f), intercept %.3f (%.3f), %d pairs | pre 2013-16: slope %.3f (%.3f), %d pairs\n",
            coef(mpost)["dE"], se(mpost)["dE"], coef(mpost)[1], se(mpost)[1], nrow(post), coef(mpre)["dE"], se(mpre)["dE"], nrow(pre)))
cat("\n(3) Reference: unscaled diff-in-disc on the high-TAX side, pair-demeaned (log points, robust 95% CI)\n")
print(ref[, .(y, cell = sprintf("%.3f [%.3f, %.3f]", est, lo, hi), n_eff)])
sink()
pd <- rbind(p1[h == H & term == "hx_dE", .(y, b, se, what = "(1) pooled local-linear: per $100/yr exposure gap")],
            rbind(ser[, .(y, b, se)], data.table(y = 2017L, b = 0, se = 0))[, what := "(2) series: per-pair tau on dE across pairs"],
            rbind(ref[, .(y, b = est, se = (hi - lo) / 3.92)], data.table(y = 2017L, b = 0, se = 0))[, what := "(3) unscaled, high-tax side (log points)"])
g <- ggplot(pd, aes(y, b)) + geom_hline(yintercept = 0, colour = "grey60") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey40") +
  geom_pointrange(aes(ymin = b - 1.96 * se, ymax = b + 1.96 * se), size = .2, colour = "#2a64a8") + geom_line(colour = "#2a64a8") +
  facet_wrap(~ what, ncol = 1, scales = "free_y") + scale_x_continuous(breaks = seq(2013, 2024, 2)) +
  labs(x = NULL, y = "change in the rent discontinuity since 2017", title = "Asking rents at state borders: difference in discontinuities on the SALT-exposure gap",
       subtitle = sprintf("Tract %s (Altos), h = %d mi, within-tract change since 2017; 95%% CI (SE by county / robust across pairs)", IDX, H)) + theme_minimal(base_size = 9)
ggsave(file.path(OUT, "figures", paste0(P, ".png")), g, width = 8, height = 9, dpi = 150)
g2 <- ggplot(post, aes(dE, tau)) + geom_hline(yintercept = 0, colour = "grey60") + geom_pointrange(aes(ymin = tau - 1.96 * se, ymax = tau + 1.96 * se), colour = "grey40", size = .2) +
  geom_abline(intercept = coef(mpost)[1], slope = coef(mpost)["dE"], colour = "#c0392b") + geom_text(aes(label = pair), size = 2.2, vjust = -.6) +
  labs(x = "exposure gap between the sides, $100/yr per owner household", y = "diff-in-disc of log rent, 2018-24 vs 2017 (per pair)",
       title = "Per-pair rent diff-in-disc against the cross-border exposure gap", subtitle = sprintf("WLS slope %.3f (%.3f)", coef(mpost)["dE"], se(mpost)["dE"])) + theme_minimal(base_size = 9)
ggsave(file.path(OUT, "figures", paste0(P, "_pairs.png")), g2, width = 8, height = 6, dpi = 150)
