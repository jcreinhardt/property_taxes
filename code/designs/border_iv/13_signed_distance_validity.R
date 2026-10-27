# Formerly code/analysis/salt_local/c4_iv_signed_distance.R (moved in the 2026-10-27 restructuring).
# Q1 follow-up: is the directional instrument (near x high side) valid, or does it rely on offsetting selection on the two
# sides? (1) Binned means of the balance covariates and first-stage variables against signed distance to the tract's
# nearest state border (+ = high-tax side, - = low-tax side), -50..+50 mi, 5-mi bins, after removing pair means (pair FE
# only, so level differences between sides remain visible). (2) One-sided designs: near (< 10 mi) vs far (>= 10 mi) within
# the high side only and within the low side only (pair FE, SE by county), and their difference, in SD units.
suppressPackageStartupMessages({library(data.table); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_iv"); invisible(out_dir("designs/border_iv/figures"))
d <- readRDS(file.path(DATA, "built/geo/iv_tract_side.rds"))[T == 30]
d[, sd_mi := ifelse(high == 1, dist_mi, -dist_mi)]
lab <- c(x_cheap = "cheaper cross-state tract within 30 min (first stage)", tax_gap = "own rate - p10 reachable (pp, first stage)",
         med = "own median effective rate (%)", ach = "district achievement (SEDA, SD)", ses = "district SES (SEDA)", linc = "log median HH income",
         lhval = "log median home value", sh_ba = "share BA+", sh_white = "share white", sh_own = "owner share", yrbuilt = "median year built", lpop = "log population")
w <- d[abs(sd_mi) <= 50]
long <- rbindlist(lapply(names(lab), \(v) { x <- w[is.finite(get(v)), .(pair, sd_mi, y = get(v))]
  x[, yr := y - mean(y), by = pair][, bin := floor(sd_mi / 5) * 5 + 2.5]
  x[, .(m = mean(yr), se = sd(yr) / sqrt(.N), n = .N), by = bin][, var := lab[v]] }))
long[, var := factor(var, lab)]
g <- ggplot(long, aes(bin, m)) + geom_hline(yintercept = 0, colour = "grey70") + geom_vline(xintercept = 0, colour = "black") +
  annotate("rect", xmin = -10, xmax = 10, ymin = -Inf, ymax = Inf, alpha = .08, fill = "red") +
  geom_pointrange(aes(ymin = m - 1.96 * se, ymax = m + 1.96 * se), size = .15) + geom_line(aes(group = bin > 0), linewidth = .3) +
  facet_wrap(~ var, scales = "free_y", ncol = 3) + scale_x_continuous(breaks = seq(-50, 50, 10)) +
  labs(x = "signed distance to nearest state border (miles): low-tax side < 0 < high-tax side", y = "mean, demeaned within border pair",
       title = "Tract characteristics by signed distance to the state border", subtitle = "5-mile bins, 95% CI; shaded = the < 10 mi 'near' band; tracts with sales 2017-19") +
  theme_minimal(base_size = 8)
ggsave(file.path(OUT, "figures/c4_iv_signed_distance.png"), g, width = 11, height = 10, dpi = 150)
# one-sided designs
ys <- names(lab)
one <- rbindlist(lapply(ys, \(v) { sdy <- sd(d[[v]], na.rm = TRUE)
  rbindlist(lapply(c(1, 0), \(h) { x <- d[high == h & is.finite(get(v))]
    m <- feols(as.formula(sprintf("%s ~ near | pair", v)), x, cluster = ~cty)
    data.table(var = v, side = ifelse(h == 1, "high side: near vs far", "low side: near vs far"), est = coef(m)[1] / sdy, se = se(m)[1] / sdy) })) }))
dif <- one[, .(est = est[side == "high side: near vs far"] - est[side == "low side: near vs far"],
               se = sqrt(sum(se^2)), side = "difference (= directional instrument)"), by = var]
one <- rbind(one, dif)
fwrite(one, file.path(OUT, "c4_iv_one_sided.csv"))
sink(file.path(OUT, "c4_iv_one_sided.txt"))
cat("Near (< 10 mi) vs far (>= 10 mi) within each side, pair FE, SE by county; coefficients in SD units of the variable\n")
print(dcast(one[, .(var, side, cell = sprintf("%.3f (%.3f)", est, se))], var ~ side, value.var = "cell")[match(ys, var)])
sink(); cat(readLines(file.path(OUT, "c4_iv_one_sided.txt")), sep = "\n")
