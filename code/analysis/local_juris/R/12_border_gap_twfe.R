# Year profile of state-border rate gaps net of pair composition: gap_pt = a_p + b_t + e_pt (pair and year FE, 2017 = 0),
# SEs clustered by pair. Full (unbalanced) panel vs balanced panel (pairs observed in all 9 years). Measures: effective rate
# (bill / sale price, R/11) and CoreLogic-reported rate (R/10a); gap in pp and relative to the pair-year mean level.
suppressPackageStartupMessages({library(data.table); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- file.path(ROOT, "output/analysis/local_juris")
e <- fread(file.path(OUT, "border_pair_gaps_effective.csv"))[, .(pair, y, gap_pp, rel = gap_pp / ((eff_1 + eff_2) / 2), measure = "effective (bill / price)")]
c <- fread(file.path(OUT, "border_pair_gaps_cl.csv"))[, .(pair, y, gap_pp, rel = gap_pp / ((rate_1 + rate_2) / 2), measure = "CoreLogic-reported rate")]
d <- rbind(e, c)
d[, balanced := uniqueN(y) == 9, by = .(measure, pair)]
res <- rbindlist(lapply(split(d, by = "measure"), \(m) rbindlist(lapply(c("full", "balanced"), \(s) {
  x <- if (s == "balanced") m[balanced == TRUE] else m
  rbindlist(lapply(c("gap_pp", "rel"), \(v) {
    f <- feols(as.formula(paste(v, "~ i(y, ref = 2017) | pair")), x, cluster = ~pair)
    ct <- as.data.table(coeftable(f), keep.rownames = "term")
    rbind(data.table(y = 2017L, coef = 0, se = 0),
          ct[, .(y = as.integer(sub("y::", "", term)), coef = Estimate, se = `Std. Error`)])[,
      `:=`(measure = m$measure[1], panel = s, outcome = v, pairs = uniqueN(x$pair), obs = nrow(x), base = mean(x[y == 2017][[v]]))]
  }))
}))))
fwrite(res, file.path(OUT, "border_gap_twfe.csv"))
g <- ggplot(res[outcome == "gap_pp"], aes(y, coef, colour = panel, shape = panel)) + geom_hline(yintercept = 0, colour = "grey60") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey60") +
  geom_pointrange(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se), position = position_dodge(width = .4), size = .3) +
  facet_wrap(~ measure, ncol = 1, scales = "free_y") + scale_x_continuous(breaks = 2013:2021) +
  labs(x = NULL, y = "change in absolute gap vs 2017 (pp), pair FE", colour = NULL, shape = NULL,
       title = "State-border rate gaps: year effects with pair fixed effects",
       subtitle = "95% CIs clustered by state pair; balanced = pairs observed in all 9 years; 1% building sample") +
  theme_minimal(base_size = 10) + theme(legend.position = "bottom")
ggsave(file.path(OUT, "figures/border_gap_twfe.png"), g, width = 7.5, height = 6.5, dpi = 150)
sink(file.path(OUT, "border_gap_twfe.txt"))
for (k in split(res, by = c("measure", "outcome"))) {
  cat("\n==", k$measure[1], "|", k$outcome[1], "\n")
  w <- dcast(k, y ~ panel, value.var = c("coef", "se"))
  print(as.data.frame(w[, lapply(.SD, round, 3)]), row.names = FALSE)
  cat(sprintf("full: %d pairs, %d obs, 2017 mean %.3f | balanced: %d pairs, %d obs, 2017 mean %.3f\n",
      k[panel == "full"]$pairs[1], k[panel == "full"]$obs[1], k[panel == "full"]$base[1],
      k[panel == "balanced"]$pairs[1], k[panel == "balanced"]$obs[1], k[panel == "balanced"]$base[1]))
}
sink(); cat(readLines(file.path(OUT, "border_gap_twfe.txt")), sep = "\n")
