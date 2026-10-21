# Border-IV design on the tract-year panel: did the SALT cap change outcomes in tracts that have a cheap cross-border outside
# option (near a state border, on its high-tax side) relative to tracts far from the border on the same side and to tracts
# near the same border on the low side? Triple difference event study:
#   Y_it = a_i + g_{pair x high x t} + h_{pair x dbin x t} + sum_t b_t 1[t] x near_i x high_i
# near = < 10 mi; ref 2017; SE by county. Also split by county SALT exposure (above vs below median of tracts in the sample),
# where a stronger response in high-exposure areas is the SALT-specific prediction.
# Outcomes: log tax per parcel, log median price, median effective rate, log assessment ratio, sales turnover.
suppressPackageStartupMessages({library(data.table); library(fixest); library(ggplot2)})
ROOT <- "/Users/jr2728/PhD/projects/property_taxes"; OUT <- file.path(ROOT, "output/analysis/salt_local")
ty <- readRDS(file.path(ROOT, "data/local_juris/tract_year.rds"))[y >= 2013 & y <= 2024]
iv <- unique(readRDS(file.path(ROOT, "data/local_juris/iv_tract_side.rds"))[, .(tract, pair, high, dbin, near, cty, dist_mi)])
ex <- fread(file.path(ROOT, "data/local_juris/salt_exposure_county2016.csv"))[, cty := sprintf("%05d", county)]
d <- merge(merge(ty, iv, by = "tract"), ex[, .(cty, ex_z)], by = "cty")
d[, ok := uniqueN(y[n >= .8 * max(n)]) == 12, by = tract]
d[, `:=`(nh = near * high, l_tax_pp = log(bill / n), l_p50 = ifelse(n_sales >= 5, log(p50), NA), eff = ifelse(n_sales >= 5, 100 * eff50, NA),
         l_ratio = ifelse(n_sales >= 5, log(ratio50), NA), turnover = 100 * fcoalesce(n_sales, 0L) / n)]
med_ex <- median(unique(d[, .(cty, ex_z)])$ex_z)
d[, hi_ex := ex_z > med_ex]
ys <- c("l_tax_pp", "l_p50", "eff", "turnover", "l_ratio")
run <- function(dd, v, lab) {
  dd <- dd[is.finite(get(v))]
  f <- feols(as.formula(sprintf("%s ~ i(y, nh, ref = 2017) | tract + pair^high^y + pair^dbin^y", v)), dd, cluster = ~cty)
  ct <- as.data.table(coeftable(f), keep.rownames = "term")
  rbind(data.table(y = 2017L, coef = 0, se = 0), ct[, .(y = as.integer(sub("y::(\\d+):nh", "\\1", term)), coef = Estimate, se = `Std. Error`)])[,
    `:=`(outcome = v, sample = lab, tracts = uniqueN(dd$tract), nh_tracts = uniqueN(dd[nh == 1, tract]))] }
res <- rbindlist(lapply(ys, \(v) rbind(run(d[ok == TRUE], v, "all"), run(d[ok == TRUE & hi_ex == TRUE], v, "high exposure"),
                                       run(d[ok == TRUE & hi_ex == FALSE], v, "low exposure"))))
fwrite(res, file.path(OUT, "b3_tract_iv_ddd.csv"))
sink(file.path(OUT, "b3_tract_iv_ddd.txt"))
cat("Triple difference: near (< 10 mi) x high-tax side x year, ref 2017; tract + pair x side x year + pair x distance-bin x year FE; SE by county\n")
for (k in split(res, by = c("outcome", "sample"))) {
  cat("\n==", k$outcome[1], "|", k$sample[1], "| tracts:", k$tracts[1], "| near-high tracts:", k$nh_tracts[1], "\n")
  print(as.data.frame(dcast(k, . ~ y, value.var = "coef")[, -1][, lapply(.SD, round, 4)]), row.names = FALSE)
  print(as.data.frame(dcast(k, . ~ y, value.var = "se")[, -1][, lapply(.SD, round, 4)]), row.names = FALSE) }
sink(); cat(readLines(file.path(OUT, "b3_tract_iv_ddd.txt")), sep = "\n")
g <- ggplot(res, aes(y, coef, colour = sample)) + geom_hline(yintercept = 0, colour = "grey60") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey60") +
  geom_pointrange(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se), position = position_dodge(.5), size = .15) +
  facet_wrap(~ outcome, scales = "free_y", ncol = 2) + scale_x_continuous(breaks = seq(2013, 2024, 2)) +
  scale_colour_manual(values = c(all = "black", `high exposure` = "#e34a33", `low exposure` = "#2b8cbe")) +
  labs(x = NULL, y = "near x high-side effect (vs 2017)", colour = NULL, title = "Tracts with a cheap cross-border option: before vs after the SALT cap",
       subtitle = "Triple difference (near < 10 mi x high-tax side x year); tract, pair x side x year, pair x distance x year FE; 95% CI by county") +
  theme_minimal(base_size = 9) + theme(legend.position = "bottom")
ggsave(file.path(OUT, "figures/b3_tract_iv_ddd.png"), g, width = 10, height = 9, dpi = 150)
