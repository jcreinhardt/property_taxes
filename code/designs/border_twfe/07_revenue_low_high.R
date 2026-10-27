# Formerly code/analysis/local_juris/R/16_border_revenue_low_high.R (moved in the 2026-10-27 restructuring).
# Property-tax revenue on the lower- vs higher-tax side of each state border pair, 2013-2021.
# Revenue: 1% random parcel panel within 20 mi of the border (border1/border_panel_1pct, all parcels with a bill), total
# = 100 x sum of bills per side-year. Roll gaps would move totals, so keep only counties whose parcel count is >= 90% of
# their maximum in every year (constant-coverage counties). Ordering: each side's effective rate = median bill / price of
# arm's-length sales 2013-2017 in the per-side sample (bsample1024, same filters as R/13), fixed over time; the side with
# the lower rate is "lower-tax". Pairs need >= 100 such sales per side and revenue on both sides in all years.
suppressPackageStartupMessages({library(data.table); library(duckdb); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_twfe"); invisible(out_dir("designs/border_twfe/figures")); L <- file.path(DATA, "built")
con <- dbConnect(duckdb())
p <- as.data.table(dbGetQuery(con, sprintf("SELECT clip, y, fips, pair, st_a, max(bill) bill FROM '%s/border1/border_panel_1pct.parquet'
  WHERE bill > 0 GROUP BY ALL", D)))
s <- as.data.table(dbGetQuery(con, sprintf("SELECT * FROM '%s/bsample1024/border_sample_sales.parquet' WHERE y BETWEEN 2013 AND 2017", D)))
dbDisconnect(con, shutdown = TRUE)
NY <- uniqueN(p$y)
cty <- unique(p[, .(clip, fips, y)])[, .N, by = .(fips, y)][, .(ny = .N, ok = min(N) >= .9 * max(N)), by = fips]
keep <- cty[ny == NY & ok == TRUE, fips]
cat("counties:", nrow(cty), "| constant-coverage:", length(keep), "| parcel-years kept:", round(mean(p$fips %in% keep), 3), "\n")
rev <- p[fips %in% keep, .(rev = 100 * sum(bill), n = 100 * .N, mbill = mean(bill)), by = .(pair, st_a, y)]
s <- s[price > 10000 & bill > 0 & !newcon][bill / price >= .0005 & bill / price <= .10]
er <- s[, .(n_sales = .N, eff = median(bill / price)), by = .(pair, st_a)][n_sales >= 100]
er[, k := .N, by = pair]; er <- er[k == 2][order(pair, eff)][, side := c("lower-tax side", "higher-tax side"), by = pair]
d <- merge(rev, er[, .(pair, st_a, eff, side)], by = c("pair", "st_a"))
fwrite(d, file.path(L, "corelogic/border_revenue_side_year.csv"))  # all pairs incl. unbalanced, for R/17
d[, full := .N == 2 * NY, by = pair]; d <- d[full == TRUE]
cat("pairs:", uniqueN(d$pair), "\n")
agg <- d[, .(mean_rev_m = mean(rev) / 1e6, med_rev_m = median(rev) / 1e6, parcels_k = mean(n) / 1e3, mean_bill = mean(mbill),
             eff_pct = 100 * mean(eff)), by = .(side, y)]
agg[, idx := 100 * mean_rev_m / mean_rev_m[y == 2017], by = side]
# per-pair ratio higher/lower and its change vs 2017
r <- dcast(d, pair + y ~ side, value.var = "rev")[, lr := log(`higher-tax side` / `lower-tax side`)]
r[, dlr := lr - lr[y == 2017], by = pair]
rr <- r[, .(mean_log_ratio = mean(lr), mean_change_vs_2017 = mean(dlr), se = sd(dlr) / sqrt(.N)), by = y]
fwrite(agg, file.path(OUT, "border_revenue_low_high.csv")); fwrite(er, file.path(OUT, "border_revenue_side_order.csv"))
sink(file.path(OUT, "border_revenue_low_high.txt"))
cat("pairs:", uniqueN(d$pair), "| constant-coverage counties:", length(keep), "of", nrow(cty), "\n\n")
print(as.data.frame(dcast(agg, y ~ side, value.var = c("mean_rev_m", "idx", "parcels_k", "mean_bill"))[, lapply(.SD, round, 1)]), row.names = FALSE)
cat("\nlog(higher / lower revenue), mean across pairs, and change vs 2017:\n"); print(as.data.frame(rr[order(y), lapply(.SD, round, 3)]), row.names = FALSE)
sink(); cat(readLines(file.path(OUT, "border_revenue_low_high.txt")), sep = "\n")
pl <- rbind(agg[, .(side, y, v = mean_rev_m, panel = "mean total revenue per side ($m)")],
            agg[, .(side, y, v = idx, panel = "index, 2017 = 100")],
            agg[, .(side, y, v = mean_bill, panel = "mean bill per parcel ($)")])
pl[, panel := factor(panel, unique(panel))]
g <- ggplot(pl, aes(y, v, colour = side)) + geom_line() + geom_point(size = 1.2) +
  geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey60") + facet_wrap(~ panel, ncol = 1, scales = "free_y") +
  scale_x_continuous(breaks = 2013:2021) + scale_colour_manual(values = c("lower-tax side" = "#2b8cbe", "higher-tax side" = "#e34a33")) +
  labs(x = NULL, y = NULL, colour = NULL, title = "Property-tax revenue within 20 mi of state borders: lower- vs higher-tax side",
       subtitle = sprintf("%d pairs; sides ordered by 2013-17 median effective rate (bill / price);\n1%% parcel panel x 100, counties with constant roll coverage",
                          uniqueN(d$pair))) +
  theme_minimal(base_size = 10) + theme(legend.position = "bottom")
ggsave(file.path(OUT, "figures/border_revenue_low_high.png"), g, width = 7.5, height = 8, dpi = 150)
