# Range of own-minus-neighbour tax-rate differences by signed distance to the nearest state border (- = low-tax side,
# + = high-tax side; sides as in local_juris/R/26). For each tract: the differences own median - neighbour median over all
# tracts reachable within 30 min drive (CSDS matrix; any-sales tract medians, as in R/22 with TRACT_MIN=1), summarised as
# min (own - most expensive), p10, mean, p90, max (own - cheapest = gap_min). Then averaged over tracts in 2.5-mile bins,
# separately for all neighbours, same-state neighbours and cross-state neighbours. Tracts with no neighbour of a type are
# dropped from that panel (not zero-filled), so the cross-state panel thins out away from the border.
suppressPackageStartupMessages({library(data.table); library(duckdb); library(ggplot2)})
source("code/R/paths.R"); OUT <- file.path(ROOT, "output/analysis/salt_local"); TT <- 30; BW <- 2.5
d <- readRDS(file.path(ROOT, "data/local_juris/iv_tract_side.rds"))
d <- unique(d[, .(tract, st, pair, high, dist_mi)])[, sd_mi := ifelse(high == 1, dist_mi, -dist_mi)][abs(sd_mi) <= 50]
con <- dbConnect(duckdb())
t <- as.data.table(dbGetQuery(con, sprintf("SELECT tract, 100 * med AS med FROM '%s/data/local_juris/tract/tract_eff.parquet'", ROOT)))
duckdb_register(con, "t", t); duckdb_register(con, "o", d[, .(tract)])
p <- as.data.table(dbGetQuery(con, sprintf("SELECT m.o, m.d, a.med - b.med AS diff, substr(m.o, 1, 2) = substr(m.d, 1, 2) AS same
  FROM '%s/data/external/csds_ttm/tract_drive_le90.parquet' m JOIN o ON o.tract = m.o JOIN t a ON a.tract = m.o JOIN t b ON b.tract = m.d
  WHERE m.o <> m.d AND m.m <= %d", ROOT, TT)))
dbDisconnect(con, shutdown = TRUE)
summ <- function(x) x[, .(mn = min(diff), p10 = quantile(diff, .1), mean = mean(diff), p90 = quantile(diff, .9), mx = max(diff), k = .N), by = o]
s <- rbind(summ(p)[, nb := "all reachable tracts"], summ(p[same == TRUE])[, nb := "same-state tracts"],
           summ(p[same == FALSE])[, nb := "cross-state tracts"])
s <- merge(s, d, by.x = "o", by.y = "tract")
s[, bin := (floor(sd_mi / BW) + .5) * BW]
b <- s[, c(lapply(.SD, mean), n = .N, se_mean = sd(mean) / sqrt(.N)), by = .(nb, bin), .SDcols = c("mn", "p10", "mean", "p90", "mx", "k")]
b[, nb := factor(nb, c("all reachable tracts", "same-state tracts", "cross-state tracts"))]
fwrite(b[order(nb, bin)], file.path(OUT, "c7_gap_range_signed_distance.csv"))
g <- ggplot(b[n >= 30], aes(bin)) + geom_hline(yintercept = 0, colour = "grey60") + geom_vline(xintercept = 0) +
  geom_ribbon(aes(ymin = mn, ymax = mx, fill = "min to max"), alpha = .25) +
  geom_ribbon(aes(ymin = p10, ymax = p90, fill = "p10 to p90"), alpha = .45) +
  geom_line(aes(y = mean, colour = "mean")) + geom_point(aes(y = mean, colour = "mean"), size = .7) +
  facet_wrap(~ nb, ncol = 3) + scale_fill_manual(NULL, values = c("min to max" = "#7aa6d8", "p10 to p90" = "#2a64a8")) +
  scale_colour_manual(NULL, values = c(mean = "black")) + scale_x_continuous(breaks = seq(-50, 50, 10)) +
  annotate("text", x = -25, y = Inf, vjust = 1.5, label = "low-tax side", size = 3) + annotate("text", x = 25, y = Inf, vjust = 1.5, label = "high-tax side", size = 3) +
  labs(x = "signed distance to nearest state border (miles)", y = "own median rate - neighbour median rate (pp)",
       title = sprintf("Own tract's effective rate minus rates of tracts within %d min drive, by distance to the state border", TT),
       subtitle = sprintf("Tract-level min / p10 / mean / p90 / max of the differences, averaged over tracts in %.1f-mile bins (bins with >= 30 tracts). Above 0: neighbours are cheaper.", BW)) +
  theme_minimal(base_size = 9) + theme(legend.position = "bottom")
ggsave(file.path(OUT, "figures/c7_gap_range_signed_distance.png"), g, width = 12, height = 5, dpi = 150)
print(b[nb != "same-state tracts" & abs(bin) <= 11.25, .(nb, bin, mn = round(mn, 2), p10 = round(p10, 2), mean = round(mean, 2), p90 = round(p90, 2), mx = round(mx, 2), n)])
print(b[, .(mean_mean = round(weighted.mean(mean, n), 3), mx = round(weighted.mean(mx, n), 2), mn = round(weighted.mean(mn, n), 2)), by = .(nb, side = sign(bin), near = abs(bin) < 10)][order(nb, side, near)])
