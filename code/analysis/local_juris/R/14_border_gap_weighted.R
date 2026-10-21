# Mean state-border effective-rate gap by year, weighted by the pair's border population: number of residential parcels
# within 20 mi on both sides in the 1% building sample (2016; data/local_juris/border_parcels_1pct.parquet), fixed over years.
# Gaps from the per-side sample (R/13). All pairs observed in the year, and the balanced panel; unweighted for comparison.
suppressPackageStartupMessages({library(data.table); library(duckdb); library(ggplot2)})
ROOT <- "/Users/jr2728/PhD/projects/property_taxes"; OUT <- file.path(ROOT, "output/analysis/local_juris")
SFX <- Sys.getenv("BORDER_SFX", "")
con <- dbConnect(duckdb())
w <- as.data.table(dbGetQuery(con, sprintf("SELECT pair, count(*) w FROM '%s/data/local_juris/border_parcels_1pct.parquet' GROUP BY pair", ROOT)))
dbDisconnect(con, shutdown = TRUE)
g <- merge(fread(file.path(OUT, paste0("border_sample_gaps_effective", SFX, ".csv"))), w, by = "pair", all.x = TRUE)
cat("pairs without a 1% weight:", g[is.na(w), uniqueN(pair)], "\n"); g[is.na(w), w := 0]
m <- rbindlist(lapply(c("all pairs", "balanced"), \(p) {
  x <- if (p == "all pairs") g else g[balanced == TRUE]
  x[, .(panel = if (p == "balanced") sprintf("balanced (%d pairs)", uniqueN(x$pair)) else p, pairs = .N, gap_w = weighted.mean(gap_pp, w), gap_u = mean(gap_pp), rel_w = weighted.mean(rel, w), rel_u = mean(rel),
        level_w = weighted.mean((eff_1 + eff_2) / 2, w)), by = y]
}))
fwrite(m, file.path(OUT, paste0("border_gap_weighted_means", SFX, ".csv")))
long <- melt(m, id.vars = c("panel", "y"), measure.vars = c("gap_w", "gap_u", "rel_w", "rel_u"))
long[, `:=`(outcome = fifelse(variable %like% "gap", "Mean absolute gap, percentage points", "Mean absolute gap / pair mean effective rate"),
            weight = fifelse(variable %like% "_w", "weighted by border population", "unweighted"))]
p <- ggplot(long, aes(y, value, colour = weight, linetype = panel)) + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey70") +
  geom_line() + geom_point(size = 1.2) + facet_wrap(~ factor(outcome, c("Mean absolute gap, percentage points", "Mean absolute gap / pair mean effective rate")), ncol = 1, scales = "free_y") + scale_x_continuous(breaks = min(g$y):max(g$y)) +
  labs(x = NULL, y = NULL, colour = NULL, linetype = NULL, title = "Effective property-tax rate gaps across state borders, by year",
       subtitle = "Homes within 20 miles; weights = pair's homes within 20 miles in the 1% sample (2016)") +
  theme_minimal(base_size = 10) + theme(legend.position = "bottom", legend.box = "vertical")
ggsave(file.path(OUT, paste0("figures/border_gap_weighted_means", SFX, ".png")), p, width = 7.5, height = 6.5, dpi = 150)
print(m[, lapply(.SD, \(v) if (is.numeric(v)) round(v, 3) else v)][order(panel, y)])
cat("\nTop weights (share of border population):\n"); tot <- sum(w[["w"]]); print(w[order(-w)][1:10][, share := round(100 * w / tot, 1)][])
