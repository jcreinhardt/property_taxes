# Effective tax rates across state borders: tax bill / arm's-length sale price for border parcels (<= 20 mi of a shared state
# border; 1% building sample) that sell, 2013-2021. Bill of the same tax year as the sale (the pre-sale assessment; later
# bills can already reflect sale-based reassessment). New-construction sales dropped; effective rates kept if 0.05-10%.
# Side mean per state pair x year (>= MIN sales on each side), |A - B|, boxplot across pairs by year. Data: SALT panel
# (data/salt_cap/panel/panel.parquet: bill, price, sale by parcel-year), border assignment from R/09.
suppressPackageStartupMessages({library(data.table); library(duckdb); library(ggplot2)})
source("code/R/paths.R"); OUT <- file.path(ROOT, "output/analysis/local_juris"); MIN <- 30
con <- dbConnect(duckdb())
b <- as.data.table(dbGetQuery(con, sprintf("
  SELECT b.pair, b.st_a, p.clip, p.y, p.bill, p.price, p.sale = 1 AND p.price > 10000 AND p.bill > 0 AND NOT coalesce(p.newcon, false) usable
  FROM '%1$s/data/local_juris/border_parcels_1pct.parquet' b JOIN '%1$s/data/salt_cap/panel/panel.parquet' p USING (clip)
  WHERE p.y BETWEEN 2013 AND 2021 AND p.in_roll", ROOT)))
dbDisconnect(con, shutdown = TRUE)
b[, eff := fifelse(usable, bill / price, NA_real_)][!(eff >= .0005 & eff <= .10), eff := NA]
pu <- unique(b[, .(clip, y, eff)])
cov_y <- pu[, .(border_parcels = .N, with_eff_rate = sum(!is.na(eff)), pct = round(100 * mean(!is.na(eff)), 2)), by = y][order(y)]
side <- b[!is.na(eff), .(n = .N, eff = mean(eff), eff_med = median(eff)), by = .(pair, st_a, y)]
sides_ok <- side[n >= MIN]
sides_ok[, k := seq_len(.N), by = .(pair, y)]
w <- dcast(sides_ok, pair + y ~ k, value.var = c("st_a", "eff", "eff_med", "n"))
gaps <- w[!is.na(eff_2), .(pair, y, st_1 = st_a_1, eff_1 = round(100 * eff_1, 3), st_2 = st_a_2, eff_2 = round(100 * eff_2, 3), n_1, n_2,
                           gap_pp = 100 * abs(eff_1 - eff_2), gap_med_pp = 100 * abs(eff_med_1 - eff_med_2))]
fwrite(gaps, file.path(OUT, "border_pair_gaps_effective.csv"))
g <- ggplot(gaps, aes(factor(y), gap_pp)) + geom_boxplot(outlier.shape = NA, fill = "grey90", width = .6) +
  geom_jitter(width = .12, height = 0, size = 1, alpha = .5) +
  labs(x = NULL, y = "absolute gap in effective rate between sides, pp",
       title = "Effective property-tax rates (bill / sale price) across state borders",
       subtitle = sprintf("Arm's-length sales within 20 miles of the border, bill of the sale year; state pairs with >= %d sample\nsales on each side (1%% building sample); each dot is a state pair", MIN)) +
  theme_minimal(base_size = 10)
ggsave(file.path(OUT, "figures/border_rate_gaps_effective_box.png"), g, width = 7.5, height = 4.8, dpi = 150)
sink(file.path(OUT, "border_rate_gaps_effective.txt"))
cat("Border parcels (<= 20 mi) in the roll, 1% sample: parcels with an effective rate (sold that year, usable)\n"); print(as.data.frame(cov_y), row.names = FALSE)
cat("\nSides with >= ", MIN, " sales by year: ", paste(sides_ok[, .N, by = y][order(y)]$N, collapse = " "), " (of ", uniqueN(b$pair) * 2, " sides)\n", sep = "")
cat("\nPairs with a gap, by year (gap of side means; median-based gap in the last column):\n")
print(as.data.frame(gaps[, .(pairs = .N, median_gap_pp = round(median(gap_pp), 2), p75 = round(quantile(gap_pp, .75), 2), max_gap_pp = round(max(gap_pp), 2),
                             median_gap_of_medians = round(median(gap_med_pp), 2)), by = y][order(y)]), row.names = FALSE)
cat("\nLargest gaps, 2016:\n"); print(as.data.frame(gaps[y == 2016][order(-gap_pp)][1:15]), row.names = FALSE)
sink(); cat(readLines(file.path(OUT, "border_rate_gaps_effective.txt")), sep = "\n")
