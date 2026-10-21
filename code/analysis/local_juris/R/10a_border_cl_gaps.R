# CL-provided rates only (fast version of 10_border_rate_gaps.R): border parcels (<= 20 mi; 1% building sample), 2013-2021.
# Unit per state-year (fraction, percent or mills) = the one closer to the median bill/AV; state-years > 5x off bill/AV are unusable.
# Coverage, state-pair x year side means, |A - B|, boxplot across pairs by year.
suppressPackageStartupMessages({library(data.table); library(duckdb); library(ggplot2)})
ROOT <- "/Users/jr2728/PhD/projects/property_taxes"; IN <- file.path(ROOT, "data/local_juris/border1")
OUT <- file.path(ROOT, "output/analysis/local_juris"); dir.create(file.path(OUT, "figures"), showWarnings = FALSE)
con <- dbConnect(duckdb()); b <- as.data.table(dbGetQuery(con, sprintf("SELECT * FROM '%s/border_panel_1pct.parquet'", IN))); dbDisconnect(con, shutdown = TRUE)
u <- b[rate_pct > 0 & bill > 0 & av > 0, .(m_rate = median(rate_pct), m_ba = median(bill / av), n_cl = .N), by = .(st, y)]
u[, div := c(1, 100, 1000)[apply(abs(log(outer(m_rate, c(1, 100, 1000), "/") / m_ba)), 1, which.min)]]   # fraction (VA), percent, mills
u[, usable := abs(log(m_rate / div / m_ba)) <= log(5)]
b <- merge(b, u[, .(st, y, div, usable)], by = c("st", "y"), all.x = TRUE)
b[, cl := fifelse(rate_pct > 0 & usable %in% TRUE, rate_pct / div, NA_real_)][!(cl > .0001 & cl < .5), cl := NA]
fwrite(u[order(st, y)], file.path(OUT, "border_cl_rate_units.csv"))
pu <- unique(b[, .(clip, y, st, cl)])
cov_y <- pu[, .(parcels = .N, cl_pct = round(100 * mean(!is.na(cl)), 1)), by = y][order(y)]
cov_st <- pu[, .(cl_pct = round(100 * mean(!is.na(cl)), 1)), by = .(st, y)] |> dcast(st ~ y, value.var = "cl_pct")
cov_st <- cov_st[cov_st[, rowSums(.SD, na.rm = TRUE) > 0, .SDcols = -1]]
MIN <- 30
side <- b[, .(n = sum(!is.na(cl)), cl = mean(cl, na.rm = TRUE)), by = .(pair, st_a, y)][n >= MIN]
side[, k := seq_len(.N), by = .(pair, y)]
w <- dcast(side, pair + y ~ k, value.var = c("st_a", "cl", "n"))
gaps <- w[!is.na(cl_2), .(pair, y, st_1 = st_a_1, rate_1 = round(100 * cl_1, 3), st_2 = st_a_2, rate_2 = round(100 * cl_2, 3), n_1, n_2,
                          gap_pp = 100 * abs(cl_1 - cl_2))]
fwrite(gaps, file.path(OUT, "border_pair_gaps_cl.csv"))
g <- ggplot(gaps, aes(factor(y), gap_pp)) + geom_boxplot(outlier.shape = NA, fill = "grey90", width = .6) +
  geom_jitter(width = .12, height = 0, size = 1.2, alpha = .6) +
  labs(x = NULL, y = "absolute gap in rate between sides, pp",
       title = "CoreLogic-reported property-tax rates: gaps across state borders",
       subtitle = sprintf("Parcels within 20 miles of the border; state pairs with >= %d sample parcels with a CL rate\non each side (1%% building sample); each dot is a state pair", MIN)) +
  theme_minimal(base_size = 10)
ggsave(file.path(OUT, "figures/border_rate_gaps_cl_box.png"), g, width = 7.5, height = 4.5, dpi = 150)
sink(file.path(OUT, "border_rate_gaps_cl.txt"))
cat("Border parcels (<= 20 mi), 1% sample: % with a usable CL-provided rate, by year\n"); print(as.data.frame(cov_y), row.names = FALSE)
cat("\nBy state and year (states with any usable CL rate):\n"); print(as.data.frame(cov_st), row.names = FALSE)
cat("\nUnusable state-years (normalised CL rate > 5x off median bill/AV):\n"); print(as.data.frame(u[usable == FALSE, .(st, y, m_rate, m_ba = round(m_ba, 4), n_cl)]), row.names = FALSE)
cat("\nPairs with a gap, by year:\n"); print(as.data.frame(gaps[, .(pairs = .N, median_gap_pp = round(median(gap_pp), 2), max_gap_pp = round(max(gap_pp), 2)), by = y][order(y)]), row.names = FALSE)
cat("\nPair gaps, 2016:\n"); print(as.data.frame(gaps[y == 2016][order(-gap_pp)]), row.names = FALSE)
sink(); cat(readLines(file.path(OUT, "border_rate_gaps_cl.txt")), sep = "\n")
