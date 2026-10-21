# Border effective-rate gaps from the per-side sample: on each side of each state pair, max(5000, 1% of the 2016 residential
# parcels within 20 mi) parcels of the full roll (SOM job f_border_sample.sql). Effective rate = bill of the sale year /
# arm's-length price (price > 10k, no new construction, 0.05-10%). Side means per year (>= MIN sales), |A - B| per pair-year,
# boxplot by year, and gap_pt = a_p + b_t with 2017 = 0 (SE clustered by pair), full and balanced panel, in pp and relative.
suppressPackageStartupMessages({library(data.table); library(duckdb); library(fixest); library(ggplot2)})
ROOT <- "/Users/jr2728/PhD/projects/property_taxes"; OUT <- file.path(ROOT, "output/analysis/local_juris"); MIN <- 30
# window: env BORDER_IN (data dir under data/local_juris), BORDER_Y0/Y1, BORDER_SFX (output suffix); defaults = 2013-2021 run
IN <- file.path(ROOT, "data/local_juris", Sys.getenv("BORDER_IN", "bsample"))
Y0 <- as.integer(Sys.getenv("BORDER_Y0", "2013")); Y1 <- as.integer(Sys.getenv("BORDER_Y1", "2021")); SFX <- Sys.getenv("BORDER_SFX", "")
NY <- Y1 - Y0 + 1
con <- dbConnect(duckdb()); q <- \(s) as.data.table(dbGetQuery(con, s))
s <- q(sprintf("SELECT * FROM '%s/border_sample_sales.parquet'", IN)); sc <- q(sprintf("SELECT * FROM '%s/side_counts.parquet'", IN))
ro <- q(sprintf("SELECT * FROM '%s/border_sample_roll.parquet'", IN)); dbDisconnect(con, shutdown = TRUE)
s <- s[y >= Y0 & y <= Y1]; ro <- ro[y >= Y0 & y <= Y1]
s[, eff := fifelse(price > 10000 & bill > 0 & !newcon, bill / price, NA_real_)][!(eff >= .0005 & eff <= .10), eff := NA]
side <- s[!is.na(eff), .(n = .N, eff = mean(eff)), by = .(pair, st_a, y)]
cov <- merge(ro, side[, .(pair, st_a, y, n_sales = n)], by = c("pair", "st_a", "y"), all.x = TRUE)[is.na(n_sales), n_sales := 0]
ok <- side[n >= MIN][, k := seq_len(.N), by = .(pair, y)]
w <- dcast(ok, pair + y ~ k, value.var = c("st_a", "eff", "n"))
gaps <- w[!is.na(eff_2), .(pair, y, st_1 = st_a_1, eff_1 = 100 * eff_1, st_2 = st_a_2, eff_2 = 100 * eff_2, n_1, n_2, gap_pp = 100 * abs(eff_1 - eff_2))]
gaps[, rel := gap_pp / ((eff_1 + eff_2) / 2)][, balanced := uniqueN(y) == NY, by = pair]
fwrite(gaps, file.path(OUT, paste0("border_sample_gaps_effective", SFX, ".csv")))
res <- rbindlist(lapply(c("full", "balanced"), \(p) rbindlist(lapply(c("gap_pp", "rel"), \(v) {
  x <- if (p == "balanced") gaps[balanced == TRUE] else gaps
  f <- feols(as.formula(paste(v, "~ i(y, ref = 2017) | pair")), x, cluster = ~pair)
  ct <- as.data.table(coeftable(f), keep.rownames = "term")
  rbind(data.table(y = 2017L, coef = 0, se = 0), ct[, .(y = as.integer(sub("y::", "", term)), coef = Estimate, se = `Std. Error`)])[,
    `:=`(panel = p, outcome = v, pairs = uniqueN(x$pair), obs = nrow(x), base = mean(x[y == 2017][[v]]))]
}))))
fwrite(res, file.path(OUT, paste0("border_sample_twfe", SFX, ".csv")))
g1 <- ggplot(gaps, aes(factor(y), gap_pp)) + geom_boxplot(outlier.shape = NA, fill = "grey90", width = .6) +
  geom_jitter(width = .12, height = 0, size = .9, alpha = .5) +
  labs(x = NULL, y = "absolute gap in effective rate between sides, pp", title = "Effective property-tax rates across state borders (bill / sale price)",
       subtitle = sprintf("Per side max(5,000, 1%%) of homes within 20 miles (full roll); pairs with >= %d sales on each side; one dot per pair", MIN)) +
  theme_minimal(base_size = 10)
ggsave(file.path(OUT, paste0("figures/border_sample_gaps_box", SFX, ".png")), g1, width = 7.5, height = 4.6, dpi = 150)
g2 <- ggplot(res, aes(y, coef, colour = panel, shape = panel)) + geom_hline(yintercept = 0, colour = "grey60") +
  geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey60") +
  geom_pointrange(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se), position = position_dodge(width = .4), size = .3) +
  facet_wrap(~ factor(outcome, c("gap_pp", "rel"), c("absolute gap, percentage points", "absolute gap relative to the pair mean effective rate")), ncol = 1, scales = "free_y") +
  scale_x_continuous(breaks = Y0:Y1) +
  labs(x = NULL, y = "change vs 2017, pair FE", colour = NULL, shape = NULL, title = "State-border effective-rate gaps: year effects (2017 = 0)",
       subtitle = "Per-side sample max(5,000, 1%) of homes within 20 mi; 95% CIs clustered by pair") +
  theme_minimal(base_size = 10) + theme(legend.position = "bottom")
ggsave(file.path(OUT, paste0("figures/border_sample_twfe", SFX, ".png")), g2, width = 7.5, height = 6.5, dpi = 150)
sink(file.path(OUT, paste0("border_sample_twfe", SFX, ".txt")))
cat("Sides:", nrow(sc), " border parcels:", sum(sc$n_border), " sampled:", sum(sc$n_sample), "\n")
cat("Sales per side-year (sampled, usable): median", median(cov$n_sales), " p10", quantile(cov$n_sales, .1), " share of side-years >= MIN:", round(mean(cov$n_sales >= MIN), 2), "\n")
cat("\nPairs per year and gaps:\n"); print(as.data.frame(gaps[, .(pairs = .N, balanced = sum(balanced), median_gap_pp = round(median(gap_pp), 2), p75 = round(quantile(gap_pp, .75), 2)), by = y][order(y)]), row.names = FALSE)
for (k in split(res, by = "outcome")) { cat("\n== TWFE", k$outcome[1], "(2017 = 0)\n")
  print(as.data.frame(dcast(k, y ~ panel, value.var = c("coef", "se"))[, lapply(.SD, round, 3)]), row.names = FALSE)
  cat(sprintf("full: %d pairs, %d obs, 2017 mean %.3f | balanced: %d pairs, %d obs, 2017 mean %.3f\n",
      k[panel == "full"]$pairs[1], k[panel == "full"]$obs[1], k[panel == "full"]$base[1], k[panel == "balanced"]$pairs[1], k[panel == "balanced"]$obs[1], k[panel == "balanced"]$base[1])) }
sink(); cat(readLines(file.path(OUT, paste0("border_sample_twfe", SFX, ".txt"))), sep = "\n")
