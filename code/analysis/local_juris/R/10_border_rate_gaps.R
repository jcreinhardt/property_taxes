# Border parcels (<= 20 mi of a shared state border; 1% building sample), 2013-2021:
#  (a) CoreLogic-provided rate: total_property_tax_rate_percent, unit (fraction, percent or mills) chosen per state-year as the one
#      closer to the median bill/AV; state-years whose normalised rate is > 5x off the median bill/AV are treated as unusable.
#  (b) imputed nominal rate: median regression bill ~ a + r*AV per tax-area cell x year on the non-homestead fit sample
#      (<= 500 parcels from the full roll, >= 30, >= 50% of bills within 1% of the line), else the pooled cell fit if it passes.
#      Market-value version: r x median(AV/MV) x sales ratio (cell-year, else county-year); kept if 0.05-6%.
# Coverage shares, state-pair x year side means, |side A - side B|, and boxplots across pairs by year.
suppressPackageStartupMessages({library(data.table); library(duckdb); library(quantreg); library(ggplot2)})
source("code/R/paths.R"); IN <- file.path(ROOT, "data/local_juris/border1")
OUT <- file.path(ROOT, "output/analysis/local_juris"); dir.create(file.path(OUT, "figures"), showWarnings = FALSE)
con <- dbConnect(duckdb()); q <- \(s) as.data.table(dbGetQuery(con, s))
bp <- q(sprintf("SELECT * FROM '%s/border_panel_1pct.parquet'", IN))
fs <- q(sprintf("SELECT * FROM '%s/fitsample_border.parquet'", IN))
ra <- q(sprintf("SELECT * FROM '%s/ratio_border.parquet'", IN))
dbDisconnect(con, shutdown = TRUE)

# ---- (b) fits ----
f <- function(bill, av) {
  r <- tryCatch(coef(rq.fit(cbind(1, av), bill, tau = .5, method = "br")), error = function(e) c(NA_real_, NA_real_))
  list(a = r[1], slope = r[2], on1 = mean(abs(bill - (r[1] + r[2] * av)) <= .01 * bill))
}
t0 <- Sys.time()
fn <- fs[hs == FALSE, f(bill, av), by = .(cell, y)]
fp <- fs[, f(bill, av), by = .(cell, y)]
cat("fits:", nrow(fn), "non-homestead,", nrow(fp), "pooled;", round(as.numeric(difftime(Sys.time(), t0, units = "mins")), 1), "min\n")
avmv <- fs[hs == FALSE & mv > 0, .(avmv = median(av / mv)), by = .(cell, y)]
rates <- merge(fn[, .(cell, y, s_non = slope, on_non = on1)], fp[, .(cell, y, s_all = slope, on_all = on1)], by = c("cell", "y"), all = TRUE)
rates[, imp := fcase(on_non >= .5 & s_non > 0 & s_non < .5, s_non, on_all >= .5 & s_all > 0 & s_all < .5, s_all)]
rates <- merge(rates, avmv, by = c("cell", "y"), all.x = TRUE)
rates[, fips := substr(cell, 1, 5)]
rates <- merge(rates, ra[lvl == "cell", .(cell, y, r_cell = ratio)], by = c("cell", "y"), all.x = TRUE)
rates <- merge(rates, ra[lvl == "county", .(fips, y, r_cty = ratio)], by = c("fips", "y"), all.x = TRUE)
rates[, imp_mv := imp * avmv * fcoalesce(r_cell, r_cty)]
rates[!(imp_mv >= .0005 & imp_mv <= .06), imp_mv := NA]

# ---- (a) CL rate units ----
b <- merge(bp, rates[, .(cell, y, imp, imp_mv)], by = c("cell", "y"), all.x = TRUE)
u <- b[rate_pct > 0 & bill > 0 & av > 0, .(m_rate = median(rate_pct), m_ba = median(bill / av), n_cl = .N), by = .(st, y)]
u[, div := c(1, 100, 1000)[apply(abs(log(outer(m_rate, c(1, 100, 1000), "/") / m_ba)), 1, which.min)]]   # fraction (VA), percent, mills
u[, usable := abs(log(m_rate / div / m_ba)) <= log(5)]
b <- merge(b, u[, .(st, y, div, usable)], by = c("st", "y"), all.x = TRUE)
b[, cl := fifelse(rate_pct > 0 & usable %in% TRUE, rate_pct / div, NA_real_)]
b[!(cl > .0001 & cl < .5), cl := NA]
fwrite(u[order(st, y)], file.path(OUT, "border_cl_rate_units.csv"))

# ---- coverage (unique parcel-years; a parcel near two pairs counts once) ----
pu <- unique(b[, .(clip, y, st, cl, imp, imp_mv)])
cov_y <- pu[, .(parcels = .N, cl_pct = round(100 * mean(!is.na(cl)), 1), imputed_pct = round(100 * mean(!is.na(imp)), 1),
                either_pct = round(100 * mean(!is.na(cl) | !is.na(imp)), 1), imputed_mv_pct = round(100 * mean(!is.na(imp_mv)), 1)), by = y][order(y)]
cov_st <- pu[y == 2016, .(parcels = .N, cl_pct = round(100 * mean(!is.na(cl)), 1), imputed_pct = round(100 * mean(!is.na(imp)), 1),
                          either_pct = round(100 * mean(!is.na(cl) | !is.na(imp)), 1)), by = st][order(-parcels)]

# ---- pair gaps ----
MIN <- 30   # parcels with a rate per side in the 1% sample (~3,000 real parcels)
side <- b[, .(n_cl = sum(!is.na(cl)), cl = mean(cl, na.rm = TRUE), n_imp = sum(!is.na(imp)), imp = mean(imp, na.rm = TRUE),
              n_mv = sum(!is.na(imp_mv)), imp_mv = mean(imp_mv, na.rm = TRUE)), by = .(pair, st_a, y)]
gap <- function(v, n) {
  s <- side[get(n) >= MIN, .(pair, st_a, y, v = get(v))]
  s[, k := seq_len(.N), by = .(pair, y)]
  w <- dcast(s, pair + y ~ k, value.var = "v")
  if (!"2" %in% names(w)) return(NULL)
  w[!is.na(`2`), .(pair, y, measure = v, gap_pp = 100 * abs(`1` - `2`))]
}
gaps <- rbind(gap("cl", "n_cl"), gap("imp", "n_imp"), gap("imp_mv", "n_mv"))
gaps[, measure := factor(measure, c("cl", "imp", "imp_mv"),
  c("(a) CoreLogic-provided rate (% of AV)", "(b) imputed nominal rate (% of AV)", "(b') imputed rate, % of market value"))]
fwrite(gaps, file.path(OUT, "border_pair_gaps.csv")); fwrite(side, file.path(OUT, "border_pair_sides.csv"))
pairs_y <- gaps[, .(pairs = .N, median_gap_pp = round(median(gap_pp), 2), mean_gap_pp = round(mean(gap_pp), 2)), by = .(measure, y)][order(measure, y)]

g <- ggplot(gaps, aes(factor(y), gap_pp)) + geom_boxplot(outlier.size = .6, fill = "grey90") +
  facet_wrap(~ measure, ncol = 1, scales = "free_y") +
  labs(x = NULL, y = "absolute gap in rate between sides, pp",
       title = "Property-tax rate gaps across state borders (parcels within 20 miles)",
       subtitle = sprintf("State pairs with >= %d sample parcels with a rate on each side; 1%% building sample of CoreLogic", MIN)) +
  theme_minimal(base_size = 10)
ggsave(file.path(OUT, "figures/border_rate_gaps_box.png"), g, width = 7.5, height = 8.5, dpi = 150)

sink(file.path(OUT, "border_rate_gaps.txt"))
cat("Border parcels (<= 20 mi), 1% sample. Coverage by year (% of border parcel-years):\n"); print(as.data.frame(cov_y), row.names = FALSE)
cat("\nCoverage by state, 2016:\n"); print(as.data.frame(cov_st), row.names = FALSE)
cat("\nCL rate units by state-year: see border_cl_rate_units.csv; usable state-years:", sum(u$usable), "of", nrow(u), "\n")
cat("\nPair gaps (pp) by year:\n"); print(as.data.frame(pairs_y), row.names = FALSE)
sink(); cat(readLines(file.path(OUT, "border_rate_gaps.txt")), sep = "\n")
