# Formerly code/analysis/local_juris/R/21_border_vacancy.R (moved in the 2026-10-27 restructuring).
# USPS vacancy (CoreLogic situs DPV vacant flag) on the lower- vs higher-tax side of state borders (cluster/i_border_vacancy.sql).
# Same per-side sample as R/13 and R/19 (bsample1024).
# Vacancy = parcels flagged vacant / parcels with the flag filled, in %, per side-year; side-years need >= 1000 filled parcels
# and a fill rate >= 50%. Sides ordered as in R/16 (2013-17 median effective rate, >= 100 sales per side).
# TWFE: side-year rate on side FE + pair x year FE + year x lower-side dummies (2017 = 0), SE by pair; and each side alone.
suppressPackageStartupMessages({library(data.table); library(duckdb); library(fixest); library(ggplot2); library(patchwork)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_twfe"); invisible(out_dir("designs/border_twfe/figures")); D <- file.path(DATA, "built/corelogic/bsample1024")
Y0 <- 2013L; Y1 <- 2024L; MIN <- 1000
con <- dbConnect(duckdb())
r <- as.data.table(dbGetQuery(con, sprintf("SELECT * FROM '%s/border_sample_vacancy.parquet'", D)))
dbDisconnect(con, shutdown = TRUE)
er <- fread(file.path(OUT, "border_revenue_side_order.csv"))[, .(pair, st_a, eff, side)]
sy <- merge(r, er, by = c("pair", "st_a"))
sy <- sy[y >= Y0 & y <= Y1 & n_filled >= MIN & n_filled >= .5 * n_roll][, rate := 100 * n_vac / n_filled]
sy <- sy[, if (.N == 2) .SD, by = .(pair, y)]
sy[, `:=`(low = as.integer(side == "lower-tax side"), sid = paste(pair, st_a))]
sy[, balanced := uniqueN(y) == (Y1 - Y0 + 1), by = pair]
fwrite(sy, file.path(DATA, "built/corelogic/border_vacancy_side_year.csv"))
agg <- rbind(sy[, .(panel = "full", pairs = uniqueN(pair), mean_rate = mean(rate), wmean_rate = weighted.mean(rate, n_filled)), by = .(side, y)],
             sy[balanced == TRUE, .(panel = "balanced", pairs = uniqueN(pair), mean_rate = mean(rate), wmean_rate = weighted.mean(rate, n_filled)), by = .(side, y)])
tidy <- function(f, lab, p) { ct <- as.data.table(coeftable(f), keep.rownames = "term")
  rbind(data.table(y = 2017L, coef = 0, se = 0), ct[, .(y = as.integer(sub("^y::(\\d+).*", "\\1", term)), coef = Estimate, se = `Std. Error`)])[,
    `:=`(spec = lab, panel = p)] }
tw <- rbindlist(lapply(c("full", "balanced"), \(p) { d <- if (p == "full") sy else sy[balanced == TRUE]; rbind(
  tidy(feols(rate ~ i(y, low, ref = 2017) | sid + pair^y, d, cluster = ~pair), "lower minus higher side (pair x year FE)", p),
  tidy(feols(rate ~ i(y, ref = 2017) | sid, d[low == 1], cluster = ~pair), "lower-tax side (side FE)", p),
  tidy(feols(rate ~ i(y, ref = 2017) | sid, d[low == 0], cluster = ~pair), "higher-tax side (side FE)", p)) }))
fwrite(agg, file.path(OUT, "border_vacancy.csv")); fwrite(tw, file.path(OUT, "border_vacancy_twfe.csv"))
sink(file.path(OUT, "border_vacancy.txt"))
cat("USPS vacancy rate (% of parcels with the flag filled), mean across pairs\n")
for (p in c("full", "balanced")) { cat("\n==", p, "\n")
  print(as.data.frame(dcast(agg[panel == p], y ~ side, value.var = c("mean_rate", "wmean_rate", "pairs"))[, lapply(.SD, round, 2)]), row.names = FALSE) }
cat("\nTWFE (pp, 2017 = 0, SE by pair)\n")
for (k in split(tw, by = c("panel", "spec"))) { cat("\n==", k$panel[1], "|", k$spec[1], "\n")
  print(as.data.frame(dcast(k, . ~ y, value.var = "coef")[, -1][, lapply(.SD, round, 2)]), row.names = FALSE)
  print(as.data.frame(dcast(k, . ~ y, value.var = "se")[, -1][, lapply(.SD, round, 2)]), row.names = FALSE) }
sink(); cat(readLines(file.path(OUT, "border_vacancy.txt")), sep = "\n")
col <- c("lower-tax side" = "#2b8cbe", "higher-tax side" = "#e34a33")
g1 <- ggplot(agg[panel == "full"], aes(y, mean_rate, colour = side)) + geom_line() + geom_point(size = 1.2) +
  geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey60") + scale_colour_manual(values = col) +
  scale_x_continuous(breaks = Y0:Y1) + labs(x = NULL, y = "USPS vacant (% of parcels)", colour = NULL,
  title = "USPS vacancy near state borders: lower- vs higher-tax side",
  subtitle = sprintf("CoreLogic situs DPV vacant flag; %d pairs, per-side sample within 20 mi; mean across pairs", uniqueN(sy$pair))) +
  theme_minimal(base_size = 10) + theme(legend.position = "bottom")
g2 <- ggplot(tw[panel == "full" & spec == "lower minus higher side (pair x year FE)"], aes(y, coef)) + geom_hline(yintercept = 0, colour = "grey60") +
  geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey60") + geom_pointrange(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se), size = .25) +
  scale_x_continuous(breaks = Y0:Y1) + labs(x = NULL, y = "pp vs 2017",
  subtitle = "TWFE: lower minus higher side, side FE + pair x year FE, 2017 = 0, 95% CI clustered by pair") + theme_minimal(base_size = 10)
ggsave(file.path(OUT, "figures/border_vacancy.png"), wrap_plots(g1, g2, ncol = 1, heights = c(1.2, 1)), width = 7.5, height = 7.5, dpi = 150)
