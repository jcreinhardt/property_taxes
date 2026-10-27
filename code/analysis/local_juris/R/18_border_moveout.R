# Out-migration on the lower- vs higher-tax side of state borders, from Data Axle (Bouchet: bouchet/g_moveout_bkey_1pct.sql).
# Unit: residential address (1% building sample, tax-roll coordinates) within 20 mi of a shared border, per side
# (border20_sides.geojson). Move-out t-1 -> t = the primary family_id at the address changes between the two annual files;
# main measure on addresses with one primary family and Data Axle records in both files (no backfill), as in dataaxle.md 4.5c.
# Year = destination file t (2013 = 2012->13 ... 2021 = 2020->21). Files are not 12 months apart, so rates are annualised by
# elapsed months between cut-offs (2012-03, 2013-03, 2014-03, 2015-04, 2016-06, 2017-05, 2018-07, 2019-06, 2020-06, 2021-06).
# Sides ordered as in R/16 (2013-17 median effective rate of sales, >= 100 sales per side).
# TWFE: side-year rate on side FE + pair x year FE + year x lower-side dummies (2017 = 0), SE by pair; and each side alone.
suppressPackageStartupMessages({library(data.table); library(duckdb); library(sf); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- file.path(ROOT, "output/analysis/local_juris"); D <- file.path(ROOT, "data/local_juris")
IN <- Sys.getenv("MOVE_IN", file.path(D, "border_moveout/moveout_bkey_1pct.parquet")); SFX <- Sys.getenv("MOVE_SFX", "_1pct"); MIN <- 50
con <- dbConnect(duckdb()); m <- as.data.table(dbGetQuery(con, sprintf("SELECT * FROM '%s'", IN))); dbDisconnect(con, shutdown = TRUE)
m[, y := y + 1L]  # label by destination file
cut <- c(`2012` = 3, `2013` = 3 + 12, `2014` = 3 + 24, `2015` = 4 + 36, `2016` = 6 + 48, `2017` = 5 + 60, `2018` = 7 + 72,
         `2019` = 6 + 84, `2020` = 6 + 96, `2021` = 6 + 108)
mon <- data.table(y = 2013:2021, months = diff(cut))
# assign addresses to border sides (an address near a tri-point can sit in two pairs)
loc <- unique(m[, .(bkey, lat, lon)])
sides <- st_read(file.path(D, "border20_sides.geojson"), quiet = TRUE)
pts <- st_as_sf(loc, coords = c("lon", "lat"), crs = 4326)
j <- st_join(pts, sides[, c("pair", "st_a")], join = st_within, left = FALSE)
bs <- data.table(bkey = j$bkey, pair = j$pair, st_a = j$st_a)
cat("addresses:", nrow(loc), "| in a border side:", uniqueN(bs$bkey), "\n")
er <- fread(file.path(OUT, "border_revenue_side_order.csv"))[, .(pair, st_a, eff, side)]
x <- merge(merge(m, bs, by = "bkey", allow.cartesian = TRUE), er, by = c("pair", "st_a"))
sy <- rbind(x[both_obs & one_prim, .(n = .N, raw = mean(id_chg)), by = .(pair, st_a, side, y)][, sample := "clean"],
            x[, .(n = .N, raw = mean(id_chg)), by = .(pair, st_a, side, y)][, sample := "backfilled"],
            x[both_obs & one_prim & oo_sfr, .(n = .N, raw = mean(id_chg)), by = .(pair, st_a, side, y)][, sample := "clean, owner-occ. SFR"])
sy <- merge(sy, mon, by = "y")[, rate := 100 * raw * 12 / months][n >= MIN]
sy <- sy[, if (.N == 2) .SD, by = .(sample, pair, y)]
sy[, `:=`(low = as.integer(side == "lower-tax side"), sid = paste(pair, st_a))]
fwrite(sy, file.path(D, paste0("border_moveout_side_year", SFX, ".csv")))
agg <- sy[, .(pairs = uniqueN(pair), addresses = sum(n), mean_rate = mean(rate), wmean_rate = weighted.mean(rate, n)), by = .(sample, side, y)]
tidy <- function(f, lab, smp) { ct <- as.data.table(coeftable(f), keep.rownames = "term")
  rbind(data.table(y = 2017L, coef = 0, se = 0), ct[, .(y = as.integer(sub("^y::(\\d+).*", "\\1", term)), coef = Estimate, se = `Std. Error`)])[,
    `:=`(spec = lab, sample = smp)] }
tw <- rbindlist(lapply(unique(sy$sample), \(s) { d <- sy[sample == s]; rbind(
  tidy(feols(rate ~ i(y, low, ref = 2017) | sid + pair^y, d, cluster = ~pair), "lower minus higher side (pair x year FE)", s),
  tidy(feols(rate ~ i(y, ref = 2017) | sid, d[low == 1], cluster = ~pair), "lower-tax side (side FE)", s),
  tidy(feols(rate ~ i(y, ref = 2017) | sid, d[low == 0], cluster = ~pair), "higher-tax side (side FE)", s)) }))
fwrite(agg, file.path(OUT, paste0("border_moveout", SFX, ".csv"))); fwrite(tw, file.path(OUT, paste0("border_moveout_twfe", SFX, ".csv")))
sink(file.path(OUT, paste0("border_moveout", SFX, ".txt")))
cat("Annualised move-out rate (% of addresses / yr), mean across pairs; year = destination file\n")
for (s in unique(agg$sample)) { cat("\n==", s, "\n")
  print(as.data.frame(dcast(agg[sample == s], y ~ side, value.var = c("mean_rate", "wmean_rate", "pairs", "addresses"))[, lapply(.SD, round, 2)]), row.names = FALSE) }
cat("\nTWFE (pp, 2017 = 0, SE by pair)\n")
for (k in split(tw, by = c("sample", "spec"))) { cat("\n==", k$sample[1], "|", k$spec[1], "\n")
  print(as.data.frame(k[, .(y, coef = round(coef, 2), se = round(se, 2))]), row.names = FALSE) }
sink(); cat(readLines(file.path(OUT, paste0("border_moveout", SFX, ".txt"))), sep = "\n")
col <- c("lower-tax side" = "#2b8cbe", "higher-tax side" = "#e34a33", "lower minus higher side (pair x year FE)" = "black",
         "lower-tax side (side FE)" = "#2b8cbe", "higher-tax side (side FE)" = "#e34a33")
a <- agg[sample == "clean"]; t <- tw[sample == "clean" & spec == "lower minus higher side (pair x year FE)"]
g1 <- ggplot(a, aes(y, mean_rate, colour = side)) + geom_line() + geom_point(size = 1.2) +
  geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey60") + scale_colour_manual(values = col) +
  scale_x_continuous(breaks = 2013:2021) + labs(x = NULL, y = "move-out rate (%/yr)", colour = NULL,
  title = "Out-migration near state borders: lower- vs higher-tax side",
  subtitle = sprintf("Primary family at address changes (Data Axle, annualised); %d pairs; 1%% building sample within 20 mi",
                     uniqueN(sy[sample == "clean", pair]))) + theme_minimal(base_size = 10) + theme(legend.position = "bottom")
g2 <- ggplot(t, aes(y, coef)) + geom_hline(yintercept = 0, colour = "grey60") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey60") +
  geom_pointrange(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se), size = .25) + scale_x_continuous(breaks = 2013:2021) +
  labs(x = "destination file year (2013 = 2012->13 move)", y = "pp vs 2017",
       subtitle = "TWFE: lower minus higher side, side FE + pair x year FE, 2017 = 0, 95% CI clustered by pair") + theme_minimal(base_size = 10)
ggsave(file.path(OUT, paste0("figures/border_moveout", SFX, ".png")), patchwork::wrap_plots(g1, g2, ncol = 1, heights = c(1.2, 1)),
       width = 7.5, height = 7.5, dpi = 150)
