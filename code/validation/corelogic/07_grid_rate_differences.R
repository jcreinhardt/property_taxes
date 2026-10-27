# Formerly code/analysis/local_juris/R/20_grid_rate_differences.R (moved in the 2026-10-27 restructuring).
# Local variation in effective property-tax rates across space. 5 km grid (EPSG:5070) of arm's-length home sales 2017-2019
# (cluster/h_grid_eff.sql: up to 50 sales per cell, effective rate = sale-year bill / price). Cell value = median effective
# rate over cells with >= MIN sales. For each cell and radius r in {10, 25, 50} mi: neighbours = cells whose centres lie
# within r; max |median_i - median_j| (the largest rate difference reachable within r) and the mean |difference|.
# Noise yardstick: SE of a cell median ~ 1.2533 (IQR / 1.349) / sqrt(n) (robust to heavy tails), so the max over many neighbours rises with r mechanically.
suppressPackageStartupMessages({library(data.table); library(duckdb); library(sf); library(ggplot2); library(patchwork)})
source("code/R/paths.R"); OUT <- Sys.getenv("GRID_OUT", out_dir("validation/corelogic")); D <- Sys.getenv("GRID_DIR", file.path(DATA, "built/corelogic/grid5k"))
TIG <- path.expand("~/Library/Caches/tigris"); MIN <- as.integer(Sys.getenv("GRID_MIN", "20")); AGG <- as.integer(Sys.getenv("GRID_AGG", "1")); CELL <- 5000 * AGG; MI <- 1609.344
NOISE <- Sys.getenv("GRID_NOISE", "0") == "1"  # benchmark: replace each cell median by pure sampling noise N(0, SE)
SFX <- sprintf("_%dkm_min%d", 5 * AGG, MIN); if (NOISE) SFX <- paste0(SFX, "_noise")  # GRID_AGG merges AGG x AGG blocks of 5 km cells into larger cells
con <- dbConnect(duckdb()); s <- as.data.table(dbGetQuery(con, sprintf("SELECT * FROM '%s/grid_sales.parquet'", D))); dbDisconnect(con, shutdown = TRUE)
s[, `:=`(cx = cx %/% AGG, cy = cy %/% AGG)]
g <- s[, .(n = .N, med = 100 * median(eff), se = 100 * 1.2533 * IQR(eff) / 1.349 / sqrt(.N), st = names(which.max(table(stf)))), by = .(cx, cy)][n >= MIN]
if (NOISE) { set.seed(1); g[, med := rnorm(.N, 0, se)] }
cat("cells with >=", MIN, "sales:", nrow(g), "| sales in them:", sum(g$n), "| median SE of cell median (pp):", round(median(g$se), 3), "\n")
setkey(g, cx, cy)
res <- rbindlist(lapply(c(10, 25, 50), \(r) {
  R <- r * MI / CELL; k <- ceiling(R)
  off <- CJ(dx = -k:k, dy = -k:k)[dx^2 + dy^2 <= R^2 & !(dx == 0 & dy == 0)]
  acc <- g[, .(cx, cy, med, mx = 0, sm = 0, nn = 0L)]
  for (i in seq_len(nrow(off))) {
    nb <- g[, .(cx = cx - off$dx[i], cy = cy - off$dy[i], m2 = med)]
    j <- nb[acc, on = .(cx, cy), nomatch = NULL][, .(cx, cy, d = abs(m2 - med))]
    acc[j, on = .(cx, cy), `:=`(mx = pmax(mx, i.d), sm = sm + i.d, nn = nn + 1L)]
  }
  acc[, .(cx, cy, radius = r, max_diff = fifelse(nn > 0, mx, NA_real_), mean_diff = fifelse(nn > 0, sm / nn, NA_real_), n_nb = nn)] }))
res <- merge(res, g[, .(cx, cy, med, n, se, st)], by = c("cx", "cy"))
fwrite(res, file.path(OUT, paste0("grid_rate_differences", SFX, ".csv")))
sink(file.path(OUT, paste0("grid_rate_differences", SFX, ".txt")))
cat(5 * AGG, "km cells, >=", MIN, "sales (2017-19), median effective rate in %; |differences| in pp\n")
cat("cells:", nrow(g), "| median SE of a cell median:", round(median(g$se), 3), "pp | cell medians p10/p50/p90:",
    round(quantile(g$med, c(.1, .5, .9)), 2), "\n\n")
print(as.data.frame(res[n_nb > 0, .(cells = .N, nb_median = as.numeric(median(n_nb)),
  max_p10 = quantile(max_diff, .1), max_p50 = median(max_diff), max_p90 = quantile(max_diff, .9),
  mean_p50 = median(mean_diff), mean_p90 = quantile(mean_diff, .9)), by = radius][, lapply(.SD, round, 2)]), row.names = FALSE)
cat("\nBy state, radius 25 mi: median of cell max_diff (pp), top 15\n")
print(as.data.frame(res[radius == 25 & n_nb > 0, .(cells = .N, max_diff = round(median(max_diff), 2), mean_diff = round(median(mean_diff), 2)), by = st][
  cells >= 30][order(-max_diff)] |> head(15)), row.names = FALSE)
sink(); cat(readLines(file.path(OUT, paste0("grid_rate_differences", SFX, ".txt"))), sep = "\n")
states <- st_transform(st_read(file.path(TIG, "cb_2020_us_state_500k.shp"), quiet = TRUE), 5070)
states <- states[!states$STUSPS %in% c("AK", "HI", "PR", "VI", "GU", "MP", "AS"), ]
res[, `:=`(x = (cx + .5) * CELL, y = (cy + .5) * CELL)]
lower48 <- res[x > -2.5e6 & x < 2.4e6 & y > 2.0e5 & y < 3.3e6]
mp <- function(d, v, lab, cap) ggplot() + geom_sf(data = states, fill = "grey95", colour = NA) +
  geom_tile(data = d, aes(x, y, fill = pmin(get(v), cap)), width = CELL, height = CELL) +
  geom_sf(data = states, fill = NA, colour = "grey30", linewidth = .15) +
  scale_fill_viridis_c(option = "magma", direction = -1, limits = c(0, cap), name = lab) +
  coord_sf(crs = 5070, datum = NA) + theme_void(base_size = 9) + theme(legend.position = "right")
m0 <- mp(lower48[radius == 10], "med", "median\neff. rate\n(%)", 3) + labs(title = sprintf("Median effective tax rate, %d km cells with >= %d sales (2017-19)", 5 * AGG, MIN))
ps <- lapply(c(10, 25, 50), \(r) mp(lower48[radius == r & n_nb > 0], "max_diff", "max abs.\ndifference\n(pp)", 2.5) +
  labs(title = sprintf("Max difference to any cell within %d mi", r)))
ggsave(file.path(OUT, paste0("figures/grid_rate_differences", SFX, ".png")), wrap_plots(c(list(m0), ps), ncol = 2), width = 14, height = 8.5, dpi = 200)
for (r in c(10, 25, 50)) ggsave(file.path(OUT, sprintf("figures/grid_max_diff_%dmi%s.png", r, SFX)), ps[[match(r, c(10, 25, 50))]], width = 10, height = 6, dpi = 250)
