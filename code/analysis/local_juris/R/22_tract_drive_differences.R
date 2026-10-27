# Local variation in effective property-tax rates between census tracts reachable by car. Tract medians from
# cluster/j_tract_eff.sql (sales 2017-19, 2010 tracts from cl.tax census_id). Drive times: UChicago CSDS tract-to-tract
# driving matrix (OpenTripPlanner, tracts within 100 km; data/external/csds_ttm, kept <= 90 min). For each tract with >= MIN
# sales and each threshold T in {15, 30, 60} min: neighbours = tracts with >= MIN sales reachable within T minutes (origin ->
# destination); max, 90th percentile and mean |median_i - median_j|. Noise benchmark (TRACT_NOISE=1): medians replaced by
# N(0, SE), SE = 1.2533 (IQR / 1.349) / sqrt(n).
suppressPackageStartupMessages({library(data.table); library(duckdb); library(sf); library(tigris); library(ggplot2); library(patchwork)})
options(tigris_use_cache = TRUE)
source("code/R/paths.R"); OUT <- file.path(ROOT, "output/analysis/local_juris"); D <- file.path(ROOT, "data/local_juris/tract")
MIN <- as.integer(Sys.getenv("TRACT_MIN", "30")); NOISE <- Sys.getenv("TRACT_NOISE", "0") == "1"; TS <- c(15, 30, 60)
SFX <- paste0("_min", MIN, if (NOISE) "_noise" else "")
con <- dbConnect(duckdb())
t <- as.data.table(dbGetQuery(con, sprintf("SELECT * FROM '%s/tract_eff.parquet' WHERE n >= %d", D, MIN)))
t[, `:=`(med = 100 * med, iqr = q75 - q25)][n < 10, iqr := median(t[n >= 10, q75 - q25])]  # IQR unreliable in tiny tracts
t[, se := 100 * 1.2533 * iqr / 1.349 / sqrt(n)]
if (NOISE) { set.seed(1); t[, med := rnorm(.N, 0, se)] }
duckdb_register(con, "t", t[, .(tract, med)])
p <- as.data.table(dbGetQuery(con, sprintf("SELECT a.tract o, b.tract d, m.m, abs(a.med - b.med) ad, a.med - b.med sd
  FROM '%s/data/external/csds_ttm/tract_drive_le90.parquet' m JOIN t a ON a.tract = m.o JOIN t b ON b.tract = m.d
  WHERE m.o <> m.d AND m.m <= %d", ROOT, max(TS))))
inmx <- dbGetQuery(con, sprintf("SELECT DISTINCT o FROM '%s/data/external/csds_ttm/tract_drive_le90.parquet'", ROOT))$o
dbDisconnect(con, shutdown = TRUE)
cat("tracts with >=", MIN, "sales:", nrow(t), "| in matrix as origin:", uniqueN(p$o), "| pairs <= 60 min:", nrow(p),
    "| median SE of tract median (pp):", round(median(t$se), 3), "\n")
res <- rbindlist(lapply(TS, \(T) p[m <= T, .(n_nb = .N, max_diff = max(ad), p90_diff = quantile(ad, .9), mean_diff = mean(ad),
  gap_min = max(sd), gap_p10 = quantile(sd, .9)), by = .(tract = o)][, minutes := T]))
# tracts in the matrix with no other tract (with sales) within T minutes: no reachable difference -> 0.
# Tracts absent from the matrix (ID not a 2010 tract or not routable) stay missing.
res <- res[CJ(tract = intersect(t$tract, inmx), minutes = TS), on = .(tract, minutes)]
res[is.na(n_nb), `:=`(n_nb = 0L, max_diff = 0, p90_diff = 0, mean_diff = 0, gap_min = 0, gap_p10 = 0)]
# gap_min = own median - lowest neighbour median (what moving to the cheapest reachable tract saves; < 0: own tract is the
# cheapest); gap_p10 = own median - 10th percentile of neighbour medians (robust to noise in the minimum)
res <- merge(res, t[, .(tract, med, n, se, st, lat, lon)], by = "tract")
fwrite(res, file.path(OUT, paste0("tract_drive_differences", SFX, ".csv")))
sink(file.path(OUT, paste0("tract_drive_differences", SFX, ".txt")))
cat("Tracts (2010) with >=", MIN, "sales 2017-19; median effective rate (%); |differences| to tracts within T drive minutes (pp)\n")
cat("tracts with no neighbour within T (set to 0), 15/30/60 min:", res[n_nb == 0, .N, by = minutes][order(minutes), N], "| absent from matrix (missing):", sum(!t$tract %in% inmx), "\n")
cat("tracts:", nrow(t), "| median SE of a tract median:", round(median(t$se), 3), "pp | tract medians p10/p50/p90:",
    round(quantile(t$med, c(.1, .5, .9)), 2), "\n\n")
print(as.data.frame(res[, .(tracts = .N, nb_median = as.numeric(median(n_nb)), max_p50 = median(max_diff), max_p90 = quantile(max_diff, .9),
  p90diff_p50 = median(p90_diff), mean_p50 = median(mean_diff), mean_p90 = quantile(mean_diff, .9)), by = minutes][, lapply(.SD, round, 2)]), row.names = FALSE)
cat("\nRelative gap: own median - neighbours' min (gap_min) or 10th pct (gap_p10), pp; share of tracts with gap > 0.5 pp\n")
print(as.data.frame(res[, .(tracts = .N, gmin_p10 = quantile(gap_min, .1), gmin_p50 = median(gap_min), gmin_p90 = quantile(gap_min, .9),
  gp10_p10 = quantile(gap_p10, .1), gp10_p50 = median(gap_p10), gp10_p90 = quantile(gap_p10, .9),
  sh_gmin_gt05 = mean(gap_min > .5), sh_gp10_gt05 = mean(gap_p10 > .5), sh_cheapest = mean(n_nb > 0 & gap_min < 0)), by = minutes][, lapply(.SD, round, 2)]), row.names = FALSE)
cat("\nBy state, 30 min: median over tracts (pp), states with >= 100 tracts, by max_diff\n")
print(as.data.frame(res[minutes == 30, .(tracts = .N, max_diff = round(median(max_diff), 2), p90_diff = round(median(p90_diff), 2),
  mean_diff = round(median(mean_diff), 2)), by = st][tracts >= 100][order(-max_diff)]), row.names = FALSE)
sink(); cat(readLines(file.path(OUT, paste0("tract_drive_differences", SFX, ".txt"))), sep = "\n")
if (!NOISE) {
  tr <- st_transform(tracts(cb = TRUE, year = 2019, progress_bar = FALSE), 5070)
  tr <- tr[!tr$STATEFP %in% c("02", "15", "60", "66", "69", "72", "78"), c("GEOID")]
  states <- st_transform(states(cb = TRUE, year = 2020, progress_bar = FALSE), 5070)
  states <- states[!states$STUSPS %in% c("AK", "HI", "PR", "VI", "GU", "MP", "AS"), ]
  mp <- function(d, v, lab, cap, title) { x <- merge(tr, d[, .(GEOID = tract, val = pmin(get(v), cap))], by = "GEOID")
    ggplot() + geom_sf(data = states, fill = "grey95", colour = NA) + geom_sf(data = x, aes(fill = val), colour = NA) +
      geom_sf(data = states, fill = NA, colour = "grey30", linewidth = .15) +
      scale_fill_viridis_c(option = "magma", direction = -1, limits = c(0, cap), name = lab) +
      coord_sf(crs = 5070, datum = NA) + labs(title = title) + theme_void(base_size = 9) }
  m0 <- mp(res[minutes == 15], "med", "median\neff. rate\n(%)", 3, sprintf("Median effective tax rate by tract (>= %d sales, 2017-19)", MIN))
  for (v in c("max_diff", "p90_diff")) {
    ps <- lapply(TS, \(T) mp(res[minutes == T], v, sprintf("%s\n(pp)", sub("_diff", " abs.\ndifference", v)), 2.5,
      sprintf("%s difference to tracts within %d drive minutes", if (v == "max_diff") "Max" else "90th-pct", T)))
    ggsave(file.path(OUT, sprintf("figures/tract_drive_%s%s.png", v, SFX)), wrap_plots(c(list(m0), ps), ncol = 2), width = 14, height = 8.5, dpi = 200)
  }
}
# Metro zooms (tracts are tiny where people live): median rate and 30-min p90 difference, NY and Chicago areas
if (!NOISE) {
  zoom <- function(v, T, lab, cap, bb, title) { x <- merge(tr, res[minutes == T, .(GEOID = tract, val = pmin(get(v), cap))], by = "GEOID")
    bb <- st_bbox(st_transform(st_as_sfc(st_bbox(c(xmin = bb[1], ymin = bb[2], xmax = bb[3], ymax = bb[4]), crs = 4326)), 5070))
    ggplot() + geom_sf(data = states, fill = "grey95", colour = NA) + geom_sf(data = x, aes(fill = val), colour = NA) +
      geom_sf(data = states, fill = NA, colour = "grey20", linewidth = .3) +
      scale_fill_viridis_c(option = "magma", direction = -1, limits = c(0, cap), name = lab) +
      coord_sf(crs = 5070, xlim = bb[c(1, 3)], ylim = bb[c(2, 4)], datum = NA) + labs(title = title) + theme_void(base_size = 9) }
  ny <- c(-75.4, 40.2, -72.6, 41.6); chi <- c(-88.6, 41.2, -86.9, 42.5)
  z <- wrap_plots(zoom("med", 30, "median\n(%)", 3, ny, "NY metro: median effective rate"),
                  zoom("p90_diff", 30, "p90 abs.\ndiff (pp)", 2.5, ny, "NY metro: 90th-pct diff within 30 min"),
                  zoom("med", 30, "median\n(%)", 3, chi, "Chicago: median effective rate"),
                  zoom("p90_diff", 30, "p90 abs.\ndiff (pp)", 2.5, chi, "Chicago: 90th-pct diff within 30 min"), ncol = 2)
  ggsave(file.path(OUT, sprintf("figures/tract_drive_metro_zoom%s.png", SFX)), z, width = 12, height = 10, dpi = 200)
}
# Relative maps: own median minus the lowest (or 10th-percentile) neighbour median within T minutes; diverging scale
if (!NOISE) {
  mpd <- function(d, v, lab, cap, title, bb = NULL) { x <- merge(tr, d[, .(GEOID = tract, val = pmax(pmin(get(v), cap), -cap))], by = "GEOID")
    g <- ggplot() + geom_sf(data = states, fill = "grey95", colour = NA) + geom_sf(data = x, aes(fill = val), colour = NA) +
      geom_sf(data = states, fill = NA, colour = "grey30", linewidth = if (is.null(bb)) .15 else .3) +
      scale_fill_gradient2(low = "#2166ac", mid = "grey97", high = "#b2182b", midpoint = 0, limits = c(-cap, cap), name = lab) +
      labs(title = title) + theme_void(base_size = 9)
    if (is.null(bb)) g + coord_sf(crs = 5070, datum = NA) else {
      b <- st_bbox(st_transform(st_as_sfc(st_bbox(c(xmin = bb[1], ymin = bb[2], xmax = bb[3], ymax = bb[4]), crs = 4326)), 5070))
      g + coord_sf(crs = 5070, xlim = b[c(1, 3)], ylim = b[c(2, 4)], datum = NA) } }
  for (v in c("gap_min", "gap_p10")) {
    nm <- if (v == "gap_min") "lowest" else "10th-pct"
    ps <- lapply(TS, \(T) mpd(res[minutes == T], v, sprintf("own -\n%s\nnearby (pp)", nm), 2,
      sprintf("Own median minus %s tract median within %d drive minutes", nm, T)))
    ggsave(file.path(OUT, sprintf("figures/tract_drive_%s%s.png", v, SFX)), wrap_plots(c(list(m0), ps), ncol = 2), width = 14, height = 8.5, dpi = 200)
  }
  z <- wrap_plots(mpd(res[minutes == 30], "gap_p10", "own - p10\n(pp)", 2, "NY metro: own minus 10th-pct within 30 min", ny),
                  mpd(res[minutes == 30], "gap_min", "own - min\n(pp)", 2, "NY metro: own minus lowest within 30 min", ny),
                  mpd(res[minutes == 30], "gap_p10", "own - p10\n(pp)", 2, "Chicago: own minus 10th-pct within 30 min", chi),
                  mpd(res[minutes == 30], "gap_min", "own - min\n(pp)", 2, "Chicago: own minus lowest within 30 min", chi), ncol = 2)
  ggsave(file.path(OUT, sprintf("figures/tract_drive_gap_metro_zoom%s.png", SFX)), z, width = 12, height = 10, dpi = 200)
}
