# Formerly code/analysis/local_juris/R/27_school_district_gaps.R (moved in the 2026-10-27 restructuring).
# School-district map of the outside option. District median effective rate = sales-weighted median of tract medians
# (tracts located in districts as in R/24; sales 2017-19). Districts: TIGER cb 2019 unified + elementary. Neighbours =
# districts sharing a boundary (intersecting after a 50 m buffer). For each district: gap_min = own - cheapest neighbour,
# gap_x = own - cheapest neighbour in another state (cross-state option), achievement difference to that cheapest
# neighbour (SEDA 4.1). Maps: lower 48 and NY / Chicago / Philadelphia-NJ zooms.
suppressPackageStartupMessages({library(data.table); library(duckdb); library(sf); library(tigris); library(ggplot2); library(patchwork)})
options(tigris_use_cache = TRUE)
source("code/R/paths.R"); OUT <- out_dir("designs/border_iv"); invisible(out_dir("designs/border_iv/figures"))
con <- dbConnect(duckdb()); t <- as.data.table(dbGetQuery(con, sprintf("SELECT tract, leaid, n, med, ach FROM '%s/data/built/geo/iv_tract.parquet'", ROOT)))
dbDisconnect(con, shutdown = TRUE)
dm <- t[!is.na(leaid), .(n = sum(n), med = { o <- order(med); m <- med[o]; w <- cumsum(n[o]) / sum(n); m[which(w >= .5)[1]] },
                          ach = ach[1], tracts = .N), by = leaid][n >= 20]
sd <- rbind(school_districts(cb = TRUE, year = 2019, type = "unified", progress_bar = FALSE)[, c("GEOID", "STATEFP")],
            school_districts(cb = TRUE, year = 2019, type = "elementary", progress_bar = FALSE)[, c("GEOID", "STATEFP")])
sd <- st_make_valid(st_transform(sd[as.integer(sd$STATEFP) <= 56 & !sd$STATEFP %in% c("02", "15"), ], 5070)); sd <- sd[!duplicated(sd$GEOID), ]
sd <- merge(sd, dm, by.x = "GEOID", by.y = "leaid", all.x = TRUE)
nbl <- st_intersects(st_buffer(sd, 50), sd)
e <- rbindlist(lapply(seq_along(nbl), \(i) if (length(nbl[[i]]) > 1) data.table(i = i, j = setdiff(nbl[[i]], i))))
e[, `:=`(gi = sd$GEOID[i], gj = sd$GEOID[j], xst = sd$STATEFP[i] != sd$STATEFP[j], mi = sd$med[i], mj = sd$med[j], ai = sd$ach[i], aj = sd$ach[j])]
e <- e[!is.na(mi) & !is.na(mj)]
g <- e[, .(n_nb = .N, gap_min = mi[1] - min(mj), ach_diff_cheapest = aj[which.min(mj)] - ai[1],
           gap_x = if (any(xst)) mi[1] - min(mj[xst]) else NA_real_, ach_diff_x = if (any(xst)) aj[xst][which.min(mj[xst])] - ai[1] else NA_real_,
           has_x = any(xst)), by = .(GEOID = gi)]
sd <- merge(sd, g, by = "GEOID", all.x = TRUE)
d <- as.data.table(st_drop_geometry(sd))[!is.na(med)]
fwrite(d, file.path(OUT, "school_district_gaps.csv"))
sink(file.path(OUT, "school_district_gaps.txt"))
cat("Districts with >= 20 sales:", nrow(d), "| with a priced neighbour:", sum(!is.na(d$gap_min)), "| on a state line:", sum(d$has_x, na.rm = TRUE), "\n")
cat("own - cheapest adjacent district (pp): p50", round(median(d$gap_min, na.rm = TRUE), 2), "p90", round(quantile(d$gap_min, .9, na.rm = TRUE), 2),
    "| share > 0.5 pp:", round(mean(d$gap_min > .5, na.rm = TRUE), 3), "\n")
cat("state-line districts, own - cheapest district across the line: p50", round(median(d$gap_x, na.rm = TRUE), 2), "p90", round(quantile(d$gap_x, .9, na.rm = TRUE), 2),
    "| share > 0.5 pp:", round(mean(d$gap_x > .5, na.rm = TRUE), 3), "\n")
cat("Achievement (SD) of the cheapest adjacent district minus own, mean:", round(mean(d$ach_diff_cheapest, na.rm = TRUE), 3),
    "| when gap > 0.5:", round(d[gap_min > .5, mean(ach_diff_cheapest, na.rm = TRUE)], 3), "\n")
cat("Cross-state cheapest minus own achievement, mean:", round(mean(d$ach_diff_x, na.rm = TRUE), 3), "| when gap_x > 0.5:",
    round(d[gap_x > .5, mean(ach_diff_x, na.rm = TRUE)], 3), "\n")
cat("corr(gap_min, ach_diff_cheapest):", round(cor(d$gap_min, d$ach_diff_cheapest, use = "complete"), 3), "\n")
sink(); cat(readLines(file.path(OUT, "school_district_gaps.txt")), sep = "\n")
states <- st_transform(states(cb = TRUE, year = 2020, progress_bar = FALSE), 5070)
states <- states[!states$STUSPS %in% c("AK", "HI", "PR", "VI", "GU", "MP", "AS"), ]
mp <- function(v, lab, title, bb = NULL, cap = 2) {
  x <- sd; x$val <- pmax(pmin(x[[v]], cap), -cap)
  gg <- ggplot() + geom_sf(data = x, aes(fill = val), colour = if (is.null(bb)) NA else "grey70", linewidth = .05) +
    geom_sf(data = states, fill = NA, colour = "black", linewidth = if (is.null(bb)) .2 else .5) +
    scale_fill_gradient2(low = "#2166ac", mid = "grey97", high = "#b2182b", midpoint = 0, limits = c(-cap, cap), na.value = "grey85", name = lab) +
    labs(title = title) + theme_void(base_size = 9)
  if (is.null(bb)) gg + coord_sf(crs = 5070, datum = NA) else {
    b <- st_bbox(st_transform(st_as_sfc(st_bbox(c(xmin = bb[1], ymin = bb[2], xmax = bb[3], ymax = bb[4]), crs = 4326)), 5070))
    gg + coord_sf(crs = 5070, xlim = b[c(1, 3)], ylim = b[c(2, 4)], datum = NA) } }
mm <- function(v, title, bb = NULL) { x <- sd; x$val <- pmin(x$med, 3)
  gg <- ggplot() + geom_sf(data = x, aes(fill = val), colour = if (is.null(bb)) NA else "grey70", linewidth = .05) +
    geom_sf(data = states, fill = NA, colour = "black", linewidth = if (is.null(bb)) .2 else .5) +
    scale_fill_viridis_c(option = "magma", direction = -1, limits = c(0, 3), na.value = "grey85", name = "median\neff. rate (%)") +
    labs(title = title) + theme_void(base_size = 9)
  if (is.null(bb)) gg + coord_sf(crs = 5070, datum = NA) else {
    b <- st_bbox(st_transform(st_as_sfc(st_bbox(c(xmin = bb[1], ymin = bb[2], xmax = bb[3], ymax = bb[4]), crs = 4326)), 5070))
    gg + coord_sf(crs = 5070, xlim = b[c(1, 3)], ylim = b[c(2, 4)], datum = NA) } }
ggsave(file.path(OUT, "figures/school_district_gaps_us.png"),
       wrap_plots(mm("med", "Median effective tax rate by school district (sales 2017-19)"),
                  mp("gap_min", "own - cheapest\nadjacent (pp)", "Own rate minus cheapest adjacent district"),
                  mp("gap_x", "own - cheapest\nacross state\nline (pp)", "State-line districts: own rate minus cheapest adjacent district in the other state"), ncol = 2),
       width = 16, height = 10, dpi = 200)
zz <- list(NY = c(-75.4, 40.4, -72.9, 41.7), Chicago = c(-88.5, 41.1, -86.8, 42.5), Philly = c(-76.2, 39.5, -74.3, 40.6),
           `Kansas City` = c(-95.2, 38.6, -94.1, 39.5), Cincinnati = c(-85.2, 38.8, -84.0, 39.5), Portland = c(-123.3, 45.2, -122.2, 45.9))
z <- wrap_plots(unlist(lapply(names(zz), \(k) list(mm("med", paste(k, ": median rate"), zz[[k]]), mp("gap_min", "own -\ncheapest\nadjacent", paste(k, ": own - cheapest adjacent"), zz[[k]]))), recursive = FALSE), ncol = 4)
ggsave(file.path(OUT, "figures/school_district_gaps_zoom.png"), z, width = 18, height = 13, dpi = 170)
