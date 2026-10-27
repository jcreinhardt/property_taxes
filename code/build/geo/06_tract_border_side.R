# Formerly code/analysis/local_juris/R/26_iv_high_side.R (moved in the 2026-10-27 restructuring).
# Tract -> state-border pair and side (the build half of local_juris/R/26_iv_high_side.R; the directional IV estimation is
# code/designs/border_iv/02_directional_iv.R). Each tract is assigned to its nearest state-border pair (TIGER cb 2020 state
# boundaries, shared-boundary lines per pair; the nearest pair must involve the tract's own state); a side is "high" if the
# median of its tract medians within 20 mi exceeds the other side's; near = within 10 mi.
# Input: data/built/geo/iv_tract_nb.rds (designs/border_iv/01). Output: data/built/geo/iv_tract_side.rds.
suppressPackageStartupMessages({library(data.table); library(sf)})
source("code/R/paths.R"); MI <- 1609.344
TIG <- path.expand("~/Library/Caches/tigris")
d <- readRDS(file.path(DATA, "built/geo/iv_tract_nb.rds"))
st <- st_read(file.path(TIG, "cb_2020_us_state_500k.shp"), quiet = TRUE)
st <- st_make_valid(st_transform(st[!st$STUSPS %in% c("AK", "HI", "PR", "VI", "GU", "MP", "AS"), ], 5070))
sides <- st_read(file.path(DATA, "built/geo/border20_sides.geojson"), quiet = TRUE)
pairs <- unique(as.data.table(st_drop_geometry(sides))[, .(pair, a = pmin(st_a, st_b), b = pmax(st_a, st_b))])
bd <- st_boundary(st); lines <- list()
for (i in seq_len(nrow(pairs))) {
  ia <- which(st$STUSPS == pairs$a[i]); ib <- which(st$STUSPS == pairs$b[i])
  x <- suppressWarnings(st_intersection(st_buffer(bd[ia, ], 200), bd[ib, ]))
  if (nrow(x)) lines[[length(lines) + 1]] <- st_sf(pair = pairs$pair[i], a = pairs$a[i], b = pairs$b[i], geometry = st_union(st_geometry(x)))
}
pl <- do.call(rbind, lines)
fips <- as.data.table(tigris::fips_codes)[, .(st = state_code, ab = state)] |> unique()
u <- unique(d[, .(tract, lat, lon, st, dist_mi, med)])
u <- merge(u, fips, by = "st")
pts <- st_transform(st_as_sf(u, coords = c("lon", "lat"), crs = 4326), 5070)
u[, pair := pl$pair[st_nearest_feature(pts, pl)]]
u <- merge(u, as.data.table(st_drop_geometry(pl))[, .(pair, a, b)], by = "pair")
u <- u[ab == a | ab == b]   # nearest pair must involve own state
side <- u[dist_mi < 20, .(side_med = median(med), n20 = .N), by = .(pair, ab)]
side[, k := .N, by = pair]; side <- side[k == 2 & n20 >= 20][, high := as.integer(side_med == max(side_med)), by = pair]
u <- merge(u, side[, .(pair, ab, high, side_med)], by = c("pair", "ab"))
d <- merge(d, u[, .(tract, pair, high, side_med)], by = "tract")
d[, near := as.integer(dist_mi < 10)]
cat("tracts with a ranked nearest pair:", uniqueN(d$tract), "| pairs:", uniqueN(d$pair), "\n")
saveRDS(d, file.path(DATA, "built/geo/iv_tract_side.rds"))
