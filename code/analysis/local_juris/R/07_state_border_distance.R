# Parcels near a state border: distance from each parcel point (1% building sample, 2016) to the nearest boundary shared
# with another US state (coastlines and international borders excluded; DC counts as a state). cb 2020 1:500k states,
# projected to EPSG:5070. Internal borders = all state boundaries minus the outer US boundary (100 m tolerance).
suppressPackageStartupMessages({library(sf); library(dplyr); library(duckdb)})
source("code/R/paths.R"); OUT <- file.path(ROOT, "output/analysis/local_juris")
TIG <- path.expand("~/Library/Caches/tigris"); MI <- 1609.344
st <- st_read(file.path(TIG, "cb_2020_us_state_500k.shp"), quiet = TRUE) |> filter(!STUSPS %in% c("AK", "HI", "PR", "VI", "GU", "MP", "AS")) |>
  st_transform(5070) |> st_make_valid()
outer <- st_union(st) |> st_boundary() |> st_buffer(100)
internal <- st_boundary(st) |> st_union() |> st_difference(outer)
con <- dbConnect(duckdb())
p <- dbGetQuery(con, sprintf("SELECT clip, st, lat, lon, bill16 > 0 AND av16 > 0 has_bill FROM '%s/data/salt_cap/panel/parcels.parquet'", ROOT))
dbDisconnect(con, shutdown = TRUE)
pts <- p |> filter(!is.na(lat), lat != 0, !st %in% c("AK", "HI", "GU", "VI", "PR")) |> st_as_sf(coords = c("lon", "lat"), crs = 4269) |> st_transform(5070)
segs <- st_cast(st_sf(geometry = internal), "LINESTRING") |> st_segmentize(5000)   # small pieces so the spatial index helps
nn <- st_nearest_feature(pts, segs)
pts$dist_mi <- as.numeric(st_distance(pts, segs[nn, ], by_element = TRUE)) / MI
d <- st_drop_geometry(pts)
saveRDS(d |> select(clip, st, dist_mi), file.path(OUT, "state_border_distance_1pct.rds"))
tab <- tibble(within_mi = c(5, 10, 20, 30, 50, 75, 100), pct = sapply(c(5, 10, 20, 30, 50, 75, 100), \(m) round(100 * mean(d$dist_mi <= m), 1)))
by_st <- d |> group_by(st) |> summarise(n = n(), pct_within30 = round(100 * mean(dist_mi <= 30), 1), median_mi = round(median(dist_mi))) |> arrange(desc(pct_within30))
sink(file.path(OUT, "state_border_distance.txt"))
cat("Parcels with coordinates, lower 48 + DC:", nrow(d), "of", nrow(p), "in the 1% sample\n\n% within X miles of a border with another state:\n")
print(as.data.frame(tab), row.names = FALSE)
cat("\nBy state:\n"); print(as.data.frame(by_st), row.names = FALSE)
sink(); cat(readLines(file.path(OUT, "state_border_distance.txt")), sep = "\n")
