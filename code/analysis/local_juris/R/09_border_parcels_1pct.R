# 1% building sample: parcels within 20 mi of a shared state border, assigned to state-pair sides (border20_sides.geojson).
# Writes data/local_juris/border_parcels_1pct.parquet (clip, pair, st_a, st_b) and the sample's clip list for the cluster.
suppressPackageStartupMessages({library(sf); library(dplyr); library(duckdb)})
ROOT <- "/Users/jr2728/PhD/projects/property_taxes"; sf_use_s2(FALSE)
sides <- st_read(file.path(ROOT, "data/local_juris/border20_sides.geojson"), quiet = TRUE)
con <- dbConnect(duckdb())
p <- dbGetQuery(con, sprintf("SELECT clip, st, lat, lon FROM '%s/data/salt_cap/panel/parcels.parquet'", ROOT))
pts <- p |> filter(!is.na(lat), lat != 0) |> st_as_sf(coords = c("lon", "lat"), crs = 4269) |> st_transform(4326)
j <- st_join(pts, sides |> select(pair, st_a, st_b), join = st_within, left = FALSE) |> st_drop_geometry()
cat("parcels in sample:", nrow(p), " with coords:", nrow(pts), " border rows:", nrow(j), " border parcels:", n_distinct(j$clip),
    " (", round(100 * n_distinct(j$clip) / nrow(pts), 1), "% of parcels with coords)\n")
cat("own state = st_a:", round(100 * mean(j$st == j$st_a), 1), "%\n")
dbWriteTable(con, "j", j |> select(clip, pair, st_a, st_b)); dbWriteTable(con, "c", p |> select(clip))
dbExecute(con, sprintf("COPY j TO '%s/data/local_juris/border_parcels_1pct.parquet' (FORMAT parquet)", ROOT))
dbExecute(con, sprintf("COPY c TO '%s/data/local_juris/sample_clips_1pct.parquet' (FORMAT parquet)", ROOT))
dbDisconnect(con, shutdown = TRUE)
