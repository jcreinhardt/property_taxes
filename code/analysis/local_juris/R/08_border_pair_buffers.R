# 20-mile buffers around each shared state border (lower 48 + DC), one polygon per (state, neighbour) side:
# the part of state A within 20 mi of its border with B. Output GeoJSON (EPSG:4326) for DuckDB spatial on the cluster.
suppressPackageStartupMessages({library(sf); library(dplyr)})
ROOT <- "/Users/jr2728/PhD/projects/property_taxes"; TIG <- path.expand("~/Library/Caches/tigris"); MI <- 1609.344
st <- st_read(file.path(TIG, "cb_2020_us_state_500k.shp"), quiet = TRUE) |>
  filter(!STUSPS %in% c("AK", "HI", "PR", "VI", "GU", "MP", "AS")) |> st_transform(5070) |> st_make_valid() |> select(STUSPS, STATEFP)
adj <- st_is_within_distance(st, st, 200)
pairs <- do.call(rbind, lapply(seq_len(nrow(st)), \(i) data.frame(i = i, j = setdiff(adj[[i]], i))))
bnd <- st_boundary(st)
sides <- lapply(seq_len(nrow(pairs)), \(k) {
  i <- pairs$i[k]; j <- pairs$j[k]
  shared <- st_intersection(st_geometry(bnd)[i], st_buffer(st_geometry(bnd)[j], 200))
  if (!length(shared) || all(st_is_empty(shared)) || sum(st_length(shared)) < units::set_units(1000, m)) return(NULL)
  side <- st_intersection(st_buffer(st_union(shared), 20 * MI), st_geometry(st)[i])
  st_sf(st_a = st$STUSPS[i], st_b = st$STUSPS[j], statefp = st$STATEFP[i], border_km = round(as.numeric(sum(st_length(shared))) / 1000), geometry = st_union(side))
})
sides <- do.call(rbind, Filter(Negate(is.null), sides)) |> mutate(pair = ifelse(st_a < st_b, paste(st_a, st_b, sep = "-"), paste(st_b, st_a, sep = "-")))
cat("sides:", nrow(sides), " pairs:", n_distinct(sides$pair), "\n")
out <- file.path(ROOT, "data/local_juris/border20_sides.geojson"); unlink(out)
st_write(sides |> st_transform(4326), out, quiet = TRUE)
print(sides |> st_drop_geometry() |> distinct(pair, border_km) |> arrange(border_km) |> head(5))
