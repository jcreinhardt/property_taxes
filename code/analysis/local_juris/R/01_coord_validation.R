# Validate CoreLogic parcel coordinates (parcel_level_latitude/longitude) against Census boundaries, 1% building sample (2016).
# Point-in-polygon county vs the roll's fips; incorporated place vs the roll's municipality_name; unified school district vs
# school_district_name; share of points near a boundary (where generalised 1:500k maps can mis-assign).
suppressPackageStartupMessages({library(sf); library(dplyr); library(duckdb); library(stringr)})
sf_use_s2(FALSE)
source("code/R/paths.R")
TIG  <- path.expand("~/Library/Caches/tigris")
OUT  <- file.path(ROOT, "output/analysis/local_juris"); dir.create(OUT, showWarnings = FALSE, recursive = TRUE)

con <- dbConnect(duckdb())
p <- dbGetQuery(con, sprintf("SELECT clip, st, lpad(fips::VARCHAR,5,'0') fips, lat, lon, upper(trim(muni_name)) muni,
  upper(trim(school_dist)) sd FROM '%s/data/salt_cap/panel/parcels.parquet' WHERE lat IS NOT NULL AND lat <> 0", ROOT))
dbDisconnect(con, shutdown = TRUE)
pts <- st_as_sf(p, coords = c("lon", "lat"), crs = 4269, remove = FALSE)

nm_ok <- function(a, b) { a <- norm(a); b <- norm(b); ok <- a != "" & b != "" &
  (a == b | str_detect(b, fixed(ifelse(a == "", "#", a))) | str_detect(a, fixed(ifelse(b == "", "#", b)))); coalesce(ok, FALSE) }
norm <- function(x) x |> toupper() |> str_replace_all("[^A-Z ]", " ") |>
  str_remove_all("\\b(CITY|TOWN|TOWNSHIP|TWP|VILLAGE|BOROUGH|BORO|OF|THE|CDP|SCHOOL|DISTRICT|SCHOOLS|PUBLIC|UNIFIED|USD|ISD|CONSOLIDATED|INDEPENDENT|COMMUNITY|COUNTY|AREA|CSD|SD)\\b") |>
  str_squish()

# ---- counties ----
cty <- st_read(file.path(TIG, "cb_2019_us_county_500k.shp"), quiet = TRUE) |> st_transform(4269) |> select(GEOID)
j <- st_join(pts, cty, join = st_within)
cty_tab <- j |> st_drop_geometry() |> mutate(res = case_when(is.na(GEOID) ~ "outside any county (water/off-map)",
  GEOID == fips ~ "same county", TRUE ~ "other county")) |> count(res) |> mutate(pct = round(100 * n / sum(n), 2))
bad_cty <- j |> st_drop_geometry() |> filter(!is.na(GEOID), GEOID != fips) |> count(st, fips, GEOID, sort = TRUE) |> head(12)

# ---- incorporated places and unified school districts (2017) ----
rd <- function(kind) {
  fs <- list.files(TIG, sprintf("^cb_2017_\\d{2}_%s_500k\\.shp$", kind), full.names = TRUE)
  do.call(rbind, lapply(fs, \(f) st_read(f, quiet = TRUE) |> st_transform(4269) |> select(any_of(c("GEOID", "NAME", "LSAD")))))
}
plc <- rd("place")
# cb place files carry LSAD; CDPs have LSAD 57. Keep incorporated places only.
inc <- plc |> filter(LSAD != "57")
jp <- st_join(pts |> select(clip, st, muni), inc |> rename(pname = NAME), join = st_within) |> st_drop_geometry() |>
  distinct(clip, .keep_all = TRUE)
plc_tab <- jp |> filter(!is.na(muni)) |> mutate(res = case_when(
  is.na(pname) & str_detect(muni, "UNINC|UNINCORPORATED|COUNTY|TWP|TOWNSHIP") ~ "outside places; roll says county/twp",
  is.na(pname) ~ "outside places; roll names a municipality",
  nm_ok(pname, muni) ~ "in place; name matches",
  TRUE ~ "in place; name differs")) |> count(res) |> mutate(pct = round(100 * n / sum(n), 1))
plc_state <- jp |> filter(!is.na(muni), !is.na(pname)) |>
  mutate(ok = nm_ok(pname, muni)) |>
  group_by(st) |> summarise(n = n(), match_pct = round(100 * mean(ok), 1)) |> arrange(match_pct)

unsd <- rd("unsd")
ju <- st_join(pts |> select(clip, st, sd), unsd |> rename(uname = NAME), join = st_within) |> st_drop_geometry() |>
  distinct(clip, .keep_all = TRUE) |> filter(!is.na(sd), !is.na(uname)) |>
  mutate(ok = nm_ok(uname, sd))
ex_plc <- jp |> filter(!is.na(muni), !is.na(pname), !nm_ok(pname, muni)) |> count(st, muni, pname, sort = TRUE) |>
  group_by(st) |> slice_head(n = 3) |> ungroup() |> filter(st %in% c("DC", "IN", "IL", "DE", "NY", "CA", "OH"))
ex_sd <- ju |> filter(!ok) |> count(st, sd, uname, sort = TRUE) |> group_by(st) |> slice_head(n = 2) |> ungroup() |>
  filter(st %in% c("VT", "IN", "OR", "MO", "NY", "OH", "WA", "IL"))
sd_state <- ju |> group_by(st) |> summarise(n = n(), match_pct = round(100 * mean(ok), 1)) |> arrange(desc(n))

# ---- distance to nearest incorporated-place boundary (projected, metres) ----
set.seed(1); smp <- pts[sample(nrow(pts), 50000), ] |> st_transform(5070)
bnd <- inc |> st_transform(5070) |> st_boundary()
d <- as.numeric(st_distance(smp, bnd[st_nearest_feature(smp, bnd), ], by_element = TRUE))
near_tab <- tibble(within_m = c(10, 25, 50, 100, 250), pct = sapply(c(10, 25, 50, 100, 250), \(m) round(100 * mean(d <= m), 2)))

sink(file.path(OUT, "coord_validation.txt"))
cat("Points with coordinates:", nrow(p), "\n\n== County (point vs roll fips) ==\n"); print(cty_tab); print(bad_cty)
cat("\n== Incorporated place vs roll municipality_name ==\n"); print(plc_tab); print(as.data.frame(plc_state), row.names = FALSE)
cat("\nlargest name mismatches:\n"); print(as.data.frame(ex_plc), row.names = FALSE)
cat("\n== Unified school district vs roll school_district_name (points inside a UNSD with a roll name) ==\n")
cat("overall match %:", round(100 * mean(ju$ok), 1), " n =", nrow(ju), "\n"); print(as.data.frame(sd_state), row.names = FALSE)
cat("\nlargest name mismatches:\n"); print(as.data.frame(ex_sd), row.names = FALSE)
cat("\n== Distance to nearest incorporated-place boundary (50k random points) ==\n"); print(near_tab)
sink()
cat(readLines(file.path(OUT, "coord_validation.txt")), sep = "\n")
