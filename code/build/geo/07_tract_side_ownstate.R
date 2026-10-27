# Formerly code/analysis/salt_local/c9_border_assign_district_coverage.R (moved in the 2026-10-27 restructuring).
# Coverage checks for the border instrument.
# A. Border-pair assignment. local_juris/R/26 assigns each tract (located at the mean coordinates of its 2017-19 sales) to the
#    nearest shared state-boundary line of any pair, drops the tract if that pair does not include its own state, and drops
#    pairs where either side has < 20 tracts within 20 mi (needed to rank the sides). Here: count the losses by reason, and
#    an alternative that takes the nearest line among the pairs that include the tract's own state and ranks sides with
#    >= 5 tracts within 20 mi.
# B. School districts per parcel. 1% CoreLogic residential parcel sample (data/built/corelogic/parcel_1pct/parcels.parquet, lower 48 +
#    DC), point-in-polygon into TIGER cb 2019 unified, elementary and secondary districts. Also: does the parcel's district
#    equal the one assigned to its tract (unified, else elementary, at the tract's mean sales location, R/24)?
# C. Shapefile universe: TIGER district polygons vs F-33 FY2016 districts with enrolment.
suppressPackageStartupMessages({library(data.table); library(sf); library(tigris); library(duckdb)})
options(tigris_use_cache = TRUE); sf_use_s2(FALSE)
source("code/R/paths.R"); OUT <- out_dir("build/geo"); TIG <- path.expand("~/Library/Caches/tigris")
sink(file.path(OUT, "c9_border_assign_district_coverage.txt"), split = TRUE)

# ---- A. border assignment ----
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
pl <- do.call(rbind, lines); pld <- as.data.table(st_drop_geometry(pl))
fips <- unique(as.data.table(tigris::fips_codes)[, .(st = state_code, ab = state)])
u <- merge(unique(d[, .(tract, lat, lon, st, dist_mi, med)]), fips, by = "st")
pts <- st_transform(st_as_sf(u, coords = c("lon", "lat"), crs = 4326), 5070)
u[, pair_any := pl$pair[st_nearest_feature(pts, pl)]]
# alternative: nearest line among own-state pairs
D <- st_distance(pts, pl)                                   # tracts x pair lines (m)
own <- outer(u$ab, pld$a, "==") | outer(u$ab, pld$b, "==")
D[!own] <- Inf
u[, pair_own := pld$pair[max.col(-D, ties.method = "first")]]
u[, has_own := rowSums(own) > 0]
u[, dist_own := apply(D, 1, min) / 1609.344]
u <- merge(u, pld[, .(pair_any = pair, a_any = a, b_any = b)], by = "pair_any")   # after D: merge reorders rows
u[, own_any := ab == a_any | ab == b_any]
rank_sides <- function(x, pcol, k) {
  s <- x[dist_mi < 20, .(side_med = median(med), n20 = .N), by = c(pcol, "ab")]
  s[, m := .N, by = pcol]; s[m == 2 & n20 >= k][, high := as.integer(side_med == max(side_med)), by = pcol][] }
s0 <- rank_sides(u[own_any == TRUE], "pair_any", 20)
s1 <- rank_sides(u[has_own == TRUE][, dist_mi := dist_own], "pair_own", 5)
cat("== A. Border-pair assignment (tracts with sales, drive data and controls:", nrow(u), ")\n")
cat("Current rule: nearest line of any pair involves another pair of states:", u[own_any == FALSE, .N],
    "| own pair, but a side has < 20 tracts within 20 mi:", u[own_any == TRUE][!s0, on = .(pair_any, ab)][, .N],
    "| kept:", u[own_any == TRUE][s0, on = .(pair_any, ab), nomatch = 0][, .N], "\n")
cat("  of the 'other pair' tracts, within 10 mi of a border:", u[own_any == FALSE & dist_mi < 10, .N], "| median distance (mi):", round(u[own_any == FALSE, median(dist_mi)], 1), "\n")
cat("  pairs with a side < 20 tracts:", paste(setdiff(unique(u[own_any == TRUE, pair_any]), s0$pair_any), collapse = " "), "\n")
k1 <- u[has_own == TRUE][s1, on = .(pair_own, ab), nomatch = 0]
cat("Own-state nearest line, rank with >= 5 tracts per side within 20 mi: kept", nrow(k1), "tracts,", uniqueN(k1$pair_own), "pairs | no internal border (none):",
    u[has_own == FALSE, .N], "| side < 5 tracts:", u[has_own == TRUE][!s1, on = .(pair_own, ab)][, .N], "\n")
cat("  pairs still unranked:", paste(setdiff(unique(u[has_own == TRUE, pair_own]), s1$pair_own), collapse = " "), "\n")
cat("  tracts that change pair vs current rule (among those kept by both):",
    merge(k1[, .(tract, pair_own)], u[own_any == TRUE][s0, on = .(pair_any, ab), nomatch = 0][, .(tract, pair_any)], by = "tract")[pair_own != pair_any, .N], "\n")
cat("  kept within 50 mi:", k1[dist_own <= 50, .N], "(current rule:", readRDS(file.path(DATA, "built/geo/iv_tract_side.rds"))[T == 30 & dist_mi <= 50, .N], ")\n")
saveRDS(k1[, .(tract, pair = pair_own, ab, dist_own)][s1[, .(pair = pair_own, ab, high, side_med)], on = .(pair, ab), nomatch = 0],
        file.path(DATA, "built/geo/iv_tract_side_ownstate.rds"))

# ---- B. school districts per parcel ----
con <- dbConnect(duckdb())
p <- as.data.table(dbGetQuery(con, sprintf("SELECT clip, st, lpad(fips::VARCHAR, 5, '0') || lpad(tract::VARCHAR, 6, '0') AS tract_cl, lat, lon,
  nullif(trim(school_dist), '') AS cl_sd FROM '%s/data/built/corelogic/parcel_1pct/parcels.parquet'
  WHERE st NOT IN ('AK', 'HI', 'PR', 'GU', 'VI') AND lat IS NOT NULL AND lat <> 0", ROOT)))
nall <- dbGetQuery(con, sprintf("SELECT count(*) n FROM '%s/data/built/corelogic/parcel_1pct/parcels.parquet' WHERE st NOT IN ('AK','HI','PR','GU','VI')", ROOT))$n
dbDisconnect(con, shutdown = TRUE)
pp <- st_transform(st_as_sf(p[, .(clip, lat, lon)], coords = c("lon", "lat"), crs = 4326), 5070)
sdj <- function(type, nm) {
  s <- st_transform(school_districts(cb = TRUE, year = 2019, type = type, progress_bar = FALSE)[, c("GEOID", "NAME")], 5070)
  j <- as.data.table(st_drop_geometry(st_join(pp, s[, "GEOID"], join = st_within)))[, .(g = GEOID[1]), by = clip]
  setnames(j, "g", nm); list(j = j, s = s) }
U <- sdj("unified", "unsd"); E <- sdj("elementary", "elsd"); S <- sdj("secondary", "scsd")
p <- Reduce(\(a, b) merge(a, b, by = "clip", all.x = TRUE), list(p, U$j, E$j, S$j))
nd <- \(g) !is.na(g) & substr(g, 3, 7) %in% c("99997", "99998", "99999")    # "School District Not Defined" pseudo-polygons
p[, `:=`(any_real = (!is.na(unsd) & !nd(unsd)) | (!is.na(elsd) & !nd(elsd)) | (!is.na(scsd) & !nd(scsd)),
         any_poly = !is.na(unsd) | !is.na(elsd) | !is.na(scsd))]
cat("\n== B. School districts per parcel (1% residential sample, lower 48 + DC)\n")
cat("parcels:", nall, "| with coordinates:", nrow(p), sprintf("(%.1f%%)", 100 * nrow(p) / nall), "\n")
print(p[, .(unified = mean(!is.na(unsd)), elementary = mean(!is.na(elsd)), secondary = mean(!is.na(scsd)),
            `elem+sec (dual system)` = mean(!is.na(elsd) & !is.na(scsd)), `any polygon` = mean(any_poly),
            `any real district (not 'not defined')` = mean(any_real), `CoreLogic school_dist field` = mean(!is.na(cl_sd)))][, lapply(.SD, \(x) round(100 * x, 2))])
cat("States with < 99% of parcels in a real district polygon:\n")
print(p[, .(n = .N, pct_real = round(100 * mean(any_real), 1), pct_poly = round(100 * mean(any_poly), 1)), by = st][pct_real < 99][order(pct_real)])
# parcels in the instrument's tracts: tract via CoreLogic tract field (parcels without it are not matched)
iv <- unique(readRDS(file.path(DATA, "built/geo/iv_tract_side.rds"))[, .(tract, leaid)])
q <- merge(p, iv, by.x = "tract_cl", by.y = "tract")
q[, own := fcoalesce(unsd, elsd)]
cat("\nParcels matched to an instrument-sample tract (via CoreLogic tract id):", nrow(q), "\n")
print(q[, .(`in a real district` = mean(any_real), `tract has assigned leaid` = mean(!is.na(leaid)),
            `parcel district == tract's leaid` = mean(own == leaid, na.rm = TRUE),
            `tract wholly in one district (share of parcels)` = NA_real_)][, 1:3][, lapply(.SD, \(x) round(100 * x, 2))])
tsplit <- q[!is.na(own), .(nd = uniqueN(own)), by = tract_cl]
cat("Tracts (with >= 1 sampled parcel) whose sampled parcels fall in > 1 district:", round(100 * mean(tsplit$nd > 1), 1), "%\n")

# ---- C. shapefile universe ----
f <- readRDS(file.path(DATA, "built/census_finances/district_finance.rds"))[y == 2016 & enr > 0]
f[, st := substr(leaid, 1, 2)]; f <- f[!st %in% c("02", "15", "72")]
geo <- unique(c(U$s$GEOID, E$s$GEOID, S$s$GEOID))
cat("\n== C. Shapefile universe\nTIGER cb 2019 polygons: unified", nrow(U$s), "| elementary", nrow(E$s), "| secondary", nrow(S$s),
    "| of which 'not defined' pseudo-districts:", sum(nd(geo)), "\n")
cat("F-33 FY2016 districts with enrolment (lower 48 + DC):", nrow(f), "| with a TIGER polygon:", sum(f$leaid %in% geo),
    sprintf("(%.1f%% of districts, %.1f%% of enrolment, %.1f%% of property-tax revenue)", 100 * mean(f$leaid %in% geo),
            100 * f[leaid %in% geo, sum(enr)] / sum(f$enr), 100 * f[leaid %in% geo, sum(ptax, na.rm = TRUE)] / sum(f$ptax, na.rm = TRUE)), "\n")
cat("F-33 districts without a polygon, by school level code (schlev) — largest groups:\n")
print(f[!leaid %in% geo, .(n = .N, enr = sum(enr), ptax = sum(ptax, na.rm = TRUE)), by = schlev][order(-enr)])
print(f[!leaid %in% geo][order(-enr), .(leaid, name, enr, ptax, schlev)][1:12])
sink()
