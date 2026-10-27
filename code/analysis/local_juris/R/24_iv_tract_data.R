# Tract-level data for the border-distance IV: does proximity to a state border give a tract a cheaper reachable outside
# option, without also giving it better schools or different demographics? Builds data/local_juris/iv_tract.parquet.
# Inputs: tract medians (j_tract_eff.sql, sales 2017-19), drive-time gaps (R/22, any-sales version, 30 min), distance to the
# nearest internal state border (as R/07/R/23), school districts (TIGER cb 2019 unified + elementary; tract located at the
# mean coordinates of its sales) with SEDA 4.1 pooled cohort-standardised achievement (subgroup all) and SES, ACS 2013-17 tract
# covariates (tidycensus, cached in data/external/acs17_tract.csv).
suppressPackageStartupMessages({library(data.table); library(duckdb); library(sf); library(tigris); library(tidycensus)})
options(tigris_use_cache = TRUE)
source("code/R/paths.R"); OUT <- file.path(ROOT, "output/analysis/local_juris"); MI <- 1609.344
TIG <- path.expand("~/Library/Caches/tigris")
con <- dbConnect(duckdb())
t <- as.data.table(dbGetQuery(con, sprintf("SELECT * FROM '%s/data/local_juris/tract/tract_eff.parquet'", ROOT)))
dbDisconnect(con, shutdown = TRUE)
t <- t[!is.na(lat) & !substr(tract, 1, 2) %in% c("02", "15", "72")][, med := 100 * med]
g <- fread(file.path(OUT, "tract_drive_differences_min1.csv"), colClasses = c(tract = "character"))
g <- dcast(g, tract ~ minutes, value.var = c("gap_p10", "gap_min", "p90_diff", "mean_diff", "n_nb"))
t <- merge(t, g, by = "tract", all.x = TRUE)
# distance to the nearest internal state border
st <- st_read(file.path(TIG, "cb_2020_us_state_500k.shp"), quiet = TRUE)
st <- st_make_valid(st_transform(st[!st$STUSPS %in% c("AK", "HI", "PR", "VI", "GU", "MP", "AS"), ], 5070))
internal <- st_difference(st_union(st_boundary(st)), st_buffer(st_boundary(st_union(st)), 100))
segs <- st_segmentize(st_cast(st_sf(geometry = internal), "LINESTRING"), 5000)
pts <- st_transform(st_as_sf(t, coords = c("lon", "lat"), crs = 4326, remove = FALSE), 5070)
t[, dist_mi := as.numeric(st_distance(pts, segs[st_nearest_feature(pts, segs), ], by_element = TRUE)) / MI]
# school district of the tract location: unified, else elementary
sd <- rbind(school_districts(cb = TRUE, year = 2019, type = "unified", progress_bar = FALSE)[, "GEOID"],
            school_districts(cb = TRUE, year = 2019, type = "elementary", progress_bar = FALSE)[, "GEOID"])
sd <- st_transform(sd, 5070)
j <- st_join(pts[, "tract"], sd, join = st_within, left = TRUE)
j <- as.data.table(st_drop_geometry(j))[, .(leaid = GEOID[1]), by = tract]
t <- merge(t, j, by = "tract", all.x = TRUE)
seda <- fread(file.path(ROOT, "data/external/seda41/seda_geodist_pool_cs_4.1.csv"), colClasses = c(sedalea = "character"))
seda <- seda[subgroup == "all", .(leaid = sprintf("%07d", as.integer(sedalea)), ach = cs_mn_avg_ol, ach_se = cs_mn_avg_ol_se)]
cov <- fread(file.path(ROOT, "data/external/seda41/seda_cov_geodist_pool_4.1.csv"), colClasses = c(sedalea = "character"))
sescol <- intersect(c("sesall", "sesavgall", "ses_all"), names(cov))[1]
cov <- cov[, .(leaid = sprintf("%07d", as.integer(sedalea)), ses = get(sescol), perfrl, totenrl)]
t <- merge(merge(t, seda, by = "leaid", all.x = TRUE), cov, by = "leaid", all.x = TRUE)
# ACS 2013-17
acs_f <- file.path(ROOT, "data/external/acs17_tract.csv")
if (!file.exists(acs_f)) {
  v <- c(inc = "B19013_001", hval = "B25077_001", pop = "B01003_001", white = "B02001_002", ed_tot = "B15003_001",
         ba = "B15003_022", ma = "B15003_023", prof = "B15003_024", phd = "B15003_025", ten_tot = "B25003_001", own = "B25003_002",
         yrbuilt = "B25035_001", age = "B01002_001")
  fs <- intersect(unique(substr(t$tract, 1, 2)), unique(fips_codes$state_code[fips_codes$state_code < "57"]))
  a <- rbindlist(lapply(fs, \(s) tryCatch(as.data.table(get_acs("tract", variables = v, state = s, year = 2017, output = "wide")),
    error = \(e) { message("ACS failed for state ", s, ": ", conditionMessage(e)); NULL })), fill = TRUE)
  fwrite(a, acs_f)
}
a <- fread(acs_f, colClasses = c(GEOID = "character"))
a <- a[, .(tract = GEOID, inc = incE, hval = hvalE, pop = popE, sh_white = whiteE / popE, sh_ba = (baE + maE + profE + phdE) / ed_totE,
           sh_own = ownE / ten_totE, yrbuilt = yrbuiltE, age = ageE)]
t <- merge(t, a, by = "tract", all.x = TRUE)
t[, st := substr(tract, 1, 2)][, cty := substr(tract, 1, 5)]
arrow_ok <- FALSE
con <- dbConnect(duckdb()); duckdb_register(con, "t", t)
dbExecute(con, sprintf("COPY t TO '%s/data/local_juris/iv_tract.parquet' (FORMAT parquet)", ROOT)); dbDisconnect(con, shutdown = TRUE)
cat("tracts:", nrow(t), "| with gap30:", sum(!is.na(t$gap_p10_30)), "| with district:", sum(!is.na(t$leaid)), "| with SEDA ach:", sum(!is.na(t$ach)),
    "| with ACS income:", sum(!is.na(t$inc)), "\n")
