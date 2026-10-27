# Formerly code/analysis/salt_local/d3_family_moves.R (moved in the 2026-10-27 restructuring).
# Direction of cross-border moves (Data Axle 1% family sample, Bouchet f_family_moves_1pct.sql). Origin and destination are
# placed in 2010 tracts (TIGER cb 2019) and matched to the border design (R/26: nearest state-border pair, high/low side,
# distance). Cross-border moves = origin and destination within 20 mi of the same pair, on opposite sides. Outcome per year:
# share of these moves going high -> low side; and per pair-year with pair FE (ref 2017), weighted by moves; split by the
# pair's SALT contrast (high-side state more exposed than the low side, return-weighted 2016 county exposure).
suppressPackageStartupMessages({library(data.table); library(duckdb); library(sf); library(tigris); library(fixest)})
options(tigris_use_cache = TRUE)
source("code/R/paths.R"); OUT <- out_dir("designs/border_iv")
FM <- Sys.getenv("FM_IN", "family_moves_1pct.parquet"); SFX <- Sys.getenv("FM_SFX", "")
con <- dbConnect(duckdb()); mv <- as.data.table(dbGetQuery(con, sprintf("SELECT * FROM read_parquet('%s/data/built/dataaxle/border_moveout/%s') WHERE ost <> dst  -- cross-border moves change state", ROOT, FM))); dbDisconnect(con, shutdown = TRUE)
tr <- tracts(cb = TRUE, year = 2019, progress_bar = FALSE)[, "GEOID"]
loc <- function(lat, lon) { p <- st_transform(st_as_sf(data.table(i = seq_along(lat), lat, lon), coords = c("lon", "lat"), crs = 4326), st_crs(tr))
  j <- st_join(p, tr, join = st_within, left = TRUE); j <- j[!duplicated(j$i), ]; j$GEOID }
mv[, `:=`(otr = loc(olat, olon), dtr = loc(dlat, dlon))]
iv <- unique(readRDS(file.path(DATA, "built/geo/iv_tract_side.rds"))[, .(tract, pair, high, dist_mi)])
mv <- merge(mv, iv[, .(otr = tract, opair = pair, ohigh = high, odist = dist_mi)], by = "otr")
mv <- merge(mv, iv[, .(dtr = tract, dpair = pair, dhigh = high, ddist = dist_mi)], by = "dtr")
x <- mv[opair == dpair & odist < 20 & ddist < 20 & ohigh != dhigh][, hl := as.integer(ohigh == 1)]
ex <- fread(file.path(DATA, "built/exposure/salt_exposure_county2016.csv"))
exs <- ex[, .(ex_st = weighted.mean(excess_pp, returns)), by = .(st = sprintf("%02d", county %/% 1000))]
fips <- unique(as.data.table(tigris::fips_codes)[, .(st = state_code, ab = state)])
side <- unique(readRDS(file.path(DATA, "built/geo/iv_tract_side.rds"))[, .(pair, high, st)])[, .(st = names(which.max(table(st)))), by = .(pair, high)]
side <- merge(side, exs, by = "st"); con2 <- dcast(side, pair ~ high, value.var = "ex_st"); setnames(con2, c("0", "1"), c("ex_low", "ex_high"))
con2[, salt_contrast := as.integer(ex_high > ex_low)]
x <- merge(x, con2[, .(opair = pair, salt_contrast)], by = "opair", all.x = TRUE)
yr <- x[, .(moves = .N, share_hl = mean(hl)), by = .(y, salt_contrast)][order(salt_contrast, y)]
py <- x[, .(n = .N, share_hl = mean(hl)), by = .(pair = opair, y, salt_contrast)]
m1 <- feols(share_hl ~ i(y, ref = 2017) | pair, py, weights = ~n, cluster = ~pair)
m2 <- feols(share_hl ~ i(y, salt_contrast, ref = 2017) | pair + y, py, weights = ~n, cluster = ~pair)
sink(file.path(OUT, paste0("d3_family_moves", SFX, ".txt")))
cat("Data Axle family moves (", FM, "), interstate, matched to border tracts:", nrow(mv), "; cross-border (opposite sides, both < 20 mi of the same pair):", nrow(x), "\n")
cat("\nShare of cross-border moves going high -> low side, by year and pair SALT contrast (1 = high-tax side's state more exposed):\n")
print(dcast(yr, y ~ salt_contrast, value.var = c("share_hl", "moves"))[, lapply(.SD, \(v) if (is.double(v)) round(v, 3) else v)])
cat("\nPair FE, year effects (ref 2017), weighted by moves:\n"); print(as.data.table(coeftable(m1), keep.rownames = "term")[, .(term, est = round(Estimate, 3), se = round(`Std. Error`, 3))])
cat("\nDifference for pairs where the high side is the more SALT-exposed state (pair + year FE):\n")
print(as.data.table(coeftable(m2), keep.rownames = "term")[, .(term, est = round(Estimate, 3), se = round(`Std. Error`, 3))])
sink(); cat(readLines(file.path(OUT, paste0("d3_family_moves", SFX, ".txt"))), sep = "\n")
