# Formerly code/analysis/salt_local/c10_district_data_coverage.R (moved in the 2026-10-27 restructuring).
# Which district-level data cover the TIGER school-district universe? Universe: TIGER cb 2019 unified, elementary and
# secondary polygons in the lower 48 + DC, excluding "not defined" pseudo-districts. Coverage is reported as a share of
# districts and as a share of parcels (1% CoreLogic residential sample, point-in-polygon; a parcel counts for its unified
# district, else its elementary district; secondary districts reported separately).
# Sources on disk: F-33 / Annual Survey of School System Finances FY2012-2023 (data/external/elsec), SEDA 4.1 achievement
# and SES (data/external/seda41), CoreLogic parcels with a 2016 bill and value (the 1% sample).
suppressPackageStartupMessages({library(data.table); library(sf); library(tigris); library(duckdb)})
options(tigris_use_cache = TRUE); sf_use_s2(FALSE)
source("code/R/paths.R"); OUT <- out_dir("validation/coverage")
sdp <- function(type) { s <- school_districts(cb = TRUE, year = 2019, type = type, progress_bar = FALSE)
  s <- s[!s$STATEFP %in% c("02", "15", "60", "66", "69", "72", "78") & !substr(s$GEOID, 3, 7) %in% c("99997", "99998", "99999"), "GEOID"]
  s$type <- type; st_transform(s, 5070) }
P <- rbind(sdp("unified"), sdp("elementary"), sdp("secondary"))
U <- as.data.table(st_drop_geometry(P))
# parcels
con <- dbConnect(duckdb())
p <- as.data.table(dbGetQuery(con, sprintf("SELECT clip, st, lat, lon, bill16, av16, mv16, rate16 FROM '%s/data/built/corelogic/parcel_1pct/parcels.parquet'
  WHERE st NOT IN ('AK', 'HI', 'PR', 'GU', 'VI') AND lat IS NOT NULL AND lat <> 0", ROOT)))
dbDisconnect(con, shutdown = TRUE)
pp <- st_transform(st_as_sf(p[, .(clip, lat, lon)], coords = c("lon", "lat"), crs = 4326), 5070)
j <- as.data.table(st_drop_geometry(st_join(pp, P, join = st_within)))[!is.na(GEOID)]
j <- dcast(unique(j, by = c("clip", "type")), clip ~ type, value.var = "GEOID")
p <- merge(p, j, by = "clip", all.x = TRUE)
p[, lea := fcoalesce(unified, elementary)]
p[, has_tax := !is.na(bill16) & bill16 > 0 & ((!is.na(av16) & av16 > 0) | (!is.na(mv16) & mv16 > 0))]
# sources
fin <- readRDS(file.path(DATA, "built/census_finances/district_finance.rds"))
seda <- fread(file.path(DATA, "external/seda41/seda_geodist_pool_cs_4.1.csv"), colClasses = c(sedalea = "character"))[subgroup == "all"]
seda <- unique(seda[!is.na(cs_mn_avg_ol), .(leaid = sprintf("%07d", as.integer(sedalea)))])$leaid
cov <- fread(file.path(DATA, "external/seda41/seda_cov_geodist_pool_4.1.csv"), colClasses = c(sedalea = "character"))
sescol <- intersect(c("sesall", "sesavgall", "ses_all"), names(cov))[1]
ses <- unique(cov[!is.na(get(sescol)), sprintf("%07d", as.integer(sedalea))])
yrs <- sort(unique(fin$y))
fin_any <- lapply(setNames(yrs, yrs), \(yy) fin[y == yy, leaid])
fin_pt <- lapply(setNames(yrs, yrs), \(yy) fin[y == yy & ptax > 0, leaid])
bal <- fin[, .N, by = leaid][N == length(yrs), leaid]
cl_any <- unique(p$lea); cl_tax <- unique(p[has_tax == TRUE, lea])
src <- c(list(`F-33 in all 12 years (2012-23)` = bal, `F-33 FY2016` = fin_any[["2016"]], `F-33 FY2016, property tax > 0` = fin_pt[["2016"]],
              `F-33 FY2023` = fin_any[["2023"]], `SEDA 4.1 achievement` = seda, `SEDA 4.1 SES` = ses,
              `CoreLogic 1% sample: >= 1 parcel` = cl_any, `CoreLogic 1% sample: >= 1 parcel with 2016 bill and value` = cl_tax))
res <- rbindlist(lapply(names(src), \(nm) { s <- src[[nm]]
  data.table(source = nm,
    unified = mean(U[type == "unified", GEOID] %in% s), elementary = mean(U[type == "elementary", GEOID] %in% s),
    secondary = mean(U[type == "secondary", GEOID] %in% s),
    parcels = mean(p$lea %in% s), `parcels (secondary layer, where present)` = p[!is.na(secondary), mean(secondary %in% s)]) }))
res[, (2:6) := lapply(.SD, \(x) round(100 * x, 1)), .SDcols = 2:6]
sink(file.path(OUT, "c10_district_data_coverage.txt"), split = TRUE)
cat("Universe: TIGER cb 2019 polygons, lower 48 + DC, excl. 'not defined':", U[type == "unified", .N], "unified,", U[type == "elementary", .N], "elementary,",
    U[type == "secondary", .N], "secondary | parcels with coordinates:", nrow(p), "(in a district:", round(100 * mean(!is.na(p$lea)), 2), "%)\n")
cat("Cells: % of districts (by type) or % of parcels whose district is in the source\n")
print(res)
cat("\nF-33 coverage by year (% of unified + elementary polygons; % of parcels):\n")
print(rbindlist(lapply(yrs, \(yy) data.table(y = yy, districts = round(100 * mean(U[type != "secondary", GEOID] %in% fin_any[[as.character(yy)]]), 1),
  parcels = round(100 * mean(p$lea %in% fin_any[[as.character(yy)]]), 2)))))
cat("\nParcel share of districts missing SEDA achievement, by state (top 10):\n")
print(p[!is.na(lea), .(n = .N, pct_no_seda = round(100 * mean(!lea %in% seda), 1)), by = st][order(-pct_no_seda)][1:10])
sink()
