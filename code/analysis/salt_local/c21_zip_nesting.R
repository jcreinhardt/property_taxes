# How cleanly do ZIP codes nest in census tracts and school districts? 1% CoreLogic residential parcel sample (lower 48 + DC):
# parcel ZIP (zip5), 2010 tract (CoreLogic tract field), school district by point-in-polygon (TIGER cb 2019 unified, else
# elementary). For each ZIP (and each tract / district): share of its parcels in its modal partner unit; parcel-weighted.
suppressPackageStartupMessages({library(data.table); library(sf); library(tigris); library(duckdb)})
options(tigris_use_cache = TRUE); sf_use_s2(FALSE)
source("code/R/paths.R"); OUT <- file.path(ROOT, "output/analysis/salt_local")
con <- dbConnect(duckdb())
p <- as.data.table(dbGetQuery(con, sprintf("SELECT clip, lpad(zip5::VARCHAR, 5, '0') zip, CASE WHEN tract IS NOT NULL THEN lpad(fips::VARCHAR, 5, '0') || lpad(tract::VARCHAR, 6, '0') END tr,
  lat, lon FROM '%s/data/salt_cap/panel/parcels.parquet' WHERE st NOT IN ('AK','HI','PR','GU','VI') AND lat IS NOT NULL AND lat <> 0 AND zip5 IS NOT NULL", ROOT)))
dbDisconnect(con, shutdown = TRUE)
pp <- st_transform(st_as_sf(p[, .(clip, lat, lon)], coords = c("lon", "lat"), crs = 4326), 5070)
sdj <- \(type) { s <- st_transform(school_districts(cb = TRUE, year = 2019, type = type, progress_bar = FALSE)[, "GEOID"], 5070)
  as.data.table(st_drop_geometry(st_join(pp, s, join = st_within)))[, .(g = GEOID[1]), by = clip] }
u <- sdj("unified"); e <- sdj("elementary")
p <- merge(p, u[, .(clip, un = g)], by = "clip", all.x = TRUE); p <- merge(p, e[, .(clip, el = g)], by = "clip", all.x = TRUE)
p[, sd := fcoalesce(un, el)]
nest <- function(a, b, lab) { x <- p[!is.na(get(a)) & !is.na(get(b))]
  z <- x[, .N, by = c(a, b)][, `:=`(tot = sum(N), k = .N), by = a][, .(top = max(N) / tot[1], k = k[1], tot = tot[1]), by = a]
  data.table(mapping = lab, units = nrow(z), parcels = sum(z$tot), `parcel share in modal partner` = round(sum(z$top * z$tot) / sum(z$tot), 3),
             `units wholly in one partner (parcel-wtd)` = round(sum(z$tot[z$k == 1]) / sum(z$tot), 3),
             `units with >= 90% in modal (parcel-wtd)` = round(sum(z$tot[z$top >= .9]) / sum(z$tot), 3),
             `median partners per unit` = median(z$k)) }
res <- rbind(nest("zip", "tr", "ZIP -> tract"), nest("tr", "zip", "tract -> ZIP"), nest("zip", "sd", "ZIP -> school district"),
             nest("sd", "zip", "school district -> ZIP"), nest("tr", "sd", "tract -> school district"))
fwrite(res, file.path(OUT, "c21_zip_nesting.csv"))
sink(file.path(OUT, "c21_zip_nesting.txt"), split = TRUE)
cat("Parcels with ZIP and coordinates:", nrow(p), "| with CoreLogic tract:", p[!is.na(tr), .N], "| in a district:", p[!is.na(sd), .N], "\n")
print(res)
sink()
