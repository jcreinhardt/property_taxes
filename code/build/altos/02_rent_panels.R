# Altos rental listings -> annual rent panels at ZIP, county and tract level (data/built/altos/).
# Input: the Bouchet exports of 01_altos_unit_month.sql copied to data/altos/ (altos_zip_month.parquet, altos_tract_year.parquet,
# altos_file_stats.parquet). Cells are geography x year x dwelling type (apartment / house / other) x bedrooms (0-4, -1 unknown)
# with n listings (ZIP: unit-months; tract: unit-years), mean log asking rent (mlp) and median rent (p50).
# Rent index per geography-year: lr_adj = geography x year fixed effect of mlp ~ geo x year + type x beds (cells weighted by n),
# i.e. mean log rent holding the type x bedroom mix fixed; lr_raw = n-weighted mean log rent; p50_apt2 = median rent of
# 2-bedroom apartments. County = ZIP's main county in the HUD ZIP-county crosswalk (largest residential share).
# Years are calendar years of the listing (the SALT cap first binds for tax year 2018).
suppressPackageStartupMessages({library(data.table); library(duckdb); library(fixest); library(readxl)})
source("code/R/paths.R"); IN <- file.path(DATA, "altos"); OUT <- file.path(DATA, "built/altos"); dir.create(OUT, showWarnings = FALSE, recursive = TRUE)
con <- dbConnect(duckdb()); q <- \(s) as.data.table(dbGetQuery(con, s))
zm <- q(sprintf("SELECT zip, y, m, type, beds, n, p50, mlp FROM '%s/altos_zip_month.parquet'", IN))
tyr <- q(sprintf("SELECT tract, y, type, beds, n, p50, mlp FROM '%s/altos_tract_year.parquet'", IN))
fs <- q(sprintf("SELECT * FROM '%s/altos_file_stats.parquet' ORDER BY fd", IN)); dbDisconnect(con, shutdown = TRUE)
fwrite(fs, file.path(OUT, "file_stats.csv"))
hud <- as.data.table(read_excel(file.path(DATA, "hud_zip_to_county_crosswalk.xlsx")))[order(ZIP, -RES_RATIO)][, .SD[1], by = ZIP][, .(zip = ZIP, fips = COUNTY)]
# ZIP x year cells (unit-months summed over months; mean log rent weighted; p50 = median of the monthly medians)
zy <- zm[, .(n = sum(n), mlp = weighted.mean(mlp, n), p50 = median(p50)), by = .(zip, y, type, beds)]
cy <- merge(zy, hud, by = "zip")[, .(n = sum(n), mlp = weighted.mean(mlp, n), p50 = median(p50)), by = .(fips, y, type, beds)]
index <- function(cells, id) {
  cells <- cells[n > 0 & is.finite(mlp)]; cells[, `:=`(gy = paste(get(id), y), cell = paste(type, beds))]
  m <- feols(mlp ~ 1 | gy + cell, cells, weights = ~n)
  f <- fixef(m)$gy; fe <- data.table(gy = names(f), lr_adj = unname(f))
  tot <- cells[, .(n = sum(n), lr_raw = weighted.mean(mlp, n), p50_apt2 = p50[type == "apartment" & beds == 2][1],
                   n_apt2 = sum(n[type == "apartment" & beds == 2]), sh_house = sum(n[type == "house"]) / sum(n)), by = .(gy, g = get(id), y)]
  merge(tot, fe, by = "gy")[, gy := NULL][order(g, y)] |> setnames("g", id) }
z <- index(zy, "zip"); z <- merge(z, hud, by = "zip", all.x = TRUE)[, st := substr(fips, 1, 2)]
cc <- index(cy, "fips")[, st := substr(fips, 1, 2)]
tr <- index(tyr, "tract")[, `:=`(fips = substr(tract, 1, 5), st = substr(tract, 1, 2))]
saveRDS(z, file.path(OUT, "zip_year.rds")); saveRDS(cc, file.path(OUT, "county_year.rds")); saveRDS(tr, file.path(OUT, "tract_year.rds"))
sink(file.path(out_dir("build/altos"), "02_rent_panels.txt"), split = TRUE)
cat("Altos weekly files:", nrow(fs), " first", as.character(min(fs$fd)), " last", as.character(max(fs$fd)), "\n")
cat("Listings per week by year (mean), share with usable rent / coordinates / 5-digit ZIP, apartment share, median rent:\n")
print(as.data.frame(fs[, .(files = .N, rows = round(mean(n)), units = round(mean(units)), sh_price = round(mean(sh_price_ok), 3), sh_coord = round(mean(sh_coord), 3),
                            sh_zip5 = round(mean(sh_zip5), 3), sh_apt = round(mean(sh_apt), 3), p50 = median(p50)), by = .(y = year(fd))]), row.names = FALSE)
for (k in list(list("ZIP", z, "zip"), list("county", cc, "fips"), list("tract", tr, "tract"))) {
  d <- k[[2]]; cat("\n==", k[[1]], "x year:", nrow(d), "rows,", uniqueN(d[[k[[3]]]]), "units\n")
  print(as.data.frame(d[, .(units = .N, n10 = sum(n >= 10), n50 = sum(n >= 50), listings = sum(n), lr_adj = round(mean(lr_adj), 3), lr_raw = round(mean(lr_raw), 3),
                            p50_apt2 = median(p50_apt2, na.rm = TRUE)), by = y][order(y)]), row.names = FALSE) }
sink()
