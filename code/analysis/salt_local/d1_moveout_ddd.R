# Out-migration in the border design (Data Axle, Bouchet extract local_juris/bouchet/g_moveout_bkey_1pct.sql): address-level
# move-out = primary family_id at the address changes between consecutive annual files; clean pairs (one primary family,
# observed in both files). Addresses (1% building sample, tax-roll coordinates) assigned to 2010 tracts (TIGER cb 2019).
# Rates annualised by elapsed months between file cut-offs (dataaxle.md 4.5c); year = destination file (2013 = 2012->13).
# Designs on address-year data: (1) border DDD: move ~ i(y, near x high, ref 2017) | tract + pair^high^y + pair^dbin^y;
# (2) county exposure: move ~ i(y, ex_z, ref 2017) | tract + st^y; (3) near x high x exposure. SE by county.
suppressPackageStartupMessages({library(data.table); library(duckdb); library(sf); library(tigris); library(fixest); library(ggplot2)})
options(tigris_use_cache = TRUE)
ROOT <- "/Users/jr2728/PhD/projects/property_taxes"; OUT <- file.path(ROOT, "output/analysis/salt_local")
con <- dbConnect(duckdb()); m <- as.data.table(dbGetQuery(con, sprintf("SELECT * FROM '%s/data/local_juris/border_moveout/moveout_bkey_1pct.parquet'", ROOT)))
dbDisconnect(con, shutdown = TRUE)
m[, y := y + 1L]
cut <- c(`2012` = 3, `2013` = 15, `2014` = 27, `2015` = 40, `2016` = 54, `2017` = 65, `2018` = 79, `2019` = 90, `2020` = 102, `2021` = 114)
mon <- data.table(y = 2013:2021, months = diff(cut))
loc <- unique(m[, .(bkey, lat, lon)])
tr <- tracts(cb = TRUE, year = 2019, progress_bar = FALSE)[, "GEOID"]
pts <- st_as_sf(loc, coords = c("lon", "lat"), crs = 4326) |> st_transform(st_crs(tr))
j <- st_join(pts, tr, join = st_within, left = FALSE)
loc <- data.table(bkey = j$bkey, tract = j$GEOID)
m <- merge(m[both_obs & one_prim], loc, by = "bkey")
m <- merge(m, mon, by = "y")[, mv := 100 * as.integer(id_chg) * 12 / months]
iv <- unique(readRDS(file.path(ROOT, "data/local_juris/iv_tract_side.rds"))[, .(tract, pair, high, dbin, near, cty)])
ex <- fread(file.path(ROOT, "data/local_juris/salt_exposure_county2016.csv"))[, cty := sprintf("%05d", county)]
m[, cty := substr(tract, 1, 5)][, st := substr(tract, 1, 2)]
m <- merge(m, ex[, .(cty, ex_z)], by = "cty")
cat("address-years:", nrow(m), "| addresses:", uniqueN(m$bkey), "| mean annualised move-out %:", round(mean(m$mv), 2), "\n")
tid <- function(mod, pat, lab) as.data.table(coeftable(mod), keep.rownames = "term")[grepl(pat, term), .(y = as.integer(sub("y::(\\d+):.*", "\\1", term)),
  coef = Estimate, se = `Std. Error`, spec = lab)]
r <- tid(feols(mv ~ i(y, ex_z, ref = 2017) | tract + st^y, m, cluster = ~cty), "ex_z", "county exposure (per SD)")
r <- rbind(r, tid(feols(mv ~ i(y, ex_z, ref = 2017) | tract + st^y, m[oo_sfr == TRUE], cluster = ~cty), "ex_z", "county exposure, owner-occ. SFR"))
mb <- merge(m, iv[, .(tract, pair, high, dbin, near)], by = "tract")[, nh := near * high]
r <- rbind(r, tid(feols(mv ~ i(y, nh, ref = 2017) | tract + pair^high^y + pair^dbin^y, mb, cluster = ~cty), ":nh$", "border DDD near x high"),
          tid(feols(mv ~ i(y, nh, ref = 2017) + i(y, I(nh * ex_z), ref = 2017) | tract + pair^high^y + pair^dbin^y, mb, cluster = ~cty), "ex_z", "border DDD x exposure"))
r <- rbind(r, unique(r[, .(spec)])[, `:=`(y = 2017L, coef = 0, se = 0)])
fwrite(r, file.path(OUT, "d1_moveout_ddd.csv"))
sink(file.path(OUT, "d1_moveout_ddd.txt"))
cat("Data Axle address move-outs (annualised %), 1% building sample; border sample addresses:", uniqueN(mb$bkey), "| near-high:", uniqueN(mb[nh == 1, bkey]), "\n")
for (k in split(r, by = "spec")) { cat("\n==", k$spec[1], "\n")
  print(as.data.frame(dcast(k, . ~ y, value.var = "coef")[, -1][, lapply(.SD, round, 3)]), row.names = FALSE)
  print(as.data.frame(dcast(k, . ~ y, value.var = "se")[, -1][, lapply(.SD, round, 3)]), row.names = FALSE) }
sink(); cat(readLines(file.path(OUT, "d1_moveout_ddd.txt")), sep = "\n")
