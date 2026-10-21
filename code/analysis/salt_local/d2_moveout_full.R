# Full-data Data Axle move-outs by tract-year (Bouchet m_moveout_tract_full.sql), same designs as d1 at the tract-year level,
# weighted by the number of address pairs; annualised by file cut-offs.
suppressPackageStartupMessages({library(data.table); library(duckdb); library(fixest)})
ROOT <- "/Users/jr2728/PhD/projects/property_taxes"; OUT <- file.path(ROOT, "output/analysis/salt_local")
con <- dbConnect(duckdb()); m <- as.data.table(dbGetQuery(con, sprintf("SELECT * FROM '%s/data/local_juris/border_moveout/moveout_tract_full.parquet'", ROOT))); dbDisconnect(con, shutdown = TRUE)
# y is already the destination file in the extract
cut <- c(`2012` = 3, `2013` = 15, `2014` = 27, `2015` = 40, `2016` = 54, `2017` = 65, `2018` = 79, `2019` = 90, `2020` = 102, `2021` = 114)
m <- merge(m, data.table(y = 2013:2021, months = diff(cut)), by = "y")[, mv := 100 * moves / n * 12 / months][n >= 20]
iv <- unique(readRDS(file.path(ROOT, "data/local_juris/iv_tract_side.rds"))[, .(tract, pair, high, dbin, near)])
ex <- fread(file.path(ROOT, "data/local_juris/salt_exposure_county2016.csv"))[, cty := sprintf("%05d", county)]
m[, `:=`(cty = substr(tract, 1, 5), st = substr(tract, 1, 2))]; m <- merge(m, ex[, .(cty, ex_z)], by = "cty")
tid <- function(mod, pat, lab) as.data.table(coeftable(mod), keep.rownames = "term")[grepl(pat, term), .(y = as.integer(sub("y::(\\d+):.*", "\\1", term)), coef = Estimate, se = `Std. Error`, spec = lab)]
mb <- merge(m, iv, by = "tract")[, nh := near * high]
r <- rbind(tid(feols(mv ~ i(y, ex_z, ref = 2017) | tract + st^y, m, cluster = ~cty, weights = ~n), "ex_z", "county exposure (per SD)"),
           tid(feols(mv ~ i(y, nh, ref = 2017) | tract + pair^high^y + pair^dbin^y, mb, cluster = ~cty, weights = ~n), ":nh$", "border DDD near x high"),
           tid(feols(mv ~ i(y, nh, ref = 2017) + i(y, I(nh * ex_z), ref = 2017) | tract + pair^high^y + pair^dbin^y, mb, cluster = ~cty, weights = ~n), "ex_z", "border DDD x exposure"))
r <- rbind(r, unique(r[, .(spec)])[, `:=`(y = 2017L, coef = 0, se = 0)])
fwrite(r, file.path(OUT, "d2_moveout_full.csv"))
sink(file.path(OUT, "d2_moveout_full.txt"))
cat("Full Data Axle move-outs by tract-year (annualised %); tract-years:", nrow(m), "| tracts:", uniqueN(m$tract), "| address pairs:", sum(m$n), "| mean:", round(weighted.mean(m$mv, m$n), 2), "\n")
for (k in split(r, by = "spec")) { cat("\n==", k$spec[1], "\n")
  print(as.data.frame(dcast(k, . ~ y, value.var = "coef")[, -1][, lapply(.SD, round, 3)]), row.names = FALSE)
  print(as.data.frame(dcast(k, . ~ y, value.var = "se")[, -1][, lapply(.SD, round, 3)]), row.names = FALSE) }
sink(); cat(readLines(file.path(OUT, "d2_moveout_full.txt")), sep = "\n")
