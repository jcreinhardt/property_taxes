# Formerly code/analysis/salt_local/b7_repeat_sales_pooled.R (moved in the 2026-10-27 restructuring).
# Lean pooled repeat-sales border test on a larger share of parcels (the event-study version in b6 runs out of laptop memory
# above ~30%): log price ~ nh x post + nhx x post + nlx x post + nhx x late + nlx x late + exposure x post + exposure x late
# | parcel + pair^side^year + pair^dbin^year; SE by county. post = 2018+, late = 2021+.
suppressPackageStartupMessages({library(data.table); library(duckdb); library(fixest)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_iv"); SH <- as.numeric(Sys.getenv("RS_SHARE", "0.6"))
con <- dbConnect(duckdb())
s <- as.data.table(dbGetQuery(con, sprintf("SELECT clip, tract, y::SMALLINT y, ln(price)::FLOAT lp FROM '%s/data/built/corelogic/kpanel/repeat_sales.parquet'
  WHERE NOT newcon AND hash(clip) %% 1000 < %d", ROOT, round(SH * 1000)))); dbDisconnect(con, shutdown = TRUE)
iv <- unique(readRDS(file.path(DATA, "built/geo/iv_tract_side.rds"))[, .(tract, pair, high, dbin, near, cty)])
tz <- fread(file.path(DATA, "built/exposure/salt_exposure_tract_zip2016.csv"), colClasses = c(tract = "character"))[, .(tract, exz_z)]
d <- merge(merge(s, iv, by = "tract"), tz, by = "tract"); rm(s); gc()
d <- d[, if (.N >= 2) .SD, by = clip]
d[, `:=`(post = as.integer(y >= 2018), late = as.integer(y >= 2021), nh = near * high)]
d[, `:=`(nhx = nh * exz_z, nlx = near * (1 - high) * exz_z, tract = NULL)]
d[, `:=`(pg = .GRP), by = .(pair, high, y)]; d[, `:=`(dg = .GRP), by = .(pair, dbin, y)]
m <- feols(lp ~ nh:post + nhx:post + nlx:post + nhx:late + nlx:late + exz_z:post + exz_z:late | clip + pg + dg, d, cluster = ~cty, lean = TRUE, mem.clean = TRUE)
sink(file.path(OUT, sprintf("b7_repeat_sales_pooled_%02d.txt", round(100 * SH))))
cat("Pooled repeat-sales border test, share of parcels", SH, "| sales", nrow(d), "| parcels", uniqueN(d$clip), "\n")
print(as.data.table(coeftable(m), keep.rownames = "term")[, .(term, est = round(Estimate, 4), se = round(`Std. Error`, 4))])
sink(); cat(readLines(file.path(OUT, sprintf("b7_repeat_sales_pooled_%02d.txt", round(100 * SH)))), sep = "\n")
