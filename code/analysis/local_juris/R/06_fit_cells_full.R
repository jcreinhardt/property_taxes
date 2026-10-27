# Full roll, step C (local): median-regression rate per cell x homestead group from the cluster fit sample (<= 2000 parcels
# per group). Writes cell_fits16.parquet (cell, hs, n_grp, a, slope) for the on-line evaluation on the cluster.
suppressPackageStartupMessages({library(data.table); library(duckdb); library(quantreg)})
source("code/R/paths.R"); DIR <- file.path(ROOT, "data/local_juris/full")
con <- dbConnect(duckdb()); s <- as.data.table(dbGetQuery(con, sprintf("SELECT cell, hs, n_grp, bill, av FROM '%s/fitsample16.parquet'", DIR)))
f <- function(bill, av) { r <- tryCatch(coef(rq.fit(cbind(1, av), bill, tau = .5, method = "br")), error = function(e) c(NA_real_, NA_real_)); list(a = r[1], slope = r[2]) }
t0 <- Sys.time()
fits <- s[, c(list(n_grp = n_grp[1], n_fit = .N), f(bill, av)), by = .(cell, hs)]
# pooled fit per cell (homesteads and non-homesteads together), from the same sample
pool <- s[, c(list(n_fit = .N), f(bill, av)), by = cell][, hs := NA]
fits <- rbind(fits, pool, fill = TRUE)
cat("cells x groups fitted:", nrow(fits), " in", round(as.numeric(difftime(Sys.time(), t0, units = "mins")), 1), "min\n")
duckdb::dbWriteTable(con, "fits", fits); dbExecute(con, sprintf("COPY fits TO '%s/cell_fits16.parquet' (FORMAT parquet)", DIR))
dbDisconnect(con, shutdown = TRUE)
