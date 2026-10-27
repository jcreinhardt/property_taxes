# State-to-state SOI migration dyads, files 1112-2122 (2026-10-27). One row per origin x destination x file:
# returns (n1), exemptions (n2), AGI ($k), from the outflow tables only (the inflow tables are the same flows seen
# from the destination). Built for the Diop & Green (2026) replication (designs/exposure_did/16), which uses their
# year convention: their year t = file t(t+1), the address change between the TY(t) and TY(t+1) returns (moves
# during ~t+1); our other scripts label the same file t+1. Both labels are kept (y1, y2).
# Source: data/SOI/migration_state/stateoutflowYYYY.csv (y1_statefips = origin, y2_statefips = destination).
# Dyads below the SOI disclosure threshold (20 returns) are absent in the source and stay absent here.
# Output: data/built/soi/state_dyads.csv (file, y1, y2, o, d, n1, n2, agi); DC (11) and foreign (57) kept as codes.
suppressPackageStartupMessages(library(data.table))
source("code/R/paths.R"); M <- file.path(DATA, "SOI/migration_state"); OUTD <- file.path(DATA, "built/soi"); dir.create(OUTD, showWarnings = FALSE, recursive = TRUE)
rd <- function(y1) {
  x <- fread(file.path(M, sprintf("stateoutflow%02d%02d.csv", y1 - 2000, y1 - 1999)), colClasses = "character", encoding = "Latin-1"); setnames(x, tolower(names(x)))
  x <- x[, .(o = as.integer(y1_statefips), d = as.integer(y2_statefips), n1 = as.numeric(n1), n2 = as.numeric(n2), agi = as.numeric(agi))]
  x[d %in% 1:57 & d != o][, y1 := y1] }                      # drops the 96-98 summary rows and the non-migrant / same-state rows
files <- rbindlist(lapply(2011:2021, rd))
files[, `:=`(file = sprintf("%02d%02d", y1 - 2000, y1 - 1999), y2 = y1 + 1L)]
setcolorder(files, c("file", "y1", "y2", "o", "d", "n1", "n2", "agi")); setorder(files, y1, o, d)
stopifnot(!anyDuplicated(files, by = c("y1", "o", "d")))
fwrite(files, file.path(OUTD, "state_dyads.csv"))
cat("dyads per file (50 states, excl. DC and foreign):\n"); print(files[o <= 56 & o != 11 & d <= 56 & d != 11, .(dyads = .N, origins = uniqueN(o), out_k = round(sum(n1) / 1e3)), by = file])
