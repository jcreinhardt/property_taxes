# School-district finance panel FY2012-2023 (split out of salt_local/a3_district_panel.R, 2026-10-27). Census Annual Survey
# of School System Finances, district files elsecYY.txt in data/external/elsec/ (elsec22.xlsx: the 2022 txt is truncated):
# property tax T06, total local revenue TLOCREV, state revenue TSTREV, current spending TCURELSC, enrollment V33, keyed by
# NCESID (= SEDA / TIGER LEAID). Amounts in $ thousands. Districts with a 7-digit id and enrollment > 0.
# Output: data/built/census_finances/district_finance.rds (y, leaid, conum, name, enr, ptax, loc, strev, spend, schlev).
suppressPackageStartupMessages({library(data.table)})
source("code/R/paths.R"); E <- file.path(DATA, "external/elsec")
rd <- function(yy) { if (yy == 22) x <- as.data.table(readxl::read_excel(file.path(E, "elsec22.xlsx"), col_types = "text")) else {
  fn <- file.path(E, sprintf("elsec%02d.txt", yy)); hd <- gsub('"', "", strsplit(readLines(fn, 1), ",")[[1]])
  x <- fread(fn, skip = 1, header = FALSE, colClasses = "character", fill = TRUE); setnames(x, seq_along(hd), hd) }
  x[, .(y = 2000L + yy, leaid = NCESID, conum = CONUM, name = NAME, enr = as.numeric(V33), ptax = as.numeric(T06), loc = as.numeric(TLOCREV),
        strev = as.numeric(TSTREV), spend = as.numeric(TCURELSC), schlev = SCHLEV)] }
p <- rbindlist(lapply(12:23, rd))
p <- p[nchar(leaid) == 7 & leaid != "" & enr > 0]
cat("district-years:", nrow(p), "| districts:", uniqueN(p$leaid), "\n"); print(p[, .N, by = y][order(y)])
saveRDS(p, file.path(DATA, "built/census_finances/district_finance.rds"))
