# SALT deductions and AGI by state x tax year 2011-2022 from the SOI Historic Table 2 state files (2026-10-27), for the
# Diop & Green (2026) SALT measure (itemized state and local tax deductions / AGI, a state-year panel).
# Items (all returns, summed over AGI stubs): N1 returns, N04470 itemizers, A00100 AGI, A18425 state and local income
# taxes, A18500 real estate taxes, A18300 total taxes paid (all $k; 2018+ amounts are as claimed on Schedule A lines
# 5a-5c, before the $10k limit, but by the much smaller post-TCJA itemizer pool).
# Sources: data/SOI/state_returns/11in54cm.xlsx (wide layout: items in rows, state blocks across; the first column of
# each block is "All returns"), 12in54cmcsv.csv .. 22in55cmcsv.csv (13 is an xlsx with the csv layout).
# Output: data/built/exposure/soi_state_salt_panel.csv (fips, st, ty, n1, n_item, agi, salt_inc, salt_re, salt_tot).
suppressPackageStartupMessages({library(data.table); library(readxl)})
source("code/R/paths.R"); R <- file.path(DATA, "SOI/state_returns")
fips <- fread(file.path(DATA, "SOI/migration_state/stateoutflow1617.csv"), colClasses = "character")[y1_statefips == y2_statefips, .(fips = as.integer(y1_statefips), st = y2_state)]
names_up <- c(toupper(state.name), "DISTRICT OF COLUMBIA"); abb <- c(state.abb, "DC")

rd_csv <- function(ty) {                                   # TY2012-2022: csv (2013 as xlsx), one row per state x AGI stub
  f <- list.files(R, sprintf("^%02din5[45]cm.*\\.(csv|xlsx)$", ty - 2000), full.names = TRUE); stopifnot(length(f) == 1)
  x <- if (grepl("xlsx$", f)) as.data.table(read_excel(f)) else fread(f); setnames(x, toupper(names(x)))
  num <- c("N1", "N04470", "A00100", "A18425", "A18500", "A18300")
  x[, (num) := lapply(.SD, \(v) as.numeric(gsub(",", "", v))), .SDcols = num]
  x[AGI_STUB > 0 & STATE %in% abb, .(ty = ty, n1 = sum(N1), n_item = sum(N04470), agi = sum(A00100), salt_inc = sum(A18425), salt_re = sum(A18500), salt_tot = sum(A18300)), by = .(st = STATE)] }
rd_2011 <- function() {                                    # TY2011: wide xlsx
  x <- as.matrix(suppressMessages(read_excel(file.path(R, "11in54cm.xlsx"), col_names = FALSE, .name_repair = "minimal")))
  hdr <- which(x[, 1] == "Item"); stopifnot(length(hdr) == 1)
  nm <- trimws(gsub("\\[.*\\]", "", x[hdr, ])); cols <- which(nm %in% names_up)
  item <- function(pat, off = 0) { r <- grep(pat, x[, 1]); stopifnot(length(r) == 1); as.numeric(x[r + off, cols]) }
  data.table(st = abb[match(nm[cols], names_up)], ty = 2011L, n1 = item("^Number of returns$"), n_item = item("^Total itemized deductions"),
             agi = item("^Adjusted gross income \\(AGI\\)"), salt_inc = item("^State and local income taxes:", 1), salt_re = item("^Real estate taxes:", 1), salt_tot = item("^Total taxes paid:", 1)) }
p <- rbind(rd_2011(), rbindlist(lapply(2012:2022, rd_csv)))
p <- merge(fips, p, by = "st"); setorder(p, ty, fips)
stopifnot(p[, .N, by = ty][, all(N == 51)])
fwrite(p, file.path(DATA, "built/exposure/soi_state_salt_panel.csv"))
cat("SALT / AGI (%), income taxes and income + real estate, 50 states excl. DC:\n")
print(p[st != "DC", .(inc_mean = 100 * mean(salt_inc / agi), inc_sd = 100 * sd(salt_inc / agi), inc_max = 100 * max(salt_inc / agi), incre_mean = 100 * mean((salt_inc + salt_re) / agi), incre_sd = 100 * sd((salt_inc + salt_re) / agi), itemizers = round(100 * sum(n_item) / sum(n1), 1)), by = ty][, lapply(.SD, round, 2), by = ty])
