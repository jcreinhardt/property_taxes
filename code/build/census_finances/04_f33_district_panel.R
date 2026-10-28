# School-district finance panel FY2012-2023 (split out of salt_local/a3_district_panel.R, 2026-10-27). Census Annual Survey
# of School System Finances, district files elsecYY.txt in data/external/elsec/ (elsec22.xlsx: the 2022 txt is truncated):
# property tax T06, total local revenue TLOCREV, state revenue TSTREV, current spending TCURELSC, enrollment V33, keyed by
# NCESID (= SEDA / TIGER LEAID). Amounts in $ thousands. Districts with a 7-digit id and enrollment > 0.
# Since 2026-10-28 also the revenue-by-source, spending-by-function and -object, capital outlay, payments, debt and cash
# items (F-33 item codes in `more` below; a missing code in a year gives NA).
# Output: data/built/census_finances/district_finance.rds (y, leaid, conum, cbsa (F-33 CBSA code, 'N' if none), name, enr, ptax, loc, strev, spend, schlev, + more).
suppressPackageStartupMessages({library(data.table)})
source("code/R/paths.R"); E <- file.path(DATA, "external/elsec")
rd <- function(yy) { if (yy == 22) x <- as.data.table(readxl::read_excel(file.path(E, "elsec22.xlsx"), col_types = "text")) else {
  fn <- file.path(E, sprintf("elsec%02d.txt", yy)); hd <- gsub('"', "", strsplit(readLines(fn, 1), ",")[[1]])
  x <- fread(fn, skip = 1, header = FALSE, colClasses = "character", fill = TRUE); setnames(x, seq_along(hd), hd) }
  out <- x[, .(y = 2000L + yy, leaid = NCESID, conum = CONUM, cbsa = CBSA, name = NAME, enr = as.numeric(V33), ptax = as.numeric(T06), loc = as.numeric(TLOCREV),
        strev = as.numeric(TSTREV), spend = as.numeric(TCURELSC), schlev = SCHLEV)]
  for (k in names(more)) { cols <- more[[k]]; out[, (k) := if (all(cols %in% names(x))) rowSums(x[, lapply(.SD, as.numeric), .SDcols = cols], na.rm = FALSE) else NA_real_] }
  out }
# F-33 item codes (Census Annual Survey of School System Finances documentation)
more <- list(totrev = "TOTALREV", fedrev = "TFEDREV", title1 = "C14", idea = "C15", nutrition = "C25", st_formula = "C01", st_sped = "C05", st_transp = "C06",
  st_comp = "C07", loc_parent = "T02", loc_salestax = "T09", loc_inctax = "T40", loc_othertax = "T99", loc_othersys = "D11", loc_cities = "D23",
  tuition = "A07", lunch = "A09", activity = "A13", fees = "A15", private_contrib = "U50",
  totexp = "TOTALEXP", inst = "TCURINST", ssvc = "TCURSSVC", pupil_support = "E17", staff_support = "E07", gen_admin = "E08", sch_admin = "E09",
  om = "V40", transp = "V45", other_support = "V90", food = "E11", nonelsec = "NONELSEC", capout = "TCAPOUT", construction = "F12", land = "G15",
  equip = c("K09", "K10"), pay_private = "V91", pay_charter = "V92", pay_state = "L12", pay_local = "M12", pay_othersys = "Q11", interest = "I86",
  salaries = "Z32", benefits = "Z33", inst_salaries = "V11", inst_benefits = "V13", debt_begin = "_19H", debt_issued = "_21F", debt_retired = "_31F",
  debt_end = "_41F", stdebt_end = "_66V", cash = c("W01", "W31", "W61"))
p <- rbindlist(lapply(12:23, rd))
p <- p[nchar(leaid) == 7 & leaid != "" & enr > 0]
cat("district-years:", nrow(p), "| districts:", uniqueN(p$leaid), "\n"); print(p[, .N, by = y][order(y)])
cat("share non-missing by year for the added items (NA = code absent that year):\n"); print(p[, lapply(.SD, \(v) round(mean(!is.na(v)), 2)), by = y, .SDcols = names(more)][order(y)][, 1:12])
saveRDS(p, file.path(DATA, "built/census_finances/district_finance.rds"))
