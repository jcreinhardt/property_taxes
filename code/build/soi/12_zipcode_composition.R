# ZIP-year household composition from the SOI ZIP files 2011-2022 (data/SOI/zipcode_returns_w_agi/YYzpallagi.csv), summed over
# AGI classes: returns (N1), individuals (N2: exemptions through 2017, persons on the return from 2018), dependents (NUMDEP,
# 2012+), filing status (MARS1 single, MARS2 joint, MARS4 head of household; 2012+), elderly returns (ELDERLY, 60+, 2015-18
# only). Output data/built/soi/zip_composition.csv with the ratios used as outcomes.
suppressPackageStartupMessages(library(data.table))
source("code/R/paths.R"); D <- file.path(DATA, "SOI/zipcode_returns_w_agi"); dir.create(file.path(DATA, "built/soi"), showWarnings = FALSE)
z <- rbindlist(lapply(list.files(D, pattern = "zpallagi\\.csv$", full.names = TRUE), \(f) { a <- fread(f); setnames(a, toupper(names(a)))
  for (k in c("NUMDEP", "MARS1", "MARS2", "MARS4", "ELDERLY")) if (!k %in% names(a)) a[, (k) := NA_real_]
  a[ZIPCODE != 0 & ZIPCODE != 99999, .(y = 2000 + as.integer(substr(basename(f), 1, 2)), zip = sprintf("%05d", ZIPCODE), n1 = sum(N1), n2 = sum(N2), numdep = sum(NUMDEP), mars1 = sum(MARS1), mars2 = sum(MARS2), mars4 = sum(MARS4), elderly = sum(ELDERLY)), by = .(ZIPCODE)][, ZIPCODE := NULL] }))
z[, `:=`(ind_per_ret = n2 / n1, dep_per_ret = numdep / n1, sh_single = mars1 / n1, sh_joint = mars2 / n1, sh_hoh = mars4 / n1, sh_elderly = elderly / n1, lret = log(n1), lind = log(n2))]
fwrite(z[order(zip, y)], file.path(DATA, "built/soi/zip_composition.csv"))
cat("ZIP-years:", nrow(z), "| ZIPs:", uniqueN(z$zip), "\n"); print(z[, .(zips = .N, ret_m = round(sum(n1) / 1e6, 1), ind_per_ret = round(weighted.mean(ind_per_ret, n1), 3), dep_per_ret = round(weighted.mean(dep_per_ret, n1, na.rm = TRUE), 3), sh_joint = round(weighted.mean(sh_joint, n1, na.rm = TRUE), 3), sh_hoh = round(weighted.mean(sh_hoh, n1, na.rm = TRUE), 3), sh_elderly = round(weighted.mean(sh_elderly, n1, na.rm = TRUE), 3)), by = y][order(y)])
