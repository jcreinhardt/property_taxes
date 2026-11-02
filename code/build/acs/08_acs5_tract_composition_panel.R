# Yearly ACS 5-year tract composition panel 2013-2023 on 2010 tracts from the 07 downloads, mapped from 2020 tracts by land-area
# shares as in 06: shares recomputed from allocated counts, average household size averaged with allocated households.
# Output data/built/acs/acs_tract_composition_year.rds: sh_u18, sh_65, sh_alone (householder living alone), sh_hh_kids
# (households with own children < 18), hhsize, pop, hh.
suppressPackageStartupMessages(library(data.table))
source("code/R/paths.R"); X <- file.path(DATA, "external")
rel <- fread(file.path(X, "tract_rel/tab20_tract20_tract10_natl.txt"), sep = "|", colClasses = "character", encoding = "UTF-8"); setnames(rel, 1, "OID_TRACT_20")
rel <- rel[, .(t20 = GEOID_TRACT_20, t10 = GEOID_TRACT_10, a = as.numeric(AREALAND_PART) / as.numeric(AREALAND_TRACT_20))][is.finite(a) & a > 0]
cnt <- c("tot", "u18", "hh", "alone", "hh_kids", "p65")
acs <- rbindlist(lapply(2013:2023, \(yy) { fn <- file.path(X, sprintf("acs/acs5_tract_comp_%d.csv", yy)); if (!file.exists(fn)) return(NULL)
  a <- fread(fn, colClasses = c(GEOID = "character")); setnames(a, sub("E$", "", names(a)))
  a[, p65 := rowSums(.SD, na.rm = TRUE), .SDcols = c(paste0("m65", letters[1:6]), paste0("f65", letters[1:6]))]
  a <- a[, c("GEOID", cnt, "hhsize"), with = FALSE]
  on2010 <- mean(a$GEOID %in% rel$t10) > mean(a$GEOID %in% rel$t20)
  if (!on2010) { m <- merge(rel, a, by.x = "t20", by.y = "GEOID")[, w := a * fcoalesce(as.numeric(hh), 0) * is.finite(hhsize)]
    a <- m[, c(lapply(.SD[, cnt, with = FALSE] * a, sum, na.rm = TRUE), list(hhsize = sum(w * fcoalesce(hhsize, 0)) / sum(w))), by = .(GEOID = t10)] }
  a[, .(tract = GEOID, y = yy, on2010 = on2010, sh_u18 = u18 / tot, sh_65 = p65 / tot, sh_alone = alone / hh, sh_hh_kids = hh_kids / hh, hhsize, pop = tot, hh)] }))
for (k in c("sh_u18", "sh_65", "sh_alone", "sh_hh_kids", "hhsize")) set(acs, which(!is.finite(acs[[k]])), k, NA)
cat("years:", paste(unique(acs$y), collapse = " "), "| on 2010 tracts:", paste(unique(acs[on2010 == TRUE, y]), collapse = " "), "\n")
print(acs[, .(tracts = .N, sh_u18 = round(mean(sh_u18, na.rm = TRUE), 3), sh_65 = round(mean(sh_65, na.rm = TRUE), 3), sh_alone = round(mean(sh_alone, na.rm = TRUE), 3), sh_hh_kids = round(mean(sh_hh_kids, na.rm = TRUE), 3), hhsize = round(mean(hhsize, na.rm = TRUE), 2)), by = y][order(y)])
saveRDS(acs[, -"on2010"], file.path(DATA, "built/acs/acs_tract_composition_year.rds"))
