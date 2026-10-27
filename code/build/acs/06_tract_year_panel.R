# Formerly code/analysis/salt_local/c18_acs_seda_panels.R (moved in the 2026-10-27 restructuring).
# Yearly covariate panels for the border RD by year (c16).
# (1) ACS 5-year tract panel 2013-2023 on 2010 tracts. Releases on 2020 tracts are mapped to 2010 tracts with the Census
#     2020-2010 tract relationship file (data/external/tract_rel), allocating each 2020 tract by the share of its land area
#     that falls in each 2010 tract (area weights; no population weighting available at tract level). Counts are allocated;
#     shares are recomputed from allocated counts; medians are averaged with allocated weights (income: occupied units,
#     home value: owner units, year built: housing units, age: population).
# (2) SEDA 4.1 yearly district panel 2009-2019 (seda_geodist_long_cs_4.1.csv, cohort scale, subgroup all): mean achievement
#     averaged over grades 3-8 and subjects per district-year (precision-weighted); SES and FRL from seda_cov_geodist_long_4.1.
suppressPackageStartupMessages({library(data.table)})
source("code/R/paths.R"); X <- file.path(DATA, "external")
t10 <- unique(readRDS(file.path(DATA, "built/geo/iv_tract_side.rds"))$tract)
rel <- fread(file.path(X, "tract_rel/tab20_tract20_tract10_natl.txt"), sep = "|", colClasses = "character", encoding = "UTF-8")
setnames(rel, 1, "OID_TRACT_20")
rel <- rel[, .(t20 = GEOID_TRACT_20, t10 = GEOID_TRACT_10, a = as.numeric(AREALAND_PART) / as.numeric(AREALAND_TRACT_20))][is.finite(a) & a > 0]
cnt <- c("pop", "white", "ed_tot", "ba", "ma", "prof", "phd", "ten_tot", "own", "hu")
med <- c(inc = "ten_tot", hval = "own", yrbuilt = "hu", age = "pop")
acs <- rbindlist(lapply(2013:2023, \(yy) {
  a <- fread(file.path(X, sprintf("acs/acs5_tract_%d.csv", yy)), colClasses = c(GEOID = "character"))
  a <- a[, c("GEOID", paste0(c(cnt, names(med)), "E")), with = FALSE]; setnames(a, sub("E$", "", names(a)))
  a[yrbuilt == 0, yrbuilt := 1939]   # from the 2018 release "1939 or earlier" is coded 0 (1939 before)
  on2010 <- mean(a$GEOID %in% rel$t10) > mean(a$GEOID %in% rel$t20)   # vs the full 2010 and 2020 tract lists
  if (!on2010) {
    m <- merge(rel, a, by.x = "t20", by.y = "GEOID")
    for (k in names(med)) m[, (paste0("w_", k)) := a * fcoalesce(as.numeric(get(med[k])), 0) * is.finite(get(k))]
    a <- m[, c(lapply(.SD[, cnt, with = FALSE] * a, sum, na.rm = TRUE),
               lapply(setNames(names(med), names(med)), \(k) sum(get(paste0("w_", k)) * fcoalesce(as.numeric(get(k)), 0)) / sum(get(paste0("w_", k))))),
           by = .(GEOID = t10)]
  }
  a[, .(tract = GEOID, y = yy, on2010 = on2010, linc = log(inc), lhval = log(hval), lpop = log(pop), sh_white = white / pop,
        sh_ba = (ba + ma + prof + phd) / ed_tot, sh_own = own / ten_tot, yrbuilt, age)] }))
for (k in c("linc", "lhval", "lpop", "sh_white", "sh_ba", "sh_own", "yrbuilt", "age")) set(acs, which(!is.finite(acs[[k]])), k, NA)
acs[!between(yrbuilt, 1900, 2025), yrbuilt := NA]   # ACS jam / special values (e.g. 0 in some releases)
cat("ACS panel: release years on 2010 tracts:", paste(unique(acs[on2010 == TRUE, y]), collapse = " "), "| mapped from 2020 tracts:",
    paste(unique(acs[on2010 == FALSE, y]), collapse = " "), "\n")
print(acs[tract %in% t10, .(tracts = .N, share_of_iv_tracts = round(.N / length(t10), 3), linc = round(mean(!is.na(linc)), 3)), by = y])
# continuity check across the 2019 -> 2020 geography switch
w <- dcast(acs[y %in% 2019:2020 & tract %in% t10], tract ~ y, value.var = "linc")
cat("corr(log income 2019, 2020) on 2010 tracts:", round(cor(w$`2019`, w$`2020`, use = "complete"), 3), "\n")
saveRDS(acs[, -"on2010"], file.path(DATA, "built/acs/acs_tract_year.rds"))

# ---- SEDA yearly ----
s <- fread(file.path(X, "seda41/seda_geodist_long_cs_4.1.csv"), select = c("sedalea", "year", "grade", "subject", "cs_mn_all", "cs_mnse_all"), colClasses = c(sedalea = "character"))
cat("\nSEDA long columns:", paste(names(s), collapse = " "), "\n")
mn <- intersect(c("cs_mn_all", "mn_all"), names(s))[1]; se <- intersect(c("cs_mnse_all", "mnse_all"), names(s))[1]
s <- s[is.finite(get(mn)) & is.finite(get(se)) & get(se) > 0, .(leaid = sprintf("%07d", as.integer(sedalea)), y = year, mn = get(mn), w = 1 / get(se)^2)]
s <- s[, .(ach = weighted.mean(mn, w), n_cells = .N), by = .(leaid, y)]
cv <- fread(file.path(X, "seda41/seda_cov_geodist_long_4.1.csv"), select = c("sedalea", "year", "grade", "sesall", "perfrl"), colClasses = c(sedalea = "character"))
cat("SEDA cov long columns:", paste(head(names(cv), 40), collapse = " "), "\n")
sescol <- intersect(c("sesall", "sesavgall", "ses_all"), names(cv))[1]
cv <- cv[, .(ses = mean(get(sescol), na.rm = TRUE), perfrl = mean(perfrl, na.rm = TRUE)), by = .(leaid = sprintf("%07d", as.integer(sedalea)), y = year)]
seda <- merge(s, cv, by = c("leaid", "y"), all = TRUE)
for (k in c("ses", "perfrl")) set(seda, which(!is.finite(seda[[k]])), k, NA)
print(seda[, .(districts = .N, ach = sum(!is.na(ach)), ses = sum(!is.na(ses)), frl = sum(!is.na(perfrl))), by = y][order(y)])
saveRDS(seda, file.path(DATA, "built/acs/seda_district_year.rds"))
