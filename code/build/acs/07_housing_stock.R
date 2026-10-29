# Housing-stock variables from the ACS (tidycensus; CENSUS_API_KEY in ~/.Renviron), lower 48 + DC.
# (1) County panels, ACS 1-year 2013-2023 (no 2020 release; counties >= 65k population) and ACS 5-year 2013-2023 (all
#     counties): housing units (B25001), occupied / vacant (B25002), vacant for rent / for sale / seasonal (B25004),
#     tenure (B25003), median year built (B25035). Downloads -> data/external/acs/acs{1,5}_county_YYYY.csv;
#     panel -> data/built/acs/acs_county_year.rds (one row per survey x county x release year).
# (2) Tract ACS 5-year 2013-2023: occupied / vacant units (B25002), one file per release year
#     (data/external/acs/acs5_tract_hs_YYYY.csv), same geography vintages as 05_acs5_tract_download.R; merged into the
#     tract panel by 06_tract_year_panel.R (log units, vacancy share).
suppressPackageStartupMessages({library(data.table); library(tidycensus)})
source("code/R/paths.R"); A <- file.path(DATA, "external/acs"); dir.create(A, showWarnings = FALSE)
v <- c(hu = "B25001_001", occ = "B25002_002", vac = "B25002_003", vac_rent = "B25004_002", vac_sale = "B25004_004",
       vac_seas = "B25004_006", ten_tot = "B25003_001", own = "B25003_002", yrbuilt = "B25035_001")
pull <- function(geo, vars, yy, survey, state = NULL) {
  for (k in 1:3) { r <- tryCatch(as.data.table(suppressMessages(get_acs(geo, variables = vars, state = state, year = yy, survey = survey, output = "wide"))),
                                 error = \(e) { message(survey, " ", geo, " ", yy, " ", state, " try ", k, ": ", substr(conditionMessage(e), 1, 120)); NULL })
                   if (!is.null(r)) return(r); Sys.sleep(5) }
  NULL }
# ---- (1) counties ----
for (sv in c("acs1", "acs5")) for (yy in setdiff(2013:2023, if (sv == "acs1") 2020)) {
  fn <- file.path(A, sprintf("%s_county_%d.csv", sv, yy)); if (file.exists(fn)) next
  a <- pull("county", v, yy, sv); if (is.null(a)) next
  a[, NAME := NULL]; fwrite(a, fn); message(sv, " county ", yy, ": ", nrow(a), " counties") }
cty <- rbindlist(lapply(c("acs1", "acs5"), \(sv) rbindlist(lapply(2013:2023, \(yy) {
  fn <- file.path(A, sprintf("%s_county_%d.csv", sv, yy)); if (!file.exists(fn)) return(NULL)
  a <- fread(fn, colClasses = c(GEOID = "character"))[substr(GEOID, 1, 2) < "57" & !substr(GEOID, 1, 2) %in% c("02", "15", "72")]
  a <- a[, c("GEOID", paste0(names(v), "E")), with = FALSE]; setnames(a, sub("E$", "", names(a)))
  a[yrbuilt == 0, yrbuilt := 1939][!between(yrbuilt, 1900, 2025), yrbuilt := NA]
  a[, .(survey = sv, fips = GEOID, y = yy, hu, occ, vac, vac_rent, vac_sale, vac_seas, ten_tot, own, yrbuilt)] }))))
saveRDS(cty, file.path(DATA, "built/acs/acs_county_year.rds"))
print(cty[, .(counties = .N, hu_m = round(sum(hu, na.rm = TRUE) / 1e6, 1), vac_pct = round(100 * sum(vac, na.rm = TRUE) / sum(hu, na.rm = TRUE), 2),
              own_pct = round(100 * sum(own, na.rm = TRUE) / sum(ten_tot, na.rm = TRUE), 2)), by = .(survey, y)][order(survey, y)])
# ---- (2) tracts ----
fs <- setdiff(unique(fips_codes$state_code[fips_codes$state_code < "57"]), c("02", "15"))
for (yy in 2013:2023) {
  fn <- file.path(A, sprintf("acs5_tract_hs_%d.csv", yy)); if (file.exists(fn)) next
  a <- rbindlist(lapply(fs, \(s) pull("tract", v[c("occ", "vac")], yy, "acs5", s)), fill = TRUE)
  a[, NAME := NULL]; fwrite(a, fn); message("acs5 tract ", yy, ": ", nrow(a), " tracts") }
