# ACS 5-year tract estimates 2013-2023 (tidycensus; CENSUS_API_KEY in ~/.Renviron), lower 48 + DC, same variables as the
# 2013-17 pull in local_juris/R/24 plus housing units (crosswalk weights). One file per release year in data/external/acs/.
# Releases 2013-2019 are on 2010 tracts; from 2020 on, 2020 tracts (checked against the 2010 IDs in c18).
suppressPackageStartupMessages({library(data.table); library(tidycensus)})
source("code/R/paths.R"); A <- file.path(ROOT, "data/external/acs"); dir.create(A, showWarnings = FALSE)
v <- c(inc = "B19013_001", hval = "B25077_001", pop = "B01003_001", white = "B02001_002", ed_tot = "B15003_001",
       ba = "B15003_022", ma = "B15003_023", prof = "B15003_024", phd = "B15003_025", ten_tot = "B25003_001", own = "B25003_002",
       yrbuilt = "B25035_001", age = "B01002_001", hu = "B25001_001")
fs <- setdiff(unique(fips_codes$state_code[fips_codes$state_code < "57"]), c("02", "15"))
for (yy in 2013:2023) {
  fn <- file.path(A, sprintf("acs5_tract_%d.csv", yy))
  if (file.exists(fn)) next
  a <- rbindlist(lapply(fs, \(s) {
    for (k in 1:3) { r <- tryCatch(as.data.table(suppressMessages(get_acs("tract", variables = v, state = s, year = yy, survey = "acs5", output = "wide"))),
                                   error = \(e) { message(yy, " ", s, " try ", k, ": ", conditionMessage(e)); NULL })
                     if (!is.null(r)) return(r); Sys.sleep(5) }
    NULL }), fill = TRUE)
  a[, NAME := NULL]
  fwrite(a, fn); message(yy, ": ", nrow(a), " tracts")
}
