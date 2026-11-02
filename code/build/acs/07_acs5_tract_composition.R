# ACS 5-year tract composition variables 2013-2023 (tidycensus, CENSUS_API_KEY in ~/.Renviron), lower 48 + DC, one file per
# release in data/external/acs/acs5_tract_comp_YYYY.csv: population by age (B01001: total, 65+ by sex), population under 18
# (B09001), households (B11001: total, householder living alone), households with own children < 18 (B11005), average
# household size (B25010). Companion to 05 (which has median age); the panel on 2010 tracts is built in 08.
suppressPackageStartupMessages({library(data.table); library(tidycensus)})
source("code/R/paths.R"); A <- file.path(DATA, "external/acs")
v <- c(tot = "B01001_001", m65a = "B01001_020", m65b = "B01001_021", m65c = "B01001_022", m65d = "B01001_023", m65e = "B01001_024", m65f = "B01001_025",
       f65a = "B01001_044", f65b = "B01001_045", f65c = "B01001_046", f65d = "B01001_047", f65e = "B01001_048", f65f = "B01001_049",
       u18 = "B09001_001", hh = "B11001_001", alone = "B11001_008", hh_kids = "B11005_002", hhsize = "B25010_001")
fs <- setdiff(unique(fips_codes$state_code[fips_codes$state_code < "57"]), c("02", "15"))
for (yy in 2013:2023) {
  fn <- file.path(A, sprintf("acs5_tract_comp_%d.csv", yy)); if (file.exists(fn)) next
  a <- rbindlist(lapply(fs, \(s) { for (k in 1:3) { r <- tryCatch(as.data.table(suppressMessages(get_acs("tract", variables = v, state = s, year = yy, survey = "acs5", output = "wide"))),
      error = \(e) { message(yy, " ", s, " try ", k, ": ", conditionMessage(e)); NULL }); if (!is.null(r)) return(r); Sys.sleep(5) }; NULL }), fill = TRUE)
  a[, NAME := NULL]; a <- a[, c("GEOID", paste0(names(v), "E")), with = FALSE]; fwrite(a, fn); message(yy, ": ", nrow(a), " tracts") }
