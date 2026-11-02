# ZIP (ZCTA 2020) -> school district crosswalk by area, for ZIP-level SOI measures at the district level. ZCTA polygons
# (TIGER cb 2020, tigris, cached) intersected with the unified + elementary district polygons used in 05 (cb 2019). For each
# ZCTA x district piece: the share of the ZCTA's land area in the district (sh_zcta, sums to 1 over districts for ZCTAs
# covered by districts) and the share of the district's area that the ZCTA covers (sh_district). Pieces below 1% of the
# ZCTA's area are dropped. Output: data/built/geo/zcta_district_area.csv (zcta, leaid, sh_zcta, sh_district, area_km2).
suppressPackageStartupMessages({library(data.table); library(sf); library(tigris)}); options(tigris_use_cache = TRUE); sf_use_s2(FALSE)
source("code/R/paths.R")
z <- st_transform(zctas(cb = TRUE, year = 2020, progress_bar = FALSE)[, c("GEOID20")], 5070); names(z)[1] <- "zcta"
z <- z[!substr(z$zcta, 1, 3) %in% c("995", "996", "997", "998", "999", "967", "968", "006", "007", "009"), ]   # drop AK, HI, PR
sd <- rbind(school_districts(cb = TRUE, year = 2019, type = "unified", progress_bar = FALSE)[, "GEOID"],
            school_districts(cb = TRUE, year = 2019, type = "elementary", progress_bar = FALSE)[, "GEOID"])
sd <- st_transform(sd, 5070); names(sd)[1] <- "leaid"; sd <- st_make_valid(sd); z <- st_make_valid(z)
cat("ZCTAs:", nrow(z), "| districts:", nrow(sd), "\n")
z$a_z <- as.numeric(st_area(z)); sd$a_d <- as.numeric(st_area(sd))
x <- st_intersection(z, sd)
x$a <- as.numeric(st_area(x))
r <- as.data.table(st_drop_geometry(x))[, .(zcta, leaid, area_km2 = a / 1e6, sh_zcta = a / a_z, sh_district = a / a_d)][sh_zcta >= .01]
cat("pieces:", nrow(r), "| ZCTAs with a district:", uniqueN(r$zcta), "| districts with a ZCTA:", uniqueN(r$leaid), "\n")
cat("ZCTAs per district: mean", round(nrow(r) / uniqueN(r$leaid), 2), "| districts per ZCTA: mean", round(nrow(r) / uniqueN(r$zcta), 2), "| ZCTAs wholly in one district (sh >= .99):", r[sh_zcta >= .99, .N], "\n")
fwrite(r, file.path(DATA, "built/geo/zcta_district_area.csv"))
