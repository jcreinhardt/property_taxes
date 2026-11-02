# What part of the US do the density cutoffs of 24 cover? For the contiguous US (ACS 2013-17 tract population, TIGER cb
# 2019 land area): share of population in tracts below D per sq mi; share of school districts (unified + elementary, cb
# 2019 polygons, tract centroids assigned to districts), of their population and of F-33 2017 pupils in districts whose
# own density (population / land area) is below D. D = 5,000, 2,000, 1,000, 500, 250, 100.
suppressPackageStartupMessages({library(data.table); library(sf); library(tigris)}); options(tigris_use_cache = TRUE)
source("code/R/paths.R"); OUT <- out_dir("designs/border_rd"); CUTS <- c(5000, 2000, 1000, 500, 250, 100)
a <- fread(file.path(DATA, "external/acs17_tract_extra.csv"), colClasses = c(GEOID = "character"))[, .(tract = GEOID, pop = popE)]
tr <- tracts(cb = TRUE, year = 2019, progress_bar = FALSE); tr <- tr[!substr(tr$GEOID, 1, 2) %in% c("02", "15", "72"), ]
t <- as.data.table(st_drop_geometry(tr))[, .(tract = GEOID, aland = as.numeric(ALAND))]
t <- merge(t, a, by = "tract")[is.finite(pop) & aland > 0][, dens := pop / (aland / 2589988)]
sd <- rbind(school_districts(cb = TRUE, year = 2019, type = "unified", progress_bar = FALSE)[, "GEOID"], school_districts(cb = TRUE, year = 2019, type = "elementary", progress_bar = FALSE)[, "GEOID"])
ce <- st_centroid(st_transform(tr[tr$GEOID %in% t$tract, "GEOID"], 5070)); sd <- st_transform(sd, 5070)
j <- as.data.table(st_drop_geometry(st_join(ce, sd, join = st_within, left = FALSE)))[, .(tract = GEOID.x, leaid = GEOID.y)][!duplicated(tract)]
t <- merge(t, j, by = "tract", all.x = TRUE); cat("tracts:", nrow(t), "| in a district polygon:", sum(!is.na(t$leaid)), "\n")
dd <- t[!is.na(leaid), .(pop = sum(pop), aland = sum(aland), n_tr = .N), by = leaid][, dens := pop / (aland / 2589988)]
f <- readRDS(file.path(DATA, "built/census_finances/district_finance.rds"))[y == 2017 & is.finite(enr), .(leaid, enr)]
dd <- merge(dd, f, by = "leaid", all.x = TRUE); cat("districts with tracts:", nrow(dd), "| with F-33 enrolment:", sum(!is.na(dd$enr)), "\n")
cov <- rbindlist(lapply(CUTS, \(D) data.table(cut = D, tract_pop_share = t[dens < D, sum(pop)] / t[, sum(pop)], tract_share = t[, mean(dens < D)],
  district_share = dd[, mean(dens < D)], district_pop_share = dd[dens < D, sum(pop)] / dd[, sum(pop)], pupil_share = dd[!is.na(enr)][dens < D, sum(enr)] / dd[!is.na(enr), sum(enr)])))
cov[, (names(cov)[-1]) := lapply(.SD, round, 3), .SDcols = -1]
fwrite(cov, file.path(OUT, "c37_density_cutoff_coverage.csv")); fwrite(dd, file.path(DATA, "built/geo/district_density_2017.csv"))
cat("\nContiguous US: share of tracts / population in tracts below D per sq mi; share of districts, their population and F-33 pupils in districts below D\n"); print(as.data.frame(cov), row.names = FALSE)
cat("\ndistrict density quantiles:\n"); print(round(quantile(dd$dens, c(.1, .25, .5, .75, .9))))
