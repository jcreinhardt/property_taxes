# Map of the school districts with density below 1,000 per sq mi (25: population / land area from tract centroids, cb 2019
# unified + elementary polygons, contiguous US), marking those in the border sample: districts with at least half of their
# population in tracts within 50 mi of a ranked border (T = 30), on the low- or the high-tax side. Counts by category.
suppressPackageStartupMessages({library(data.table); library(sf); library(tigris); library(ggplot2)}); options(tigris_use_cache = TRUE)
source("code/R/paths.R"); OUT <- out_dir("designs/border_rd"); invisible(out_dir("designs/border_rd/figures")); D0 <- as.numeric(Sys.getenv("DENS_CUT", 1000)); MX <- 50
dd <- fread(file.path(DATA, "built/geo/district_density_2017.csv"), colClasses = c(leaid = "character"))
bs <- rbindlist(lapply(c("low", "high"), \(s) readRDS(file.path(DATA, sprintf("built/geo/%s_side_balance_tracts.rds", s)))[dist_mi < MX & is.finite(pop), .(leaid, side = s, pop)]))
bs <- bs[, .(pop_s = sum(pop)), by = .(leaid, side)]; dd <- merge(dd, dcast(bs, leaid ~ side, value.var = "pop_s", fill = 0), by = "leaid", all.x = TRUE)
dd[is.na(low), low := 0][is.na(high), high := 0][, side := fcase(low / pop >= .5, "low", high / pop >= .5, "high", default = "none")]
dd[, cat := factor(fcase(dens >= D0, "above the cutoff", side == "low", "below, border sample: low-tax side", side == "high", "below, border sample: high-tax side", default = "below the cutoff, not near a ranked border"),
                   c("below, border sample: low-tax side", "below, border sample: high-tax side", "below the cutoff, not near a ranked border", "above the cutoff"))]
cnt <- dd[, .(districts = .N, population_m = round(sum(pop) / 1e6, 1), pupils_m = round(sum(enr, na.rm = TRUE) / 1e6, 2), median_dens = round(median(dens))), by = cat][order(cat)]
sink(file.path(OUT, sprintf("c38_rural_district_map_%.0f.txt", D0)), split = TRUE); cat("Districts by the", D0, "per sq mi cutoff and border-sample membership (>= 50% of population within", MX, "mi of a ranked border):\n"); print(as.data.frame(cnt), row.names = FALSE)
cat("\nborder-sample districts below the cutoff by state (low / high side):\n"); print(dcast(dd[cat %in% levels(cat)[1:2], .N, by = .(st = substr(leaid, 1, 2), side)], st ~ side, value.var = "N", fill = 0)[order(-low - high)], nrows = 60); sink()
sd <- rbind(school_districts(cb = TRUE, year = 2019, type = "unified", progress_bar = FALSE)[, "GEOID"], school_districts(cb = TRUE, year = 2019, type = "elementary", progress_bar = FALSE)[, "GEOID"])
sd <- merge(sd, dd[, .(GEOID = leaid, cat)], by = "GEOID"); sd <- st_transform(sd, 5070)
st <- st_transform(states(cb = TRUE, year = 2019, progress_bar = FALSE), 5070); st <- st[!st$STUSPS %in% c("AK", "HI", "PR", "GU", "VI", "MP", "AS"), ]
g <- ggplot() + geom_sf(data = sd, aes(fill = cat), colour = NA) + geom_sf(data = st, fill = NA, colour = "grey25", linewidth = .25) +
  scale_fill_manual(NULL, values = c("#2a64a8", "#c0392b", "#d9c9a3", "grey88")) +
  labs(title = sprintf("School districts with population density below %s per sq mi, and the border sample among them", format(D0, big.mark = ",")),
       subtitle = sprintf("%s districts below the cutoff (%s above); border sample: >= 50%% of the district's population within %d mi of a ranked border, %d low-side and %d high-side districts below the cutoff",
                          format(sum(dd$dens < D0), big.mark = ","), format(sum(dd$dens >= D0), big.mark = ","), MX, cnt$districts[1], cnt$districts[2])) +
  theme_void(base_size = 9) + theme(legend.position = "bottom", plot.title = element_text(size = 11))
ggsave(file.path(OUT, sprintf("figures/c38_rural_district_map_%.0f.png", D0)), g, width = 13, height = 8.5, dpi = 150, bg = "white")
