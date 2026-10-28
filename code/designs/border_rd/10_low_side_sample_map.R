# Map of the low-tax-side sample behind 08/09: tracts (TIGER cb 2019 500k) coloured by distance window and whether their
# border pair is in the rural subset of 09 (output/designs/border_rd/c22_low_side_metro_pairs.csv); the high-tax side of the
# same pairs in grey; state outlines. Reads data/built/geo/low_side_balance_tracts.rds (08) and iv_tract_side.rds.
suppressPackageStartupMessages({library(data.table); library(sf); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_rd"); invisible(out_dir("designs/border_rd/figures")); TIG <- path.expand("~/Library/Caches/tigris")
d <- readRDS(file.path(DATA, "built/geo/low_side_balance_tracts.rds"))[, .(tract, pair, dist_mi)]
metro <- fread(file.path(OUT, "c22_low_side_metro_pairs.csv"))$pair
d[, grp := fifelse(dist_mi >= 50, "low side, 50+ mi (08 only)", fifelse(pair %in% metro, "low side < 50 mi, metro pair", "low side < 50 mi, rural pair"))]
h <- readRDS(file.path(DATA, "built/geo/iv_tract_side.rds"))[T == 30 & high == 1, .(tract, pair, dist_mi)][, grp := "high-tax side of the same pairs"]
s <- rbind(d, h)
tr <- st_read(file.path(TIG, "cb_2019_us_tract_500k.shp"), quiet = TRUE)[, c("GEOID", "STATEFP")]
tr <- st_transform(tr[!tr$STATEFP %in% c("02", "15", "60", "66", "69", "72", "78"), ], 5070)
m <- merge(tr, s, by.x = "GEOID", by.y = "tract")
st <- st_transform(st_read(file.path(TIG, "cb_2020_us_state_500k.shp"), quiet = TRUE), 5070)
st <- st[!st$STUSPS %in% c("AK", "HI", "PR", "VI", "GU", "MP", "AS"), ]
lv <- c("low side < 50 mi, rural pair", "low side < 50 mi, metro pair", "low side, 50+ mi (08 only)", "high-tax side of the same pairs")
m$grp <- factor(m$grp, lv)
n <- s[, .N, by = grp][match(lv, grp)]
g <- ggplot() + geom_sf(data = st, fill = "white", colour = "grey55", linewidth = .2) +
  geom_sf(data = m, aes(fill = grp), colour = NA) + geom_sf(data = st, fill = NA, colour = "grey35", linewidth = .25) +
  scale_fill_manual(NULL, values = c("#2e8b57", "#2a64a8", "#a9c4e4", "grey78"), labels = sprintf("%s (%s tracts)", lv, format(n$N, big.mark = ","))) +
  labs(title = "Tracts in the low-tax-side balance sample (designs/border_rd/08-09)",
       subtitle = sprintf("%d ranked border pairs; low side = lower median effective rate within 20 mi; rural pairs exclude %d pairs with a metro core (>= 0.5m, >= 30%% of its people) inside 50 mi",
                          uniqueN(d$pair), length(metro))) +
  coord_sf(datum = NA) + theme_void(base_size = 9) + theme(legend.position = "bottom", plot.title.position = "plot") + guides(fill = guide_legend(nrow = 2, override.aes = list(colour = NA)))
ggsave(file.path(OUT, "figures/c23_low_side_sample_map.png"), g, width = 11, height = 7.5, dpi = 170, bg = "white")
cat("tracts drawn:", nrow(m), "of", nrow(s), "\n"); print(n)
