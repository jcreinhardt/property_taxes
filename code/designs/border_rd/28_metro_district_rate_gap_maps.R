# Regional maps around border metros: school districts (cb 2019 unified + elementary) coloured by the gap between their own
# 2016 effective property tax rate (CoreLogic tract_year eff50 = median bill / sale price of parcels sold, sales-weighted mean over
# the tracts whose centroid falls in the district; bill / market value is unavailable in CT, MA, NH and 25% above eff50 in NY) and the lowest rate among their adjacent districts (st_touches): positive = a neighbour is cheaper by that
# much, negative = every neighbour is dearer. State lines in black, the CBSA outline dashed. Windows of +-90 mi around the
# CBSA centroid for ten metros on state borders. Text output: districts, median gap, share with a cheaper neighbour by
# > 0.5 pp, and how often the cheapest neighbour lies across the state line.
suppressPackageStartupMessages({library(data.table); library(sf); library(tigris); library(ggplot2); library(patchwork)}); options(tigris_use_cache = TRUE)
source("code/R/paths.R"); OUT <- out_dir("designs/border_rd"); invisible(out_dir("designs/border_rd/figures")); HW <- 90 * 1609.34; CAP <- 1.5
metros <- c(`35620` = "New York", `37980` = "Philadelphia", `47900` = "Washington", `14460` = "Boston", `16980` = "Chicago", `17140` = "Cincinnati", `41180` = "St. Louis", `28140` = "Kansas City", `31140` = "Louisville", `38900` = "Portland")
# district rates from 2016 tracts
tr <- tracts(cb = TRUE, year = 2019, progress_bar = FALSE); tr <- tr[!substr(tr$GEOID, 1, 2) %in% c("02", "15", "72"), ]
sd <- rbind(school_districts(cb = TRUE, year = 2019, type = "unified", progress_bar = FALSE)[, c("GEOID", "NAME")], school_districts(cb = TRUE, year = 2019, type = "elementary", progress_bar = FALSE)[, c("GEOID", "NAME")])
sd <- st_transform(sd, 5070); ce <- st_centroid(st_transform(tr[, "GEOID"], 5070))
j <- as.data.table(st_drop_geometry(st_join(ce, sd[, "GEOID"], join = st_within, left = FALSE)))[, .(tract = GEOID.x, leaid = GEOID.y)][!duplicated(tract)]
# districts whose polygon holds no tract centroid (small ones) take the tract their own centroid falls in
miss <- sd[!sd$GEOID %in% j$leaid, "GEOID"]; j2 <- as.data.table(st_drop_geometry(st_join(st_centroid(miss), st_transform(tr[, "GEOID"], 5070), join = st_within, left = FALSE)))[, .(tract = GEOID.y, leaid = GEOID.x)]
cat("districts with tract centroids:", uniqueN(j$leaid), "| assigned by own centroid:", nrow(j2), "\n"); j <- rbind(j, j2)
ty <- readRDS(file.path(DATA, "built/corelogic/tract_year.rds"))[y == 2016 & is.finite(eff50) & n_sales >= 1]   # no sale prices in the non-disclosure states (KS, MO, TX, UT, ID, MT, WY, NM, ND, SD, MS, LA): no rate there
dr <- merge(ty, j, by = "tract")[, .(rate = 100 * weighted.mean(eff50, n_sales), n_sales = sum(n_sales), tracts = .N), by = leaid]   # no minimum sales count (was >= 10 per tract, >= 30 per district)
cat("districts with a 2016 rate:", nrow(dr), "of", nrow(sd), "\n"); fwrite(dr, file.path(DATA, "built/geo/district_rate_2016.csv"))
# windows and adjacency within them
cb <- st_transform(core_based_statistical_areas(cb = TRUE, year = 2019, progress_bar = FALSE), 5070); cb <- cb[cb$GEOID %in% names(metros), ]
st <- st_transform(states(cb = TRUE, year = 2019, progress_bar = FALSE), 5070)
sd <- merge(sd, dr[, .(GEOID = leaid, rate)], by = "GEOID", all.x = TRUE); sd$st <- substr(sd$GEOID, 1, 2)
panels <- list(); summ <- list()
for (g in names(metros)) { c0 <- st_coordinates(st_centroid(st_geometry(cb[cb$GEOID == g, ]))); bb <- st_as_sfc(st_bbox(c(xmin = c0[1] - HW, xmax = c0[1] + HW, ymin = c0[2] - HW, ymax = c0[2] + HW), crs = st_crs(5070)))
  w <- sd[st_intersects(sd, bb, sparse = FALSE)[, 1], ]; nb <- st_touches(w); r <- w$rate
  w$gap <- sapply(seq_len(nrow(w)), \(i) { k <- nb[[i]]; k <- k[is.finite(r[k])]; if (!is.finite(r[i]) || !length(k)) NA_real_ else r[i] - min(r[k]) })
  w$cross <- sapply(seq_len(nrow(w)), \(i) { k <- nb[[i]]; k <- k[is.finite(r[k])]; if (!is.finite(r[i]) || !length(k)) NA else w$st[k[which.min(r[k])]] != w$st[i] })
  w$gap_abs <- sapply(seq_len(nrow(w)), \(i) { k <- nb[[i]]; k <- k[is.finite(r[k])]; if (!is.finite(r[i]) || !length(k)) NA_real_ else max(abs(r[i] - r[k])) })
  d <- as.data.table(st_drop_geometry(w))[is.finite(gap)]
  summ[[g]] <- data.table(metro = metros[g], districts = nrow(w), with_rate = sum(is.finite(w$rate)), median_rate = round(median(w$rate, na.rm = TRUE), 2), median_gap = round(median(d$gap), 2), p90_gap = round(quantile(d$gap, .9), 2),
                          sh_gap_gt_0.5 = round(mean(d$gap > .5), 2), sh_cheapest_nb_across_state = round(mean(d$cross), 2), sh_cross_among_gap_gt_0.5 = round(mean(d[gap > .5, cross]), 2))
  w$gapc <- pmax(pmin(w$gap, CAP), -CAP); bx <- st_bbox(bb)
  panels[[g]] <- ggplot() + geom_sf(data = w, aes(fill = gapc), colour = "white", linewidth = .08) + geom_sf(data = st, fill = NA, colour = "black", linewidth = .5) +
    geom_sf(data = cb[cb$GEOID == g, ], fill = NA, colour = "grey20", linetype = "22", linewidth = .3) +
    scale_fill_gradient2("own rate − cheapest neighbour, pp", low = "#2a64a8", mid = "#f7f7f7", high = "#c0392b", limits = c(-CAP, CAP), na.value = "grey85") +
    coord_sf(xlim = c(bx["xmin"], bx["xmax"]), ylim = c(bx["ymin"], bx["ymax"]), expand = FALSE) + labs(title = metros[g]) + theme_void(base_size = 8) + theme(plot.title = element_text(size = 10, hjust = .5)) }
summ <- rbindlist(summ); fwrite(summ, file.path(OUT, "c40_metro_district_rate_gap_summary.csv"))
sink(file.path(OUT, "c40_metro_district_rate_gap_maps.txt"), split = TRUE); cat("Districts within +-90 mi of the CBSA centroid; gap = own 2016 effective rate - lowest adjacent district's rate (pp); cross = cheapest neighbour in another state.\n"); print(as.data.frame(summ), row.names = FALSE); sink()
g <- wrap_plots(panels, nrow = 2, guides = "collect") + plot_annotation(title = "School districts around border metros, coloured by the gap between their own 2016 effective tax rate and the cheapest adjacent district",
  subtitle = sprintf("CoreLogic 2016 median effective rate on sold parcels, sales-weighted by district (tract centroids in cb 2019 unified + elementary districts); windows +-90 mi; colour capped at +-%.1f pp; grey = no rate; black = state lines, dashed = CBSA", CAP), theme = theme(plot.title = element_text(size = 12))) & theme(legend.position = "bottom")
ggsave(file.path(OUT, "figures/c40_metro_district_rate_gap_maps.png"), g, width = 20, height = 9.5, dpi = 130, bg = "white")
