# Local effective-tax-rate differentials by distance to the nearest state border. Tract medians (sales 2017-19, any number of
# sales; R/22 inputs) and CSDS drive-time neighbours. For each tract and T in {15, 30, 60} min: mean |median_i - median_j| over
# all reachable tracts with sales, and separately over same-state and other-state neighbours; share of neighbours across a
# state line. Tract location = mean parcel lat/lon of its sales; distance to the nearest boundary shared with another state
# (as R/07: cb 2020 states, lower 48 + DC, outer US boundary removed). Binned means across tracts (tracts ~ equal population).
# Noise benchmark: same statistics with medians replaced by N(0, SE) (SE as in R/22).
suppressPackageStartupMessages({library(data.table); library(duckdb); library(sf); library(ggplot2)})
source("code/R/paths.R"); OUT <- file.path(ROOT, "output/analysis/local_juris")
TIG <- path.expand("~/Library/Caches/tigris"); MI <- 1609.344; TS <- c(15, 30, 60)
con <- dbConnect(duckdb())
t <- as.data.table(dbGetQuery(con, sprintf("SELECT * FROM '%s/data/local_juris/tract/tract_eff.parquet'", ROOT)))
t[, `:=`(med = 100 * med, iqr = q75 - q25)][n < 10, iqr := median(t[n >= 10, q75 - q25])][, se := 100 * 1.2533 * iqr / 1.349 / sqrt(n)]
set.seed(1); t[, noise := rnorm(.N, 0, se)]
duckdb_register(con, "t", t[, .(tract, med, noise)])
p <- as.data.table(dbGetQuery(con, sprintf("SELECT m.o, m.d, m.m, abs(a.med - b.med) ad, abs(a.noise - b.noise) adn, left(m.o, 2) <> left(m.d, 2) xst
  FROM '%s/data/external/csds_ttm/tract_drive_le90.parquet' m JOIN t a ON a.tract = m.o JOIN t b ON b.tract = m.d
  WHERE m.o <> m.d AND m.m <= %d", ROOT, max(TS))))
dbDisconnect(con, shutdown = TRUE)
# distance to nearest internal state border
st <- st_read(file.path(TIG, "cb_2020_us_state_500k.shp"), quiet = TRUE)
st <- st_make_valid(st_transform(st[!st$STUSPS %in% c("AK", "HI", "PR", "VI", "GU", "MP", "AS"), ], 5070))
internal <- st_difference(st_union(st_boundary(st)), st_buffer(st_boundary(st_union(st)), 100))
segs <- st_segmentize(st_cast(st_sf(geometry = internal), "LINESTRING"), 5000)
tl <- t[!is.na(lat) & !substr(tract, 1, 2) %in% c("02", "15", "72")]
pts <- st_transform(st_as_sf(tl, coords = c("lon", "lat"), crs = 4326), 5070)
tl[, dist_mi := as.numeric(st_distance(pts, segs[st_nearest_feature(pts, segs), ], by_element = TRUE)) / MI]
# per-tract statistics
agg <- function(v) rbindlist(lapply(TS, \(T) p[m <= T, .(n_nb = .N, all = mean(get(v)), same = mean(get(v)[!xst]),
  other = mean(get(v)[xst]), sh_cross = mean(xst)), by = .(tract = o)][, minutes := T]))
r <- rbind(agg("ad")[, src := "data"], agg("adn")[, src := "noise benchmark"])
r <- merge(r, tl[, .(tract, dist_mi)], by = "tract")
br <- c(seq(0, 50, 5), 60, 80, 100, 150, Inf)
r[, bin := cut(dist_mi, br, right = FALSE)][, mid := (br[as.integer(bin)] + pmin(br[as.integer(bin) + 1], 200)) / 2]
b <- r[, .(tracts = .N, all = mean(all), same = mean(same, na.rm = TRUE), other = mean(other, na.rm = TRUE),
           tracts_other = sum(!is.na(other)), sh_cross = mean(sh_cross)), by = .(src, minutes, bin, mid)][order(src, minutes, mid)]
fwrite(b, file.path(OUT, "tract_diff_by_border_distance.csv"))
sink(file.path(OUT, "tract_diff_by_border_distance.txt"))
cat("Mean over tracts of the mean |median_i - median_j| (pp) to tracts within T drive minutes, by distance to nearest state border\n")
cat("tracts with location and neighbours:", uniqueN(r$tract), "\n")
for (k in split(b, by = c("src", "minutes"))) { cat("\n==", k$src[1], "|", k$minutes[1], "min\n")
  print(as.data.frame(k[, .(bin, tracts, all = round(all, 3), same_state = round(same, 3), other_state = round(other, 3), tracts_other,
                            sh_nb_cross = round(sh_cross, 3))]), row.names = FALSE) }
sink(); cat(readLines(file.path(OUT, "tract_diff_by_border_distance.txt")), sep = "\n")
pl <- melt(b[tracts >= 100], id.vars = c("src", "minutes", "mid"), measure.vars = c("all", "same", "other"), variable.name = "nb")
pl <- merge(pl, b[, .(src, minutes, mid, tracts_other)], by = c("src", "minutes", "mid"))[!(nb == "other" & (is.na(value) | tracts_other < 100))]
pl[, nb := factor(nb, c("all", "same", "other"), c("all reachable tracts", "same-state tracts", "other-state tracts"))]
g <- ggplot(pl[src == "data"], aes(mid, value, colour = nb)) + geom_line() + geom_point(size = 1) +
  geom_line(data = pl[src == "noise benchmark" & nb == "all reachable tracts"], aes(linetype = "noise benchmark (all)"), colour = "grey50") +
  facet_wrap(~ sprintf("within %d drive minutes", minutes), nrow = 1) + scale_x_continuous(limits = c(0, 185), breaks = c(0, 25, 50, 75, 100, 125, 175), labels = c(0, 25, 50, 75, 100, 125, "150+")) +
  scale_colour_manual(values = c("all reachable tracts" = "black", "same-state tracts" = "#2b8cbe", "other-state tracts" = "#e34a33")) +
  labs(x = "distance from tract to nearest state border (miles; bins of 5 to 50, then 50-60, 60-80, 80-100, 100-150, 150+)", y = "mean absolute differential (pp)",
       colour = NULL, linetype = NULL, title = "Local effective-tax-rate differentials by distance to the nearest state border",
       subtitle = "Mean over tracts of the mean absolute difference between own and neighbour tract medians; tracts with any sales 2017-19;\nbins with >= 100 tracts (other-state line: >= 100 tracts with an other-state neighbour)") +
  theme_minimal(base_size = 10) + theme(legend.position = "bottom")
ggsave(file.path(OUT, "figures/tract_diff_by_border_distance.png"), g, width = 11, height = 4.8, dpi = 150)
