# Border IV, directional version. Only the higher-tax side of a state border gains a cheaper outside option from being
# near it; both sides share whatever makes border areas different (rural, older, poorer). Each tract is assigned to its
# nearest state-border pair (TIGER cb 2020 state boundaries, shared-boundary lines per pair); a side is "high" if the
# median of its tract medians within 20 mi exceeds the other side's. Instrument: near-border bins x high side.
# Spec: Y ~ i(dbin, high, ref = 50+) | pair^high + pair^dbin, SE clustered by county; plus pooled near (< 10 mi) x high.
# Y: first stage (x_cheap, tax_gap, sh_x), school access (sch_gap, x_better_sch) and own-tract balance covariates.
suppressPackageStartupMessages({library(data.table); library(sf); library(fixest)})
ROOT <- "/Users/jr2728/PhD/projects/property_taxes"; OUT <- file.path(ROOT, "output/analysis/local_juris"); MI <- 1609.344
TIG <- path.expand("~/Library/Caches/tigris")
d <- readRDS(file.path(ROOT, "data/local_juris/iv_tract_nb.rds"))
st <- st_read(file.path(TIG, "cb_2020_us_state_500k.shp"), quiet = TRUE)
st <- st_make_valid(st_transform(st[!st$STUSPS %in% c("AK", "HI", "PR", "VI", "GU", "MP", "AS"), ], 5070))
sides <- st_read(file.path(ROOT, "data/local_juris/border20_sides.geojson"), quiet = TRUE)
pairs <- unique(as.data.table(st_drop_geometry(sides))[, .(pair, a = pmin(st_a, st_b), b = pmax(st_a, st_b))])
bd <- st_boundary(st); lines <- list()
for (i in seq_len(nrow(pairs))) {
  ia <- which(st$STUSPS == pairs$a[i]); ib <- which(st$STUSPS == pairs$b[i])
  x <- suppressWarnings(st_intersection(st_buffer(bd[ia, ], 200), bd[ib, ]))
  if (nrow(x)) lines[[length(lines) + 1]] <- st_sf(pair = pairs$pair[i], a = pairs$a[i], b = pairs$b[i], geometry = st_union(st_geometry(x)))
}
pl <- do.call(rbind, lines)
fips <- as.data.table(tigris::fips_codes)[, .(st = state_code, ab = state)] |> unique()
u <- unique(d[, .(tract, lat, lon, st, dist_mi, med)])
u <- merge(u, fips, by = "st")
pts <- st_transform(st_as_sf(u, coords = c("lon", "lat"), crs = 4326), 5070)
u[, pair := pl$pair[st_nearest_feature(pts, pl)]]
u <- merge(u, as.data.table(st_drop_geometry(pl))[, .(pair, a, b)], by = "pair")
u <- u[ab == a | ab == b]   # nearest pair must involve own state
side <- u[dist_mi < 20, .(side_med = median(med), n20 = .N), by = .(pair, ab)]
side[, k := .N, by = pair]; side <- side[k == 2 & n20 >= 20][, high := as.integer(side_med == max(side_med)), by = pair]
u <- merge(u, side[, .(pair, ab, high, side_med)], by = c("pair", "ab"))
d <- merge(d, u[, .(tract, pair, high, side_med)], by = "tract")
d[, near := as.integer(dist_mi < 10)]
cat("tracts with a ranked nearest pair:", uniqueN(d$tract), "| pairs:", uniqueN(d$pair), "\n")
ys <- c("x_cheap", "tax_gap", "sh_x", "sch_gap", "x_better_sch", "ach", "ses", "linc", "lhval", "sh_ba", "sh_white", "sh_own", "yrbuilt", "lpop")
res <- rbindlist(lapply(c(30, 60), \(T0) rbindlist(lapply(ys, \(y) {
  dd <- d[T == T0]; sdy <- sd(dd[[y]], na.rm = TRUE)
  f1 <- feols(as.formula(sprintf("%s ~ i(dbin, high, ref = '[50,Inf)') | pair^high + pair^dbin", y)), dd, cluster = ~cty)
  f2 <- feols(as.formula(sprintf("%s ~ near:high | pair^high + pair^near", y)), dd, cluster = ~cty)
  rbind(as.data.table(coeftable(f1), keep.rownames = "term"), as.data.table(coeftable(f2), keep.rownames = "term"))[,
    `:=`(outcome = y, T = T0, sd_y = sdy, n = nobs(f2))] }))))
res[, term := gsub('dbin::|:high|"|\\)', "", term)]
fwrite(res, file.path(OUT, "iv_high_side.csv"))
sink(file.path(OUT, "iv_high_side.txt"))
cat("Directional border IV: near-border x high-tax side; pair x side FE + pair x distance-bin FE; SE by county\n")
cat("tracts:", d[T == 30, .N], "| pairs:", uniqueN(d$pair), "| high-side tracts:", d[T == 30, sum(high)], "\n")
for (T0 in c(30, 60)) { cat("\n== T =", T0, "min. Cells: coefficient (SE); last column also in SD units of Y\n")
  r <- res[T == T0]; r[, cell := sprintf("%.3f (%.3f)", Estimate, `Std. Error`)]
  w <- dcast(r, outcome ~ term, value.var = "cell"); w <- merge(w, unique(r[term == "near", .(outcome, near_x_high_sd = round(Estimate / sd_y, 3))]), by = "outcome")
  print(w[match(ys, outcome)]) }
sink(); cat(readLines(file.path(OUT, "iv_high_side.txt")), sep = "\n")
saveRDS(d, file.path(ROOT, "data/local_juris/iv_tract_side.rds"))
