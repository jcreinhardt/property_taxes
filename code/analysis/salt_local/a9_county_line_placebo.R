# Placebo for the district border DDD: county lines *within* states. Same construction as the state-border design (R/26),
# with adjacent county pairs in the same state instead of state pairs: each tract is assigned to the nearest boundary line
# its county shares with another county of the same state; the "high" side is the county whose tract medians within 20 mi
# of the line are higher; near = < 10 mi. Tracts within 20 mi of a state border are dropped. Districts take the majority
# assignment of their tracts. DDD on F-33 log property tax: i(y, near x high) | district + pair^side^y + pair^dbin^y.
# If the state-border effect reflects SALT / interstate competition, county lines should show nothing.
suppressPackageStartupMessages({library(data.table); library(sf); library(tigris); library(fixest)})
options(tigris_use_cache = TRUE)
ROOT <- "/Users/jr2728/PhD/projects/property_taxes"; OUT <- file.path(ROOT, "output/analysis/salt_local"); MI <- 1609.344
cty <- counties(cb = TRUE, year = 2019, progress_bar = FALSE)
cty <- st_make_valid(st_transform(cty[as.integer(cty$STATEFP) <= 56 & !cty$STATEFP %in% c("02", "15"), c("GEOID", "STATEFP")], 5070))
adj <- st_touches(cty)
pr <- unique(rbindlist(lapply(seq_along(adj), \(i) if (length(adj[[i]])) data.table(i = i, j = adj[[i]])))[i < j])
pr <- pr[cty$STATEFP[i] == cty$STATEFP[j]]
bd <- st_boundary(cty)
gl <- lapply(seq_len(nrow(pr)), \(k) { x <- suppressWarnings(st_intersection(st_geometry(bd[pr$i[k], ]), st_geometry(st_buffer(bd[pr$j[k], ], 100))))
  if (length(x) == 0 || all(st_is_empty(x))) NULL else st_cast(st_union(x), "MULTILINESTRING") })
ok <- !vapply(gl, is.null, TRUE)
lines <- st_sf(a = cty$GEOID[pr$i[ok]], b = cty$GEOID[pr$j[ok]], geometry = do.call(c, gl[ok]), crs = 5070); lines$pair <- paste(lines$a, lines$b, sep = "-")
cat("within-state county pairs with a shared line:", nrow(lines), "\n")
t <- readRDS(file.path(ROOT, "data/local_juris/iv_tract_nb.rds"))[T == 30, .(tract, lat, lon, med, cty, dist_mi, leaid)]
t <- t[dist_mi >= 20 & !is.na(lat)]
pts <- st_transform(st_as_sf(t, coords = c("lon", "lat"), crs = 4326), 5070)
# nearest line among those touching the tract's own county
res <- rbindlist(lapply(split(seq_len(nrow(t)), t$cty), \(ix) { c0 <- t$cty[ix[1]]; L <- lines[lines$a == c0 | lines$b == c0, ]
  if (nrow(L) == 0) return(NULL); nn <- st_nearest_feature(pts[ix, ], L)
  data.table(tract = t$tract[ix], pair = L$pair[nn], other = ifelse(L$a[nn] == c0, L$b[nn], L$a[nn]),
             dline = as.numeric(st_distance(pts[ix, ], L[nn, ], by_element = TRUE)) / MI) }))
t <- merge(t, res, by = "tract")
side <- t[dline < 20, .(side_med = median(med), n20 = .N), by = .(pair, cty)][, k := .N, by = pair][k == 2 & n20 >= 5][, high := as.integer(side_med == max(side_med)), by = pair]
t <- merge(t, side[, .(pair, cty, high)], by = c("pair", "cty"))
t[, `:=`(near = as.integer(dline < 10), dbin = cut(dline, c(0, 5, 10, 20, 30, 50, Inf), right = FALSE))]
dv <- t[!is.na(leaid), .(near = as.integer(mean(near) > .5), high = as.integer(names(which.max(table(high)))), pair = names(which.max(table(pair))),
                          dbin = names(which.max(table(dbin)))), by = leaid]
p <- readRDS(file.path(ROOT, "data/local_juris/district_finance.rds"))[ptax > 0][, bal := uniqueN(y) == 12, by = leaid][bal == TRUE]
d <- merge(p, dv, by = "leaid")[, `:=`(lpt = log(ptax), nh = near * high, post = as.integer(y >= 2018))]
m <- feols(lpt ~ i(y, nh, ref = 2017) | leaid + pair^high^y + pair^dbin^y, d, cluster = ~conum)
mp <- feols(lpt ~ nh:post | leaid + pair^high^y + pair^dbin^y, d, cluster = ~conum)
sink(file.path(OUT, "a9_county_line_placebo.txt"))
cat("County-line placebo (within-state county pairs; tracts >= 20 mi from a state border). Districts:", uniqueN(d$leaid), "| near-high:", uniqueN(d[nh == 1, leaid]),
    "| county pairs:", uniqueN(d$pair), "\n")
print(as.data.frame(as.data.table(coeftable(m), keep.rownames = "term")[, .(term, est = round(Estimate, 4), se = round(`Std. Error`, 4))]), row.names = FALSE)
cat("pooled near x high x post:", round(coef(mp)[1], 4), "(", round(se(mp)[1], 4), ")\n")
sink(); cat(readLines(file.path(OUT, "a9_county_line_placebo.txt")), sep = "\n")
