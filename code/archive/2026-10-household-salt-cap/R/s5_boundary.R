# s5_boundary.R — Strategy 5: boundary DiD across municipal (place) and unified school-district borders. Pre-specified 2026-10-16.
source("code/archive/2026-10-household-salt-cap/R/00_setup.R"); source("code/archive/2026-10-household-salt-cap/R/01b_load.R")
suppressPackageStartupMessages({ library(sf); library(tigris) }); options(tigris_use_cache = TRUE); sf_use_s2(FALSE)
P <- dq(sprintf("SELECT clip, st, fips, lat, lon, rate16 FROM read_parquet('%s') WHERE base_oo AND rate_ok AND lat IS NOT NULL AND lon IS NOT NULL AND lat BETWEEN 24 AND 50 AND lon BETWEEN -125 AND -66", pq("analysis_parcels")))
states <- setdiff(unique(P$st), c("AK", "HI", "GU", "VI", "PR"))
pts <- st_as_sf(P, coords = c("lon", "lat"), crs = 4326)
cache <- file.path(PANEL_DIR, "s5_segments.rds")
if (!file.exists(cache)) {
  seg_all <- list()
  for (s in states) for (kind in c("place", "unsd")) {
    poly <- tryCatch(if (kind == "place") places(s, year = 2017, cb = TRUE) else school_districts(s, year = 2017, type = "unified", cb = TRUE), error = function(e) NULL)
    if (is.null(poly) || nrow(poly) < 2) next
    poly <- st_transform(poly[, "GEOID"], 5070); p_s <- st_transform(pts[pts$st == s, ], 5070)
    j <- st_join(p_s, poly, join = st_within); j <- j[!is.na(j$GEOID), ]; if (nrow(j) == 0) next
    nb <- st_touches(poly); bd <- st_boundary(poly); idx <- match(j$GEOID, poly$GEOID)
    # distance from each parcel to the boundary of its own polygon = distance to the nearest other jurisdiction (border); neighbour = nearest touching polygon
    dself <- as.numeric(st_distance(j, bd[idx, ], by_element = TRUE))
    # nearest touching neighbour, vectorised per own polygon (st_nearest_feature on the neighbours' boundaries); same quantity as the
    # per-parcel st_distance loop it replaces (verified identical on DE places and unified districts), ~100x faster. See agent_notes_S5.md.
    nbg <- rep(NA_character_, nrow(j))
    for (k in unique(idx)) { nbs <- nb[[k]]; ii <- which(idx == k); if (!length(nbs)) next
      if (length(nbs) == 1) { nbg[ii] <- poly$GEOID[nbs]; next }
      nbg[ii] <- poly$GEOID[nbs[st_nearest_feature(j[ii, ], bd[nbs, ])]] }
    seg_all[[paste(s, kind)]] <- data.table(clip = j$clip, st = s, kind = kind, GEOID = j$GEOID, nb_geoid = nbg, dist_m = dself)
    cat(s, kind, nrow(j), "parcels\n")
  }
  seg <- rbindlist(seg_all); saveRDS(seg, cache)
} else seg <- readRDS(cache)
seg <- seg[!is.na(dist_m) & !is.na(nb_geoid)]
seg[, segment := paste(kind, pmin(GEOID, nb_geoid), pmax(GEOID, nb_geoid))]
jr <- merge(seg, P[, .(clip, rate16)], by = "clip")[, .(med_rate = median(rate16), n = .N), by = .(kind, GEOID)]
seg <- merge(seg, jr[, .(kind, GEOID, rate_own = med_rate, n_own = n)], by = c("kind", "GEOID"))
seg <- merge(seg, jr[, .(kind, nb_geoid = GEOID, rate_nb = med_rate, n_nb = n)], by = c("kind", "nb_geoid"))
seg[, rel_diff := abs(rate_own - rate_nb) / pmin(rate_own, rate_nb)]
seg <- seg[rel_diff >= 0.10 & n_own >= 10 & n_nb >= 10]
fwrite(seg[, .N, by = .(kind, within_050 = dist_m <= 804.7, within_025 = dist_m <= 402.3)], file.path(OUT_S, "table_s5_sample.csv"))
d <- load_panel(2012:2019, "oo")[rate_ok == TRUE & !is.na(valuebin)]
for (band in c(0.5, 0.25)) {
  sb <- seg[dist_m <= band * 1609.34][, .SD[which.min(dist_m)], by = clip]       # nearest border per parcel
  dd <- merge(d, sb[, .(clip, segment, kind)], by = "clip")
  dd[, segvb := paste(segment, (valuebin - 1) %/% 5)]                              # segment x 10 value bins x year
  cat(sprintf("band %.2f mi: %d parcels, %d segments\n", band, uniqueN(dd$clip), uniqueN(dd$segment)))
  for (oc in c("sale", "log_price", "exit_prim", "refi")) {
    id <- sprintf("S5_rate_%03d", as.integer(band * 100))
    r <- tryCatch(run_es(dd, oc, "rate16_pp", "clip + segvb^year", ~segment, "S5", id, label = sprintf("S5 boundary DiD (%.2f mi): %s on rate16", band, oc), fig = band == 0.5),
                  error = function(e) { cat(sprintf("  %-10s ERROR: %s\n", oc, conditionMessage(e))); NULL })
    if (is.null(r)) next
    cat(sprintf("  %-10s post=%.4g (%.4g) p_pre=%.3f N=%d clusters=%d\n", oc, r$summary$post_coef, r$summary$post_se, r$summary$pre_p, r$N, r$clusters))
    if (band == 0.5) add_ledger("S5", sprintf("Shocks continuous at the border [%s]", oc), "Pre-trends (joint test 2012-2015)",
                                sprintf("F=%.2f p=%.3f; post %.4g (%.4g)", r$summary$pre_F, r$summary$pre_p, r$summary$post_coef, r$summary$post_se), verdict_pre(r$summary$pre_p, r$summary$pre_mean_abs, r$summary$post_coef))
  }
  if (band == 0.5) {
    b <- unique(dd[, .(clip, segment, rate16, year_built, living_area, lot_sqft, lor16, agi_imp, age16, value16)])
    b[, high := as.integer(rate16 > median(rate16)), by = segment]
    bal <- rbindlist(lapply(c("year_built", "living_area", "lot_sqft", "lor16", "agi_imp", "age16", "value16"), function(v) { bb <- b[!is.na(get(v))]; sdv <- sd(bb[[v]])
      m <- feols(as.formula(sprintf("%s ~ high | segment", v)), bb, cluster = ~segment); data.table(covariate = v, diff_sd = coef(m)["high"] / sdv, se_sd = se(m)["high"] / sdv) }))
    fwrite(bal, file.path(OUT_S, "table_s5_balance.csv")); print(bal)
    add_ledger("S5", "Covariate balance across borders", "High- vs low-rate side within segment (SD units)", paste(sprintf("%s=%.2f", bal$covariate, bal$diff_sd), collapse = "; "), if (all(abs(bal$diff_sd) < 0.1)) "pass" else "fail")
    dd[, high := rate16 > median(rate16), by = segment]                             # high-rate side within segment (same split as the balance table)
    comp <- dd[!is.na(n_persons), .(n_persons = mean(n_persons), turnover = mean(exit_prim, na.rm = TRUE)), by = .(segment, high, post)]
    cw <- dcast(comp, segment + post ~ high, value.var = c("n_persons", "turnover"))
    cs <- cw[, .(d_persons = mean(n_persons_TRUE - n_persons_FALSE, na.rm = TRUE), d_turnover = mean(turnover_TRUE - turnover_FALSE, na.rm = TRUE)), by = post]
    fwrite(cs, file.path(OUT_S, "table_s5_composition.csv")); print(cs)
    add_ledger("S5", "Who lives on each side does not change because of TCJA", "Household size and surname turnover gap across the border, pre vs post",
               sprintf("persons gap pre %.3f post %.3f; turnover gap pre %.3f post %.3f", cs[post == 0, d_persons], cs[post == 1, d_persons], cs[post == 0, d_turnover], cs[post == 1, d_turnover]), "inconclusive")
  }
}
add_ledger("S5", "Services across the border do not change after 2018", "none", "untestable; school consolidations not screened", "untestable")
add_ledger("S5", "No spillovers across the border", "none", "untestable; movers may relocate across the line", "untestable")
cat("S5 done\n")
