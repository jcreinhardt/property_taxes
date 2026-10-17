# Agent notes — Strategy 5 run (s5_boundary.R), 2026-10-16

Code changes made before the run (pre-specification in `decision_log.md` untouched; bands, 10% rule and fixed effects unchanged):

1. **Performance (geometry step).** The nearest-touching-neighbour assignment used `sapply` over parcels with one `st_distance`
   call per parcel (measured 1.9–5.3 s per 1,000 parcels on DE; ~1.4M parcel×kind rows → 1–2 h). Replaced by a loop over own
   polygons calling `st_nearest_feature(parcels_in_polygon, boundaries_of_touching_neighbours)`. Same quantity (nearest touching
   polygon by boundary distance; single-neighbour and no-neighbour cases handled identically). Verified on DE places (876 parcels)
   and DE unified districts (2,000 parcels): 100% agreement, including NAs; ~100x faster. Own-boundary distance (`dist_m`) unchanged.
2. **Composition table.** `high` (high-rate side of the border) was computed against the *global* median of `rate16` inside the
   `by=` of the composition table, while the balance table used the within-segment median. Now computed within segment for both,
   as the ledger text ("gap across the border") implies.
3. **Diagnostics.** The `tryCatch` around `run_es` swallowed errors silently; it now prints the error message to the log.

Run (2026-10-16 17:30–17:35, 5 min wall clock including the geometry step for 48 states × 2 boundary types; cache
`data/salt_cap/panel/s5_segments.rds`). DC skipped automatically (fewer than two polygons of each kind). No errors; the
`NAs introduced by coercion` warnings come from `es_summary()` parsing hedonic-control names in the log_price model and are benign.
Note on the design as implemented: `dist_m` is the distance to the parcel's *own* polygon boundary (which for places includes the
edge with unincorporated land), and the neighbour is the nearest *touching* polygon; both as pre-specified.
