# designs/border_rd: regression discontinuity at state borders

Running variable: signed distance of a tract to its state border (negative on the lower-tax side), cutoff 0; rdrobust
(local linear, triangular kernel, robust bias-corrected CIs, SE by county), outcomes demeaned by border pair. Sides and
distances from `data/built/geo/iv_tract_side.rds`. This design varies the tax rate itself (≈ 0.75 pp jump).

| Script | What |
|---|---|
| `01_side_selection_levels.R` | levels on either side of the line, within pair |
| `02_profiles_by_distance.R` | means by signed distance out to 100 mi (the low-side school-district dip) |
| `03_covariate_rd.R` | RD on the rate and 11 covariates; Holm-adjusted p-values |
| `04_rd_by_year_and_diff_in_disc.R` | yearly RDs 2013–2024 (fixed h = 15 mi) on CoreLogic, ACS and SEDA panels, and the difference in discontinuities centred on 2017 |
| `05_enrolment_rd.R` | pupils per district by year, district as the unit |
| `06_gap_range_signed_distance.R` | own minus neighbour rate differences by signed distance |
| `07_tract_diff_by_distance.R` | local rate differentials by distance to the border |

Notebook: `notebook/2026-10-21-border-rate-gaps-over-time`. Check numbers: rate RD +0.747 pp; diff-in-disc 2023 −0.331 pp.
