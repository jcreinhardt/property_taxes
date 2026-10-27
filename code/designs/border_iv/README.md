# designs/border_iv: the choice-set instrument

Near a state border (< 10 mi) × on the higher-tax side, with pair × side and pair × distance-bin FE, instruments
having a ≥ 0.5 pp cheaper tract across the line within a 30-minute drive (`x_cheap`). The instrument varies the choice
set, not the rate. First stage +0.33; no reduced-form effect of the cap on prices, revenue or moves.

| Script | What |
|---|---|
| `01_first_stage_balance.R` | reachable-tract gaps at T = 30/60 (writes `data/built/geo/iv_tract_nb.rds`), first stage and balance on distance bins |
| `02_directional_iv.R` | the directional spec (near × high side) on first-stage outcomes and covariates |
| `03_district_finance_ddd.R` | school-district property tax: exposure event studies, dispersion, border DDD with robustness, 2SLS, heterogeneity |
| `04_school_district_map.R` | map of the outside option by district |
| `05_tract_ddd.R`, `06_hpi_ddd.R` | tract-year outcomes and FHFA tract HPI in the DDD |
| `07_repeat_sales_event_study.R`, `08_repeat_sales_pooled.R` | repeat-sales prices (event study; pooled version on more parcels) |
| `09_moveout_1pct_ddd.R`, `10_moveout_full.R`, `11_family_move_direction.R` | Data Axle move-outs and direction of cross-border moves |
| `12_figure.R` | summary figure of the directional IV |
| `13_signed_distance_validity.R`, `14_predicted_balance.R` (+ `_np`), `15_pair_gap_vs_school_finance.R`, `16_covariate_coverage.R` | validity checks: one-sided selection, predicted-endogenous balance, pair gaps vs school-finance regimes, covariate coverage |

Open: rerun with the own-state assignment (`data/built/geo/iv_tract_side_ownstate.rds`).
