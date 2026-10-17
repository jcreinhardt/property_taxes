# Agent notes — runs of s4_investor_ddd.R (SALT_PART=S4) and s8_married.R (SALT_PART=S8), 2026-10-16

Sample: bld1pct, panel `data/salt_cap/panel/analysis_{parcels,panel}.parquet` (built 14:33). Both scripts run sequentially
(one R process at a time; s3_core.R was running alongside, ~1.6 GB RSS). `R_MAX_VSIZE=48Gb`.

## Code changes
None. Neither script needed a fix; no specification, sample rule or outcome definition was touched.

## S4 (`run_S4.log`) — 16:50:31 to ~17:33, 43 min, peak ~2.3 GB RSS
- Completed: 16 estimates (S4_rate, S4_above, each x sale/log_price/refi/to_rental, plus `_agree` robustness), 17 ledger rows,
  8 figures + 16 event-study CSVs in `figures/`. Parts: `parts/estimates_S4_bld1pct.csv`, `parts/ledger_S4_bld1pct.csv`.
- Warnings only: 4 x "NAs introduced by coercion" from the log_price fits (hedonic term names carry no `year::`, so
  `as.integer(sub(...))` yields NA; those NAs are dropped by the `%in%` filters, so no effect on results). Not a bug.

## S8 (`run_S8.log`) — 17:33:01 to ~17:40, 7 min
- Completed: 8 estimates (S8_pooled, S8_strict x sale/log_price/exit_prim/refi), 13 ledger rows, 8 figures + 8 CSVs,
  `table_s8_balance_S8_pooled.csv`, `table_s8_balance_S8_strict.csv`. Parts: `parts/estimates_S8_bld1pct.csv`, `parts/ledger_S8_bld1pct.csv`.
- fixest NOTEs: 11.1–11.2k singleton parcel FEs removed (pooled), 6.3–6.9k (strict); expected with parcel FE. Same harmless
  coercion warning as S4 (hedonic terms).
- Not produced, as pre-specified but not implemented in the script: `table_s8_vesting_change.csv` ("vesting changes of the same
  parcel reported as an outcome"). The ledger row "Marriage/divorce do not respond to the cap" references that file, but
  s8_married.R contains no code that builds it (its comment says it needs deeds). Left as is — not improvised.
- Pre-specified balance threshold (all |diff| < 0.25 SD) fails in both runs on age16 (0.50 / 0.33 SD) and lor16 (0.59 / 0.48 SD);
  value16 and agi_imp balanced (< 0.04 SD). Reported, not altered.
