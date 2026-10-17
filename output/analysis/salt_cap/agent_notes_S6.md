# Agent notes — Strategy 6 run (s6_taxprice.R), 2026-10-16

Pre-specification in `decision_log.md` untouched. Changes to the code are listed here as they are made.

## Pre-run checks (usincometaxes 0.7.1)
- `taxsim_calculate_taxes(.data, marginal_tax_rates, return_all_information)` has no `interface` argument in 0.7.1: it always
  runs the bundled TAXSIM 35 WASM binary (08/02/23 build) locally through V8; no network needed. Tested on 8 and 50,000 rows
  (~4 s per 50k chunk, 46 output columns).
- Output columns with `return_all_information = TRUE`: `fiitax, siitax, fica, frate, srate, ..., v13_zero_bracket_amount,
  v17_itemized_deductions, ..., v26_amt_income, v27_amt_liability, ..., v35_state_itemized_deduction`. Verified on test cases
  that `v17_itemized_deductions` is 0 when the standard deduction is taken (and `v13_zero_bracket_amount` is then the standard
  deduction), so `item = v17 > 0` identifies itemisers as the script intends.
- Inputs: 723,841 base owner-occupied parcels with imputed AGI (GU/VI/PR excluded); no NA in married, st, bill16, mortint16,
  agi_bin, zip5; state abbreviations incl. DC map to SOI codes. TAXSIM rejects `sage > 0` on single returns; the script
  already sets `sage = 0` for singles.

## Code fix 1 — TAXSIM column lookup (before the run)
`grep("amt", names(r))[1]` returned `v26_amt_income` (positive for everyone), not the AMT liability. The lookup now prefers a
column matching `amt_liab` (`v27_amt_liability`) and, for itemisation, `^v17_itemized` (`v17_itemized_deductions`, ahead of
`v35_state_itemized_deduction`). Only `amt17` in `table_s6_dprice_by_agi.csv` is affected; dprice, validation and estimates
do not use the AMT column.

## Noted, not changed — decomposition marginal rate
The decomposition comment says `mtr18 = federal+state marginal rate under 2018 law`, but the code uses `frate_2018` (federal
only) for `price18_nocap`. Left as coded (the brief lists `frate` as the variable the script uses); the state part of the
finite-difference price therefore sits in `d_cap`/`d_std` rather than in the no-cap counterfactual. Flagged in the report.

## Run 1 (17:21–17:45): TAXSIM step completed and cached (`data/salt_cap/panel/taxsim_s6.parquet`, 4 x 723,841 rows,
~5 min per law/delta pass); stopped at the validation step because `cor18` came out NA.

## Code fix 2 — NA handling in the SOI validation
The 2018 SOI file has fewer ZIPs than 2016 (27,556 vs 29,872) and suppressed cells have `returns = 0`, so some ZIP (x bin)
cells have an NA/NaN 2018 itemisation share; `cor()` without `use =` then returns NA. Now `use = "complete.obs"` and
`na.rm = TRUE` for the mean errors; the number of compared cells per year is added to `table_s6_validation.csv`
(`n_cells16`, `n_cells18`). No change to the statistic or to the weak-identification rule (ZIP correlation 2016 < 0.6).
Rerun from the cache (TAXSIM not recomputed).

## Run 2 (from cache) — dprice sanity (723,841 households)
- dprice = price18 − price17, mean 0.066 (mean after-tax price of $1 of property tax 0.887 → 0.952); quantiles
  q01 −0.066, q05–q50 0, q75 0.144, q95 0.286, q99 0.346; 0.5% below −0.1 and 0.15% above 0.4 (extremes −0.85 / 1.21
  are $100 finite-difference artefacts at tax cliffs). 39% of households have dprice > 0.01.
- By AGI bin (mean): 0.011 / 0.040 / 0.067 / 0.089 / 0.189 / 0.127 for bins 1–6. Bin 6 (AGI > 200k) is lower than bin 5
  because 60% of bin-6 households are on the AMT under 2017 law (`amt17`), where SALT was already non-deductible.
  Within bins 1–5 dprice rises with bill16 up to the 10–20k group and drops for bills > 20k (AMT again).
- Validation (`table_s6_validation.csv`): ZIP-level itemisation share correlation 0.80 (2016) / 0.80 (2018) on 10.4k ZIPs;
  ZIP x AGI bin 0.79 / 0.77 on 24.6k cells. Mean error +0.11 (2016) / +0.04 (2018): homeowners itemise more than all
  filers in their ZIP, as anticipated in the decision log. Weak-identification flag: pass (0.80 ≥ 0.6).

## Decomposition (`table_s6_decomposition.csv`) — reading note, no code change
`price18_nocap = 1 − frate18 × 1[2017 itemised deductions > 2018 standard deduction]` uses the federal marginal rate only,
while `price17`/`price18` are finite differences of federal **plus state** tax. TAXSIM's state component (state property-tax
deductions/credits) is about −0.03 to −0.04 per $1 in every AGI bin, so it enters `from_std_deduction` with a positive and
`from_cap` with a negative sign; that is why `from_cap` is negative for bins 1–4 (where almost nobody would itemise even
without the cap: share with 2017 itemised deductions > 2018 standard deduction = 2.5% / 9% / 17% / 25% / 46% / 85% for
bins 1–6). For reference only (computed from the cache, not written to any output): keeping the simulated 2018 state
component in the counterfactual (`1 + dS18 − frate18 × 1[...]`) gives from_cap = 0.001 / 0.001 / 0.005 / 0.014 / 0.089 /
0.266 and from_std = 0.010 / 0.039 / 0.062 / 0.075 / 0.100 / −0.139 for bins 1–6 (bin 6's negative std part is the 2018
AMT relief: 60% of bin 6 were on the AMT in 2017, where SALT was already non-deductible). The pre-specified rows
`S6_d_cap` / `S6_d_std` in `estimates_S6_bld1pct.csv` use the as-coded split.

## Run 2 result: completed 17:43–19:30 (1 h 47 min from the TAXSIM cache; 12 event studies on 5.63M parcel-years plus the
two decomposition regressions). Outputs: `parts/estimates_S6_bld1pct.csv` (16 rows), `parts/ledger_S6_bld1pct.csv`,
`table_s6_dprice_by_agi.csv`, `table_s6_validation.csv`, `table_s6_decomposition.csv`, `figures/es_S6_*`.

## Code fix 3 — ledger "functional form" rows read the wrong file (after the run)
The block comparing coarse/base/fine betas read the merged `output/analysis/salt_cap/estimates.csv`. Under `SALT_PART=S6`
the script writes to `parts/estimates_S6_bld1pct.csv`, and the merged file is only refreshed by the report step; it had
been refreshed at 19:03 while S6 was mid-way, so the `[refi]` row listed base and coarse only (and would have been empty
had no merge happened). Now reads the part file when `SALT_PART` is set. The four ledger rows were regenerated with the
fixed code from the saved estimates (deterministic; the event studies were not rerun). Effect: `[refi]` now compares all
three specs and its verdict changes from pass to fail (range 0.0136 > 2 x max SE 0.009); the other three rows unchanged.

## Reading note on the pre-trend verdicts (no change)
For `sale` (S6_base) the 2012–2015 coefficients on dprice x year are −0.046, −0.043, −0.041, −0.030 and the 2017–2019
ones −0.056, −0.052, −0.044: every year differs from the 2016 reference by roughly the same amount, i.e. the "post effect"
is a 2016 level anomaly rather than a break in 2018. Same shape for `exit_prim` (pre −0.031/−0.020/−0.026/−0.008, post
−0.019/−0.022). The pre-trend ledger rows for sale, log_price and exit_prim are therefore "fail" as pre-specified; refi passes.
