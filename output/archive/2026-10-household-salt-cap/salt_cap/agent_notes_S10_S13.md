# Agent notes — S13 (capitalisation) and S10 (2025 reversal), 1% building sample

Run date 2026-10-16. Both scripts were run as written after Strategy 3 finished (`S3 done` in `run_s3_s10.log`,
17:37 EDT; the chain shell that was meant to run s10 after s3 had died, so s10 was launched by hand).

## Runs
- `SALT_PART=S13 R_MAX_VSIZE=48Gb Rscript code/analysis/salt_cap/R/s13_capitalization.R > output/analysis/salt_cap/run_S13.log`
  started 17:38:06, finished 17:42 (about 4 min). No errors, no code changes.
- `SALT_PART=S10 R_MAX_VSIZE=48Gb Rscript code/analysis/salt_cap/R/s10_reversal.R > output/analysis/salt_cap/run_S10.log`
  started 17:42:33. Other agents' scripts (s6_taxprice.R, s8_married.R, s3_alt_incumbents.R) ran concurrently.

## Code changes
None so far.

## Notes on the pre-specification vs the script (not changed; reported to the caller)
- `s10_reversal.R` defines `above24_40k = 1[bill24 > 40,000]` (the reversal treatment named in the decision log,
  Strategy 10 parameters) but the event-study loop runs `rate16_pp`, `above16` and `mid24` (10k < bill24 <= 40k, the
  newly deductible range) only; `above24_40k` is unused. Left as written. Only 1,380 base parcels have bill24 > 40k on
  the 1% sample.
- Kink at $40k: 2,274 owner-occupied parcels in TX/FL/WA/NV/SD/WY with 20k < bill24 < 60k, but only 32-58 arm's-length
  sales per year 2022-2025, so the `log_price` kink falls below the script's 300-observation floor and is skipped by
  design (needs the full data).

## Completion
- s10_reversal.R finished 18:52 (about 70 min; 12 event studies on the 2012-2025 panel, 9.9M parcel-years, about 5 min
  each for the full-panel outcomes). `S10 done` written. No errors; no code changes to either script.
- Kink at $40k: `sale` ran (pre n=993, post n=466 within the rdrobust bandwidth; rdrobust warns about mass points in
  bill24); `log_price` skipped (fewer than 300 sales in each period), so no `S10_kink40k` log_price row exists.
- The "NAs introduced by coercion" warnings come from `es_summary()` parsing the hedonic-control coefficient names
  (no year in them) for the log_price cells; harmless, same as in S3.
- Outputs: `parts/estimates_S13_bld1pct.csv`, `parts/ledger_S13_bld1pct.csv`, `table_s13_pv_comparison.csv`,
  `parts/estimates_S10_bld1pct.csv`, `parts/ledger_S10_bld1pct.csv`, `table_s10_phase_down.csv`, figures `es_S10_*`.
