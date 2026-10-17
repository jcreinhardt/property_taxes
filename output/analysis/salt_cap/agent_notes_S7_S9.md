# Agent notes — runs of S7 (kink) and S9 (assessment) on the 1% building sample, 2026-10-16

Runs: `SALT_PART=S7 R_MAX_VSIZE=48Gb Rscript code/analysis/salt_cap/R/s7_kink.R` (log `run_S7.log`) and
`SALT_PART=S9 ... s9_assessment.R` (log `run_S9.log`; first, failed attempt kept as `run_S9_attempt1.log`).
No specification, sample rule or outcome definition was changed. Shared files (`00_setup.R`, `01b_load.R`) untouched.

## Code changes

| file | line | what | why |
|---|---|---|---|
| `code/analysis/salt_cap/R/s9_assessment.R` | 62 | `log_price` fixed effects in S9b: `"bgk^year + cohort^ca^year"` -> `"bgk^year + cohort^ca"` | `recent_ca x 1[year=k]` is an exact function of (cohort, ca, year), so `cohort^ca^year` absorbed every DDD term; fixest dropped them silently (`notes = FALSE`), `ipre` was empty and `solve(V)` failed with "'a' is 0-diml" (attempt 1). The parcel FE that the hedonic specification replaces absorbs the time-invariant cohort x CA main effect, so `cohort^ca` is the lower-order term; `bgk^year` (the group x year FE of the `sale`/`refi` spec) is unchanged, as pre-specified for `log_price`. Note: the same one-line edit was found already applied on disk by a concurrent process (its comment says "cluster 10% run") when I went to patch it; the file is untracked in git, so this is recorded here. |

| `code/analysis/salt_cap/R/s9_assessment.R` | 65-67 (inserted after the `feols` call of the S9b loop) | guard: if any `recent_ca x year` term is in `m$collin.var`, print which, write an `untestable` ledger row and `next` to the following outcome; `yr` parse wrapped in `suppressWarnings` | Attempt 2 (with `cohort^ca`) failed at the same `solve(V)` line: on the 1% sample the `log_price` model keeps only sales, 30,327 of 43,070 block-group x year cells are singletons and in the remaining cells `recent` (91% of sellers) does not vary within cell in any pre-year, so fixest drops the 2012-15 and 2019 DDD terms (diagnostic in the scratchpad, `diag_s9b.R`). This is thin-cell non-identification, not a specification error; the guard skips the outcome and reports it rather than improvising a coarser group x year FE. `refi` (which ran after the skip) is unaffected. Logs of the two failed attempts kept as `run_S9_attempt1.log` / `run_S9_attempt2.log`. |

`s7_kink.R`: no change needed; ran to completion on the first attempt.

Result of the guard on the 1% sample: `S9b log_price` SKIPPED (5 of 7 DDD terms collinear, N = 12,743 after singleton
removal); `S9b sale` and `S9b refi` estimated. On the full data the block-group x year cells will hold several sales and
the `log_price` DDD should identify without further change.

## Observations (not changed, reported)

- S7: rdrobust warns "Mass points detected in the running variable" (bill16 is rounded to dollars/cents in many
  counties); no cell failed, every sample x outcome x bandwidth returned both periods (96 rows in `table_s7_kinks.csv`).
- S7 ledger verdicts "fail" are outcomes of the pre-specified rules, not errors: the pre-period `log_price` kink is
  0.607 (SE 0.281) i.e. 2.2 SE from zero, so the "all pre kinks within 2 SE" rule fails; the bunching excess mass in
  [$9k,$10k) is negative in both years (-0.031 in 2016, -0.041 in 2019; the difference -0.010 exceeds the 0.005 tolerance).
- S9a municipality name match (`norm_muni`, rule unchanged): NJ roll towns 551, of which 204 match a town on the NJ
  2017-22 revaluation list (the list has 207 county x municipality keys, 194 of them found in the roll; 144 list towns
  have a 2018-20 revaluation); 18 NJ keys collapse two roll names (e.g. ANDOVER BORO / ANDOVER TWP in Sussex) because
  the rule strips BORO/TWP; unmatched list entries are spelling variants (SO HACKENSACK, TETERBOROUGH, OF CLINTON,
  SOUTH ORANGE TOWNSIHP, LOCH ARBOUR, ...). CT: 167 of 167 roll towns match the 169-town OPM schedule.
- S9a schedule check (roll-based vs published): CT 5 town-years agree, 130 roll-only, 121 published-only (the
  published CT schedule is the post-PA 22-74 2023-37 zoning, so cycle1 - 5/10 is a weak proxy for the actual pre-2022
  years); NJ 171 agree, 151 roll-only, 56 published-only. Ledger verdict by the pre-specified rule: inconclusive.
