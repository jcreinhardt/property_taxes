---
date: 2026-10-29
title: Asking rents and the SALT cap on a repeat-listing (same-unit) rent index: does the ZIP gradient survive?
question: Build a same-unit rent index from the Altos repeat listings and rerun the three rent designs on it; do the 2013–14 pre-trend, the post-2018 ZIP gradient (−0.4 to −0.7% per $100 by 2021–24) and its disappearance with the 2012 rent-level control survive when composition is held fixed?
status: current
code: property_taxes 9d83b5f + branch claude/pensive-bell-8a123e (worktree; includes the uncommitted Altos scripts of claude/altos-data-hpc-location-77c629)
tags: rents, Altos, repeat listings, exposure, border RD, DDD, SALT
---

## Answer
The post-2018 ZIP gradient survives on the same-unit index at about two thirds of its size and with the same timing,
and the 2012 rent-level × year control still removes it; the 2013–14 pre-trend of the same sign does not survive but
is replaced by a pre-trend of the opposite sign: on the same-unit index, rents in more exposed ZIPs were *higher*
relative to 2017 in 2013–16 (+0.14 to +0.22% per $100), i.e. they were already growing more slowly from 2015–16 on,
and the post-2018 coefficients continue that decline. The composition break of the feed in 2019 is real (the fixed-mix
index jumps 9 log points nationally in 2019, the same-unit index 0.8) but it is not what makes the ZIP gradient: the
gradient is a smooth within-state rent-tier trend, present before the cap, absorbed by the 2012 level. Verdict as
in `2026-10-29-altos-rents-salt-cap`: not an identified cap effect. Same conventions as there (exposure
`d_salt_own` per $100/yr, winsorised p1/p99; weighted by 2016 returns; 2017 = 0; SE by county).

**(a) ZIP exposure × year (6,300 uncensored SOI ZIPs with ≥ 10 linked pairs every year 2013–24; zip + state × year FE), % per $100/yr:**

| | 2013 | 2014 | 2015 | 2016 | 2018 | 2019 | 2020 | 2021 | 2022 | 2024 |
|---|---|---|---|---|---|---|---|---|---|---|
| repeat index | +0.14 (0.03) | +0.21 (0.03) | +0.22 (0.02) | +0.14 (0.01) | −0.06 (0.01) | −0.08 (0.01) | −0.21 (0.02) | −0.26 (0.04) | −0.30 (0.06) | −0.45 (0.07) |
| repeat, + 2012 rent level × year | +0.09 (0.04) | +0.09 (0.03) | +0.07 (0.02) | +0.03 (0.01) | −0.01 (0.01) | −0.01 (0.02) | −0.02 (0.03) | +0.07 (0.04) | +0.15 (0.05) | +0.06 (0.06) |
| fixed-mix index (9,486 ZIPs, previous entry) | −0.29 (0.07) | −0.16 (0.06) | 0.03 (0.05) | 0.04 (0.04) | −0.03 (0.02) | −0.13 (0.05) | −0.26 (0.05) | −0.43 (0.07) | −0.42 (0.09) | −0.66 (0.08) |

Three components jointly: the property-tax part is −0.12 to −0.14 (SE 0.04–0.10) in 2018–20 and +0.1 to +0.2 (n.s.)
in 2021–24; the income/sales-tax part runs from +0.25 (2015) to −0.96 (0.11) in 2024; mortgage interest from −0.24
(2015) to +1.4 (0.2) in 2022–24, as with the fixed-mix index: the gradient loads on the income-tax and mortgage parts
with opposite signs, not on the property-tax part.

**(b) Border diff-in-disc on the exposure gap** (30,194 tracts with ≥ 5 linked pairs within 100 mi, 58 pairs; the
tract index is thin in 2019–22). Pooled local-linear, h = 15, per $100 gap: +0.012 (0.007), +0.009, +0.011 (0.005),
+0.005 (0.002) in 2013–16; +0.001 (2018), −0.005, −0.008, −0.016 (0.009), −0.020 (0.010), −0.019 (0.014), −0.012
(0.016) in 2018–24 (fixed-mix: pre within ±0.014, post −0.006 to −0.009). Per-pair series: post-2018 slope −0.006
(0.004) on 19 pairs, pre +0.009 (0.005) on 24 (fixed-mix −0.011 (0.006) on 42, pre 0.001). The unscaled high-tax-side
diff-in-disc turns *positive* on the same-unit index, +0.04 to +0.08 log points in 2021–24 (robust CIs exclude 0),
against −0.01 to −0.02 on the fixed-mix index; the unscaled high-exposure-side term (hx) in the pooled regression is
+0.07 to +0.08 (0.02–0.04) in 2021–24. Still no exposure discontinuity at the line (rdrobust −1.6 [−11.9, 4.1]).

**(c) Near vs far** (16,637 tracts within 50 mi seen in ≥ 6 years, 68 pairs): the gradient away from the line c_t is
+0.12 / +0.14 / +0.12 / +0.07 (SE 0.01–0.03) in 2013–16, −0.04 (2018), −0.06, −0.13, −0.15, −0.18, −0.21, −0.24
(0.04) in 2024 (fixed-mix: −0.26 / −0.24 in 2013–14, ≈ 0 2015–16, −0.39 in 2024); the extra gradient within 10 mi
b_t is ≈ 0 through 2020 and +0.08 to +0.16 (0.05–0.08) in 2021–24, so near the line the gradient is again weaker
(near-only event study −0.10 (0.07) in 2024, far −0.24 (0.04)).

**The index itself** (`output/build/altos/03_repeat_index.txt`, figure): 25.0m unit pairs, 17.0m units. Two
problems of the export: (i) the unit ids change in 2018 (hash ids up to 2018, UUIDs from 2018), so no pair links
≤ 2017 to ≥ 2019 directly and every chain across the cap runs through 2018; (ii) a listing that straddles a year end
counts as a repeat with dlp = 0 (the same spell priced in both years): 35–57% of consecutive-year pairs are exact
zeros in 2012–21, 83% in 2022, 23–29% in 2023–25, and consecutive-year pairs average +0.2%/yr against 2–5% actual
rent growth. The main index drops the exact zeros (10–18% of pairs two years apart are genuine zeros, so it overstates
growth a little); kept, the index rises 13 instead of 21 log points 2017–24. On 6,518 ZIPs balanced 2012–25 the mean
within-ZIP change 2017 → 2024 is +40.5 log points on the fixed-mix index, +21.1 on the repeat index (+13.2 zeros
kept, +24.5 on pairs ≥ 2 years apart pooled nationally); 2013: −14.9 vs −10.4. In 2018–20 the repeat index is flat
(+0.8, +0.8, +1.6) while the fixed-mix index rises +3.1, +9.4, +11.1: the 2019 jump is composition.

## How
- **Outline:** `altos_repeat_unit.parquet` (unit × consecutive listing years, dlp) → per ZIP / tract Bailey-Muth-Nourse:
  dlp on +1[y] −1[y0] year dummies, 2017 = 0, solved from the aggregated (y0, y) cells via the normal equations
  (Laplacian of the pair graph), NA for years not chained to 2017 → `ALTOS_INDEX=repeat` reruns of the three designs.
- **Choices:** |dlp| ≤ 1 (drops 0.3%); exact-zero changes dropped in `lr_rep` (kept in `lr_rep0`); no weighting of pairs
  by gap; counts `n_rep` = pairs involving the year, `n_end` = pairs ending in it; (a) ZIPs with `n_rep` ≥ 10 and an
  index in every year 2013–24, 2012 rent level = fixed-mix `lr_adj` of `zip_year.rds` (≥ 10 listings), demeaned by
  state; ZIP specs 1, 2 and 6 only (no county index built); (b), (c) tract-years with `n_rep` ≥ 5, otherwise as in the
  previous entry (h = 10/15/25, pair-years with ≥ 20 tracts per side, near = < 10 mi, ≥ 6 of 12 years).
- **Inputs:** `data/altos/altos_repeat_unit.parquet` (Bouchet job 31311040, `01b_altos_cells.sql`; not in git);
  `data/built/altos/{zip,tract}_year_repeat.rds` (this entry), `zip_year.rds` (02); exposure, tract-ZIP and border-side
  files as in the previous entry; `data/hud_zip_to_county_crosswalk.xlsx`.
- **Scripts:** `code/build/altos/03_repeat_index.R`; `ALTOS_INDEX=repeat Rscript code/designs/exposure_did/17_rents_altos.R`,
  `.../border_rd/08_rent_diff_in_disc.R`, `.../border_iv/17_rent_exposure_near_far.R`; outputs
  `output/build/altos/03_repeat_index.{txt,png}`, `output/designs/exposure_did/d17_rents_altos_repeat.*`,
  `output/designs/border_rd/d08_rent_diff_in_disc_repeat*`, `output/designs/border_iv/d17_rent_exposure_near_far_repeat.*`.
  The default runs of the three scripts reproduce their fixed-mix outputs unchanged.
- **Check:** ZIP spec 1 on the repeat index: 2024 −0.45 (0.07), 2015 +0.22 (0.02), 6,300 ZIPs; balanced-ZIP mean change
  2017 → 2024 +21.1 (repeat) vs +40.5 (fixed-mix) log points; (b) post-2018 series slope −0.006 (0.004), 19 pairs;
  (c) c_2024 −0.24 (0.04), b_2024 +0.16 (0.08).

## Open
- The straddling-spell problem needs the unit × month table on Bouchet (`altos_unit_month.parquet`): define listing
  spells and pair consecutive spells of a unit instead of consecutive calendar years; that also fixes the within-spell
  price cuts that remain among the non-zero consecutive-year pairs (2018–20 and 2023–25 growth looks too low).
- The positive high-tax-side rent diff-in-disc after 2020 on the same-unit index (+0.04 to +0.08) is the one new
  border result; worth checking against the rate-based gap of `border_rd/04` and the feed's side-specific coverage.
