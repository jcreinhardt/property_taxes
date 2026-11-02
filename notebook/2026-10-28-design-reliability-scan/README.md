---
date: 2026-10-28
title: Which event-study results have flat pre-trends and a post-2017 effect? A scan of every saved series
question: Across all designs in the repository, which results pass a mechanical test of a flat pre-period and a post-2017 effect, and do they survive reading the paths?
status: current
code: property_taxes 9d83b5f
tags: synthesis, pre-trends, event study, design assessment
---

## Answer
27 of 374 series pass mechanically; three results and one near-miss hold up, and all of them start in 2019–20.

| Result | Pre 2013–16 | Post | Verdict |
|---|---|---|---|
| Border diff-in-disc, effective rate, high minus low side (`border_rd/04`) | within ±0.1 pp | −0.09 (2019) → −0.33 pp (2023–24) | passes; per-side parcel panel (`border_twfe/04`) agrees: −0.10 to −0.12 pp 2021–24 |
| County exposure, median effective rate bill/price (`exposure_did/04`) | within ±0.006 | +0.005 (2018) → +0.033 (2024) per SD | passes; mirror of the price decline, cleaner than the price series |
| Districts near a cheaper cross-state district, high side, log property tax (`border_iv/03`) | ±1.5% n.s. | −3.1 to −3.4% from 2020 | passes at 10/20 mi, RI p = 0.04; county-line placebo −3.1% (SE 3.6); exposure interaction wrong sign |
| County prices per $100 cap burden, Z1 / SOI / IV (`exposure_did/14`, `05`) | +0.4% (2013) → 0 | −0.4% (2019) → −1.0% (2021–24) | near pass: pre-drift a quarter of the effect, opposite sign |
| Mortgage-interest component, county prices (`exposure_did/05`) | flat | +0.7% → +3.1% from 2020 | passes mechanically, wrong sign: the post-2020 boom in mortgage-heavy suburbs |
| Z2 county-government revenue (`exposure_did/14`) | flat | −1.6 to −2.6% from 2020 | unresolved vs mean reversion in rates |
| Z2 migration rates (`14`, `15`) | flat | 2020–21 only | COVID |
| Within-CBSA dispersion of district tax per pupil (`exposure_did/03`) | +0.1 to +0.5% | +0.4 → +0.9% (2021–23) on a 0.465 base | small, opposite of compression |
| Border IV repeat sales, cross-border vacancy, tract tax per parcel, county total taxes unweighted | | | each contradicted by a companion spec or a significant pre year |

Fails: all migration series (pre-trends, 2020 steps or null); school-district revenue on exposure (null, flat post);
enrolment, parcel panel, ZIP repeat sales, ZIP property-tax component (pre-trends); border diff-in-disc on district tax
per pupil (pre-trend; the DDD above is the version that holds). The mortgage-interest series is the caution: a flat
pre-period plus a post-2020 effect is also what a COVID-era confound looks like.

## How
- **Rule:** per series, weighted (1/se²) OLS of the pre-2017 coefficients on year, |t| < 2, and max |coef/se| over
  2012–16 < 2.5 ("flat pre"); at least two coefficients in 2019+ with |t| > 1.96 and all post coefficients of one sign
  ("post effect"). Series = one group of (y, coef, se) rows in a saved table; 40 tables under `output/designs/`
  (level tables, balance tables and non-event-study outputs skipped).
- **Scripts:** `score_event_studies.R` (here; reads the tables from the repo root) → `event_study_scores.csv` (here).
- **Check:** 374 series; verdict counts: pass 27, flat pre / no effect 89, pre-trend + effect 110, pre-trend / no effect 148.

## Open
- The rule is blind to magnitude; a ±0.4% pre-drift fails a series with tight SEs. Rambachan–Roth bounds would do this properly.
