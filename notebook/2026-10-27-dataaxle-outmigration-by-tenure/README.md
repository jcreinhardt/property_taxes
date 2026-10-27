---
date: 2026-10-27
title: Data Axle out-migration vs ACS by owner/renter status, 2006–2023
question: Do out-migration rates in Data Axle roughly match ACS migration rates, split by homeowners vs renters, over the whole time series?
status: current
code: property_taxes 0fcca23 (local main; outputs uncommitted at time of writing)
tags: Data Axle, ACS, moves, tenure
---

## Answer
Roughly for the total, not by tenure. Annualised by the files' cut-off months, Data Axle out-moves (primary family changes at a unit)
run 10.4–13.9% a year for all units, 1–4 pp below the ACS "moved in within 12 months" rate (12.6–16.0%) and close to the ACS
`MIGRATE1` householder rate (11.2–13.4%). Owners (Data Axle `owner_renter_status` 7–9) move out 7.5–10.5% a year, 1.5–2× the ACS
owner rate (4.5–7.1%); renters (0–3) 12.6–20%, half to two-thirds of the ACS renter rate (25–35%). The in-move measure (new primary
with length of residence ≤ 1) matches ACS owners (5.4–7.5%) and comes closer for renters (18–31%). Renters are only 10–19% of Data
Axle units (ACS ≈ 35% of households). Time patterns match the ACS: flat to slightly rising 2009–2020, lower in 2021→23; no Data Axle
trend of its own once the 2015→16 and 2017→18 pairs are annualised.

| Pair | Months | All DA out / ACS | Owners DA out / ACS | Renters DA out / ACS |
|---|---|---|---|---|
| 2007→08 | 12 | 11.3 / 15.4 | 8.8 / 5.8 | 14.5 / 34.4 |
| 2013→14 | 12 | 12.0 / 14.5 | 8.8 / 5.4 | 17.5 / 30.1 |
| 2017→18 | 14 | 12.3 / 14.2 | 10.0 / 6.5 | 18.2 / 27.7 |
| 2022→23 | 11 | 11.5 / 12.6 | 10.5 / 5.7 | 13.2 / 25.6 |

## How
- **Outline:** 1% building sample of Data Axle, all years (2022 from the quarterly-built replacement) → unit = building key × unit
  number with exactly one primary family → year grid 2006–2023, last record carried forward → pairs t→t+1: out-move (family_id
  differs), in-move (LOR ≤ 1 in t+1) by tenure in t → × 12 / months between the two files' cut-offs (`max(recency_date)`).
- **Choices:** tenure from Data Axle's modelled score (owner 7–9, renter 0–3, unknown 4–6), not CoreLogic; backfilled and
  observed-only variants (observed-only owners 6.6–11.3%, renters 19–26%); ACS = PUMS 1-year households, `MV` = 1 by tenure of the
  current unit (`pums_mv_by_year.csv`); `MIGRATE1` rates from `acs_hh_move_rates.csv`.
- **Inputs (cluster):** `~/ptax_explore/samples/da_bld1pct.parquet`, `~/ptax_explore/samples/da_annual_2022_from_quarters/`.
- **Scripts:** `output/validation/dataaxle/move_tracking/outmig_by_tenure.sql`; outputs `outmig_by_tenure.csv`, `outmig_cutoffs.csv`,
  `outmig_tenure_shares.csv`; ACS benchmark `output/validation/dataaxle/tenure_all_years_vs_acs/pums_mv_by_year.csv`
  (`code/validation/dataaxle/pums_mv.sh`); doc `text/data_exploration/dataaxle.md` §4.5.
- **Check:** 2013→14 backfilled: all 12.02% (1,330,538 units), owner 8.75%, renter 17.45%; cut-offs 2017 = 201705, 2018 = 201807.

## Full data (same day)
Run on all 2.575bn street-address rows (`output/validation/dataaxle/move_tracking/outmig_full/`, bucketed by `bkey % 10`;
`outmig_full_by_tenure.csv`): all-unit and owner rates within 0.1 pp of the sample in every pair, renters within 0.45 pp (slightly
lower in the full data), tenure shares within 0.2 pp. 2017: 95.9m owner, 26.2m renter, 21.6m unknown units.

## Open
Nothing on this question.
