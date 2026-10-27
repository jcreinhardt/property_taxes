---
date: 2026-10-16
title: How well Data Axle records move-outs, against ACS, deeds and tax owner changes
question: Does Data Axle capture the household move-out rate (ACS ≈ 13%)? Why do only ~3–5% of families register a strict move? How do tax owner changes relate to sales?
status: current
code: property_taxes 0fcca23 (local main; analysis outputs uncommitted at time of writing)
tags: Data Axle, moves, ACS, deeds, tax roll
---

## Answer
At the aggregate the Data Axle move-out rate matches the ACS, but by two offsetting errors. On the 1% address sample, backfilled panel
2012→2021, a change of the primary `family_id` at an address (≈ modal-surname change, 98% agreement) happens for 13.4% of residential
addresses a year (ACS all households 12.5–13.4%), but 10.8% at owner-occupied single-family homes (ACS owners 4.3–6.6%) and ≈ 21% at the
rest (ACS renters 24–27%). Owners are churned about twice as fast as in the ACS; renters are, if anything, under-recorded; the sample is
tilted toward owners.

Following families instead of addresses: next year 81% keep their id at the same building, 4.7% keep it and move, 0.7% are re-keyed via
a person id, 12.8% vanish and are not found; only 22–45% of vanished primaries can be re-found by name+age (mostly pre-existing
duplicate records). Hence the strict family-level mover rate (3–4.5%) catches only 25–35% of ACS household moves.

Against arm's-length sales (209k at owner-occupied SFR): a new primary appears in the sale-year file for 34%, in the next file for 68%,
in at least one of the two for 90% (both 13%); net of the 7% base at never-sold homes, Data Axle registers a new household for ≈ 83% of
sales. At homes with no deed at all 2010–2023 the primary still changes 3.7% a year. The "lateness" is the file cut-off (see
2026-10-16-dataaxle-file-cutoffs); the tax roll itself shows the new owner one roll later for most sales (35% then 9%).

Tax owner surname changes (5.6%/yr at owner-occupied SFR vs 4.4% arm's-length sales): 34.6% have an arm's-length deed in t/t+1, 43.7% in
t−1/t+2 (same sales, roll timing), 9.6% a non-arm's-length deed (inter-family, trust, quitclaim), 1.0% owner-order swaps, 0.5% parse
differences, 0.5% flickers, 10.1% no deed within ±2 years. So ≈ 88% of owner changes have a deed behind them.

## How
- **Outline:** 1% building sample → residential addresses with structure (tax pic 10/11/21/22) → yearly grid 2012–2021, Data Axle
  primary family carried forward over missing years → proxies: primary id change, modal hashed surname change → compare with ACS
  (IPUMS `MIGRATE1`, householders, HHWT) and with deeds/tax owner names at the same buildings.
- **Choices:** backfill with the most recent previous record (observed-only pairs are 0.5–0.7 pp lower); owner-occupied SFR = pic 10, not
  corporate, `owner_occupancy_code = 'O'` or mailing = situs; arm's-length sale = category A, not inter-family, price > 0; names hashed in
  query, never exported; modal surname by `row_number()` over counts.
- **Inputs (cluster, not in git):** `~/ptax_explore/samples/{da_bld1pct,tax_bld1pct,deeds_bld1pct,da_bld1pct_fampos}.parquet`;
  `data/ACS_microdata/usa_00004.csv.gz` (IPUMS extract, laptop).
- **Scripts:** `output/validation/dataaxle/move_tracking/{moveout_proxies,owner_change_why,rekey_sample,name_relink,turnover_sample}.sql`;
  outputs `moveout_rates.csv`, `moveout_validation.csv`, `acs_hh_move_rates.csv`; doc `text/data_exploration/dataaxle.md` §4.5c.
- **Check:** all residential pooled 13.36% id change (7.24m pairs); owner-occupied SFR 10.75%; sale in t+1 → 34.7% change; tax owner
  surname change 5.62%/yr with 34.6% arm's-length deed in t/t+1.

## Open
Who are the new different-surname primaries at homes that never sell (listing swaps, tenants, transient records)? Not yet checked.
