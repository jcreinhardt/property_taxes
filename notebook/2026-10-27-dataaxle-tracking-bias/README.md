---
date: 2026-10-27
title: Which movers Data Axle tracks across a move — bias along record quality, income and tenure
question: When tracking households across moves (much worse than tracking move-outs), is the coverage biased in a particular direction?
status: current
code: property_taxes 0fcca23 (local main; outputs uncommitted at time of writing)
tags: Data Axle, moves, tracking, selection
---

## Answer
Yes, along record quality and household weight, in the direction that favours the SALT population. Of 2.93m primary families that
leave a sampled building (2006–2020, 1% building sample), 24% keep their `family_id` at a new address (18% with a tenure reset), 4%
are re-keyed under a new id but traceable by person id, 15% are back at the same building within two years (a data gap), 57% are lost.
Among sellers (owner-occupied single-family with an arm's-length deed), i.e. true moves: 53% tracked, 9% re-keyed, 35% lost.

Tracked shares by characteristic (all leavers; sellers in brackets): persons listed 1 → 4+: 19 → 52% (37 → 69%); income < 30k →
150k+: 19 → 37% (35 → 64%); owner / renter 26 / 21% (53 / 26%); age unknown / 30s 17 / 37% (34 / 62%); children 44 vs 22%; reported
married 59%. The lost are disproportionately single-person (83% vs 49% of all primaries), low-income (45% < 30k vs 27%), renters (36% vs
15%), short-tenure (35% LOR ≤ 1 vs 14%) and attribute-poor (57% unknown age vs 28%) — many are probably not real household moves.
Distance of tracked moves: 29% same ZIP, 35% same county, 20% same state, 16% another state (sellers 22%); the interstate share matches
the ACS share of household moves crossing a state line (≈ 17%), so tracking does not favour local moves. Tracking fell from 29% (2006)
to 21–25% (2009–2015) and 21% in 2020; sellers 58% → 43–47% → 50–57%.

## How
- **Outline:** primaries at sampled buildings in t → next-year position of the `family_id` anywhere (all records of those families)
  → not at the same building = leaver → outcome classes (tracked / re-keyed via any of 5 person ids / returns to the same building in
  t+2..t+3 / lost) → shares by bins of Data Axle attributes in t → distance class of tracked moves (ZIP, county from state × ALS county
  code, state) → seller subset from deeds at owner-occupied SFR.
- **Choices:** years 2006–2020 (2021→22 excluded: 2022 absent from the family-position sample); tenure from `owner_renter_status`
  (7–9 / 0–3); income = `find` (modelled); home value = `estmtd_home_val`; `wealth_finder_score` unusable (mis-scaled).
- **Inputs (cluster):** `~/ptax_explore/samples/{da_bld1pct,da_bld1pct_fampos,tax_bld1pct,deeds_bld1pct}.parquet`.
- **Scripts:** `output/validation/dataaxle/move_tracking/tracked_bias.sql`; outputs `tracked_bias.csv`, `tracked_bias_composition.csv`;
  doc `text/data_exploration/dataaxle.md` §4.5c ("Who gets tracked across a move?").
- **Check:** all leavers 2,925,764 with tracked 24.0% / lost 56.8%; sellers (owner) 240,840 with tracked 53.5%; persons = 1: tracked
  19.1%, lost 63.3%.

## Open
Not run on the full data (2.9m leavers in the sample give shares to ±0.1 pp). The destination of lost movers is unobservable; the
name+age relink (22–45% reappear under new ids) could be split by the same characteristics to see whether the lost are more often
long-distance movers.
