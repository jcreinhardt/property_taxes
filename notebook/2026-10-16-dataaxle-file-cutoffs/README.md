---
date: 2026-10-16
title: Data Axle annual files are cut in different months; the 2015→16 and 2017→18 turnover spikes are timing artefacts
question: Why do Data Axle primary-ID change rates and strict mover rates spike in 2015→16 and 2017→18 when sales, tax owner changes and ACS move rates are flat?
status: current
code: property_taxes 0fcca23 (local main; analysis outputs uncommitted at time of writing)
tags: Data Axle, timing, mover rates, SALT event study
---

## Answer
The annual Data Axle files are not 12 months apart. The effective cut-off of each file (`max(recency_date)` per `record_year`, confirmed
independently by the month at which arm's-length deed sales stop appearing in the sale-year file) is: 2012-03, 2013-03, 2014-03,
2015-04, **2016-06**, 2017-05, **2018-07**, 2019-06, 2020-06, 2021-06, 2022-07, 2023-06, 2024-06. So the 2015→16 and 2017→18 pairs
cover about 14 months and 2016→17 / 2018→19 about 11. Everything move-like scales with the interval; annualised, both spikes vanish.

| Pair | Primary-ID change at residential addresses (%) | Months | Per 12 months |
|---|---|---|---|
| 2014→15 | 11.4 | 13 | 10.5 |
| 2015→16 | 13.4 | 14 | 11.5 |
| 2016→17 | 10.7 | 11 | 11.6 |
| 2017→18 | 13.4 | 14 | 11.5 |
| 2018→19 | 10.9 | 11 | 11.9 |
| 2020→21 | 13.0 | 12 | 13.0 (Covid, genuine) |

Family-level strict mover rates annualise the same way (4.80→4.1, 3.58→3.9, 4.69→4.0, 3.28→3.6). In-place re-keying (same person or
surname under a new family id) is a flat 0.5–0.7% of pairs; no state or address field stands out; geocode re-runs hit changers and
controls alike. For the SALT event study the 2017→18 pair spans May 2017–July 2018 and 2018→19 only 11 months, so an uncorrected
treatment-year jump of ~25% in any Data Axle turnover statistic is mechanical.

## How
- **Outline:** 1% building sample of Data Axle (all years) → for continuing primaries, distribution of LOR(t+1)−LOR(t) (a cut-off k months
  late moves k/12 of the clocks by two years) → implied intervals; deed sales by sale month vs presence of a new primary in the sale-year
  file → cut-off month; decomposition of excess ID changes by type; by-state excess; brand-new family ids per file.
- **Choices:** residential addresses with a structure (tax pic 10/11/21/22); one primary family per address, observed in both years;
  LOR 2–40 in t for the increment test; arm's-length sale = category A, not inter-family, price > 0.
- **Inputs (cluster only, not in git):** `~/ptax_explore/samples/da_bld1pct.parquet`, `da_fam1pct.parquet`, `deeds_bld1pct.parquet`,
  `tax_bld1pct.parquet` (built by `output/validation/dataaxle/move_tracking/samples_1pct.sql` etc. from the read-only `corelogic.db`).
- **Scripts:** `output/validation/dataaxle/move_tracking/spikes_2015_2017/{spike_timing,spike_lor_increments,spike_decomp,spike_newfids}.sql`,
  README there; written up in `text/data_exploration/dataaxle.md` §4.5c.
- **Check:** LOR(t+1)−LOR(t) = +2 for 28.8% of continuing primaries in 2015→16 and 18.0% in 2017→18 (≤ 0.7% in normal years);
  sales captured in the sale-year file 34% (2015), 52% (2016), 40% (2017), 57% (2018); `max(recency_date)` for record_year 2018 = 201807.

## Open
Any year-pair statistic (new-id shares, disappearances, LOR transitions, mover rates) should be annualised by elapsed months or
modelled per month of exposure; date individual moves by destination LOR / `recency_date` or the CoreLogic sale date.
