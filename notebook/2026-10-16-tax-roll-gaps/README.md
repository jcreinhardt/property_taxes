---
date: 2026-10-16
title: Gaps in the CoreLogic tax roll are county-year and town-year deliveries, not parcel exits
question: How consistently are parcels observed in the tax data, and are gaps whole counties/states missing? What explains the 44 odd county-years?
status: current
code: property_taxes 0fcca23 (local main; analysis outputs uncommitted at time of writing)
tags: CoreLogic, tax roll, coverage, New England
---

## Answer
Between its first and last tax year a residential parcel misses 0 years in 81.5% of cases, 1 in 15.5%, 2 in 2.2%, 3+ in 0.8% (Data Axle
at the same addresses: 61.0 / 13.9 / 8.0 / 5.3 / 3.6 / 8.3% for 0…5+). 85.5% of tax gaps sit in county-years where ≥ 90% of the county's
parcels are missing; of 740 such dropped county-years, 93.4% have no rows at all in `tax` (delivery gaps, concentrated 2013–15 and 2019:
e.g. Suffolk NY 2019, Franklin OH 2018, CT 2014, MD 2013/15, TX 2015). The 44 county-years that have a full roll but no key matches are
New England **town-level** deliveries: in the 2013/14 transition each MA/CT/ME/NH/VT town appears under only one tax year (MA towns are
either 2014-onwards, 262, or 2013-then-2015, 69; CT skips 2014; ME/NH/VT skip 2013). Of 1.37m parcels present the year before, 2.7%
key-match into the odd year but 88.4% into the year after; a normalised address key recovers nothing. Isolated parcel gaps are 0.26% of
address-years; 30% exist under another CLIP/address or are coded non-residential that year.

## How
- **Outline:** 1% building sample of `tax` (all years) → first/last year per parcel → missing years inside the span → share of gaps by
  county-year missing rate → full-table row counts for dropped county-years → for the 44 odd ones, extract all rows of those counties
  for y−1..y+1 and match by raw key, normalised key, CLIP; town × year presence matrix.
- **Choices:** residential = pic 10/11/21/22 with structure; window = first to last observation (removes splits/renames); dropped
  county-year = ≥ 20 bridging parcels and ≥ 90% missing; `fips_code` compared as string (casting defeats pushdown: 26 min vs 2 min).
- **Inputs (cluster):** `~/ptax_explore/samples/tax_bld1pct.parquet`; full `cl.tax` (read-only) for county-year counts and the odd
  county-year extracts (`odd_cy_all.parquet` etc. on the cluster).
- **Scripts:** `output/validation/dataaxle/move_tracking/{tax_window_inner,gap_evidence,gap_fulltable,gap_fulltable_b,gap_zip}.sql`,
  `odd_county_years/odd_cy_*.sql` + README; outputs `dropped_county_years_full.csv`, `iso_gaps_full.csv`, `odd_cy_recovery.csv`,
  `odd_cy_town_years.csv`; doc `text/data_exploration/dataaxle.md` §4.5c.
- **Check:** 740 dropped county-years, 93.4% with zero rows; odd county-years y−1→y raw-key match 2.7%, y−1→y+1 88.4%; Middlesex MA:
  0 of 98,090 2013 CLIPs in 2014, 97,991 in 2015.

## Open
Build a town-year coverage table (fips × situs_city × tax_year), flag town-years with far fewer rows than usual as not delivered, and
carry parcels present in y−1 and y+1 across the gap instead of counting exits and re-entries.
