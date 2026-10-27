---
date: 2026-10-16
title: Data Axle households vs ACS — surname mix, adults per household, other generations, tenure
question: Are primary families good household identifiers, are secondary families the different-surname members of the primary household, and how does length of residence compare with the ACS?
status: current
code: property_taxes 0fcca23 (local main)
tags: Data Axle, ACS, households, tenure
---

## Answer
Pooling primary and secondary families at a single-family address reproduces the ACS surname mix in 2012 (households with > 1 surname:
23.4% vs ACS central benchmark 23.9%) but overshoots from 2017 (2019: 40.4% vs 25.1%; mean adults 2.76 vs 2.02). The primary family
alone has 15–17 pp too many one-adult and 20–26 pp too few two-adult households. Adult children sit mostly inside the primary family;
the older generation is split about half/half between primary and secondary records in 2012, and both are over-counted from 2017. Ages
are known for 45–69% of adults.

Tenure (`length_of_residence` vs ACS "moved in" distribution): the ≤ 1-year bin is 4–6 pp short in every year 2006–2023 (ratio
0.71 → 0.53), the 1–2 year excess disappears by 2015, a 1992 heap migrates through the bins, renters break in 2019. The deficit is the
same (−6 to −8 pp) for families observed in every year of their span, so it is not caused by spotty records.

The hypothesis that moving primary families leave a stale secondary family at the old address is not supported: 0.3% of movers leave a
same-surname secondary; 60% of new secondary ids are brand-new records, half gone the next year.

## How
- **Outline:** full `dataaxle_annual` (all years, 2022 excluded) → households = building × unit with one primary family → adults = persons
  1–5 of all families → distinct hashed surnames by household size → vs ACS (IPUMS, adults 18+, relationship-based benchmarks:
  lower = any adult non-relative, central = assumed per-relationship probabilities, upper = any adult beyond spouse/own child); ages vs
  householder for generations; LOR spread over months into ACS MV bins.
- **Choices:** single-family = no unit number, location type S; `P_DIFF` per IPUMS RELATED code in `hh_surnames_vs_acs.R`; ACS PUMS 1-yr
  `MV` 2007–2023 (`pums_mv.sh`); data-quality split by share of span observed and by count of missing years.
- **Inputs:** cluster jobs `hh_surnames.sql`, `hh_generations.sql`, `dq_tenure.sql` (full data, `day` partition) → CSVs in
  `output/validation/dataaxle/{hh_surnames,tenure_all_years_vs_acs,stale_secondary}/`; `data/ACS_microdata/usa_00004.csv.gz`.
- **Scripts:** `code/validation/dataaxle/{hh_surnames_vs_acs,hh_generations_vs_acs,tenure_all_years_vs_acs,tenure_by_data_quality}.R`;
  doc §4.2, §4.5.
- **Check:** 2012 single-family pooled > 1 surname 23.4% (ACS central 23.9%); 2019 40.4% vs 25.1%; ≤ 1-yr tenure ratio DA/ACS 0.71 (2006) → 0.53.

## Open
Why pooled households grow after 2017 (more secondary records per address) — vendor source change or stale records accumulating.
