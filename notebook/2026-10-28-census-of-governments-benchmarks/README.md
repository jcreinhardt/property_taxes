---
date: 2026-10-28
title: Census of Governments: totals by level of government, benchmarks, survey-year coverage, and how jurisdictions nest
question: What revenue totals does our Census of Governments build give, do they match published figures, which levels of government collect the property tax, what share of each level's revenue is it, and are the jurisdictions nested?
status: current
code: property_taxes 9d83b5f
tags: data, Census of Governments, revenue, school districts
---

## Answer
- **Totals match the Census in census years.** Local property tax from our county-area table: 2012 $432.7bn, 2017
  $508.2bn, 2022 $626.8bn. Census Table 2 (2017) gives $508.1bn, and our raw-file sums match it to $0.1bn in every
  type-of-government cell. BEA local property tax receipts (calendar year): 2012 434.3, 2017 522.2, 2022 646.2. Census
  QTAX T01 state + local, four quarters to Q4: 473.9 / 567.3 / 691.3. Tax Foundation 72.1% property share of local tax
  collections FY2017 vs our 71.9%.
- **Survey years are samples, not totals.** The individual-unit file holds about 89,000 units in 2012, 2017 and 2022 and
  about 24,000 in other years, so sums are not national (2013 $399.9bn, 2019 $500.1bn, 2021 $546.7bn). School districts
  are near-complete every year (13.4–13.6k), county governments 2,000 to 2017 and 1,706 from 2019, municipalities about
  3,500 of 19,400, townships 750–1,050 of 16,000, special districts 4,400 of 37,000.
- **Who collects it, FY2017** ($bn, share of local total; property as a share of own taxes / general revenue / own-source
  general revenue): school districts 212.1 (42%; 96 / 37 / 83%), municipalities 121.3 (24%; 50 / 24 / 31%), counties
  119.6 (24%; 71 / 28 / 43%), townships 33.9 (7%; 92 / 62 / 76%), special districts 21.3 (4%; 62 / 11 / 17%); all local
  508.2 (72 / 29 / 46%). State governments 16.5 separately.
- **Database caveat.** `build/census_finances/02` treats all A-coded items as enterprise revenue, but current charges
  (hospitals, sewerage, airports) are A-coded too; `other_own_source` holds only U-coded miscellaneous revenue, so the
  implied property share of general revenue is 36% for counties instead of 28%. No design uses general revenue.
- **Nesting.** Every local government carries exactly one county code (no nulls in 2017); multi-county districts are
  attributed wholly to one county, so county areas sum to the total but describe geography approximately. Tax bases
  overlap: a parcel pays county, municipality or township, school district and special districts. Dependent school
  systems (1,542 of 13,459 F-33 districts in FY2017, 17% of pupils; all of CT, MD, MA, NC, VA, DC, AK, HI; TN 98%, VT
  96%, RI 93%; NYC) report zero property tax, which sits in the parent's T01; the county-level school-district panel is
  empty for those states.

## How
- **Outline:** raw `FinEstDAT` files (`data/census_gov_finances/raw/YYYY`) parsed in DuckDB: id = 12 characters (2017+
  and some earlier years) or 14 (record length 34), type = character 3, county = characters 4–6, item = next 3, amount
  = next 12; T01 = property tax, T* less debt codes = taxes, B/C/D = intergovernmental, U = miscellaneous, A = charges
  and utilities. DB tables `govt_finances_county_area`, `govt_finances_county`, `govt_finances_sd_by_county` summed by year.
- **Benchmarks:** Census Table 2 `17slsstab2 Revised.xlsx`; FRED W837RC1A027NBEA (BEA) and QTAXT014QEQTAXCAT1USYES (QTAX).
- **Scripts:** ad hoc (session 2026-10-28); the type-of-government query is in the report
  `output/reports/local_jurisdiction_results_2026-10-29.md` §6 in words, not kept as a script.
- **Check:** 2017 by type from the raw file: county 119.6, municipal 121.3, township 33.9, special 21.3, school 212.1.

## Open
- Fix the general-revenue columns (A-coded current charges) in `build/census_finances/02`.
- Add dependent school systems to the district revenue design via parent-government T01 or F-33 T02.
