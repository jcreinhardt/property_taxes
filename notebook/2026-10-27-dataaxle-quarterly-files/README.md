---
date: 2026-10-27
title: Data Axle quarterly files carry no within-year updates, but they replace the truncated annual 2022 file
question: Should we switch to the quarterly Data Axle files to date moves within the year? Did the quarterly pull fix the corrupted 2022 annual file?
status: current
code: property_taxes 0fcca23 (local main; code/build/dataaxle/ untracked at time of writing)
tags: Data Axle, quarterly, 2022, timing
---

## Answer
**No within-year information.** All 51 quarterly files (2012 March – 2024 December, June 2024 missing) were converted to typed parquet.
On the 1% family sample for 2013, 2019 and 2021, every quarterly record is in the same year's annual file with identical address key,
`recency_date`, length of residence, household status and primary flag (100.0% in all 12 quarter-years), nothing changes for a family
between consecutive quarters (0.0%), and `max(recency_date)` in every quarter equals the annual cut-off. The quarters differ only in
which records are included (June adds 1–5% vs March; September and December each drop 4–7%; 99% of December rows are identical to
their June row): vendor deliverability re-cuts of one annual compile. They cannot date moves or fix the uneven annual cut-offs.

**2022 is fixed.** The annual 2022 file is truncated to 29% of a normal year; the four 2022 quarterly files are the full compile:
99.9% of truncated-annual rows are in the quarters with identical values, the quarters never disagree, their union holds 1,739k sample
families (annual 2021: 1,836k; 2023: 1,614k), 80% of 2021 families are found in the 2022 quarters (25% in the truncated annual) and
80% of 2022-quarter families in annual 2023 (normal retention ≈ 81%), with uniform state coverage. Replacement built: union of the four
quarters, one row per `family_id` (earliest quarter), `record_year` = 2022, cut-off 2022-07: **173,634,431 families, 51 states**.

Defects found in the raw quarterly files: 2015 December has 35 states delivered twice (48.0m exact duplicates, removed) and MA, OH, PA,
OK, IL, GA, CO, WA, MD missing; 2023 September 9,159 duplicates (removed); the 2024 files keep the 2023 header but changed person
content (gender in `title_code`, age as a decade code, no individual ids or full last names for persons 2–5).

## How
- **Outline:** per quarterly gzip CSV: one streaming read (all VARCHAR, names hashed in-query, never stored) → raw parquet → per-column
  conversion-failure report → typed parquet partitioned by state (types as `dataaxle_annual`; booleans keep NULL when blank; MSA2000,
  old `ge_census_*`, middle names dropped) → raw deleted. Job array of 50 + one test file, 8–22 min each at 16 cores. Comparison with
  the annual files on `hash(family_id) % 100 = 0`.
- **Choices:** no global ORDER BY (183m wide rows: > 1 h and 30 GB spill; per-state files instead); 2024 mapping verified on 178k
  households common with 2023 December; 2022 replacement keeps the earliest quarter's row per family.
- **Inputs:** raw `/nfs/roberts/pi/pi_csl53/corelogic/raw_data/dataaxle/quarterly/US_Consumer_5_File_YYYY_Month.csv.gz` (read-only);
  outputs on cluster scratch `/nfs/roberts/scratch/pi_csl53/jr2728/da_quarterly/fy=YYYY/fm=MM/state=XX/*.parquet` (≈ 470 GB, purged
  after 30 days; reminder 2026-11-13) and the 2022 replacement copied to `~/ptax_explore/samples/da_annual_2022_from_quarters/`
  (12 GB, home). Annual 1% sample `~/ptax_explore/samples/da_fam1pct.parquet`.
- **Scripts:** `code/build/dataaxle/{make_quarter_sql.py,run_quarter.sbatch,run_quarter_array.sbatch,quarters.txt,
  annual_2022_from_quarters.sql}`; checks `output/validation/dataaxle/move_tracking/q_vs_annual{,_2013,_2021,_2022}.sql`; doc
  `text/data_exploration/dataaxle.md` §1 (import pipeline, item 5).
- **Check:** 2019 June = 183,492,290 rows, 0 rejected lines, 0 conversion failures; 2019 quarters vs annual: share_in_annual 1.0 and
  same_bkey 1.0 for all four; 2022 replacement 173,634,431 families, `max(recency_date)` 202207.

## Open
Delete the quarterly parquet on scratch (keep the 2022 replacement in home) or let the purge take it; decide before 2026-11-15.
