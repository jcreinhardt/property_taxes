---
name: dataaxle
description: How to use the Data Axle (Infogroup) US Consumer 5 File in corelogic.db for household/migration work — table, keys, what a family is, file timing, how to code a move, known biases, and the 1% samples. Use whenever a task touches dataaxle_annual, household panels, mover rates or Data Axle vs Census/CoreLogic comparisons.
---

# Data Axle consumer data (Bouchet only)

Full findings: `text/data_exploration/dataaxle.md` (sections 1–3 variables, 4.x structural findings, 4.5c move tracking).
Cluster/SQL rules: `bouchet` and `duckdb` skills. Names are **never** exported in clear text (hash them in-query).

## Where
- Table `cl.dataaxle_annual` in `/nfs/roberts/pi/pi_csl53/corelogic/database/corelogic.db` (read-only). Years 2006–2023
  (`record_year`), **2022 truncated to 29%** (corrupt file; do not use), 2.66bn rows, ~88 columns. Not on the SOM HPC copy.
- **Annual 2022 replacement** (built 2026-10-27 from the four 2022 quarterly files, which are the full compile with identical
  values): `~/ptax_explore/samples/da_annual_2022_from_quarters/state=XX/*.parquet` (≈ 174m families, cut-off 2022-07, quarterly
  typed schema with hashed names, `record_year` = 2022). Use it instead of `record_year = 2022` from the table.
- Codebook: `docs/codebooks/dataaxle/DataAxle import documentation.xlsx`.
- **1% samples** (`~/ptax_explore/samples/`, built by `output/validation/dataaxle/move_tracking/samples_1pct.sql` etc.):
  `da_fam1pct.parquet` (all records of 1% of `family_id`s — follow families across addresses),
  `da_bld1pct.parquet` (all records at 1% of buildings, 24.6m rows), `da_bld1pct_fampos.parquet` (every record, anywhere,
  of families/persons ever at those buildings), `tax_bld1pct.parquet`, `tax_bill_bld1pct.parquet`, `deeds_bld1pct.parquet`
  (same 1% of buildings in CoreLogic). Hashed names `ln_1..5`, `fn_1..5`. Start every analysis on these.

## Quarterly files — **no within-year information** (checked 2026-10-16)
- Every quarterly record is identical to its row in the same year's annual file (address, `recency_date`, LOR, status, primary
  flag; 100% in 2013/2019/2021), and nothing changes for a family between consecutive quarters. `max(recency_date)` is the annual
  cut-off in all four quarters. The quarters differ only in which records are included (June adds 1–5% vs March; Sep/Dec drop
  4–7% each). **Do not expect quarterly data to date moves or to fix the uneven annual cut-offs**; use the annual files and
  annualise by cut-off month. The converted copy below exists for reference only.
- Raw: 51 gzip CSVs `/nfs/roberts/pi/pi_csl53/corelogic/raw_data/dataaxle/quarterly/US_Consumer_5_File_YYYY_Month.csv.gz`
  (2012 March … 2024 December, June 2024 missing; same ~100 columns as the annual files). Read-only.
- Converted copy (ours): `/nfs/roberts/scratch/pi_csl53/jr2728/da_quarterly/fy=YYYY/fm=MM/state=XX/*.parquet`, zstd, names
  hashed (`fn_i`, `ln_i`), `bkey`, `file_year`, `file_month`, `file_date`; types as `dataaxle_annual` except booleans keep NULL
  when blank; MSA2000/CSA2000, old `ge_census_*` and middle names dropped. Read with
  `read_parquet('.../da_quarterly/fy=*/fm=*/*/*.parquet', hive_partitioning = true, union_by_name = true)`. **Scratch is purged
  after 30 days** (reminder set for 2026-11-13). All 51 quarters built 2026-10-16: ≈ 500 GB, 8–22 min each, zero rejected lines,
  zero conversion failures 2012–2023. Rows per file 124m–184m with strong seasonality (March/June files larger than Sep/Dec);
  `max(recency_date)` = the file's cut-off month.
- **Known file defects:** (1) **2015 December** is a broken delivery: 35 states appear twice (exact duplicate rows, removed in our
  copy) and **MA, OH, PA, OK, IL, GA, CO, WA, MD are missing** (< 1% of normal rows) — 100.5m unique rows vs 140m expected; treat
  those states as not observed in 2015Q4. (2) **2023 September** had 9k duplicate rows (removed). (3) **2024 files (March, Sep,
  Dec) keep the 2023 header but changed person content:** person 1 has gender in `title_code`, an age *decade* code (1–9) in
  `Gender_1`, blank age, new 2-digit numeric ethnicity code; persons 2–5 have no individual id and no full last name (first name,
  middle initial, last-name initial, gender, age decade, ethnicity code). Our 2024 parquet therefore has `age_decade_i`,
  `ln_initial_i` (2–5), `gender_i`, `fn_i`; `age_i`, `ln_i`, `individual_id_i` (2–5) and `title_code` are absent → read with
  `union_by_name = true`; person-level linking across 2023→2024 works only via `individual_id_1`/`family_id` and hashed first names.
- Builder: `code/build/dataaxle/make_quarter_sql.py YEAR Month [--from-raw] > convert_YYYY_MM.sql`, run with
  `sbatch --job-name=daq_YYYY_MM run_quarter.sbatch hh_strat/convert_YYYY_MM.sql` (16c/256G, `bigmem,day`): pass 1 streams
  the gzip once to a raw all-VARCHAR parquet (7 min), pass 2 writes a per-column conversion report and the typed parquet
  partitioned by state (7 min). Do not add a global ORDER BY (183m wide rows: > 1 h and 30 GB spill). All 50 remaining files:
  `sbatch run_quarter_array.sbatch` (array 1-50%8 over `quarters.txt`, deletes each raw parquet after the typed write).

## Keys and units
- `family_id` = household/family record, the panel key. `location_id` = `family_id` of the **primary** family at the
  address (`primary_family_ind`); other records at the address are **secondary** families (partners, roommates, adult
  children, stale duplicates). ~70% of records are primary. `individual_id_1..5` = persons (up to five per family).
- Building key shared with CoreLogic: `bkey = hash(lpad(zip,5,'0'), upper(trim(street_name)), upper(trim(house_num)))`
  for `address_type = 'S'`; tax uses `left(situs_zip_code,5)`, `situs_street_name`, `situs_house_number`; deeds `deed_situs_*`.
  93% of residential tax addresses match a Data Axle record in some year 2012–21; overall 87% of DA addresses / 85% of tax.
- Geography: use `ge_als_county_code_2010`, `ge_als_census_bg_2010`, `ge_census_state_2010`; the older `ge_census_*` fields
  die out after 2018/2020. `dpbc` empty before 2013.

## What the file is (and is not)
- A **marketing list** compiled from public records, utilities, telco and billing feeds; income, wealth, home value,
  owner/renter, marital status are **modelled**. Age is filled for 45–69% of adults.
- **Annual files are not 12 months apart.** Effective cut-off (`max(recency_date)` per `record_year`, confirmed by deed
  sales): 2012-03, 2013-03, 2014-03, 2015-04, **2016-06**, 2017-05, **2018-07**, 2019-06, 2020-06, 2021-06. Any
  year-to-year rate must be annualised by elapsed months; the raw 2015→16 and 2017→18 "spikes" (+2–3 pp in turnover)
  are this, not re-keying. For the SALT event study 2017→18 spans 14 months and 2018→19 only 11.
- Primary family ≠ household: pooled primary+secondary families match ACS household size/surname mix in 2012 but
  overshoot from 2017 (2.76 vs 2.02 adults). Primary alone has 15–17 pp too many one-adult households.
- Coverage is spotty: of residential addresses in the tax roll, 58% are seen in all ten years 2012–21; owner-occupied
  single-family 67% complete, absentee 38%. Backfill missing years with the last observed record when building panels.

## How to code a move
- **Family-level (strict):** same `family_id` in t and t+1, building/ZIP changes **and** `length_of_residence` resets
  (≤ 1; ≤ 2 for the 2021→23 pair). Catches only 25–35% of ACS household moves (3–4.5% vs 12.5%) because a moving family
  usually gets a **new `family_id`**: next year 81% same id/same building, 4.7% same id moved, 0.7% re-keyed via person id,
  12.8% id gone and not found. Only 22–45% of vanished primaries can be re-found by name+age, mostly pre-existing duplicates.
- **Address-level (move-out):** primary `family_id` at the address changes (≈ modal-surname change, 98% agreement).
  Pooled 2012–21: 13.4% of residential addresses a year (ACS all households 12.5–13.4%) — but owner-occupied SFR 10.8%
  vs ACS owners 5–7%, rentals ≈ 21% vs ACS renters 24–27%. Among arm's-length sales, a new primary appears in the sale-year
  or next file for 90% (net of base ≈ 83%); at homes that never sell the primary still changes 3.7%/yr.
- Date moves by destination `length_of_residence`/`recency_date` or the CoreLogic sale date, not the file year.
- Tenure (`length_of_residence`): ≤ 1-year bin under-counted by 4–8 pp vs ACS in every year, also for families with
  complete records; a 1992 heap migrates through the bins.

## Other known artefacts
- Mixed buildings: no-unit records next to unit records are often duplicates (§4.2b of the doc).
- Secondary families: 60% of new secondary ids are brand-new and half vanish next year; movers do **not** leave a stale
  same-surname secondary behind (0.3%).
- `household_status`, `cbsa_code`, `downgrade_*` empty before 2011; `ge_census_level_2010` code set changes in 2018;
  lat/lon re-geocoded for 8–9% in 2017→19 and 29% in 2020→21.
- Name+age uniqueness (2017): name alone 19%, name+age 53%, name+age+state 78% — not a reliable national person key.
