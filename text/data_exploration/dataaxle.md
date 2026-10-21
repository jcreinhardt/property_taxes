# Data Axle consumer data — exploration notes

Running notes on the Data Axle (formerly Infogroup) household file in the CoreLogic DuckDB database on
Bouchet, with a view to measuring migration responses to the 2018 SALT cap. Started 2026-10-08; updated as
we go. Status markers: **[sub]** = result from a subsample (stated which); **[full]** = verified on the full
table; **[pending]** = full-data check running / not yet done.

Query files and outputs: `~/ptax_explore/NN_*.sql|.out` on Bouchet. Samples: `~/ptax_explore/samples/`.
Full-data results (background batch jobs): `output/dataaxle_exploration/full/` (laptop).

---

## 1. The database and how it was created

**Source.** Data Axle's *US Consumer 5 File*: a commercial marketing list of US households compiled from
"more than 100 sources", per library and vendor descriptions: real-estate and tax-assessor records, voter
registrations, utility connections, bill processors and telco feeds ([NYU](https://guides.nyu.edu/consumer_historical),
[Data Axle](https://data-axle.com/our-data/consumer-data/), [AWS listing](https://aws.amazon.com/marketplace/pp/prodview-yw2jp3nnpkntk)).
We found no documentation that online-purchase delivery addresses are a source. The listed sources
are public records, utilities, telco and billing data. Many attributes are modelled: income (`find`), wealth,
home value, owner/renter, marital status. Library descriptions call the historical files year-end
snapshots ([UW](https://dcollab.uw.edu/data/infogroup/)); we have not verified that for our files. "5 File" =
up to five persons listed per family record.

**Location.** `/nfs/roberts/pi/pi_csl53/corelogic/database/corelogic.db` (DuckDB, ~6.5 TB, 42 tables;
owner `alr228`, group-writable → we only ever open it read-only). Raw files:
`/nfs/roberts/pi/pi_csl53/corelogic/raw_data/dataaxle/{annual,quarterly}/US_Consumer_5_File_*.csv.gz`.
Codebook: `docs/codebooks/dataaxle/DataAxle import documentation.xlsx` (field definitions and import
decisions), `Data Axle Consumer Income.pdf` (FIND income model), `Ethnicity Decode.docx`.
Team summary stats: `docs/summary_stats/dataaxle.csv`.

**Other copy (SOM HPC).** `/gpfs/project/corelogic_database/corelogic_public.db` on hpc.som.yale.edu (1.6 TB,
17 tables: tax, mortgage, ownertransfer, llma_* (5), liens_* (5), permits/permit_history/permit_projects, raw_mls).
It contains **no Data Axle table**, so Data Axle work stays on Bouchet. Built with DuckDB v0.10.4 (bundled binary); docs in
`documentation/` (CL codebooks, missing-rate summary tables, `Data_Pulls_README.txt`). Checked 2026-10-14.

**Import pipeline** (`code/import_data/10a_import_annual_dataaxle.{sh,sql}`, `code/clean_data/11a_clean_annual_dataaxle.sql`):
1. Annual CSVs 2006–2023 read with `read_csv(..., all_varchar=TRUE, union_by_name=TRUE, ignore_errors=TRUE)`
   into `raw_annual_dataaxle`, later renamed `dataaxle_annual`. Columns renamed to snake_case.
2. **2022:** the script notes "the 2022 zipped file is corrupted, so I had to copy the unzipped file from the
   library data repository" and "7 corrupted lines in the 2022 data, which are ignored". The raw 2022 `.gz` is
   4.8 GB vs 14–17 GB for other years. **2023** was only available unzipped from the library.
3. `record_year` = `substr(filename, 44, 4)`.
4. Type conversions:
   - `*_DIV_1000` money fields multiplied by 1000.
   - `*_censored` flags added. **These flag both the bottom code and the top code** (see §3.4).
   - Zeros set to NULL for `downgrade_date`, lat/lon and tracts.
   - Booleans built as `CASE WHEN x='1' THEN true ELSE false`, which **turns NULL into false** for
     `primary_family_ind`, `children_ind`, `vacant` and `usps_nostats`.
   - `downgrade_reason_code` became `IS NOT NULL`.
5. Quarterly files (`10b`/`11b`): only **2012–2015** (16 files) were loaded into `raw_quarterly_dataaxle`.
   That table is still all-VARCHAR, 2.26 bn rows. The raw folder holds 51 quarterly files, 2012-03 to 2024-12,
   including **full-size 2022 quarterlies**. Raw sizes: quarterly 608 GiB gzip (11–15 GB per file), annual 207 GiB.
   **Our own conversion (2026-10-16, `code/data_prep/dataaxle_quarterly/`):** each file is streamed once (all VARCHAR,
   names hashed) to a raw parquet, checked column by column for values that fail to convert, then written typed and
   partitioned by state to `/nfs/roberts/scratch/pi_csl53/jr2728/da_quarterly/fy=YYYY/fm=MM/` (scratch, 30-day purge).
   Test on 2019 June: 183,492,290 rows (annual 2019: 185.5m), all `family_id`s unique, 0 rejected lines, **0 conversion
   failures in all 38 typed columns**, 96.2% street addresses, 66.7% primary, `max(recency_date)` = 2019-06, 12 GB typed
   parquet, 7 + 7 min at 16 cores. **All 51 files converted the same day** (job array, 8–22 min each, ≈ 500 GB): zero rejected
   lines everywhere, zero conversion failures 2012–2023. Rows per file 124–184m with a seasonal pattern (March/June files
   10–15% larger than September/December; primary share 0.67–0.84, highest in 2012). Defects found: **2015 December** is a
   broken delivery — 35 states delivered twice (48.0m exact-duplicate rows, removed) and MA, OH, PA, OK, IL, GA, CO, WA, MD
   missing (< 1% of normal), 100.5m unique rows vs 140m in September 2015; **2023 September** 9,159 duplicate rows (removed);
   **2024 files keep the 2023 header but the person fields changed content** (verified on 178k common `FamilyID`s with
   2023 December): person 1 gender sits in `title_code`, `Gender_1` holds an age-decade code (= age // 10 for 86%), `Age_1` is
   blank, ethnicity is a new 2-digit numeric code; persons 2–5 lose the individual id and the full last name (fields are first
   name, middle initial, last-name initial, gender, age decade, ethnicity). Our 2024 parquet stores these as `age_decade_i`,
   `ln_initial_i`, `gender_i`, `fn_i`; `age_i`/`ln_i`/`individual_id_i` (2–5) are absent (read with `union_by_name = true`).
   **Quarterly `max(recency_date)` is the same for all four files of a year** and equals the annual file's cut-off (2012: 2012-03;
   2013: 2013-03; 2014: 2014-03; 2015: 2015-04; 2016: 2016-06; 2017: 2017-05; 2018: 2018-07; 2019: 2019-06; 2020: 2020-06; 2021:
   2021-06; 2022: 2022-07; 2023: 2023-06; 2024: 2024-06), although row counts and `min(recency_date)` differ by quarter.
   **[full-year 1% family samples, 2013, 2019, 2021] The quarterly files carry no within-year updates: they are re-cuts of the
   annual compile** (`move_tracking/q_vs_annual*.sql`). Every record of every quarterly file is in the annual file of the same
   year with identical address key, `recency_date`, `length_of_residence`, `household_status` and `primary_family_ind` (100.0%
   in all 12 quarter-years tested); for families present in two consecutive quarters **nothing changes** (address, recency,
   LOR, status, primary flag all 0.0% changed); 99.9% of annual records appear in at least one quarter. The quarters differ
   only in which records are included: March → June adds 1–5% and drops 0.2–4%, June → September drops 3.5–7%, September →
   December drops 6% (2019: 175.2m → 183.5m → 178.9m → 169.3m); 99% of December rows are identical to their June row. So the
   quarterly files cannot date moves within the year or fix the uneven annual cut-offs; the only extra information is the
   in/out pattern of records (vendor deliverability filters), not updated attributes. The converted quarterly parquet on
   scratch (≈ 500 GB) is therefore of little use beyond this check.

**Size.** `dataaxle_annual`: 2,661,775,520 rows, 103 columns.

| Year | Rows (m) | | Year | Rows (m) | | Year | Rows (m) |
|---|---|---|---|---|---|---|---|
| 2006 | 130.3 | | 2012 | 141.3 | | 2018 | 174.3 |
| 2007 | 134.6 | | 2013 | 159.1 | | 2019 | 185.5 |
| 2008 | 129.7 | | 2014 | 153.5 | | 2020 | 185.8 |
| 2009 | 129.8 | | 2015 | 153.7 | | 2021 | 183.4 |
| 2010 | 128.4 | | 2016 | 157.4 | | **2022** | **53.6** |
| 2011 | 136.6 | | 2017 | 163.7 | | 2023 | 161.2 |

2022 is about 30% of a normal year **uniformly across states** (28–32% of each state's 2021 count). So it is
a non-geographic subset, consistent with the corrupted/truncated source file. **[full]** Do not use 2022 from
the annual table; the 2022 quarterly raw files are a possible replacement.

**Unit of observation.** One row = one *family* (see §4.1) at one address in one year. **[full, 2017]**
Every `family_id` appears exactly once per year: 163,654,256 rows = 163,654,256 distinct families nationally
in 2017.

---

## 2. Variable overview

Fill rates are from the national 1% block sample, all years **[sub]**; the full-data version is **[pending]**.
"→" marks a change between data vintages.

| Group | Variables | Type | Filled | Notes |
|---|---|---|---|---|
| IDs | `family_id` | BIGINT | 100% | household/family key; panel key |
| | `location_id` | UBIGINT | = `address_type` fill (95→99%) | = primary family's `family_id`; NULL iff no address |
| | `individual_id_1..5` | UBIGINT | person 1 ~95%, 2 ~43%, 3 ~16%, 4 ~6%, 5 ~2% | person key |
| Record status | `primary_family_ind` | BOOL | 100% (NULL→false) | ~70% TRUE |
| | `household_status` | VARCHAR | 0% before 2011 → 100% | F / S only |
| | `downgrade_reason_code`, `downgrade_date` | BOOL, YYYYMM | | `downgrade_reason_code`=TRUE ⇔ status S |
| | `recency_date` | YYYYMM | 100% | last confirmation at this address |
| Address | `address_type` | VARCHAR | 95→99% | S street, P PO box, NULL none |
| | `house_num`, `street_*`, `unit_*`, `box_*`, `route_*` | VARCHAR | by `address_type` | see §3.2 |
| | `city`, `state`, `zip`, `zip4` | | 100% | |
| | `dpbc` | VARCHAR | **0% before 2013** → 96–99% | USPS delivery point + check digit |
| | `vacant`, `usps_nostats`, `mailability_score` | | 100% | |
| Geography | `ge_latitude_2010`, `ge_longitude_2010` | DOUBLE | 100% | precision depends on `ge_census_level_2010` |
| | `ge_census_level_2010` | VARCHAR | 100% | geocode match level; code set changes 2018 |
| | `ge_als_county_code_2010`, `ge_als_census_bg_2010` | | 100% | **use these** |
| | `ge_als_census_tract_2010` | | 87–90% before 2015 → 100% | |
| | `ge_census_state_2010` | | 100% | state FIPS |
| | `ge_census_county` / `_tract` / `_bg` | | county → 0% from 2020; tract and bg → 0% from 2018 | older vintage, avoid |
| | `cbsa_code`, `csa_code`, `cbsa_type` | | 0% before 2011 → 100% | |
| | `msa2000_*`, `csa2000_code` | | 100% until 2010 → 0% | |
| Household attributes | `find`, `ppi`, `wealth_finder_score`, `estmtd_home_val` | UBIGINT ($) | 100% | modelled; see §3.4 |
| | `owner_renter_status`, `marital_status` | 0–9 score | 100% | modelled except 0 and 9 |
| | `head_hh_age_code`, `length_of_residence`, `children_*`, `tradeline_count` | | 100% | |
| Persons | `first/middle/last_name_i`, `suffix_code_i`, `gender_i`, `age_i`, `ethnicity_code_i`, `title_code` | | as `individual_id_i` | names never exported |
| Housekeeping | `filename`, `record_year`, `*_censored` | | 100% | |

---

## 3. Variable-by-variable details

### 3.1 Identifiers

- **`family_id`** — 12-digit "unique household" ID (codebook). One row per `family_id` per year **[full, 2017]**.
  About 494 m distinct IDs over all years (team summary stats). In practice a family is a **surname group at an
  address**, not a household (§4.1). Whether it persists across moves is **[pending]**, waiting for the
  household sample.
- **`location_id`** — codebook: "uniquely identifies (link together) a primary family and its associated
  subfamilies". CT 2017 **[sub]**:
  - every primary family has `location_id` = its own `family_id` (1,365,486 of 1,365,486);
  - no subfamily has `location_id` = its own `family_id`;
  - every `location_id` with a primary has **exactly one** primary family;
  - 5,331 `location_id`s (6,109 families) have no primary family on file;
  - `location_id` IS NULL exactly when the record has no address (43,779 rows).

  So within a year, families→`location_id` is **many-to-one**, and `location_id`↔primary family is
  **one-to-one**. `location_id` is a household-group ID, **not a dwelling ID**: it should change when the
  primary family moves out. Full-data check **[pending]**.
- **`individual_id_1..5`** — person IDs, up to 5 per family. Key for following people who leave a family
  record (e.g. children moving out). Not yet explored.

### 3.2 Address fields

**How the address fields go together** (CT 2017 **[sub]**, all NULL — no empty strings):

| `address_type` | Share CT 2017 | Filled | Empty |
|---|---|---|---|
| S street | 95.8% | house num, street name (suffix 99%), ZIP, ZIP+4, `dpbc`; unit in 16% | box, route |
| NULL | 2.2% | **only ZIP, ZIP+4, lat/lon** | street, house num, `dpbc`, `location_id` |
| P PO box | 2.0% | box num, ZIP+4, `dpbc` | street |
| F street name only | 0.07% | street (suffix 78%) | house num |
| D / R / G route, general delivery | 0.01% | route num | |

**Fill rates by year, national 1% sample [sub]:**

| Field | 2006–10 | 2011–12 | 2013–14 | 2015–17 | 2018–19 | 2020–23 |
|---|---|---|---|---|---|---|
| `address_type` | 95% | 95% | 96% | 96–98% | 98% | 98–99% |
| `house_num` | 89–90% | 89–90% | 91–92% | 92–94% | 94–95% | 95–96% |
| `street_name` | 90% | 90% | 91–92% | 92–94% | 94–95% | 95–96% |
| `unit_num` | 10–11% | 12% | 13% | 13–14% | 17% | 15–17% |
| `box_num` | 4–5% | 5% | 4–5% | 4% | 3% | 2–3% |
| `dpbc` | **0%** | **0%** | 96% | 96–98% | 98% | 98–99% |

- **Unit numbers** are only filled for multi-family (78%), retirement (79%), trailer (94%, lot numbers) and
  nursing homes (25%). They are **never** filled for single-family (`location_type`=S) or undefined (U). **[sub, CT 2017]**
  Unit types: APT, UNIT, #, FL, STE, TRLR, LOT, ...
- **`dpbc`** (Delivery Point Bar Code) is the USPS **delivery point**: a 2-digit code (00–99) identifying
  a single mailbox within a ZIP+4, plus a check digit. ZIP (5) + ZIP+4 (4) + delivery point (2) = the 11-digit
  code that uniquely identifies a USPS delivery point (a mailbox). It is assigned through
  CASS-certified address matching ([USPS DMM 204](https://pe.usps.com/Archive/NHTML/DMMArchive20171106/204.htm),
  [Wikipedia](https://en.wikipedia.org/wiki/Delivery_point)). Ours has 3 characters = delivery point + check digit.
  For apartment buildings the delivery point usually distinguishes units, but when it doesn't, the unit number
  is needed. It is only available **from 2013**.
- **`city`, `state`, `zip`, `zip4`**: always filled, including records with no street address.
- **Address-quality fields:** `mailability_score` (1 best, 92%), `vacant` (0.3% TRUE), `usps_nostats` (1.7% TRUE).

**Proposed definition of a "good" address:**
- `address_type='S'` with house number, street name and ZIP filled;
- unit number filled where `location_type` is M, R or T;
- optionally also `dpbc` filled, which is only possible from 2013.

How many moves go from a good to a non-good address (and back) is **[pending]**, waiting for the household
sample (§5).

### 3.3 Geography

- **Use `ge_als_*_2010` and `ge_census_state_2010`.** They are filled in all years and agree 100% with
  `ge_census_county` where both exist **[sub, CT 2017]**. The older `ge_census_county`, `_tract` and `_bg`
  stop being delivered in 2020 (county) and 2018 (tract, bg). That explains the 31% NULL for `ge_census_bg`
  in the overall summary.
- `ge_als_census_tract_2010` is 87–90% filled before 2015. The cleaning script set tract `000000`
  ("unknown") to NULL.
- **`ge_census_level_2010` (geocode match level).** It is not defined in our codebook. Our reading from the
  data **[sub]**:

  | Code | CT 2017 rows | Rows per distinct point | Street addresses per point | Notes | Likely meaning |
  |---|---|---|---|---|---|
  | P | 84% | 1.9 | 1.25 | 100% street addresses; 0.3% of coordinates have ≤4 decimals | address point / parcel |
  | 0 | 7.4% | 2.1 | 1.38 | 100% street addresses; more multi-family (27% vs 18%) | **also address-level, similar to P**; probably a different method or source (e.g. street-segment interpolation) — unknown |
  | 4 | 1.6% | 3.3 | 2.4 | 61% multi-family; nationally 40% PO boxes | ZIP+4 centroid? |
  | 2 | 0.2% | 11 | 7.8 | | ZIP+2 / sector centroid? |
  | Z | 6.6% | 379 | 97 | 33% no address, 44% PO boxes; 100% of coordinates ≤4 decimals; only 354 points in all of CT | **5-digit ZIP centroid** |
  | X | 2018 and 2020–23 only | 3.9 (national 2021) | | 40% no address, 35% PO boxes | replaces Z after 2018? |
  | B | negligible | | | | |

  Each street address gets exactly one coordinate (≤1.0007 points per address at every level).
  **Correction:** an earlier version of these notes called code 0 "coarser"; it is not.
- **Vintage break in 2018:** Z disappears and X appears; codes 0, 4 and 2 jump in 2018. If the geocoding
  method changed, households that did not move might get a new tract or county code between 2017 and 2018,
  creating spurious moves in the treatment year. Test **[pending]**: same street address in 2017 and 2018 →
  share with a changed county or tract code, by match level, against other pairs of consecutive years.
- Metro codes switch from `msa2000_*` (to 2010) to `cbsa_code`/`csa_code` (from 2011). Census codes for
  records with Z, X or 4 are assigned from a centroid and can be wrong where a ZIP crosses a county or tract
  boundary.

### 3.4 Household attributes (modelled)

- **`find`** (income, $5k–$500k) and **`ppi`** (income adjusted for local cost of living). They come from the
  FIND model: regression on MRI survey responses, then calibrated to Census distributions at county and block-group level.
  **The `*_censored` flags mean bottom- or top-coded.** Shares in the 1% sample **[sub]**:

  | | 2006–08 | 2009–13 | 2014–15 | 2016–23 |
  |---|---|---|---|---|
  | `find` = $5k (bottom) | 3.1–3.5% | 5.4–6.0% | 1.5–1.6% | 3.0–3.5% |
  | `find` = $500k (top) | 0.14–0.20% | 0.30–0.49% | 0.20–0.24% | 0.29–0.51% |

  The bottom-code share jumps between years, which points to model re-estimations. Top-coding is rare.
  Whether `find` separates the upper tail well enough for SALT exposure is open.
- **`estmtd_home_val`**: $5k–$9.999m, the same value for every family at an address. **= 0 for 18–26% of
  rows** (declining over time); not flagged and not NULL. Probably "no estimate" (renters?) — to be checked
  against `owner_renter_status` **[pending]**. Bottom code 0.15–0.25%; top code ≤0.01%.
- **`wealth_finder_score`**: $0–$9.999m; never top-coded in the sample.
- **`owner_renter_status`**: 9 = reported owner (45%), 7–8 likely owner (21%), 4–6 unknown (22%), 1–3 likely
  renter (11%), 0 = reported renter (1%).
- **`marital_status`**: only 0 (reported single) and 9 (reported married) are reported, about 5% together.
  The rest is inferred or modelled.
- **`head_hh_age_code`**: A–I = under 65 in 5-year bands; J = 65+ inferred (3%); K/L/M = 65–69/70–74/75+ reported (21%).
- **`length_of_residence`**: years since the "arrival date" at the address; codebook: months since arrival vs system date, rounded to years (0–17 months = 1, 18–29 = 2, 30–41 = 3, …; capped at current year − 1959). 100% filled (2006: 130.3m of 130.3m, min 1). Median 12 for primary families and
  6 for subfamilies **[sub, CT 2017]**. Probably the date Data Axle first saw the family there, not the actual
  move-in date. Arrival years heap at a fixed 1992 (and 1979) break in every file year: see §4.5. Test **[pending]**: does it reset to 1 when the address changes?
- **`children_hh_count`** (0–7) and **`children_ind`** are consistent: about 21% have children.

### 3.5 Persons (1–5 per family)

- Number of persons listed per **primary** family, 1% sample **[sub]**:

  | Persons | 2006 | 2012 | 2013 | 2017 | 2018 | 2023 |
  |---|---|---|---|---|---|---|
  | 1 | 53.1% | 54.2% | 52.1% | 48.9% | 51.4% | 51.1% |
  | 2 | 34.1% | 30.3% | 28.3% | 27.5% | 26.5% | 26.4% |
  | 3 | 9.4% | 10.6% | 11.9% | 12.8% | 12.1% | 12.1% |
  | 4 | 2.6% | 3.6% | 5.1% | 6.5% | 6.0% | 6.4% |
  | 5 (cap) | 0.8% | 1.3% | 2.6% | 4.3% | 4.0% | 4.0% |
  | Mean | 1.64 | 1.68 | 1.78 | 1.90 | 1.85 | 1.86 |

- **Persons 1–5 are adults only.** The minimum age is 18 and 0% are under 18 (1% sample, 2006/2013/2017/2018).
  Children are only counted in `children_hh_count`. `age_i` is NULL for 47% of persons in 2006, 18% in 2017
  and **100% in 2023**. So size comparisons have to add the children count.
- **Comparison with Census, 2017** [sub, 1% block sample]. Reference: CPS ASEC table HH-4, households by size
  (`www2.census.gov/.../households/hh4.xls`).

  | Size | Primary family: adults listed | Primary family: adults + children | All families at a `location_id`: adults + children* | **CPS households 2017** |
  |---|---|---|---|---|
  | 1 | 48.9% | 43.7% | 49.6% | **27.9%** |
  | 2 | 27.5% | 22.5% | 21.2% | **34.5%** |
  | 3 | 12.8% | 11.6% | 10.5% | **15.5%** |
  | 4 | 6.5% | 7.8% | 6.9% | **12.8%** |
  | 5+ | 4.3% (cap) | 14.4% | 11.8% | **9.3%** |
  | Mean | 1.90 | 2.39 | 2.20 | **2.54** |

  \*In a block sample, families at one `location_id` are often not all in the sample, which biases this
  column down; the full-data version is running.

  Readings:
  1. **Far too many one-person units** (44–50% vs 28%) and too few two-person households (21–23% vs 35%).
     That fits partners with different surnames being split into separate families, and single-person
     records that are really parts of larger households. Grouping at `location_id` does not fix it in the sample.
  2. **The 5+ group is too large** once children are added (14% vs 9%). `children_hh_count` may be
     overstated or modelled.
  3. The CPS mean (2.54) is close to the adults+children mean of primary families (2.39), but the shapes differ a lot.
  4. **Counts:** 163.7 m families in 2017 vs 126.2 m CPS households. Primary families (~70%) ≈ 114 m,
     somewhat below CPS. CT 2017 primary families matched the Census closely (§4.1). National count by
     state **[pending]**.
  - There are structural breaks in the number of persons listed: 2013 (more persons) and 2018 (one-person share +2.5 pp).
**[full] Family size, all years** (`full2/B_B.csv`): matches the subsample (2017 primary families:
49.7/27.2/12.5/6.3/4.2%, mean 1.88; adults + children mean 2.37).
- **Coding breaks:** 2012→13 (more persons listed, 5-person cap 1.4%→2.6%); **2015→16 children count**
  (6+ in adults+children jumps 2.6%→7.0%, mean 2.06→2.33); 2017→18 stable.
- Summing over a `location_id` gives mean 3.0 from 2016 on, with 7–10% at 7+: too high, so likely
  double counting (children counted per family).
- Counts 2017: 117.1 m primary families and 114.1 m distinct `location_id`s vs ≈120 m ACS households;
  about 3 m families per year have no `location_id`. By state: `B_B_state2017.csv`.
- `gender_i`: M, F, U. `title_code`: 9 (53%), 1 (33%), 0 (6%) — undocumented. `suffix_code_i`: J (Jr),
  S (Sr), 2–5 (II…V).

---

## 4. Structural findings relevant for migration

### 4.1 What a "family" is
- CT 2017 **[sub]**: 2.03 m families, of which **1.41 m are primary** — close to the Census count of CT
  households (≈1.37–1.38 m). **Primary families ≈ households.**
- Subfamilies (30% of rows) **almost never share the primary family's surname: 0.1%** (hashed last names).
  Subfamilies also have shorter length of residence (median 6 vs 12 years).
- Interpretation: Data Axle groups persons **by surname at an address**. People with the same surname are
  persons 1–5 of one family record; anyone with a different surname becomes a subfamily.
  - Consequence 1: couples or partners with different surnames are probably split into two families, so a
    joint move counts twice. Test **[pending]**: gender and age of subfamily person 1 vs primary person 1.
  - Consequence 2: a same-surname adult child moving out is not a family move; it is a person leaving
    the persons 1–5 slots, visible only through `individual_id`.

### 4.2 Several families at one address (same year)
- Common. CT 2017, all address types, key = ZIP+ZIP+4+`dpbc`+unit **[sub]**: 38% of single-family delivery
  points have 2 or more families. Extreme cases (up to 1,254 families) come from records without an address
  collapsing into one key. **Needs redoing on `address_type='S'` only**; full data **[pending]**.
- Possible contributor: 2–3-family houses coded as single-family with no unit numbers.

### 4.2a Surnames within families, unit numbers, families per building
**[sub]** CT 2017 with hashed surnames of persons 1–5 (`ct2017_names.parquet`); family-level unit flips from
the ID-range panel. Full data **[pending]** (background agent, `output/dataaxle_exploration/full3/`).

**Surnames within a family record:** 98% of 2-person and 97% of 5-person families have one surname. Only
2–2.6% have two, and ≤0.6% three or more. So **a family record is almost always one surname.** Together with
subfamilies almost never sharing the primary surname (0.1–0.15%), a different surname at the same address
practically always creates a separate family record.

**Unit-number patterns by building** (building = ZIP + house number + street, `address_type='S'`; each
building–year classified by whether its families carry a unit number):

| Pattern | Buildings | Families | Share of buildings | Share of families | Mean families | Mean distinct units |
|---|---|---|---|---|---|---|
| never unit | 1,015,623 | 1,565,597 | 95.2% | 80.7% | 1.54 | 0 |
| **mixed** (some with, some without) | 29,966 | 253,998 | 2.8% | **13.1%** | 8.5 | 4.8 |
| always unit | 20,927 | 120,927 | 2.0% | 6.2% | 5.8 | 4.2 |

- Mixed buildings are almost all multi-family (29.6 k of 30.0 k). In mixed buildings with 2–3 or 4–10
  families, **about half the families are listed without a unit**; in buildings with 11+ families, 7%.
- So within a year, **13% of all families live at a building where unit numbers are reported only
  sometimes.** Unit-level address keys are unreliable there; use building-level keys for those buildings.
- **Within a family over time, the unit rarely flips** (ID-range panel, same family at the same building in
  t and t+1, 6.64 m pairs): unit kept 571,582; never a unit 6,052,808; unit dropped 3,630 (0.6% of pairs
  with a unit); unit added 13,087 (0.2% of all); unit number changed 6,189. Of family–building
  combinations ever seen with a unit, 4.6% are also seen there without one at some point.
  **So the with/without-unit mix is mostly *across* families in a building, not the same family switching.**
  Moves based on building + unit will rarely be spurious because of unit flips.

**Families per building, mixed buildings dropped (never-unit buildings only):**

| Modal `location_type` | Buildings | 1 family | 2 | 3–5 | 6+ | 2+ primary households | Mean families |
|---|---|---|---|---|---|---|---|
| S single-family | 1,007,122 | 62.3% | 27.6% | 9.8% | 0.3% | **0.0%** | 1.52 |
| M multi-family | 8,114 | 11.9% | 51.8% | 32.3% | 3.9% | 88.1% | 2.82 |
| N nursing home | 245 | 3.7% | 2.4% | 8.2% | 85.7% | 96.3% | 28.6 |
| R retirement | 140 | 36.4% | 16.4% | 17.1% | 30.0% | 63.6% | 9.07 |

- **38% of single-family buildings without units have 2+ family records, but never 2+ primary
  households:** extra families at a single-family address are always subfamilies of the one primary
  household (one `location_id`). Multi-family buildings without units (8 k) usually hold several primary
  households, i.e. separate apartments with missing unit numbers.

**Who are the subfamilies? Partner test** (never-unit single-family buildings with exactly 2 families,
277,073 pairs; primary person 1 vs subfamily person 1):

| | Subfamily vs primary | Benchmark: person 1 vs 2 within 2-person primary families |
|---|---|---|
| Opposite sex | 55.6% | 70.7% |
| Same sex | 34.7% | |
| Age within 10 years | 53.0% (24% age missing) | 68.8% |
| Opposite sex and age within 10 years | 29.5% (≈39% of pairs with both ages) | |
| Subfamily ≥18 years younger | 24.0% | |
| Subfamily ≥18 years older | 11.7% | |
| Both single-person families | 33.4% | |

Reading: subfamilies are a **mix**. Roughly a third to two fifths look like **partners with different
surnames** (opposite sex, similar age). Roughly a third look like **another generation** (adult children,
parents, in-laws with a different surname). The rest are roommates, siblings or relatives of the same sex.
For a household-level move measure, the natural unit is the **primary household (`location_id`) at a
single-family address**, with subfamilies attached. A subfamily leaving is a split, not a household move.

**[full] All years** (`output/dataaxle_exploration/full3/`), confirming the CT results nationally:
- One surname per family: np=2 98.5–98.9%, np=5 96.8–97.8% in every year.
- Buildings: never unit 96.5–97.3% of buildings (80–83% of families), mixed 1.3–1.9% (11–15% of
  families), always unit 0.8–2.2%. In mixed buildings with 2–3 families about **50% are listed without a
  unit in every year**; in 11+-family buildings this falls from 30% (2006) to 7% (2023): units get filled in
  over time in large buildings, not in small ones.
- Never-unit single-family buildings: 2+ `location_id` = 0.00% in every year (except 0.22% in 2014), so
  for S buildings **`location_id` effectively is the building**. One-family share falls from 84.5% (2006)
  to 65.8% (2017) and 60.3% (2019): more and more subfamilies get attached.
- Partner test, national 2017: opposite sex 53.2%, age within 10y 48.5%, both 23.7% (CT 29.5%);
  subfamily ≥18y younger 26.5%. **Only about 1 in 4 pairs looks like a couple.**
- Unit flips, full panel (1.72 bn same-building pairs): dropped 0.77% of pairs with a unit, added 0.45% of
  pairs without; 7.5% of family–building combinations ever with a unit are also seen without one.
  **Flips spike in vintage years (08→09, 12→13, 15→16, 17→18).**
- Data breaks: **2012→13** (families 127→145 m, more persons and subfamilies, fewer missing ages),
  **2017→18** (families 154→165 m, more always-unit buildings, more unit flips), 2016 (unit coverage).
  2022 building-level numbers are unusable (partial file); 2023 has no ages.

### 4.2b Are no-unit records in mixed buildings duplicates?
Hypothesis: in a building with units, mail addressed without the unit makes Data Axle create an extra
"family" at the bare street address, possibly with many subfamilies. **[sub, CT 2017]** For buildings
with both unit and no-unit records (mixed):

| Records | Families | `location_id`s | Families per `location_id` | Share primary |
|---|---|---|---|---|
| without unit | 62,826 | 62,615 | 1.00 | 99.4% |
| with unit | 191,172 | 144,908 | 1.32 | 75.5% |

- **No-unit records are not one family with many subfamilies**: each is its own primary household.
- **No person in a no-unit record also appears in a unit record of the same building in the same year
  (0.0%, matched on hashed first+last name).** Data Axle seems to keep each person once per year.
- 11.0% of those persons appear in **2018** at the same building **with** a unit: the same household,
  with the address completed later.
- **Surname-only match, multi-family buildings (M) only** [sub, CT 2017]: 29,625 mixed M buildings with
  61,059 no-unit and 180,394 with-unit families. Does any surname of a family (persons 1–5) match a surname
  of a with-unit household in the same building? Baseline = with-unit families matched against households
  in *other* units of the same building:

  | Families per building | No-unit family matches | Baseline (with-unit vs other units) | No-unit families | With-unit families |
  |---|---|---|---|---|
  | 2–3 | 0.44% | 0.16% | 16,775 | 15,319 |
  | 4–10 | 1.33% | 1.06% | 34,907 | 38,039 |
  | 11–30 | 7.36% | 2.76% | 4,320 | 30,612 |
  | 31+ | 10.46% | 6.22% | 5,057 | 96,424 |
  | all | 2.27% | 4.03% | 61,059 | 180,394 |

  The pooled numbers mislead, because no-unit families sit mostly in small buildings. **Within size classes,
  no-unit families match slightly more often than the baseline** (+0.3 pp in 2–10-family buildings, +4–5 pp
  in large buildings). That is consistent with a **small** share of no-unit records being relatives of, or
  partial duplicates of, a unit household. But **≥97% of no-unit families in small and medium buildings share
  no surname with any unit household.** Full data **[pending]** (`full4/`, test 3b).

- **Are bare-address households living in otherwise unrecorded flats?** [sub, CT 2017→2018, mixed M buildings]
  - 6,215 no-unit households get a unit in 2018 at the same building. **46% receive a unit not observed
    in 2017**, against 22% for an average 2018 unit (78.4% of 2018 units were already observed in 2017).
    No-unit households disproportionately "fill" flats missing from the records, as expected if they live
    in one of the building's flats with the unit missing. The other 54% get a unit that another family
    had in 2017 (turnover, or a wrong assignment).
  - Counts: 195,914 primary households, of which 135,193 have a unit and 60,721 do not. Distinct units
    seen: 135,645 in 2017 (≈1 primary household per unit), 167,271 in 2017–18. Only in 23.6% of buildings
    are primary households ≤ units seen. Two years of data undercount the true number of units, so we
    cannot test whether every bare-address household fits into a real flat. The CoreLogic tax records
    (units per parcel) could give the true count.
  - **Interpretation:** a bare "23 Pearl St" record in a building with units is most likely **a real
    household living in one of the flats, with its unit missing**. It is **not** a separate dwelling and
    **not** a duplicate of a listed unit household (the same persons are never listed twice). For
    addresses, treat it as "somewhere in building 23 Pearl St".

- So in CT 2017 the no-unit records look like **real, distinct households whose unit number is missing**,
  not duplicates. Dropping them would remove about a quarter of the families in mixed buildings. Better:
  use the building (ZIP+street+number) as the address for mixed buildings, and carry a unit forward or
  backward from other years where it exists. Full data **[pending]** (`output/dataaxle_exploration/full4/`).

**[full] All years** (`full4/`): no-unit records in mixed buildings are always ≈1.00 family per
`location_id` and 98–99.5% primary. The same person in a unit record of the same building and year:
≈0% in every year (0.5% in 2008). The same person with a unit at the same building in t+1: 5–8% normally,
**spikes in 2008→09 (20%), 2012→13 (14%), 2015→16 (16%), 2017→18 (14%)** — bulk unit completion in
vintage years. Surname match in M buildings, 2017 national: no-unit vs baseline 1.13 vs 0.20 (2–3
families), 2.63 vs 1.12 (4–10), 6.54 vs 2.58 (11–30), 10.97 vs 8.71 (31+). Small in absolute terms, but from 2009 on
no-unit families match **3–5× as often** as with-unit families in buildings with 2–30 families (2006–08: at
or below the baseline). Pooled shares fall below the baseline only because no-unit families sit mostly in
small buildings. So **a small minority** of no-unit records are relatives of, or overlap with, a unit household
(2017: 135 k of 2.64 m no-unit families match). Unit records change in 2018 (families per `location_id` 1.3→1.45, primary
share 76%→68%).

### 4.3 Moves between good and bad addresses
**[sub]** Panel sample of 200 narrow `family_id` ranges = 0.5% of the ID space (see appendix), 7.59 m
consecutive-year pairs from 2006–2021, pairs involving 2022 excluded.
- **Good address:** `address_type='S'`, house number, street name and ZIP filled, and a unit number if
  `location_type` is M, R or T.
- **Move:** any address field changes (type, ZIP, ZIP+4, house, street parts, unit, box, route; `dpbc`
  left out because it does not exist before 2013).

Movers: **5.3% of pairs per year**; interstate 0.75%. Both look low against CPS/ACS (~10% movers, ~2%
interstate per year). The ID-range sample over-represents older, long-tenure ID cohorts; full data **[pending]**.

**Transition matrix, movers only** (pooled):

| Origin → | good | bad | Total |
|---|---|---|---|
| **good** | 318,023 (92.1%) | **27,293 (7.9%)** | 345,316 |
| **bad** | **41,530 (73.2%)** | 15,191 (26.8%) | 56,721 |

All pairs, including stayers: good→good 99.3%, good→bad 0.7%, bad→good 8.8%, bad→bad 91.2%.
With the strict definition (also `dpbc` filled; 2013+ movers): good→bad 6.3%, bad→good 82.3%.

**Are these real moves?**

| Cell | Movers | Same street address | Same ZIP | Same county | Length of residence reset (≤1) |
|---|---|---|---|---|---|
| good→good | 318 k | 10.3% | 35.8% | 63.3% | 67.5% |
| good→bad | 27 k | 10.7% | 39.4% | 47.8% | 70.5% |
| bad→good | 42 k | **27.8%** | **63.4%** | 55.9% | **30.9%** |
| bad→bad | 15 k | 13.3% | 63.7% | 77.1% | 31.7% |

- **good→bad** looks like real moves: length of residence resets 70% of the time. Where the destination is
  bad, it is a **PO box** (20 k of 42 k bad destinations), a **multi-family unit without unit number** (15 k),
  a street name with no house number (4 k), or a rural route (3.4 k).
- **bad→good** is mostly **address completion, not a move**: only 31% reset length of residence, 28% keep the
  same street address and 63% the same ZIP. Typical case: a unit number gets added, or a PO box is replaced
  by a street address.
- Even good→good "moves" reset length of residence only 67.5% of the time, and 10% keep the same street
  address. These are unit changes within a building, or cosmetic changes (suffix/direction standardisation).
  A move definition should require a changed street address or ZIP, not just any field changing.
- Movers by year are uneven: 8.9% (2006→07), 3.1–3.5% (2010–14), 6.6–6.9% (2014–16), 5.3% (2017→18),
  6.1% (2020→21). Partly an artefact of the sample (new ID blocks enter in 2014 and 2020).

**[full] Results from the full table** (all families, all consecutive year pairs; files in
`output/dataaxle_exploration/full2/`; chunked by `family_id` quantiles because IDs come in about 9 dense
blocks starting at 0, 100e9, …, 900e9):
- **Movers: 6.5% of pairs per year; interstate 0.84% of pairs (13% of movers).** Still well below
  CPS/ACS. Mover rate by pair:

  | 06-07 | 07-08 | 08-09 | 09-10 | 10-11 | 11-12 | 12-13 | 13-14 | 14-15 | 15-16 | 16-17 | **17-18** | 18-19 | 19-20 | 20-21 | 21→23 |
  |---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
  | 9.95 | 9.72 | 9.30 | 7.13 | 5.64 | 5.80 | 6.23 | 6.05 | 6.41 | 7.30 | 4.94 | **6.80** | 4.92 | 4.96 | 5.31 | 8.86 (2 yrs) |

- Pooled 2×2 among movers: gg 78.7%, gb 6.7%, bg 11.5%, bb 3.1% (subsample: 79.1/6.8/10.3/3.8).
  Length of residence ≤1: gg 72.0%, gb 71.1%, bg 36.7%, bb 41.2%; stayers 2.8%.
- **Address-"repair" spikes:** bad→good moves jump in 08-09 (22.1% of movers), 12-13 (14.7%), 15-16
  (12.5%) and **17-18 (8.7%)**. In those years bg movers keep the same building 50–58% of the time and
  reset length of residence only 26–33% of the time. These are re-standardisation events, not moves.
  **They inflate the 2017→18 mover rate (6.8% vs ~4.9% in neighbouring years) exactly in the SALT
  treatment year**, so real moves must be separated from address repairs (same building, no length of
  residence reset).
- `good_strict` = `good` from 2013 on: every good address has a `dpbc`.
- Attrition: 15–22% of families are absent the next year and 17–23% of next year's families are new;
  2012→13 has 25.4% new (2013 grows by 18 m families). 2021→2023: 30.8% absent.


### 4.4b Does `family_id` survive a move? (re-identification)
**[sub, CT 2017→2018; only moves within CT are visible]**
- **`individual_id` is a stand-alone person ID**: unique per person within a year (3,528,510 persons =
  3,528,510 distinct IDs in CT 2017). It is not nested in `family_id`.
- **Persons found in both years by `individual_id`:**

  | | Persons | Same `family_id` | New `family_id` |
  |---|---|---|---|
  | same building | 2.89 m | 99.74% | 0.26% |
  | building changed | 135,695 | **77.3%** | **22.7%** |

- **Family level**, families with at least one member changing building:
  - in 79.1 k at least one person keeps the old `family_id` (65.2 k fully, 13.9 k with some members
    splitting off);
  - in 10.4 k nobody keeps it.
  - For old→new pairs with a changed ID at a move (26,690): **identical composition 11.9%** (pure
    re-ID), joined a larger existing family 13.0%, **partial split 75.1%**.
  - **Pure whole-family re-IDs at a move ≈ 3.2 k, or about 4% of moving families.** Most ID changes at a
    move are splits: someone leaves the family record, e.g. children moving out or separation.
- **Families that disappear** (`family_id` in 2017, not in 2018 in CT): 319,104 of 2.03 m (15.7%).
  - Person 1 found in another CT family in 2018 by `individual_id`: 4.2%.
  - By hashed name with age in [age, age+2] but a **different `individual_id`**: 66,568 families (20.9%).
    Placebo windows (age+10..12, age−12..−10) give about 16.5 k, so roughly 50 k (about 15%) are excess
    matches.
  - But only 16.6% of matched 2018 records have a `family_id` new in 2018. Some people may also get a
    **new person ID**, but this match is noisy (only 75% of name hashes are unique within CT). A stricter
    test (nationally unique names) is running.
**[full] All year pairs, national** (`output/dataaxle_exploration/full4/`; persons matched nationally by
`individual_id`, so interstate moves are included):
- `individual_id` is unique within every year (205–316 m persons).
- Persons keeping their `family_id`: same building 99.5–99.8%; **building changed within the state 84.4%
  (2006→07) drifting to 79.6% (2020→21); 2017→18 80.8%**; state changed 76–79%.
- **Pure re-IDs at a move** (same people, new `family_id`) = **3.4–4.4 per 100 moving families**
  (2017→18: 3.66, i.e. 327 k families). Of `family_id` changes at a move, 11–16% are pure re-IDs, 14–18%
  joining a larger family, 65–75% partial splits.
- Families disappearing: 13.6–21.6% per pair (2017→18: 16.9%). Strict name+age match (name hash unique
  nationally in both years): placebo noise only ~1% of real matches. **25–41% of eligible disappearing
  person-1s reappear under a different `individual_id`, and 67–80% of those under a brand-new
  `family_id`.** So **person IDs are also sometimes reassigned.** Following people only by ID misses
  these. A name+age linkage recovers them (not possible for 2023, which has no ages).
- Provisional takeaway (CT): **`family_id` mostly survives moves (~77% of moving persons; whole-family re-IDs
  ≈ 4% of moving families).** Using `individual_id` alongside `family_id` would recover splits and most
  re-IDs. Interstate moves and full-data figures **[pending]** (`full4/`).

### 4.5 Move rates and tenure vs Census
**Move rates by year, strict definition** — Data Axle from the full table (`moves_strict.sql`, 5 min; `output/analysis/dataaxle/moves_strict/`).
Families present in both years (by `family_id`). **Move (strict) = building (ZIP5 + street + house number) or ZIP changes *and*
`length_of_residence` resets in t+1** (≤ 1; ≤ 2 for the two-year 2021→23 pair). Interstate (strict) = state changes and LOR resets.
The loose building/ZIP rate (no LOR condition) is kept for reference. CPS = ASEC Table A-1 (persons aged 1+, period March–March);
ACS = householders in our IPUMS extract (`data/ACS_microdata`, `MIGRATE1`, weighted by `HHWT`; survey year t+1 = moves in the
previous 12 months).

| Pair t→t+1 | DA strict: all families | DA strict: primary families | DA strict interstate: all | DA strict interstate: primary | DA loose building/ZIP change | Same building, LOR reset | CPS persons moved in US | CPS different state | ACS households moved in US (t+1) | ACS interstate (t+1) |
|---|---|---|---|---|---|---|---|---|---|---|
| 2006→07 | 6.00 | 5.61 | 0.95 | 0.90 | 8.02 | 5.11 | 12.8 | 1.7 | | |
| 2007→08 | 5.85 | 5.36 | 0.91 | 0.85 | 7.51 | 3.67 | 11.5 | 1.6 | | |
| 2008→09 | 4.91 | 4.50 | 0.71 | 0.66 | 7.21 | 2.50 | 12.1 | 1.6 | | |
| 2009→10 | 4.77 | 4.39 | 0.64 | 0.59 | 6.34 | 1.86 | 12.1 | 1.4 | | |
| 2010→11 | 3.71 | 3.46 | 0.54 | 0.51 | 4.89 | 3.28 | 11.2 | 1.6 | 13.01 | 1.99 |
| 2011→12 | 3.81 | 3.54 | 0.57 | 0.54 | 5.05 | 2.68 | 11.6 | 1.7 | 12.92 | 2.00 |
| 2012→13 | 3.78 | 3.54 | 0.60 | 0.57 | 5.14 | 2.92 | 11.4 | 1.6 | 12.90 | 2.09 |
| 2013→14 | 4.34 | 3.87 | 0.67 | 0.62 | 5.47 | 3.23 | 11.2 | 1.5 | 12.76 | 2.09 |
| 2014→15 | 4.46 | 4.00 | 0.74 | 0.68 | 5.80 | 1.90 | 11.1 | 1.6 | 12.75 | 2.12 |
| 2015→16 | 4.80 | 4.38 | 0.83 | 0.78 | 6.44 | 1.25 | 10.7 | 1.5 | 12.61 | 2.13 |
| 2016→17 | 3.58 | 3.33 | 0.57 | 0.54 | 4.47 | 3.29 | 10.6 | 1.7 | 12.49 | 2.11 |
| **2017→18** | **4.69** | **4.43** | **0.84** | **0.83** | 6.10 | 1.73 | 9.8 | 1.5 | 12.42 | 2.17 |
| 2018→19 | 3.28 | 3.06 | 0.56 | 0.54 | 4.10 | 3.40 | 9.4 | 1.5 | 12.13 | 2.12 |
| 2019→20 | 3.28 | 3.03 | 0.58 | 0.56 | 4.37 | 2.14 | 9.0 | 1.3 | 11.01 | 2.02 |
| 2020→21 | 3.78 | 3.47 | 0.72 | 0.69 | 4.88 | 2.32 | | | 11.84 | 2.28 |
| 2021→23 (2 yrs) | 6.19 | 5.65 | 1.43 | 1.37 | 8.34 | 1.88 | | | | |

- The strict rule removes a fifth to a quarter of loose building/ZIP changes (8.0 → 6.0% in 2006→07, 6.1 → 4.7% in 2017→18):
  address changes without an LOR reset (repairs, re-standardisation). Under it, Data Axle catches **about 25–35% of ACS
  household moves** (primary families 3.0–4.4% vs ACS ≈12.5%) and **about 25–40% of interstate moves** (0.5–0.8% vs ≈2.1%).
  Earlier files (2006–08) catch more (5.4–5.6% for primary families).
- Secondary families move more often than primaries under every definition (2017→18 strict 5.5% vs 4.4%), as expected for
  partners, roommates and adult children.
- **The 2017→18 jump survives the strict rule**: 4.69% vs 3.58% (2016→17) and 3.28% (2018→19); interstate 0.84% vs 0.57/0.56%.
  2015→16 is similarly elevated (4.80%, interstate 0.83%). The Census series show no jumps. **Resolved (§4.5c):** the files are
  cut at different months (2016-06, 2017-05, 2018-07, 2019-06), so these pairs cover 14 months and the next ones 11; annualised
  the series is flat (4.1, 3.9, 4.0, 3.6). So the 2018 bump is not only address
  repair; families also get a fresh LOR in that vintage. A vintage effect in 2018 still has to be ruled out (e.g. against the
  quarterly files, or with a placebo outcome that should not respond to SALT) before reading 2017→18 as a SALT response.
- LOR also resets without a move for 1.3–5.1% of families per year, highest in 2006→07 (5.1%) and otherwise peaking where the
  strict move rate is *low* (2010→11, 2016→17, 2018→19: 3.3–3.4%). LOR is re-derived by vintage, so a reset alone is not evidence of a move, and the
  strict rule inherits LOR's vintage noise.
- Superseded loose table (any address field change: 9.95, 9.72, 9.30, 7.13, 5.64, 5.80, 6.23, 6.05, 6.41, 7.30, 4.94, 6.80, 4.92,
  4.96, 5.31, 8.86 for 2006→07 … 2021→23): `output/dataaxle_exploration/full2/`.

**[full] Mover rates on a gap-filled panel** (`moves_filled.sql`, 6 min; `output/analysis/dataaxle/moves_strict/moves_filled.csv`). Each
`family_id` is treated as present every year from its first to its last observed year; missing years (and the truncated 2022 file)
carry the last observed address forward. A gap followed by the same address counts as staying; a gap followed by a new address
counts as staying at the old address until the year before the new address is first observed. Strict move = building/ZIP change
and LOR at the new address ≤ years since last seen at the old one.

| Pair | Family-years (m) | % with filled t+1 | Strict, all | Strict, primary | Strict interstate | % of strict moves ending a gap | Observed-only strict, primary (table above) |
|---|---|---|---|---|---|---|---|
| 2006→07 | 114.3 | 7.6 | 5.54 | 5.27 | 0.88 | 0 | 5.61 |
| 2009→10 | 135.1 | 19.4 | 4.29 | 4.09 | 0.57 | 16.0 | 4.39 |
| 2012→13 | 153.8 | 15.8 | 3.50 | 3.39 | 0.54 | 16.5 | 3.54 |
| 2015→16 | 163.9 | 18.1 | 4.14 | 4.00 | 0.71 | 14.3 | 4.38 |
| 2016→17 | 172.8 | 17.4 | 3.22 | 3.12 | 0.51 | 14.1 | 3.33 |
| **2017→18** | 176.6 | 14.6 | **4.20** | **4.13** | **0.75** | 14.0 | 4.43 |
| 2018→19 | 183.5 | 9.7 | 3.18 | 3.07 | 0.55 | 15.6 | 3.06 |
| 2019→20 | 179.2 | 9.2 | 3.26 | 3.08 | 0.58 | 13.8 | 3.03 |
| 2020→21 | 169.1 | 5.5 | 3.83 | 3.55 | 0.73 | 13.9 | 3.47 |
| 2021→22→23 | 136.3 | (2022 filled) | 6.60 (2 yrs) | 6.02 (2 yrs) | 1.52 | 100 | 5.65 |

All pairs in the CSV.
- **Filling barely changes the rates.** It adds 5–19% more family-years (the denominator) and recovers 9–22% more moves (moves that
  end a gap), so rates move by less than ±0.4 pp. Data Axle still records only about a quarter to a third of ACS household moves.
  Missing years are not where the missing moves are.
- Gaps followed by a new address are common: 8–10% of 2–3-year gaps end at a new address (strict), against about 3–4% per year
  otherwise. Dating these moves to the last gap year is a choice; LOR at the new address could place them earlier.
- **The 2017→18 bump remains** (4.20% vs 3.22% and 3.18%; interstate 0.75% vs 0.51/0.55%).


**Tenure.** Data Axle `length_of_residence` for primary families, 1% sample [sub]:

| Year | LOR=1 | 2 | 3–5 | 6–10 | 11–20 | 21–30 | 31+ | Median | Mean |
|---|---|---|---|---|---|---|---|---|---|
| 2006 | 16.3 | 9.4 | 16.8 | 19.9 | 23.5 | 8.8 | 5.3 | 7 | 10.2 |
| 2012 | 15.1 | 7.2 | 15.8 | 19.7 | 25.2 | 9.2 | 7.8 | 8 | 11.7 |
| 2017 | 11.8 | 6.1 | 13.8 | 17.7 | 23.1 | 16.4 | 11.1 | 11 | 14.2 |
| 2018 | 13.5 | 6.1 | 11.3 | 19.5 | 24.2 | 15.1 | 10.3 | 10 | 13.8 |
| 2023 | 9.3 | 6.1 | 14.9 | 15.5 | 22.4 | 17.1 | 14.7 | 12 | 15.7 |

- LOR=1 means 0–17 months at the address, so with timely updating it should be roughly 1.5× the annual
  mover rate (≈18–19% using ACS). It is 9–16%, and declining faster than the Census mover rate.
  **Moves are picked up late, or missed**, consistent with the low move rates above.
- The median rises from 7 to 12 years (2006→2023) — much faster than plausible; partly mechanical
  (records age in the file without re-confirmation).
**[full] Length of residence, primary families, all years** (`output/dataaxle_exploration/full5/lor.csv`):

| Year | LOR=1 | 2 | 3–5 | 6–10 | 11–20 | 21–30 | 31+ | Median | Mean | LOR=1 renter / unknown / owner |
|---|---|---|---|---|---|---|---|---|---|---|
| 2006 | 17.9 | 8.8 | 17.1 | 19.7 | 22.8 | 8.5 | 5.3 | 7 | 10.0 | 37.9 / 31.4 / 7.8 |
| 2009 | 16.2 | 7.8 | 16.3 | 18.7 | 24.8 | 9.5 | 6.7 | 8 | 11.0 | 45.4 / 28.8 / 4.3 |
| 2012 | 14.3 | 7.0 | 15.9 | 19.0 | 26.0 | 9.8 | 8.1 | 9 | 12.0 | 37.1 / 24.9 / 5.0 |
| 2015 | 12.6 | 6.1 | 15.1 | 18.7 | 24.4 | 14.0 | 9.2 | 10 | 13.0 | 34.4 / 20.6 / 5.9 |
| 2017 | 13.1 | 6.6 | 13.4 | 17.0 | 23.7 | 15.7 | 10.4 | 10 | 13.8 | 36.4 / 21.9 / 6.7 |
| 2018 | 13.0 | 7.1 | 12.4 | 18.4 | 22.8 | 15.6 | 10.8 | 10 | 13.8 | 31.3 / 21.6 / 7.2 |
| 2019 | 11.1 | 7.0 | 14.1 | 17.3 | 23.4 | 15.9 | 11.3 | 11 | 14.2 | 23.0 / 22.4 / 5.5 |
| 2023 | 10.1 | 6.0 | 15.4 | 16.7 | 22.1 | 16.0 | 13.7 | 11 | 15.1 | 22.2 / 22.8 / 5.3 |

Matches the 1% sample (2017: 13.1 vs 11.8 at LOR=1). Owner LOR=1 shares are only 4–9%. The
owner/renter mix shifts in 2015 (owners 62%→67%) and 2019 (renters 14%→21%), so the owner/renter-specific
series are not comparable across those years.
- A proper comparison needs ACS "year householder moved into unit" (`MOVEDIN`), which is not in our IPUMS
  extract, and the Census API now requires a key. **[todo]** Add `MOVEDIN` to the IPUMS extract (and
  `UNITSSTR` is already there for checking single- vs multi-family).

### 4.4 Several addresses for one family (same year)
- **Never.** One row per family per year **[full, 2017]**. Second homes or dual residences are not visible:
  only one address per year is kept.

---

**[full] 2006 tenure vs ACS 2006 1-yr** (`code/analysis/dataaxle/tenure_2006_vs_acs.R`, outputs in
`output/analysis/dataaxle/tenure_2006_vs_acs/`). Primary families vs occupied units, two ways: (A) ACS PUMS `MV`
duration bins (Data Axle LOR spread uniformly over its rounded-month interval and split across bins); (B) published
B25038 calendar-year bins (Data Axle arrival year = 2006 − LOR).

| % of households (A) | ≤1 yr | 1–2 | 2–4 | 5–9 | 10–19 | 20–29 | 30+ |
|---|---|---|---|---|---|---|---|
| ACS PUMS | 16.8 | 7.2 | 20.1 | 18.9 | 17.9 | 8.8 | 10.3 |
| Data Axle | 11.9 | 10.3 | 18.9 | 20.1 | 24.3 | 8.8 | 5.6 |

| % of households (B) | 2005+ | 2000–04 | 1990–99 | 1980–89 | 1970–79 | ≤1969 |
|---|---|---|---|---|---|---|
| ACS B25038 | 21.5 | 31.1 | 24.1 | 10.4 | 6.8 | 6.1 |
| Data Axle | 17.9 | 30.2 | 32.4 | 10.9 | 6.0 | 2.6 |

- Recent movers are under-represented (≤1 yr: 11.9 vs 16.8%; renters 25 vs 36%), and Data Axle's 1–2 yr bin is too
  big. Moves show up in the file with a lag of about a year (consistent with §4.5).
- Excess at 10–19 years and deficit at 30+: single-year LOR heaps at 10 and 13 and halves from 14 to 15 (2006 − 14 = 1992).
  This looks like left-censoring: arrival dates before about 1992 (when the file starts?) get recorded as the first
  sighting. So 1990–99 is inflated (32 vs 24%) and ≤1969 is halved (2.6 vs 6.1%).
- 5–9 and 20–29 years match closely.
- **Fixed-date break (check 3, `lor_counts.csv`, all years):** the year-to-year drop is not at a fixed LOR but at a
  fixed **arrival year**. In every file 2006–2023, primary families with arrival year 1991 are about half as many as
  with 1992 (ratio 0.49–0.54; at LOR 14 in 2006, 25 in 2017, 31 in 2023). A second, smaller drop sits at 1979
  (1978 from 2016; ratio 0.50–0.61).

> **Note.** Data Axle carries previous-address and name histories, so `length_of_residence` is in principle a real
> history of arrival dates (back to 1959 per the codebook). But some lengths of stay are fishy: the break at a fixed
> 1992 arrival year (and 1979) looks like a source added to the compilation in 1992. Families at their address
> before then seem to be dated to their first sighting in that source. Treat arrival dates before about 1992 as
> left-censored. Owners' recent arrivals match ACS (deed or assessor dates likely), while renters' arrivals lag by about a year.

**[full] Tenure vs ACS, all years 2006–2023** (`code/analysis/dataaxle/tenure_all_years_vs_acs.R`; outputs in
`output/analysis/dataaxle/tenure_all_years_vs_acs/`). Same method A as 2006: ACS 1-yr PUMS `MV` (keyless download of every
`csv_hus.zip`, tallied by `code/analysis/dataaxle/pums_mv.sh`; no 2020 file) vs Data Axle primary-family LOR spread over months. Data Axle 2022 dropped.

*Data Axle − ACS, percentage points, all households:*

| Year | ≤1 yr | 1–2 | 2–4 | 5–9 | 10–19 | 20–29 | 30+ |
|---|---|---|---|---|---|---|---|
| 2006 | −4.9 | +3.2 | −1.2 | +1.2 | +6.3 | 0.0 | −4.7 |
| 2009 | −3.9 | +2.9 | −2.8 | +0.4 | +6.5 | +0.6 | −3.6 |
| 2012 | −5.1 | +2.2 | −1.6 | +0.5 | +5.8 | +0.9 | −2.6 |
| 2015 | −6.1 | +0.4 | −2.5 | +2.7 | +2.9 | +4.2 | −1.5 |
| 2017 | −5.4 | +0.3 | −5.5 | +2.8 | +2.5 | +5.7 | −0.3 |
| 2019 | −6.4 | −0.4 | −4.8 | +2.2 | +4.1 | +5.1 | +0.2 |
| 2021 | −5.4 | −0.4 | −4.2 | +0.8 | +4.6 | +4.1 | +0.5 |
| 2023 | −5.9 | −1.4 | −5.2 | +0.2 | +5.1 | +4.0 | +3.2 |

*≤1 year at the address, % of households (ACS / Data Axle):* all 16.8/11.9 (2006), 14.6/9.5 (2012), 14.2/8.8 (2017),
13.8/7.4 (2019), 12.6/6.7 (2023). Owners 7.7/5.2, 4.7/3.3, 6.6/4.4, 6.4/3.7, 5.7/3.6. Renters 35.6/25.3, 32.2/24.7, 27.7/24.2,
27.1/15.3, 25.6/14.8.

- **Recent movers are missing in every year**: 4–6 pp too few households at ≤1 year, so Data Axle captures roughly 55–70% of
  the ACS recent-mover share; the ratio drifts down from 0.71 (2006) to 0.53 (2023). Owners are closer (ratio 0.58–0.70) than renters until 2018.
- **The one-year lag fades.** The excess at 1–2 years (+3 pp until 2012, +9–11 pp for renters) shrinks to zero by 2015 and turns
  negative. Early files record moves late; later files miss more of them altogether (the ≤1 yr deficit does not shrink).
- **The 1992 heap moves through the bins.** The excess sits at 10–19 years in 2006–2012 (+6–7 pp; owners +9), shifts to 20–29 years
  from 2014 on (+4–6 pp), and the 30+ deficit (−5 pp in 2006) disappears by 2018 and becomes +3 in 2023, when arrival year 1992
  turns 31. This is the fixed-arrival-year censoring found above, now visible in every year.
- **Renters break in 2019**: Data Axle renters at ≤1 year drop from 21% to 15% while ACS stays at 27%. This coincides with the
  2019 owner/renter model change (renters 14%→21% of primary families), so the renter series is not comparable across 2019.
- 5–9 years matches within ±3 pp throughout; 2–4 years is 2–6 pp short, consistent with moves recorded late or missed.

**[full] Tenure gap by family-level data quality** (`dq_tenure.sql`, 3.5 min; `code/analysis/dataaxle/tenure_by_data_quality.R`;
`tenure_gap_by_share_observed.png`, `tenure_gap_by_missing_years.png`). Span = first to last `record_year` of the `family_id` anywhere
in Data Axle (2022 excluded); quality = years observed / span, or missing years inside the span. Each group vs the same ACS distribution.
Primary families 2019: complete span 67%, 80–99% 18%, 50–79% 11%, < 50% 2%, single-year 2% (1 missing year 12.5%, 2: 6.5%, 3: 4%, 4+: 8%).

| Data Axle − ACS, pp, 2017 | ≤1 yr | 1–2 | 2–4 | 5–9 | 10–19 | 20–29 |
|---|---|---|---|---|---|---|
| Complete span | −6.2 | +0.2 | −4.8 | +1.1 | +0.8 | +7.1 |
| 1 missing year | −6.3 | 0.0 | −3.0 | +6.9 | +2.7 | +3.4 |
| 3 missing years | −7.4 | −1.3 | −8.2 | +10.4 | +8.1 | +3.3 |
| 6+ missing years | −12.6 | −5.9 | −16.8 | −0.5 | +31.5 | +7.9 |
| Seen one year only | +42.5 | +23.3 | −13.7 | −11.6 | −19.5 | −10.0 |

- **The recent-mover deficit is not caused by spotty records.** Families observed in every year of their span (two thirds) are
  6–8 pp short at ≤1 year in every year 2006–2023, about the same as the whole file. The 1992 heap shows up in this group too (+7
  at 20–29 years in 2017).
- Gappier families look worse (≤1 yr −10 to −13 pp with 4+ missing years), but this is partly mechanical: many missing years
  require a long span, which selects long-tenure families. Single-year families are the mirror image (+42 pp at ≤1 yr).
- Families seen in < 50% of their span had *too many* recent arrivals in 2008–2012 (+7 to +12 pp at ≤1 yr) and too few after
  2017; with 2% of families, they do not move the total.

### 4.5c Why are Data Axle mover rates low? (1% samples)

**Samples** (`~/ptax_explore/samples/`, built by `samples_1pct.sql`, `tax_bld1pct.sql`, `deeds_bld1pct.sql`, `fampos_bld1pct.sql`; copies in
`output/analysis/dataaxle/move_tracking/`). Building key `bkey` = hash(ZIP5, street name, house number), identical across sources.
`da_fam1pct` (1% of `family_id`s, all years, 2.3 GB); `da_bld1pct` (all DA records at 1% of buildings, 24.6m rows, 5.7m families);
`da_bld1pct_fampos` (every DA record, any address, of the families and persons ever seen at those buildings); `tax_bld1pct` (tax rows at the
same buildings, ~50 columns, all tax years); `deeds_bld1pct` (ownertransfer rows by deed situs key, 4.0m). Names hashed throughout.

**Where are primary families one year later?** (`rekey_sample.sql`, primaries at sampled buildings, 2006→07 … 2020→21 pooled, 15.7m)

| Next year | % of primaries |
|---|---|
| Same `family_id`, same building | 81.4 |
| Same `family_id`, other address (tracked move) | 4.7 (3.6 with LOR reset) |
| `family_id` gone; a person ID reappears at the same building under a new `family_id` | 0.3 |
| `family_id` gone; a person ID reappears elsewhere under a new `family_id` (re-keyed move) | 0.7 (0.4 with LOR reset) |
| `family_id` gone; no person ID found anywhere | 12.8 |

Of primaries whose `family_id` is absent next year (`absent_return.sql`): 33% return later to the same building (coverage gap), 5% return
elsewhere, **62% never return under that ID** (≈ 8.5% of primaries per year).

**Name + age relink of those exits** (`name_relink.sql`; person 1's hashed last + first name and age, searched in all of Data Axle in t+1 under
another `family_id`; age in [a, a+2]; placebo [a+10, a+12]). Split by how many persons nationally carry the name pair in year t:

| Name frequency | Never return under ID: match | placebo | match at another address | …with LOR ≤ 1 | other state | Return later: match | placebo |
|---|---|---|---|---|---|---|---|
| Unique (n = 108k) | 21.8% | 0.1% | 16.7% | 7.0% | 3.6% | 8.0% | 0.1% |
| 2–5 (n = 126k) | 49.7% | 4.4% | 44.2% | 9.2% | 15.2% | 34.9% | 4.8% |
| 6–50 | 68.6% | 37.9% | | | | 60.8% | 39.7% |
| > 50 | 96.9% | 88.3% | | | | 96.9% | 90.5% |

- With rare names (placebo ≈ 0–4%), **22–45% of permanently vanished primary families reappear elsewhere under a new family record**
  the next year. Unique-name pairs are a lower bound (they include misspellings that never recur). Families that return later under their
  own ID match far less often (8–35%), as they should.
- **Most of these re-found persons are not new arrivals at their new address**: only 7–9% (of all exits) carry LOR ≤ 1 there, against
  17–44% found at another address. The person apparently already had a second record at the new address (a duplicate created earlier, with
  its own LOR clock), and the old record is dropped later. So in Data Axle a move often appears as *a new record elsewhere, then deletion of
  the old one*, not as an address change on one record. This breaks both the ID link and the LOR-reset rule.
- About 11% of exits keep their person ID (same `individual_id` under another `family_id`), so person-ID linkage recovers only a minority.
- Rough size: 8.5% of primaries exit per year × 0.22–0.45 re-found elsewhere ≈ 2–4 pp per year. Added to tracked moves (4.7%) and
  person-ID re-keys (0.7%), Data Axle would show about 7.5–9.5% of primaries moving per year, closer to but still below ACS (≈ 12.5%).

**Tax and deed turnover vs Data Axle at the same houses** (`turnover_sample.sql`; owner-occupied, non-corporate single-family parcels in the
building sample; sample tax years before 2010 are too thin):

| t → t+1 | Tax roll: owner surname changes | Deeds: arm's-length sales (category A, not inter-family, price > 0) | DA: primary `family_id` changes | DA: …with LOR reset |
|---|---|---|---|---|
| 2010 | 5.1% | 4.5% | 9.4% | 3.1% |
| 2012 | 4.5% | 4.0% | 11.1% | 3.9% |
| 2014 | 5.0% | 4.2% | 12.1% | 4.1% |
| 2016 | 5.5% | 4.8% | 11.9% | 4.0% |
| **2017** | 5.2% | 4.8% | **14.4%** | **5.2%** |
| 2018 | 5.2% | 4.9% | 12.3% | 3.8% |
| 2019 | 5.9% | 5.2% | 12.3% | 3.8% |
| 2020 | 5.6% | 5.1% | 13.9% | 4.7% |

- Owner turnover is 4–6% per year in both the roll and the deeds, in line with ACS owner move rates.
- **At the address, Data Axle sees about the right number of new households** (ID change with LOR reset 3.8–5.2%), but the primary ID
  changes 2–3× as often as houses sell: re-keying in place and record churn.
- **The 2017→18 bump appears only in Data Axle** (14.4% / 5.2%), not in the tax roll or deeds (5.2% / 4.8%): a 2018 vintage effect, not
  more moves.
- **Bottom line:** Data Axle records the arrival at the destination reasonably well but often fails to link it to the household's previous
  record. The origin record is dropped and the person shows up under a new (often pre-existing duplicate) family record elsewhere. Family-ID
  move rates therefore understate moves by roughly half; linking by name + age recovers part of the gap.

**How identifying is name + age?** (`name_uniq.sql`, full 2017 file, persons 1–5 with first and last name: 279.5m, 226.6m with an age; every
`individual_id` distinct.) Share of persons whose key is shared by k persons nationally:

| Key | Unique | 2 | 3–5 | 6–20 | 21–100 | > 100 |
|---|---|---|---|---|---|---|
| Last + first name | 19% | 10% | 11% | 15% | 17% | 27% |
| Name + exact age | 53% | 16% | 12% | 11% | 6% | 1.5% |
| Name + age ± 1 | 43% | 15% | 13% | 14% | 11% | 5% |
| Name + age + state | 78% | 15% | 5% | 2% | 0.2% | 0% |

Name alone identifies a fifth of persons; adding age, about half; adding state, four fifths. Part of the "2" bins is Data Axle duplicates
(the same person in two records under different `individual_id`s), so true uniqueness is somewhat higher.

**How consistently is a residential parcel occupied in Data Axle, 2012–2021?** (`parcel_years.sql`, 1% building sample.) Residential tax
addresses (single family, condo, 2–4 units, apartment; structure on the roll): 939k, of which 93.2% have a DA record in at least one year.
Among those, years with any DA record: 10 years 58.5%, 9: 12.0%, 8: 7.0%, 7: 5.2%, 6: 4.1%, 5: 3.1%, 4: 3.0%, 3: 2.5%, 2: 2.4%, 1: 2.2%
(with a primary family: almost identical). Owner-occupied single family (643k): mean 8.8 years, 64.9% all 10, 82.3% ≥ 8, 5.3% ≤ 3;
restricted to houses built before 2012 and on the roll all 10 years: 71.0% all 10, 89.1% ≥ 8. Absentee/corporate single family: mean
7.3–7.5 years, 35% all 10, 12–15% ≤ 3. Condos and 2–4 units: owner-occupied 57–71% all 10, absentee 40–46%.

**Same question for the tax table** (`tax_years.sql`, 1% building sample, residential addresses with a structure, 2012–2021; 938k). Tax years
present: 10 years 65.3%, 9: 15.3%, 8: 6.9%, 7: 2.8%, 6 or fewer: 9.7%. Single family built before 2012 (721k): **70.1% all 10 years, 92.3% ≥ 8**
(Data Axle, owner-occupied single family: 64.9% / 82.3%). 7.1% of address-years are missing: 2.7 pp because the county is absent from the
sample that year (counties in the sample grow from 2,458 in 2012 to 2,971 in 2021; the tax table's county coverage fills in over time, plus
upstate NY in 2019) and 4.4 pp with the county present (re-parcelling, situs address missing or changed in some years). 86.9% of these
houses have no missing year while their county is on the roll. So the tax data is somewhat more complete than Data Axle, mostly at the
county level; address-level gaps exist in both at a similar order of magnitude.

**Coverage from first appearance to 2021** (`tax_window.sql`, same 939k residential addresses). Window = first tax year (≥ 2012) to 2021.
Tax: mean 95.5% of window years observed; all years 77.5%, 90–99% 12.3%, 80–89% 5.6%, < 80% 4.6%. Data Axle over the same window (addresses
matched in any year, 93.2%): mean 87.0%; all years 60.4%, 90–99% 10.6%, 80–89% 8.1%, 50–79% 13.2%, < 50% 7.3%. Addresses first on the roll in
2012: tax 79.7% complete vs DA 62.2%; first on the roll 2015–2020 (new construction or newly covered): tax 77–93% complete vs DA 29–48%
(DA picks up new addresses with a lag). Among addresses complete in tax, DA has every year for 66.5% of owner-occupied single family (84.4%
≥ 80%) but only 37% of absentee/corporate single family (61–64% ≥ 80%, 14–18% below half); condos and 2–4 units: owner-occupied 61–71%,
absentee 41–46%. **Once a parcel appears, the tax roll keeps it (≈ 95% of years); Data Axle's household-level coverage is the weaker link,
especially for rentals.**

*Missing years between first tax year and 2021* (`tax_window_missing.sql`): tax 0 missing 77.5%, 1: 16.3%, 2: 2.6%, 3: 0.8%, 4: 0.6%, 5+: 2.2%
(mean 0.37 for addresses first seen in 2012). Data Axle, same addresses and window: 0: 60.4%, 1: 13.8%, 2: 8.0%, 3: 5.3%, 4: 3.7%, 5+: 8.9%
(mean 1.18 for 2012 addresses). Among addresses with no missing tax year, DA missing years: owner-occupied single family 0: 66.5%,
1: 13.8%, 2: 7.3%, 3–4: 7.3%, 5+: 5.2% (mean 0.86); absentee single family 37.2 / 15.8 / 11.2 / 15.1 / 20.7% (mean 2.29). Where the single missing tax year
falls is not yet checked (addresses first seen in 2013 have one missing year 38.5% of the time).
*Between first and last tax year* (`tax_window_inner.sql`; drops exits from the roll such as parcel splits or re-addressing; 79% of addresses span
all 10 years): tax missing 0: 81.5%, 1: 15.5%, 2: 2.2%, 3+: 0.8%. Data Axle over the same window: 0: 61.0%, 1: 13.9%, 2: 8.0%, 3: 5.3%, 4: 3.6%,
5+: 8.3%. By occupancy (tax complete): owner-occupied single family DA 0: 66.6%, 1: 13.7%, 2: 7.2%, 3–4: 7.2%, 5+: 5.2%; absentee single family
37.5 / 16.0 / 11.3 / 15.0 / 20.2%. **Single missing tax years are state-year delivery gaps** (`which_year.sql`): 2019 30% of them (NY 28k of 46k,
AR 9k), 2014 21% (IN, CT, MA, AL), 2015 16% (TX, MD, GA), 2013 14% (MD). So inside a parcel's life on the roll, tax gaps are almost all
whole state- or county-years that can be bridged mechanically; Data Axle's are household-level and longer.
*Are tax gaps whole county-years?* (`gap_evidence.sql`.) Bridging address-years = present in y−1 and y+1 (6.62m); 2.79% are missing in y. For each
county-year, the share of its bridging parcels missing in y is bimodal: **85.5% of all gaps sit in county-years where ≥ 90% of the county's
sampled parcels are missing** (typically 100%: Suffolk NY 2019 4,029/4,029, Franklin OH 2018 3,664/3,665, Nassau NY 2017, Travis TX 2015,
Hartford/Fairfield/New Haven CT 2014, Montgomery MD 2013, Jefferson AL 2014 …), 1.1% in county-years 50–90% missing, 4.3% in 10–50%, and
**9.1% are isolated parcels** in county-years with < 10% missing. Fully dropped county-years (≥ 20 bridging parcels): 177 in 2013, 142 in 2014,
174 in 2015, 35 in 2016, 6 in 2017, 30 in 2018, 157 in 2019, 19 in 2020. Within covered county-years the gap rate is **0.26%** of address-years.
The isolated gaps are more often re-parcelled (CLIP set differs across the gap 7.1% vs 0.3% for no-gap bridges) or new/rebuilt (year built ≥ y−1:
4.5% vs 0.6%), but most have no visible parcel change. Full-table check (`gap_fulltable.sql`, `dropped_county_years_full.csv`): of the 740 dropped
county-years, **93.4% have no rows at all in `tax` for that year** (delivery gaps), 0.7% have rows but none with a situs address, and 5.9%
(44, almost all MA/NH/ME 2014 and CT 2015) have a full roll with addresses that does not key-match the adjacent years. A full-table check of
these 44 (`move_tracking/odd_county_years/`, README there; `odd_cy_*.sql`) shows they are **town-level delivery gaps, not a formatting
vintage**: New England rolls are kept by town, and in the 2013/2014 transition CoreLogic delivered each town under only one `tax_year`. Of the
1.37m y−1 residential parcels in these county-years, 2.7% key-match into year y (identical with a normalised key and with CLIP: 2.6%) but 88.4%
match into y+1; 95.7% sit in towns with < 5% of their y+1 rows present in y. Middlesex MA 2013 = Lowell, Waltham, Watertown … (60 towns), 2014 =
Cambridge, Newton, Somerville … (93 towns), overlap only stray rows; 0 of the 98k 2013 CLIPs appear in 2014, 97,991 reappear in 2015. Across all
MA towns 2011–17 the pattern is either 2014-onwards (262 towns) or 2013-then-2015 (69 towns); CT: 69 towns skip 2014; ME/NH/VT: most towns
skip 2013. Field formatting is identical across years. Fix: treat (fips, situs_city, tax_year) with far fewer rows than usual as not delivered
and carry parcels present in y−1 and y+1 across the gap, rather than normalising addresses. (Job note: `fips_code::INT` casts in WHERE defeat
predicate pushdown, 26 min vs 2 min.) Row
counts in the year after a dropped year equal the year before (median ratio 1.0 in 2013–15 and 2019): no catch-up double roll, so not a
tax-year relabelling. Isolated gaps (`gap_fulltable_b.sql`, `iso_gaps_full.csv`; 16.6k): **30% have a row for one of the parcel's CLIPs in the gap year**: 14.6% at the same
address but coded non-residential that year (property-type recoding, often a teardown/rebuild), 12.6% under another situs address
(re-addressing, split/merge), 3.1% with no situs address; 70% have no row for any adjacent-year CLIP. Of those, a third are town-level
delivery gaps (`gap_zip.sql`): 32% of isolated gaps sit in ZIP-years with ≥ 10% of the ZIP's parcels missing, almost entirely MA, CT and ME
(79–97% of their isolated gaps), whose rolls are kept by town rather than county; MA/CT/ME/UT/NC are 3–8× over-represented among isolated
gaps relative to their share of address-years. The remaining ≈ 11k gaps (0.17% of address-years) are genuinely parcel-level.
**Move-out proxies at the address, backfilled panel** (`moveout_proxies.sql`; outputs `moveout_rates.csv`, `moveout_validation.csv`,
`moveout_vs_taxowner.csv`, `acs_hh_move_rates.csv`). 1% building sample, residential addresses with a structure, 2012–2021. Missing Data Axle
years (and 2022) are backfilled with the most recent previous record at the address. Proxies for t→t+1: (1) the primary `family_id` changes,
(2) the modal hashed surname of primary-family persons changes, (3) the modal surname across all persons at the address changes.

| Annual rate, t→t+1 pooled | Primary ID change | Modal surname (primary) | Modal surname (all persons) | Arm's-length sale in t+1 | Tax owner surname change | ACS householders moved |
|---|---|---|---|---|---|---|
| All residential addresses (7.24m pairs) | 13.4% | 13.2% | 14.8% | 3.2% | 5.8% | 12.5–13.4% (all tenures) |
| Owner-occupied non-corporate SFR (5.35m) | **10.8%** | **10.5%** | 12.2% | 4.4% | 5.5% | **5.2–6.8%** (owners) |
| …SFR sold arm's-length at least once 2013–20 (1.59m) | 21.6% | 21.1% | 22.4% | 13.7% | 13.7% | |

Observed-only pairs (no backfill) are 0.5–0.7 pp lower. By year, owner-occupied SFR ID change: 8.8 (2012→13), 9.5, 10.5, **12.6 (2015→16)**,
10.3, **12.5 (2017→18)**, 10.3, 10.3, 11.7 (2020→21); deed sales 3.4–4.8% and tax owner changes 4.7–6.2% with no such bumps.

*Validation against sales (arm's-length SFR subsample):*

| Sale timing relative to pair t→t+1 | Pairs | ID change | Modal surname (primary) | Both | ID only | Surname only | Tax owner change |
|---|---|---|---|---|---|---|---|
| Sale in t+1 | 218k | 34.7% | 34.6% | 33.9% | 0.8% | 0.7% | 35.1% |
| Sale in t | 212k | **66.2%** | 64.6% | 63.9% | 2.4% | 0.7% | 9.3% |
| Sale in t−1 or t+2 | 369k | 12.0% | 11.3% | 10.5% | 1.5% | 0.8% | 28.1% |
| No sale within t−1…t+2 | 794k | **10.5%** | **10.4%** | 9.7% | 0.8% | 0.7% | 2.6% |

- **The two proxies are the same signal.** Primary-ID change and modal-surname change agree on 98% of pairs (ID-only 0.8%, surname-only
  0.7%); the all-persons surname adds ≈ 1.5 pp of noise from secondary families. Neither removes the excess turnover, so in-place
  re-keying with the same surname is rare (≈ 1 pp); the excess is a *different surname* appearing as the primary household.
- **Data Axle detects most sales, with a lag**: the primary changes for 66% of homes in the pair ending the sale year and 35% in the next
  pair (the tax roll updates the owner mostly one roll later, 35% then 9%). Roughly three quarters of arm's-length sales show a new primary
  within two years.
- **But there is a lot of turnover without a sale**: within the sold subsample, at pairs with no sale within ±2 years the primary changes
  10.5% per year (tax owner surname: 2.6%); at owner-occupied homes with *no deed at all* 2010–2023 the primary changes only ≈ 3.7% per
  year (7.2% over two pairs, 11.4% over four). So homes that sell at some point are also churn-prone outside the sale (pre-sale move-outs,
  rentals, flips). Against ACS owner move rates of 5–7%, address-level turnover in Data Axle (10.8%) is about twice the true owner move-out
  rate. Over all residential addresses the 13.4% looks like the ACS 12.5–13.4%, but only because spurious turnover at owner homes offsets
  the moves Data Axle misses.
- **Timing around a sale** (`owner_change_why.sql`, 209k arm's-length sales at owner-occupied SFR, sale year s): the primary changes in the
  pair s−2→s−1 for 14.5% (pre-sale move-out; base ≈ 3.7%), in s−1→s (the sale-year file) for 34.2%, in s→s+1 for 68.3%, in s+1→s+2 for
  15.0%; **in at least one of the two pairs around the sale 89.7%** (both: 12.8%), in any of the four 93.8%. Net of the never-sold base
  (7.2% / 11.4%), Data Axle registers a new household for ≈ 83% of sales. Whether the s→s+1 pair is "late" depends on the annual
  file's snapshot date; the tax roll itself shows the new owner one roll later for most sales (35% / 9%).
- **Why tax owner surnames change 5.6% a year when 4.4% sell arm's-length** (same job, 4.94m pairs): of the changes, 34.6% have an
  arm's-length deed in t or t+1, 43.7% have a deed in t−1 or t+2 (the same sales, recorded one roll off), 9.6% have a non-arm's-length deed
  in t/t+1 (category B/C: inter-family, trust, quitclaim, mostly unpriced), 1.0% are owner-order swaps (new owner 1 = old owner 2, e.g.
  surviving spouse), 0.5% name-parse differences, 0.5% one-year flickers, and 10.1% (0.57 pp of pairs) have no deed within ±2 years
  (unrecorded transfers, estates, corrections). So ≈ 88% of tax owner changes have a deed behind them; the gap to the arm's-length rate
  is non-arm's-length transfers (≈ 0.5 pp) plus deed-less changes (≈ 0.6 pp).
- **The 2015→16 and 2017→18 bumps are file cut-off timing, not re-keying** (`move_tracking/spikes_2015_2017/`, README there;
  `spike_timing.sql`, `spike_decomp.sql`, `spike_newfids.sql`). The annual files are not 12 months apart: effective cut-off =
  `max(recency_date)` per `record_year`, confirmed by the month at which arm's-length sales stop appearing in the sale-year file:
  2012-03, 2013-03, 2014-03, 2015-04, **2016-06**, 2017-05, **2018-07**, 2019-06, 2020-06, 2021-06. So 2015→16 and 2017→18 span
  ≈ 14 months, 2016→17 and 2018→19 ≈ 11. Annualised ID-change rates (one primary, both observed): 10.7, 10.5, 11.5, 11.6, 11.5,
  11.9, 11.1, 13.0 (2020→21, Covid) for 2013→14 … 2020→21 — flat. Two-year sums are smooth even uncorrected (24.1/24.3/24.1 pp).
  Evidence: for continuing primaries `length_of_residence` jumps by 2 for 28.8% (2015→16) and 18.0% (2017→18) and by 0 for 13.2%
  (2016→17) and 5.5% (2018→19), implying intervals of 13.0/15.5/10.4/14.2/11.3 months; sales captured in the sale-year file 34%
  (2015), 52% (2016), 40% (2017), 57% (2018), captured by the next file a flat 82–87%. The excess ID changes decompose into
  tracked-elsewhere (+0.6/+0.9 pp), brand-new ids with LOR ≤ 1 (+0.6/+0.9), promoted sub-families (+0.2/+0.6) and returning ids;
  in-place re-keys (same person id or surname under a new id) are a flat 0.5–0.7% of pairs. Every state shows the excess;
  address fields, dpbc, tract do not change more than in control years; geocode re-runs (8–9% in 2017→19, 29% in 2020→21) hit
  changers and controls alike. Family-level strict rates annualise the same way (4.80→4.1, 3.58→3.9, 4.69→4.0, 3.28→3.6).
  **Correction:** date each file at its cut-off and annualise all year-pair statistics by elapsed months; date moves by the
  destination LOR / `recency_date` or the CoreLogic sale date. For the SALT event study the 2017→18 pair spans May 2017–Jul 2018
  and 2018→19 only 11 months, so an uncorrected treatment-year jump of ~25% is mechanical.
- Open: who are the new different-surname primaries at homes that did not sell — a listing swap among household members, tenants in
  "owner-occupied" homes, or transient brand-new records (the 60% brand-new secondary IDs above)? Check whether the previous primary's
  surname persists at the address and whether the new primary is still there in t+2.
**Is CoreLogic's `owner_occupancy_code` trustworthy?** (`move_tracking/oo_flag/oo_flag_xtab*.sql`, SFR with structure, tax years 2012–2021, 1%
address sample, 7.7m parcel-years). The code is a deterministic rule, not an independent observation: O (80.3%) = the mailing address equals
the situs address (74.5 pp) **or** `homestead_exempt_indicator` is set (the remaining 2.6–3.1 pp are 94–100% homestead-flagged, whatever
the mailing address); A (17%) = mail elsewhere and no homestead flag (only 0.04% of parcel-years are A with mail at the property); blank
(4.6%) = mail to a PO box (75% of blanks) or unparseable; M (0.1%) = multiple owners. Hence its accuracy is the accuracy of the mailing
rule. The homestead flag is populated only where a homestead programme exists and the county delivers it: MN 77%, LA 68%, IN 65%, HI 64%,
FL 63%, WI 62%, IL 58%, TX 57%, ID/CA 50%, …, SC 16%, UT 1% (both have large owner-occupant assessment-ratio discounts that are not
flagged), and 0 in states without a programme (OR, MO, WA, CT, CO, NH, AZ, NV, MA, ME, VA, TN, NJ, KS, DE, MT). In FL the gap between O
(80%) and homestead (63%) is 17 pp; part is owners not claiming or counties not reporting, part is false positives of the mailing rule
(rentals and second homes with the bill mailed to the property).

*Bill-based test* (`oo_flag/tax_bill_bld1pct.sql` → `tax_bill_bld1pct.parquet`, 11.1m SFR parcel-years 2012–21; `oo_bill.sql`,
`oo_bill_refine.sql`, `oo_bill_mi.sql`; outputs `oo_bill_*.csv`, `oo_refine_*.csv`). Effective rate = `total_tax_amount` /
`calculated_total_value`; "full rate" = its 90th percentile in the county × `tax_area_code` × year cell (≥ 20 parcels); a parcel
is *discounted* if its rate is < 80% of the full rate. States with a **clear bimodal distribution** (2019, lower mode / share
below 0.85 of full rate): SC 0.25–0.30 / 77% (4% vs 6% assessment ratio + school-operating exemption), ID 0.50 / 77%, IN 0.45 /
66%, AL 0.45–0.50 / 70% (class III vs II), WV 0.50 / 65% (class II vs III/IV), MS 0.50 / 64%, MI ≈ 0.5 / 72% (18 school mills;
broad because MI has no tax-area code so cells are counties); UT shows the 0.55 mode only for 9% because assessed values are
already net of the 45% primary-residence exemption. Fixed-dollar exemptions give a continuous, value-dependent discount, not a
second mode: FL (66% below 0.85, peak 0.70), TX, NY, GA, LA, IL, MN. Apparent bimodality in RI, MA, CT, TN, KS, CO, NH, DE is
town/city rate variation inside a county (no or sparse tax-area codes). Not visible: CA ($7k exemption), OH (2.5%), NJ, PA, WA,
VA, OR. `total_tax_exemption_amount` is filled only in CA/FL, `calculated_total_tax_exemption_amount` in CA, FL, OK, NM, NY, WV.

Accuracy of `owner_occupancy_code` against the bill discount, pooled 2013–2020, ratio-discount states:

| State | SFR pairs | Discounted | Coded O | P(O \| discounted) | P(O \| not discounted) | …among never-sold | P(discounted \| O) | P(discounted \| not O) | P(mail here \| disc.) | P(mail here \| not disc.) |
|---|---|---|---|---|---|---|---|---|---|---|
| SC | 77.6k | 74.7% | 78.7% | 91.3% | 38.3% | 25.5% | 88.4% | 34.6% | 87.7% | 34.5% |
| IN | 102.9k | 65.2% | 82.5% | 96.5% | 55.3% | 50.4% | 77.3% | 13.6% | 92.5% | 51.7% |
| AL | 79.7k | 66.6% | 77.3% | 90.7% | 48.3% | 38.7% | 80.1% | 28.4% | 84.1% | 42.7% |
| MS | 22.9k | 63.2% | 78.0% | 97.8% | 40.9% | 36.8% | 81.8% | 9.2% | 92.9% | 36.4% |
| WV | 19.8k | 61.5% | 78.6% | 90.7% | 57.3% | 53.2% | 73.6% | 31.7% | 81.9% | 49.4% |
| MI (2019) | 29.2k | 72.3% | 84.7% | 92.6% | 60.7% | | 82.2% | 42.9% | | |

- **Recall is good, specificity poor.** 91–98% of bill-verified owner-occupants are coded O, but 38–61% of homes *without* the
  owner-occupant discount are also coded O, and the share barely falls when recently sold homes (exemption not yet claimed) or
  ever-sold homes are excluded (26–53%). 12–26% of coded-O homes show no discount; 9–43% of coded-A homes do (SC/WV/AL/MI ≈ 30–40%).
  The code therefore over-states owner-occupancy by roughly 5–15 pp in these states (coded O 77–85% vs discounted 61–75%), and
  mis-labels a third of absentee-coded homes in the high-discount states. The mailing-address rule alone behaves the same way
  (P(mail here | not discounted) 35–52%).
- Caveats: the discount is itself noisy (flips between consecutive years for 7–13% of parcels vs 3–8% for the code); some
  non-discounted owner-occupants simply never applied (take-up is high but not universal), and in IN/ID the fixed caps shrink the
  discount for expensive homes. Discounted-but-coded-A homes have mail elsewhere (1–7% mail here), no homestead flag, higher
  values than other A homes and 10–20% recent sales: owner-occupants whose bill goes to another address or a work/PO address in
  a different format, plus exemptions that carry over the sale year to an investor buyer.
- Practical: treat O as "bill mailed to the property"; where the bill fields exist, use the discount; otherwise combine O with a
  Data Axle primary family at the address and the homestead flag.

### 4.5b [full] Household counts under building × unit de-duplication, 2006–2013

(`code/analysis/dataaxle/household_strategies.R`; Bouchet job `hh_strat_full.sql`, 6 min, full data; outputs in
`output/analysis/dataaxle/household_strategies/`.) Building = street address (`address_type='S'`, house number and street
name filled) on zip + house number + pre-direction + name + suffix + post-direction; unit = `upper(trim(unit_num))`,
blank = NULL. **S1:** each distinct building × unit combination (NULL unit = its own combination) is one household.
**S2a:** drop the NULL-unit combination if the same building has ≥1 real unit in the same year. **S2b:** drop it only if
the building has ≥2 distinct real units. Records without a usable street address (13–14m) count as one household each.

| Millions | 2006 | 2007 | 2008 | 2009 | 2010 | 2011 | 2012 | 2013 |
|---|---|---|---|---|---|---|---|---|
| ACS households (B11001) | 111.6 | 112.4 | 113.1 | 113.6 | 114.6 | 115.0 | 116.0 | 116.3 |
| DA family records | 130.3 | 134.6 | 129.7 | 129.8 | 128.4 | 136.6 | 141.3 | 159.1 |
| DA primary families | 113.6 | 115.7 | 111.4 | 111.3 | 109.6 | 113.8 | 114.9 | 120.9 |
| S1 building × unit | 106.0 | 108.3 | 105.1 | 106.5 | 105.1 | 110.0 | 111.4 | 117.5 |
| S2a | 104.5 | 106.7 | 103.6 | 105.1 | 103.7 | 108.6 | 109.9 | 116.0 |
| S2b | 105.0 | 107.2 | 104.1 | 105.6 | 104.2 | 109.1 | 110.4 | 116.5 |
| Mixed buildings (NULL + ≥1 unit) | 1.51 | 1.59 | 1.52 | 1.38 | 1.34 | 1.36 | 1.40 | 1.50 |
| Family records at dropped NULL-unit addresses (S2a) | 6.15 | 6.37 | 5.52 | 4.19 | 4.10 | 3.88 | 3.98 | 4.11 |

- **S1 is already 4–9m below ACS** (2006–2012). Collapsing to building × unit merges every no-unit apartment building into one
  "household", so it undercounts, not overcounts. S2a/S2b only remove a further 1.0–1.6m.
- S2a/S2b remove few combinations (one per mixed building) even though 4–6m family records sit at those NULL-unit
  addresses. Most of the S1 collapse happens *within* the NULL-unit address (several families at one no-unit address).
- Primary families track ACS levels best (within ±5m) up to 2012. 2013 jumps in every Data Axle series (vintage break, §1).

**[full] Building × unit combinations holding several primary families, 2006–2013** (`multiprim_full.sql`, 6 min;
`output/analysis/dataaxle/household_strategies/multiprim_combos_2006_2013.csv`). Primary families with a building key only
(12.6–13.4m primaries per year have none).

| | 2006 | 2007 | 2008 | 2009 | 2010 | 2011 | 2012 | 2013 |
|---|---|---|---|---|---|---|---|---|
| Combos with a primary (m) | 92.4 | 94.5 | 91.8 | 92.5 | 91.7 | 96.1 | 97.3 | 102.7 |
| Combos with 2+ primaries (m) | 2.95 | 2.78 | 2.51 | 2.15 | 1.90 | 1.68 | 1.60 | 1.64 |
| % of combos | 3.2 | 2.9 | 2.7 | 2.3 | 2.1 | 1.8 | 1.6 | 1.6 |
| % of primaries in such combos | 11.2 | 10.7 | 9.7 | 7.9 | 7.3 | 6.2 | 6.0 | 5.9 |
| Excess primaries (primaries − combos, m) | 8.3 | 8.2 | 7.0 | 5.5 | 5.1 | 4.6 | 4.5 | 4.7 |

- **A combination with a real unit never holds two primaries** (0 in every year). Data Axle allows one primary per
  building + unit. All multi-primary combinations are bare street addresses.
- 2006: 0.94m of 1.48m bare addresses in mixed buildings (63%) hold 2+ primaries (4.5m excess). So do 2.0m of 79.0m
  never-unit addresses (2.6%; 3.8m excess, mostly apartment buildings recorded without units). 2012: 50% and 1.1%.
- Tail is long: 2012 has 80k addresses with 10+ primaries. The decline 2006 → 2012 means more units get recorded over time.

**Are secondary families different-surname household members? Order of magnitude, 2012.** Data Axle: 141.3m
family records − 114.9m primary = **26.4m secondary families** (16.7m in 2006, 38.2m in 2013). ACS 2012
(IPUMS `RELATED`, adults 18+, households): adults whose surname plausibly differs from the householder's:

| ACS 2012 adults (m) | Count | Different surname? |
|---|---|---|
| Unmarried partners | 6.9 | almost always |
| Housemates / roommates | 5.7 | almost always |
| Roomers / boarders | 1.5 | almost always |
| Other nonrelatives | 2.6 | almost always |
| **Nonrelatives, subtotal** | **16.7** | |
| Children-, parents-, siblings-in-law | 2.9 | often (couples count once) |
| Stepchildren, grandchildren, other relatives | 5.9 | about half |
| Parents, siblings of householder | 7.6 | sometimes (e.g. married female householder) |
| Wives who kept their own surname | ≈ 5 | assume about 10% of 55.7m spouses |

Plausible total: about **22–30m different-surname adults**, in somewhat fewer groups, against 26.4m secondary families: **same
order of magnitude.** The composition also matches the partner test (§4.2a). Partners are ≈ 28% of the ACS candidates vs
24–30% "opposite sex, similar age" pairs. Roommates/boarders are ≈ 35–40% vs 35% same-sex pairs. In-law/step/grand
generations are ≈ 20–25% vs 24–27% "≥18 y younger". Caveat: the Data Axle count grows 16.7 → 26.4 → 38.2m (2006 → 2012 →
2013) while ACS nonrelative counts move slowly, so the *level* of secondary families is driven by compilation vintage.

**[full] Pooled households vs ACS: surname mix by number of adults, 2006–2023** (`hh_surnames.sql`, 9.5 min on 16 cores;
`code/analysis/dataaxle/hh_surnames_vs_acs.R`; outputs in `output/analysis/dataaxle/hh_surnames/`). Household = all family
records (primary + secondary) at one building × unit in a year with exactly one primary family. *Single-family* = no unit number,
all families `location_type` S (78–82% of building × unit combinations); *multi-unit* = has a unit number (13–19%). Adults = persons
1–5 of every family; surnames hashed in-query. ACS (IPUMS 2011–2023, households, adults 18+; single-family = 1-family detached or
attached, multi-unit = 2+ units) has no surnames, so the benchmark comes from relationships to the householder: *lower* = an
adult nonrelative is present; *upper* = an adult other than householder, spouse or own/adopted child, else 25% if a spouse is
present; *central* = 1 − ∏(1 − p) over adults with assumed p (spouse 0.20, own child 0.03, stepchild 0.6, child-in-law 0.5,
parent 0.4, parent-in-law 0.7, sibling 0.3, sibling-in-law 0.7, grandchild 0.4, other relative 0.6, nonrelative 0.95).

*Share of households with more than one surname, single-family:*

| Adults | DA 2012 | ACS 2012 central (lower–upper) | DA 2019 | ACS 2019 central (lower–upper) |
|---|---|---|---|---|
| 2 | 22% | 27% (10–34) | 34% | 28% (11–35) |
| 3 | 41% | 41% (13–53) | 52% | 43% (15–56) |
| 4 | 50% | 54% (18–65) | 58% | 56% (20–68) |
| 5 | 54% | 67% (24–79) | 56% | 69% (27–81) |
| All households | **23.4%** | **23.9%** (8.5–29.7) | **40.4%** | **25.1%** (9.6–31.0) |
| Mean adults (DA pooled / DA primary / ACS) | 2.20 / 1.88 / 2.00 | | 2.76 / 2.13 / 2.02 | |

Multi-unit: 17% (DA 2012) vs 21% (ACS central, 15–25), rising to 34% in DA 2019 against 22%.

- **2012 supports the hypothesis.** Pooling secondary families into the primary household reproduces the ACS surname mix almost
  exactly, both overall (23.4 vs 23.9%) and by size (2–4 adults within 1–5 pp of the central benchmark). Pooled single-family
  households hold 2.20 adults against 2.00 in ACS.
- **From 2013 on, pooling overshoots.** By 2019, 40% of pooled single-family households have several surnames (ACS upper bound
  31%). Pooled households hold 2.76 adults on average, with 16% at 5+ adults against 1.8% in ACS. Secondary families accumulate
  faster than household members can: 16.7m (2006) → 26.4m (2012) → 38.2m (2013). The extra records look like people who are no
  longer there (stale secondary families) or loosely attached, not missing household members.
- **The primary family alone tracks ACS household size well** (2.13 vs 2.02 adults in 2019) but has too many one-adult and too
  few two-adult households (2019: 40 vs 25% one adult). So secondary families do contain the different-surname partners, but in
  later vintages they also contain many other records.
- Mechanical: a household with 6+ adults always has 2+ surnames in DA (one family lists at most 5 persons and families differ by
  surname). Households with 5 adults have *fewer* surnames than the benchmark, because a single 5-person family hits the cap.
- Practical rule: pooling is a reasonable household definition for 2006–2012. For later years, attach a secondary family only
  if it moved in with or after the primary family, or require person-level confirmation (e.g. a recent `recency_date`); check
  before using pooled sizes.

**[full] Primary-family size and other generations vs ACS** (`hh_generations.sql`, 3 min; `code/analysis/dataaxle/hh_generations_vs_acs.R`;
outputs in `output/analysis/dataaxle/hh_surnames/`). Same households as above. ACS sizes count adults 18+ only (children subtracted).
Generations: reference = Data Axle primary person 1 / ACS householder; *child generation* = an adult ≥ 18 years younger, *older
generation* = an adult ≥ 18 years older. Data Axle only where every listed person has an age (45–47% of households in 2009–2012,
63–69% in 2017–2019), so these shares describe a selected subset.

*Adults per household, single-family:* primary family vs ACS

| | 1 adult | 2 | 3 | 4 | 5+ | Mean |
|---|---|---|---|---|---|---|
| DA primary 2012 | 41.9% | 36.8% | 14.2% | 5.1% | 1.9% | 1.88 |
| ACS 2012 | 25.3% | 56.4% | 12.6% | 4.2% | 1.5% | 2.00 |
| DA primary 2019 | 39.6% | 29.6% | 15.4% | 8.8% | 6.6% | 2.13 |
| ACS 2019 | 25.0% | 55.7% | 12.9% | 4.6% | 1.7% | 2.02 |

Multi-unit: DA primary mean 1.26–1.32 adults vs ACS 1.57–1.60; one-adult 78–80% vs 54–55%. Mean adults per primary family
jump at 2013 (1.88 → 2.00, the persons-listed break) and then sit at ACS levels (2.0–2.1) in single-family homes.
- **The shape is wrong in every year: 15–17 pp too many one-adult and 20–26 pp too few two-adult households.** Same-surname
  couples are inside the primary family, but different-surname partners are split off as secondary families.
- From 2013 on, the 3–5+ adult tail grows too large (5+: 6.6% vs 1.7% in 2019), while the mean matches. So the primary family
  is too small at the bottom and too big at the top.

*Other generations, share of households (all ages of reference person):*

| | Child gen. in primary | Child gen. only in a secondary | Child gen., any | ACS child gen. | Older gen. in primary | Older gen. only in a secondary | Older gen., any | ACS older gen. |
|---|---|---|---|---|---|---|---|---|
| Single-family 2012 | 20.2% | 2.7% | 22.9% | 19.4% | 1.5% | 1.3% | 2.8% | 3.4% |
| Single-family 2017 | 29.9% | 5.8% | 35.7% | 19.7% | 2.8% | 3.6% | 6.4% | 3.7% |
| Single-family 2019 | 31.4% | 7.1% | 38.5% | 19.8% | 3.3% | 5.0% | 8.3% | 4.0% |
| Multi-unit 2012 | 6.1% | 2.0% | 8.1% | 9.8% | 0.7% | 1.2% | 2.0% | 2.5% |
| Multi-unit 2019 | 10.0% | 5.4% | 15.4% | 9.9% | 1.3% | 4.6% | 6.0% | 2.9% |

ACS relationships behind the child generation: adult own/step child 18.0% of single-family households, child-in-law or
grandchild 2.1%; behind the older generation: parent or parent-in-law 3.4% (2019).
- **Adult children are mostly in the primary family, not in secondaries.** They share the surname, so Data Axle lists them as
  persons 2–5. In 2012 the primary family alone (20%) matches the ACS share (19%); secondary-only cases add 3 pp.
- **The older generation is split about half and half** between the primary family and secondary families in 2012 (1.5 + 1.3%
  vs ACS 3.4%). That fits parents-in-law and mothers of married women carrying a different surname. So the suspicion holds for
  the older generation, not for adult children.
- **From 2017 on, both are overcounted.** Child generation 36–39% vs 20%, concentrated at reference ages 50–64 (52% vs 30%) and
  65+ (48% vs 18%): adult children who have moved out are still listed. Older generation 8% vs 4%, concentrated at young reference
  persons (18–34: 20% vs 7%, mostly in secondary families), which suggests either the primary person 1 is the adult child of the
  household or the older family is a stale record of former occupants.
- Same pattern as the surname test: **2012 vintages reproduce ACS household composition; later vintages keep people who have left.**

**[full] Do moving primary families leave a stale secondary family behind?** (`stale_secondary.sql`, 14 min; 5% sample of buildings,
all families there, 2006–2021; `output/analysis/dataaxle/stale_secondary/`). For each primary family at building A in t: its status in t+1
(stayed, moved to another street address, moved to a non-street address, vanished from the file) and what is at A in t+1.

| Primary family t→t+1 | n | A has one of their person IDs | A has a *new* family with their surname | …as a secondary | …as a primary | A has any new family |
|---|---|---|---|---|---|---|
| Moved, LOR reset | 2.51m | 0.1% | 1.6% | 0.3% | 1.3% | 68% |
| Moved, no LOR reset | 0.80m | 0.3% | 2.2% | 0.3% | 1.9% | 53% |
| Vanished | 10.08m | 2.4% | 5.4% | 0.5% | 5.1% | 55% |
| Stayed (baseline) | 59.4m | 0.0% | 0.4–1.1% | 0.1–0.3% | 0.3–0.9% | 25–51% |

*New families at an address (not there in t; address present in t), 2007–2020:*

| Origin | Secondary (m) | LOR ≤ 1 | Still there t+1 | Primary (m) | LOR ≤ 1 | Still there t+1 |
|---|---|---|---|---|---|---|
| Surname/person link to a primary that left A | 0.06 (0.7%) | 48% | 54% | 0.57 (6%) | 26% | 75% |
| Shares surname with the staying primary | 0.06 (0.7%) | 65% | 44% | 0.14 | 70% | 49% |
| Brand-new `family_id`, no link | **5.24 (60%)** | 76% | **49%** | 5.25 | 80% | 57% |
| Existing ID, moved in from another street address | 1.09 (12%) | 87% | 63% | 1.81 | 84% | 70% |
| Existing ID, absent or non-street address in t | 2.36 (27%) | **7%** | 66% | 1.73 | 11% | 70% |

- **The stale-secondary hypothesis is not supported.** When a primary family moves, the old address almost never gets a secondary
  family with their surname or person IDs (0.3% and 0.1%, vs 0.1–0.3% for stayers). Only 58k of 8.8m new secondary families (0.7%)
  link to a primary family that just left.
- **New secondary families are mostly brand-new IDs (60%)** with a fresh LOR, and half of them are gone a year later: newly
  compiled, short-lived records, consistent with the oversized pooled households and the excess young/old generations above.
- **27% are existing IDs reappearing after a gap**, with long LOR (only 7% at ≤ 1): the same intermittency as at the address level.
- "Vanished" primaries are partly **re-keyed in place**: 2.4% leave a family with their person IDs and 5.1% a new primary with their
  surname at the same address. The 0.57m new primaries linked to a departed primary have mostly long LOR (26% at ≤ 1), i.e. the same
  household under a new `family_id`, not a move.

### 4.7 [full] Address universe vs CoreLogic tax: unit numbers, 2019

(`code/analysis/dataaxle/units_2019_dataaxle_vs_tax.R`; Bouchet job `units_2019_full.sql`, 1:40 min; outputs in
`output/analysis/dataaxle/units_2019_dataaxle_vs_tax/`.) Unit = `upper(trim(...))`, blank = NULL.

| | Data Axle 2019 family records | CoreLogic `tax` rows, tax_year 2019 |
|---|---|---|
| Records | 185.5m | 134.5m |
| NULL / blank unit | 83.4% | 93.8% |
| Non-null unit | 16.6% | 6.2% |
| Distinct non-null unit values | 134,931 | 233,901 |
| Top values | 1, 2, A, 3, B, 4, C, 5, D, 6 (0.2–0.7% each) | A, 1, B, 2, 3, 4, C, D, 5, 101 (0.06–0.2% each) |

- Both are dominated by short unit labels (1–6, A–D). Tax has more distinct values despite fewer filled records:
  condo unit numbers like 101, 2B, etc.
- The tax table is parcel-based. A rental apartment building is typically **one parcel without a unit**; only
  condos/co-ops carry unit numbers. So tax units mark condo units, while Data Axle units mark dwelling units of any
  tenure. Matching should go building (street address) first, then unit only where the tax side has units.
- Not yet checked: whether `tax` has exactly one row per `clip` per `tax_year`.

**[full] Distinct street addresses, Data Axle 2019 vs tax 2019** (`addr_2019_full.sql`, `decomp_2019_full.sql`; outputs in
`output/analysis/dataaxle/units_2019_dataaxle_vs_tax/`). Loose key = ZIP5 + street name + house number (hashed); strict
key adds directions + suffix (Data Axle 2-letter suffix codes mapped to USPS). Data Axle: `address_type='S'`; tax: rows with
house number, street name and ZIP.

| Key | Data Axle | Tax | Both | % of DA | % of tax |
|---|---|---|---|---|---|
| Loose | 92.1m | 98.6m | 76.5m | 83.1 | 77.6 |
| Strict | 92.9m | 99.6m | 76.3m | 82.1 | 76.6 |

*Naming conventions are not the problem* (24-ZIP extract, all addresses in both sources): both store the street name
without suffix/direction, uppercase, no double spaces. Adding the tax house-number suffix, truncation, direction/abbreviation
cleanup or punctuation stripping changes matches by < 0.1 pp (82.4% of DA, 81.4% of tax matched raw). Suffix spelling
(ST/STREET, AV/AVE) cannot matter for the loose key, which omits the suffix.

*Why Data Axle addresses are missing in tax (2019):*

| Class | m | % of DA addresses |
|---|---|---|
| Matched | 76.5 | 83.1 |
| ZIP not in tax at all | 3.86 | 4.2 |
| ZIP in tax, street not | 4.17 | 4.5 |
| Street in tax, number not: inside a tax house-number range | 0.36 | 0.4 |
| … within ±10 (same parity) of a multi-building/multi-unit parcel | 0.94 | 1.0 |
| … within ±10 of another parcel | 2.62 | 2.8 |
| … other | 3.56 | 3.9 |

- **Coverage gaps are concentrated**: NY (2.66m DA addresses, 64% of NY, in ZIPs absent from tax; only 26% matched) and
  AR (0.80m, 81%; 1.7% matched) account for 90% of the "ZIP not in tax" class. AK is partial (40% matched; Anchorage
  99501 has 49 tax addresses vs 3,233 in DA). High "street not in tax" shares in WI (17%), CT (15%), KY (13%), MI (11%).
- **Address ranges are rare**: 0.7m of 134.5m tax rows have `situs_house_number_2` (some are Hawaiian hyphenated
  numbers, not ranges); only 0.4% of DA addresses fall inside one. Multi-address parcels add at most about 1–4%
  (classes 3b/3c, an upper bound: "near a parcel" is generous).
- 24.7m tax rows (18%) have no usable street address.

*Tax addresses by property indicator, share also in Data Axle:* single family (code 10) 81.4m, 84%; condo (11) 3.0m, 83%;
duplex–quad (21) 2.0m, 84%; apartment (22) 1.2m, 83%; vacant land (80) 3.5m, 2%; agricultural (70) 2.2m, 60%; commercial
(20) 1.9m, 32%; other non-residential (25–28, 50, 90) 2.4m, 18–41%. **Tax exceeds Data Axle because of non-residential
parcels** (about 11m addresses, mostly not in DA). Restricted to residential codes (10, 11, 21, 22: 87.6m addresses), Data Axle
has *more* addresses (92.1m), and 84% of residential tax addresses are in Data Axle.

**[full] New York: the tax table is missing upstate NY in tax year 2019** (`ny_zip.sql`; outputs in
`output/analysis/dataaxle/ny_zip_coverage/`). `tax` rows with an NY county FIPS: 61–62 counties in every tax year 2015–2021
*except 2019*, which has only 6: the five NYC boroughs plus Nassau (36059). Rest-of-NY rows: 4.3–5.3m per year, but 0.41m in
2019 (Nassau only). So the NY "ZIP not in tax" class above is a 2019 tax-year hole, not a structural gap. Use 2018 or 2020
for NY outside NYC, or check other tax years for similar holes (Arkansas is the other large gap).

New York City is fully covered. Official ZIP list: NYS Department of Health ZIP–county FIPS cross-reference (data.ny.gov
`juva-r6g2`, 2007): 320 NYC ZIPs (324 ZIP–county pairs). 2019:

| Borough | Official ZIPs | In Data Axle | In tax | Both | DA addresses | Tax addresses |
|---|---|---|---|---|---|---|
| Bronx | 30 | 29 | 27 | 27 | 101.0k | 83.1k |
| Brooklyn (Kings) | 48 | 44 | 42 | 42 | 257.0k | 261.4k |
| Manhattan (New York) | 149 | 105 | 87 | 85 | 51.1k | 48.1k |
| Queens | 83 | 75 | 69 | 68 | 336.6k | 331.5k |
| Staten Island (Richmond) | 14 | 13 | 13 | 12 | 115.6k | 117.6k |

The ZIPs missing from one side are PO-box/unique-building ZIPs with ≤ 52 addresses, plus 10704/10705. Those are Yonkers
(Westchester) ZIPs that the DOH list also assigns to the Bronx, and Westchester is in the 2019 hole. Address = distinct street
name + house number, so a Manhattan building counts once in both sources.

**[full] Distinct ZIPs, states and counties by year** (`zips_by_year.sql`; `output/analysis/dataaxle/ny_zip_coverage/zips_by_year.csv`).
Data Axle: 37.8–38.5k ZIPs in every year 2006–2023 (2022 partial file still 38.0k), all 51 states. `tax` by `tax_year`:
sparse before 2010 (≤ 2,104 ZIPs before 2008); from 2012 on, 32.4–35.7k ZIPs and 2,705–2,984 county FIPS codes. **2019 dips**
(32.6k ZIPs, 2,826 counties vs 2,945 in 2018 and 2,973 in 2020): the NY hole plus other missing counties. 2025 is partial
and 2026 only starting (3.8k ZIPs). Tax ZIP counts sit about 3–5k below Data Axle's (PO-box / unique ZIPs, missing counties).

**[full] County maps, 2019** (`code/analysis/dataaxle/county_overlap_2019_map.R`, `county_2019_full.sql`; outputs in
`output/analysis/dataaxle/county_overlap_2019/`). DA county = `ge_census_state_2010 * 1000 + ge_als_county_code_2010`
(both USMALLINT); tax county = `fips_code`. Medians over 3,151 counties: overlap (both / either) 58.5%; share of DA
addresses missing in tax 14.7%; share of tax addresses missing in DA 29.8% (residential parcels only: 24.1%).
- **326 counties have Data Axle addresses but no 2019 tax address**: AR 72 of 75, NY 56 of 62, KS 57 of 105,
  SD 31 of 66, MI 16, AK 16, KY 12, VA 12, MT 9, ND 8. These are tax-year holes like NY's (check adjacent tax years).
- **Share of unmatched DA addresses due to missing counties** (2019; 15.58m unmatched of 92.08m): counties with no tax
  address hold 5.43m unmatched = **34.8%** (NY alone 3.0m = 19.2%, AR 0.9m = 5.7%). Counties with tax addresses below 50%
  of DA's add 0.49m (3.1%). The remaining 9.67m (62.1%) are in covered counties, where 11.3% of DA addresses are unmatched.
  (Within "uncovered" counties 8% of DA addresses still match, because DA's geocoded county differs from the tax FIPS
  for border/ZIP-centroid addresses.)
- Partial coverage within covered counties: e.g. Providence RI (44007) 70% of DA addresses missing in tax; Hawaii and
  Honolulu 58–65%; several WI counties (Waukesha 55133, etc.) about 50%.
- Tax addresses missing in DA are highest in rural West/Southwest counties and parts of TX and AK (also among residential
  parcels), consistent with rural route / unaddressed parcels.

**[full] Are unmatched addresses typos? ZIP validity and near misses, 2019** (`typo_2019_full.sql`, 3.5 min on 7 cores;
outputs in `output/analysis/dataaxle/nyc_overlap_2019/`). ZIP list = HUD–USPS ZIP–county crosswalk, March 2019
(`data/hud/zip_to_county/ZIP_COUNTY_032019.xlsx`: 39,461 ZIPs with USPS deliveries, 34,897 with residential addresses).

| | Addresses | ZIP not in USPS list | ZIP has no residential deliveries | Other source has same street+no. in another ZIP (same ZIP3) | …same ZIP+no., street ≤ 2 edits | …same ZIP+street, no. 1 char off |
|---|---|---|---|---|---|---|
| Data Axle, all | 92.1m | 0.01% | 0.08% | | | |
| Data Axle, unmatched | 15.6m | 0.04% | 0.32% | 2.1% | 1.2% | 14.7% |
| Tax, all | 98.6m | 0.01% | 0.18% | | | |
| Tax, unmatched | 22.1m | 0.06% | 0.69% | 1.6% | 0.9% | 12.3% |

- **ZIPs are valid** in both sources; ZIP typos play no role.
- **Street-name near misses are rare (≈1%), and many are not typos.** Examples: 12TH vs 19TH, COUNTY ROAD 406 vs 793 (different
  streets, same house number), AVENUE vs AVENUE K (DA drops the letter), MC ARTHUR/MCARTHUR, PINE DALE/PINEDALE,
  ROCK SPRINGS/ROCK SPRING, abbreviations (HLND, SCH).
- **House-number near misses are common but mostly neighbours, not typos.** Placebo on the 23-ZIP extract (unmatched DA,
  covered areas): a tax-only address 1 character off on the same street 44.7%; opposite parity (cannot be the neighbour
  on the same side) 15.7%; a tax-only address ±2 or ±4 away 18.5%; same digits permuted 7.0%. 87.9% of unmatched DA
  addresses lie on a street that also has unmatched tax addresses. Reading: within covered areas, non-matches are mostly
  **different house numbers for the same structures** (multi-number buildings, corner lots, assessor parcel address vs postal
  address). Digit-permutation typos exist but are a minority.
- CoreLogic CASS-standardises situs addresses: `situs_delivery_point_validation_code` (2021 sample) is Y = USPS-confirmed for 69%
  of rows, NULL 23%, N 5% (county averages 0.79–0.92; AK 0.48).

**NYC by ZIP, 2019** (`code/analysis/dataaxle/nyc_overlap_2019_map.R`; 2010 ZCTAs). 853.7k DA vs 834.3k tax addresses, 749.8k
in both: 87.8% of DA and 89.9% of tax matched; overlap 79.9%. Staten Island, Queens and Brooklyn ≥ 85–95% overlap; Manhattan
is lowest. A third (33.5%) of unmatched NYC addresses have a near-miss partner, rising to most of Manhattan, where tax lots
carry one number of a multi-number building. 10704/10705 (Yonkers) and 11697 (Breezy Point, a cooperative with no
individual lots) are DA-only.

**Street extract: Amsterdam Ave, 10025, 2019** (`nyc_overlap_2019/amsterdam_ave_10025_2019.csv`): 67 house numbers
matched, 29 DA-only, 37 tax-only; no spelling differences. Large DA-only buildings sit inside tax house-number ranges
(711 in 701–715, 755 in 751–755, 972 in 968–974) or are presumably filed under a cross-street address in tax (733, 765, 830,
865, 868, 875; 155–235 family records each). About 20 tax-only 10–36-unit walk-ups sit between matched neighbours. **These
walk-ups are absent from Data Axle in every year 2006–2023** (`amsterdam_walkups_dataaxle_years.csv`; ZIPs 10023–10027,
any street name containing AMST): 0–4 family records per year, against 9–17 each year at the matched controls 932 and 938.
So this is a persistent Data Axle coverage gap for specific buildings, not a 2019 glitch, ZIP assignment or spelling issue.

**[full] What are the Data Axle addresses missing from tax? (2019, `resid_2019_full.sql`, 6.5 min on 11 cores; outputs in
`output/analysis/dataaxle/units_2019_dataaxle_vs_tax/residual_2019/`).** Per DA address: classification and DA attributes.

| Class | m | Families/addr | With units | USPS no-stats | Mailability = 1 | Median LOR | Owner / renter | Also in DA 2017 / 2021 |
|---|---|---|---|---|---|---|---|---|
| Matched | 76.5 | 1.85 | 3% | 3.8% | 99% | 13 | 90 / 2% | 91 / 93% |
| County not in tax | 5.43 | 1.79 | 4% | 6.9% | 97% | 15 | 87 / 3% | 90 / 92% |
| Inside tax range | 0.36 | 2.96 | 14% | 11.9% | 98% | 8 | 41 / 19% | 84 / 86% |
| ZIP near miss | 0.28 | 1.91 | 8% | 15.5% | 75% | 10 | 71 / 7% | 80 / 81% |
| Street near miss | 0.17 | 2.30 | 8% | 9.9% | 90% | 11 | 69 / 9% | 83 / 75% |
| Number one char off | 1.95 | 2.46 | 10% | 12.8% | 94% | 9 | 60 / 12% | 81 / 82% |
| **Street in tax, number missing** | **4.77** | 2.43 | 10% | 10.6% | 95% | 9 | 65 / 11% | 82 / 84% |
| Street in tax only without numbers | 0.34 | 1.52 | 2% | 4.9% | 95% | 15 | 90 / 2% | 89 / 92% |
| **Street not in tax** | **2.20** | 2.17 | 8% | 7.9% | 87% | 7 | 56 / 16% | 81 / 83% |
| ZIP not in tax | 0.10 | 2.08 | 12% | 12.5% | 90% | 8 | 57 / 28% | 84 / 88% |

- `dpbc` (USPS delivery point barcode) is filled for 100% of every class. It is assigned during CASS coding (two digits from the
  house number plus a check digit), so it shows the address was standardised, not that USPS confirmed a delivery point.
- The residual (≈7m: number missing on a known street, or street unknown to tax) are mostly **real, persistent residential
  addresses**: 81–84% also present in DA 2017 and 2021, 87–95% best mailability. But they are more often multi-family/rental
  (more families per address, more unit numbers, more renters, shorter LOR) and more often USPS "no-stats" (8–11% vs 3.8%).
  That fits apartment buildings and complexes the assessor files under one parcel address, plus newer construction.
- **DA's own geocode agrees**: matched addresses are 94% geocode level P (parcel/point), "number missing" only 63% P (25% level 0)
  and "street not in tax" 49% P (28% level 0, 17% ZIP centroid). Level P plausibly means DA itself found the address in a parcel
  source, so a third to a half of the residual also lacks a parcel match in DA's geocoder.

**[full] Nearest tax parcel and ZIP+4 for the residual (2019, `geo_2019_full.sql`, 4.7 min on 16 cores).** DA coordinates of
address-level geocodes (P, 0) vs `parcel_level_latitude/longitude`, nearest parcel on a ~50 m grid. Calibration on 1% of matched
addresses: geocode **P** is precise (median 1 m to its own parcel, 97% within 25 m, nearest parcel = own parcel 93%); level **0**
is coarse (median 28 m, nearest = own only 15%), so read P rows.

| P-geocoded | n | Parcel within 25 / 50 m | Nearest (≤ 50 m) on same street | …within 10 numbers | …and that parcel's address is in DA (multi-number parcel) | Nearest parcel's address in DA, other street (corner) | Shares ZIP+4 with a tax parcel on same street (all geocodes) |
|---|---|---|---|---|---|---|---|
| Matched (calibration) | 705k | 97 / 99% | 99% | 97% | 96% (own) | 1% | 98% |
| Street in tax, number missing | 2.35m | 63 / 83% | 73% | 48% | **35%** | 11% | 50% |
| Street not in tax | 0.41m | 34 / 59% | 1% | 1% | 0% | **30%** | 0% (3% any street) |

- **"Number missing": multi-number parcels explain about a third to a half.** For 35% the nearest parcel is on the same street, ≤ 10 numbers
  away and already carries another DA address; another 11% sit on a corner parcel filed under the cross street. The nearest
  parcel is single-family (59%), apartment (13%) or 2–4-unit (10%). Half share their ZIP+4 block face with a same-street tax parcel. For the
  other half, the block face does not appear in tax at all, and 17% have no parcel within 50 m, which suggests missing or unaddressed parcels
  (partial rolls, new construction).
- **"Street not in tax": a mix of naming differences and missing parcels.** 41% have no tax parcel within 50 m and their ZIP+4 never
  appears in tax (areas absent from the roll). Where a parcel is close it is on another street name, often a variant the
  near-miss test missed: NORTH LA VISTA / LA VISTA, EAST RIDGE / RIDGE, SHADY OAKS / SHADY OAK, WILLIAM 163 at 0 m from ADCOCK 163
  (renamed or dual-named street). The most common DA names in this class are bare numbers (2, 3, 4, 1, 5) and generic words (COUNTY ROAD
  without its number, VILLAGE, PARK, CIRCLE, MANOR), which suggests DA parsing that drops the numeric or descriptive part of the name.

**Synthesis: why the 2019 address universes differ (rough, P-geocode shares extrapolated to all geocodes).**
*Data Axle not in tax, 15.6m:* county not covered by tax 5.4m + low-coverage counties 0.5m (≈38%); same structure filed
under another number (inside a tax range 0.36m, number one character off 1.95m, multi-number or corner parcel ≈46% of the
4.77m "number missing" ≈2.2m) ≈4.5m (≈29%); naming/parsing differences (ZIP or street near miss 0.45m, name variants among
"street not in tax" ≈1.1m) ≈1.5m (≈10%); parcels or areas absent from the roll (rest of "number missing" ≈2.5m, "street not
in tax" with no parcel within 50 m ≈0.9m, tax street without numbers 0.34m, ZIP not in tax 0.1m) ≈3.8m (≈25%).
*Tax not in Data Axle, 22.1m:* non-residential parcels 8.1m (vacant land 3.5, commercial 1.3, agricultural 0.9, other 2.5);
residential 13.9m (single family 12.9, condo 0.5, 2–4 unit 0.3, apartment 0.2). The residential part is **not yet
characterised**: 12.3% of unmatched tax addresses have a DA partner one character off on the same street (the mirror image
of the same-structure class), and the Amsterdam Ave walk-ups show persistent DA gaps for specific buildings.
(Characterised below for single-family.)

**[full] Tax-only single-family addresses (2019, `taxonly_sfr_2019_full.sql`, 4.6 min on 27 cores; outputs in
`output/analysis/dataaxle/taxonly_sfr_2019/`).** Addresses with a code-10 parcel: 81.4m, 68.5m in DA 2019, **12.9m tax-only**.
Ordered classification (first rule that applies):

| Class | m | % of tax-only |
|---|---|---|
| DA has it under another number (DA-only address on the same street one character off, or DA-only P-geocode ≤ 15 m from the parcel point) | 1.76 | 13.7 |
| No structure on the roll (improvement value and living area 0/missing) | 1.65 | 12.8 |
| Built 2018 or later | 0.23 | 1.8 |
| In DA 2017 or 2021, not 2019 | 4.52 | 35.1 |
| Absentee (mailing address elsewhere, not a PO box) or corporate owner | 2.86 | 22.2 |
| USPS vacant or not deliverable | 0.53 | 4.1 |
| Other | 1.33 | 10.3 |

- Compared with matched SFR addresses, tax-only ones are far more often absentee-owned (`owner_occupancy_code` A: 48% vs 14%;
  mailing address differs from situs 67% vs 19%), corporate-owned (13% vs 4%), USPS-flagged vacant (11% vs 0.5%) or not
  DPV-confirmed (Y 81% vs 99%). So many are rentals, second homes or empty houses, i.e. no resident household for DA to find.
- **35% are in DA in an adjacent year** (2017 67%, 2021 73% of that class): DA's yearly file drops occupied-at-other-times addresses,
  consistent with vacancy, turnover between owners/tenants, or DA's household refresh lagging.
- "Other" (1.3m): 42% have a PO-box mailing address. Households receiving mail at a PO box are probably listed in DA under the box,
  not the street address (DA `address_type` ≠ 'S'), a rural pattern; tax-only shares are highest in AK, WY, VT, MT, WV, MS, NM, ID.
- `homestead_exempt_indicator` is only ever TRUE or NULL (uninformative as coded).

**[full] Street-level match shares (2019, `streets_2019_full.sql`, 1.7 min; `code/analysis/dataaxle/streets_2019_missing_shares.R`;
outputs in `output/analysis/dataaxle/streets_2019_missing_shares/`).** Street = ZIP5 + street name. Keeping streets with > 10 addresses
(union of both sources) drops 53% of streets (3.07m) but only 11% of addresses (10.5% of DA, 10.5% of tax). Kept: 2.68m streets,
of which 2.44m in both sources, 0.10m DA-only streets in ZIPs that tax covers (3.1m DA addresses), 0.10m streets in ZIPs absent from
tax (3.2m), 37k tax-only streets (0.9m).
- On streets in both sources: 55% have no DA address missing from tax; 13.5% have no tax address missing from DA; 42% have both
  kinds of non-match, 3% only DA-only, 45% only tax-only. The difference (DA missing share − CL missing share) peaks at 0 and is mostly
  negative (tax has extra non-residential and unoccupied parcels). Correlation of the two shares 0.34.
- **Pairing bound:** of the 6.69m DA-only addresses on shared streets, at most 3.85m (57.5%) can be paired one-to-one with a tax-only
  address on the same street (sum of min(DA-only, tax-only) per street); 3.13m (47%) with a residential tax-only address. So
  "same property, different number" is capped at about half of the within-street DA mismatch, and DA addresses on streets or in ZIPs
  missing from tax (6.3m on kept streets) cannot be pairs by construction.

**Breakdown with evidence levels (2019; supersedes the rough synthesis above).** Evidence: *direct* = a field or coverage fact
that is itself the cause; *candidate* = a plausible counterpart record exists (nearby number, near-miss spelling, nearby parcel,
other DA year) but sameness is not proven; *correlate* = attributes consistent with the cause; *unverified* = no test possible
(DA geocode too coarse for the parcel test).

*Data Axle addresses not in tax (15.6m):*

| Cause | m | % | Evidence |
|---|---|---|---|
| County has no tax addresses | 5.43 | 34.8 | direct |
| Inside a tax house-number range | 0.36 | 2.3 | direct |
| No tax parcel within 50 m (P-geocoded) | 0.57 | 3.7 | direct |
| Tax has the street, but only parcels without house numbers | 0.34 | 2.2 | direct |
| ZIP absent from tax (covered county) | 0.10 | 0.6 | direct |
| Tax address on the same street one character off | 1.95 | 12.5 | candidate (placebo: mostly neighbours/same structure, ~7% digit permutations) |
| ZIP or street-name near miss | 0.45 | 2.9 | candidate |
| Multi-number parcel (P: same street, ≤ 10 numbers, parcel address in DA) | 0.82 | 5.3 | candidate |
| Corner parcel filed under the cross street (P) | 0.38 | 2.4 | candidate |
| P-geocoded, parcel nearby but no link found | 0.99 | 6.4 | ambiguous |
| Not testable (geocode level 0/Z/other) | 4.21 | 27.0 | unverified |

Direct 6.8m (44%), candidate 3.6m (23%), ambiguous 1.0m (6%), unverified 4.2m (27%). If the untestable addresses behaved like
the P-geocoded ones, they would add ≈1.6m same-property cases and ≈1.1m missing parcels; likely an upper bound for same-property,
since a failed DA parcel geocode itself suggests no parcel. "Low-coverage" counties (0.49m) are inside these classes.

*Tax addresses not in Data Axle (22.1m):*

| Cause | m | % | Evidence |
|---|---|---|---|
| Non-residential parcel (vacant land 3.46, commercial 1.32, agricultural 0.88, other 2.47) | 8.14 | 36.9 | direct |
| SFR: no structure on the roll | 1.65 | 7.5 | direct |
| SFR: built 2018+ | 0.23 | 1.0 | direct |
| SFR: USPS vacant or not deliverable | 0.53 | 2.4 | direct |
| SFR: DA has a DA-only address one character off / DA point ≤ 15 m | 1.76 | 8.0 | candidate |
| SFR: in DA in 2017 or 2021, not 2019 | 4.52 | 20.5 | candidate (DA intermittency; see gaps job) |
| SFR: absentee (mail elsewhere, ~90% coded A) or company owner | 2.86 | 13.0 | correlate (rental/second home/empty) |
| SFR: other (42% PO-box mail) | 1.33 | 6.0 | unexplained (PO box: correlate) |
| Condo, 2–4 unit, apartment | 1.04 | 4.7 | not characterised |

Direct 10.6m (48%), candidate 6.3m (28%), correlate 2.9m (13%), unexplained/not characterised 2.4m (11%). Note 4.2m tax-only SFR
parcels have mail at the property and code O (owner-occupied); these fall outside the absentee class by construction.

**[full] Pooled address universes, 2012–2021** (`tm_pooled.sql`, `tm_pooled_tax.sql`, 8 + 19 min; `code/analysis/dataaxle/address_universe_treemap_pooled.R`;
outputs in `output/analysis/dataaxle/address_universe_treemap/`). Same classification as the 2019 breakdown, on all street addresses seen in any
year 2012–2021; **matched = same loose key in both sources in any year** (attributes from the latest year seen). Data Axle 109.8m, tax 112.8m,
**matched 95.6m (87% of DA, 85% of tax)**; 94.9m of these also share a year.

*Data Axle never in tax (14.2m):* coarse geocode, not testable 4.96m; other number on the same parcel/street (1 char off, multi-number, corner,
range) 4.10m; parcel or area missing from the roll 2.33m; parcel nearby, no link 1.51m; county not in tax 0.90m (5.4m in 2019 alone: upstate NY
and AR are covered in other years); ZIP/street near miss 0.43m.

*Tax never in Data Axle (17.2m):* non-residential, no structure, absentee/company owner or USPS vacant 14.5m (vacant land 4.39, absentee/corporate
3.25, other non-residential 2.52, no structure 1.68, commercial 1.25, agricultural 0.90, USPS vacant 0.53); DA address one character off / DA point
≤ 15 m 1.24m; other single-family 1.19m; built 2020+ 0.21m; other condo/2–4/apartment 0.05m.
- Pooling raises the match rate from 83/78% (2019) to 87/85%: Data Axle intermittency (an address missing in one year) and tax-roll
  coverage gaps (upstate NY in 2019) mostly wash out over ten years.
- What remains on the tax side is overwhelmingly parcels nobody lives in as a primary residence; only 1.2m single-family parcels are
  unexplained. On the DA side, two thirds are either the same property under another number or untestable.

**[full] Ownership flag vs mailing address, SFR parcels 2019 (`occ_sfr_2019_full.sql`; `taxonly_sfr_2019/occ_sfr_2019.csv`).**
`owner_occupancy_code` A covers 89% (mail elsewhere, same state), 91% (other state), 71% (company owner) of tax-only parcels; O covers
97% where mail goes to the property; 70% of PO-box parcels have no code. The flag is essentially CoreLogic's own situs-vs-mailing
comparison. Absentee ownership is common among *matched* SFR too (10.2m A-coded in DA vs 6.2m not), so rentals with tenants are in
DA; 4.2m tax-only SFR parcels have mail at the property and code O.

**[full] Addresses dropping in and out of Data Axle, 2006–2023 (`gaps_full.sql`, 11 min on 7 cores; outputs in
`output/analysis/dataaxle/dataaxle_gaps/`).** 2022 excluded (truncated file). Address key as above.
- Of 120.1m addresses ever in DA, 48.4m (40%) are missing in at least one year between their first and last appearance
  (16.1m one year, 9.7m two, 11.5m five or more). Single-year holes (present year before and after, absent in Y): 1.9–4.2m per
  year, 2–5% of the previous year's addresses; 1.86m in 2019. 2010 has the most holes (4.2m single, 14.5m any).
- **Tax SFR 2019 not in DA 2019 (12.9m):** never in DA in any year 3.53m; in DA before and after 2019 4.82m (1.27m in both 2018
  and 2020); only before 2019 3.34m; only after 1.20m.
- **Family IDs across a single-year hole (2% address sample, all hole years pooled; continuous addresses for comparison):**

| | Hole | Continuous |
|---|---|---|
| Primary families at the address the year after: same ID as the year before | 39% | 78% |
| …brand-new ID (first year in DA) | 39% | 8% |
| …ID absent from the whole file in the hole year | 84% | 11% |
| Primary families at the address the year before: absent from the whole file in the hole year | 77% | 10% |
| …present the year after (anywhere) | 60% | 84% |
| …of those, back at this address | 66% | 93% |

  The hole is at the family level: the household vanishes from DA for a year (not relisted elsewhere), and the address returns
  either with the same ID (≈40%) or with a new ID never seen before (≈40%, vs 8% of normal year-to-year turnover). Whether these
  new IDs are the same household re-keyed (same surname) or real turnover after a vacancy is tested next.
- **Surname check (`gaps_surnames.sql`, 15 min on 7 cores; `gaps_surnames_*.csv`).** All surnames (last_name_1–5, md5-hashed in the
  query) of all families at the address the year before vs the year after. Placebo: the next sampled address in the same ZIP/year.

| | Hole (786k episodes) | Continuous (22.3m) |
|---|---|---|
| Any family ID shared, before vs after | 43% | 90% |
| Any surname shared | 45% | 91% |
| Any person ID shared | 41% | 90% |
| Surname shared when no family ID is shared | 4.3% | 10.0% |
| Placebo: surname shared with another address | 0.2% | 0.3% |

  Primary families after the hole with a brand-new ID: 3.7% share a surname with the before set (normal-turnover new IDs 5.7%; placebo
  0.15%). Same-ID families: 99%. **So new IDs after a hole are new people, not re-keyed households.** A one-year hole is real
  turnover with a missing year in between (vacancy, or DA losing the outgoing household before picking up the incoming one); surnames
  add only ≈2 pp of continuity beyond family IDs. DA family IDs are reliable across holes for staying households.
- **Where do movers-in after a hole come from? (`gaps_origin.sql`, 3.8 min on 8 cores; `gaps_origin*.csv`).** Primary families at the
  address in t (first year after the hole) that were not there in t−2, traced by `family_id` (DA annual files have no previous-address
  field). Comparison: same definition at addresses without a hole (includes families that arrived in t−1).

| | After a hole (491k) | No hole (6.1m) |
|---|---|---|
| ID never seen in DA before t | **64%** | 36% |
| Seen in t−1 / t−2 / either | 24% / 21% / 26% | 57% / 23% / 58% |
| Seen only 3+ years before t | 10% | 6% |
| *Origin at last sighting (of those seen before):* same address (returning) | 25% | 77% (mostly arrived in t−1) |
| same ZIP / same county / same state / other state | 22 / 26 / 18 / 11% | 6 / 9 / 6 / 4% |
| non-street address (PO box, route) | 3% | 1% |
| Length of residence ≤ 1 at t / > 5 | 66% / 14% | 45% / 20% |

  Two thirds of post-hole movers-in have no DA history, so their origin is unobservable; 73% of these report LOR ≤ 1, consistent with
  genuinely new arrivals (new households, or people DA had not covered before). A quarter can be traced to t−1 or t−2, mostly local
  moves (same ZIP or county). A quarter of those with history are the *same* family returning to the address after ≥ 2 years away from it,
  keeping their ID.

**Who supplies the rolls? (tax_year 2021, `juris_2021_full.sql`)** No field names the supplying government; `filename` is
CoreLogic's national bulk delivery file. Codebook: `last_assessor_update_date` = date CoreLogic applied a full roll *for a
county*; `taxroll_certification_date` = assessor-certified date of the full *county* refresh. Per county: 2,933 of 2,983
counties (137m of 143m rows) have one modal certification and update date for ≥ 99% of rows (a single county roll). Only
27 counties (ME 8, IN 7, SD 4, …) have municipalities with their own dates. CT and MA counties carry 6 certification
dates (2020–2025) within tax_year 2021, but the same dates statewide and not split by town (CT: Nov 2 each year; MA: Nov 9).
That points to a statewide delivery calendar, not town-by-town delivery. Even in town-assessment states (NY, NJ, MI, VT, WI)
each county has one date. **The data identify CoreLogic's processing unit (county), not the producing government.** Which
level actually assesses has to come from outside, e.g. the Lincoln Institute's *Significant Features of the Property Tax*
(assessment administration by state).

### 4.6 [full] Structure, geocodes, fill rates, censoring — all years (`output/dataaxle_exploration/full/`)
- **`location_id` (T1), every year:** one row per `family_id`; no family maps to two `location_id`s; no
  `location_id` has 2+ primaries; every primary has `location_id = family_id`, and no subfamily does.
  `location_id` is NULL iff `address_type` is NULL. Families per location rise steadily (one-family
  locations 86.8% in 2006 → 69.6% in 2017 → 64.1% in 2019). **2022:** 10.1 m `location_id`s without
  their primary — the partial file keeps subfamilies whose primary is missing.
- **Subfamily surnames (T2):** same surname as the primary 0.45% (2006) → 0.12% (2017) → 0.09% (2019+).
  2017→18: subfamilies at an address with a different primary household jump 11.9% → 16.9%.
- **Geocodes (T4):** Z (ZIP centroid) in 2006–17 **and 2019**; X instead in 2018 and 2020–23. In 2018
  and 2020+ level 4 also absorbs PO boxes and records without an address. Decimal places separate the
  levels: P = 6 decimals (~98%), Z/X = 4. **2018: 11.5% of level-P coordinates have only 5 decimals** —
  a different geocoding vintage in exactly the treatment year.
- **Fill rates (T6):** no empty strings anywhere; `unit_num` 14.1% (2017) → 16.8% (2018) jump.
- **Censoring (T7):** bottom codes `find`=5000 3.3–5.9%, top codes 0.13–0.51%. `estmtd_home_val`=0 for
  18–26%; among those, owner/renter ≤3 (renter-ish) is 49–55% up to 2018 and **83% from 2019** (model
  change). `wealth_finder_score` range shifts by vintage (2006–09, 2010–13, 2014–17, 2018+).

## 5. Open questions / next steps

1. **Does `family_id` persist across moves?** How often does `length_of_residence` reset on an address change?
   → household 1% sample (`dataaxle_annual_hh1pct.parquet`, batch job running).
2. **Moves between good and non-good addresses** (§3.2 definition): share of address changes where origin or
   destination lacks a street, number or unit, or `dpbc`. → household sample.
3. **Spurious geography changes in 2017→2018** for families at an unchanged address (§3.3). → household sample.
4. **Family vs household size** compared with Census (ACS by county). → summed persons per `location_id`, full data.
5. ~~Partner hypothesis~~ — done on CT 2017 (§4.2a): partly confirmed. Full data pending.
6. **2022:** the unzipped library copy used for the import is not on Bouchet. The database built from it still
   has only 53.6 m rows, so either that copy is also truncated or `ignore_errors` dropped many rows. Batch job
   `gz2022` result: the annual 2022 `.gz` decompresses **cleanly** to 53,647,370 lines (no gzip error) =
   the database count. **The delivered 2022 annual file itself is partial.** The four 2022 quarterly
   files are intact (154–171 m lines each, 97 fields). Options: rebuild 2022 from a quarterly file, or bridge 2021→2023.
7. Which quarterly snapshot does the annual file correspond to? Compare 2015 annual with the 2015 quarterlies.
8. Validate `find` upper tail against IRS SOI by county.

---

## Appendix: samples and performance

- Saved samples (`~/ptax_explore/samples/`, names dropped):
  - `dataaxle_annual_system1pct.parquet` — 1% block sample, 26.3 m rows, 88 cols; cross-section only (blocks
    of ~2,048 consecutive rows, so shares can be off by a few points). Built in 26 min.
  - `dataaxle_annual_hh1pct.parquet` — all years for families with `md5_number(family_id::VARCHAR) % 100 = 0`,
    sorted by family and year; for panel work. Done (2 h 8 min, 0.89 GB).
  - `idrange_panel.parquet` — panel sample: all years for families in 200 evenly spaced narrow
    `family_id` ranges (0.5% of the ID space). Address and geography columns only. 0.46–1.16 m families
    per year. Fast to build because rows are sorted by `family_id` within each year (one range: 2.8 s;
    200 OR'ed ranges: 319 s). Caveat: IDs are assigned in blocks over time, so the yearly coverage jumps
    in 2014 and 2020 and older cohorts are over-represented. **2022 contains no IDs from some blocks**,
    consistent with a truncated, ID-sorted source file.
  - `ct2017.parquet`, `ct2017_addr.parquet` — full CT 2017 cross-section (surname as MD5 hash).
- Speed: first touch of a column from GPFS dominates (one column over 2.66 bn rows: ~100 s cold, ~2 s cached).
  Queries on the saved samples run in under 1 s.

### 4.8 Address-level exit from surnames (SALT-cap project, 2026-10-16)

Built for `code/analysis/salt_cap/` on the 1% building sample (`sql/01_build_panel.sql`): an address "exits" in file year t if no
surname present at the address (building + unit key) in t appears in t+1 or t+2. Validation against arm's-length sales of
owner-occupied single-family parcels (`output/analysis/salt_cap/table_exit_validation.csv`):
- Using **all families' surnames** at the address, exit is flagged (in t−1 or t) for 58–63% of sales in 2012–15 but only 45–51% from
  2016 on; P(exit_t | sale) drops from ~50% to 22–35%. Stale secondary records keep the seller's surname at the address after the sale
  (cf. §4.2, §4.5c on the 2017+ overshoot of pooled families).
- Using **primary-family surnames only**, 80–87% of sales are flagged in every year, with a 4–6% false-positive rate (addresses with no
  sale in t−1…t+2 and an unchanged owner on the roll) and an annual exit rate of 8–11% (roll owner turnover 5–6%): the difference is
  re-keying/record churn.
- Condo units match Data Axle at unit level for only 52% of parcels (SFR 82% at building level); 35–44% of condo address-years have no
  exit value.

### 4.9 Local tax jurisdictions in the CoreLogic tax roll (2026-10-20)
Candidate fields in `cl.tax`: `tax_area_code` (tac), `tax_rate_area_code`, `municipality_code`, `municipality_name`,
`tax_district_municipality_name`, `township`, `school_district_name`, `total_property_tax_rate_percent`. All are
county-specific codes or names, so a unit is (fips, value). 1% building sample, 2016 base year (949k residential parcels,
`data/salt_cap/panel/parcels.parquet`):

| field | fill % | distinct (fips, value) | states | counties |
|---|---:|---:|---:|---:|
| tax_area_code (∪ tax_rate_area_code) | 90.3 | 51,941 | 51 | 2,737 |
| municipality_name | 83.6 | 30,583 | 53 | 2,615 |
| municipality_code | 45.3 | 20,723 | 49 | 1,254 |
| school_district_name | 64.2 | 10,039 | 46 | 2,103 |
| township | 35.4 | 8,446 | 35 | 1,614 |
| tax_district_municipality_name | 7.2 | 1,628 | 23 | 316 |

- The tax area code is a *rate area* (a unique overlap of districts), not a government: CA alone has 12.4k codes in the 1%
  sample, TX only 344 (TX codes are coarse; there `school_district_name` gives 1,153 districts at 99% fill). Not filled at
  all in MA, RI, MI (also noted in the `corelogic` skill); there `municipality_name` (≈100% fill) is the town/township.
- `municipality_name` is the closest thing to a general-purpose government; it is ≥ 95% filled in most states but only
  15% in MD, 33% in CA, 26% in ID, 44% in OR. Names are not cleaned (spelling variants inflate counts).
- The published rate (`total_property_tax_rate_percent`) is a usable panel only in NJ (2008+), CT, MA, NH, RI (2014+, ≈95%
  of parcels), ME (≈65%), DC (2009–16 only) and IL (≈40%, patchy). MO 15–20%, WA/PA/FL ≤ 6%, others ~0 (1% system sample
  of the full SOM roll, all years). [An earlier version of this line said "18 states": that counted fill among non-null
  values only.] Elsewhere rates must be computed from bills and values (§4.11–4.12).
- Benchmark (2017 Census of Governments): 3,031 counties, 19.5k municipalities, 16.3k townships, 12.8k school districts.
- [full] Full roll (SOM job 3690559, `code/analysis/local_juris/sql/juris_counts.sql` → `data/local_juris/juris_cells.parquet`,
  county × year × field cells with parcel counts and bill/value sums). Distinct (fips, value), 2016 / ≥ 50 parcels in 2016:
  tax_area_code 138,085 / 73,444; tax_rate_area_code 105,178 / 48,022; municipality_code 48,143 / 27,444; municipality_name
  46,831 / 40,106; township 21,165 / 11,042; school_district_name 13,282 / 11,811; tax_district_municipality_name 2,056 /
  1,897; counties 2,906. `tax_year` has junk values (1991–15500): filter years before use.

### 4.10 Parcel coordinates for assigning parcels to jurisdictions (2026-10-20)
`parcel_level_latitude/longitude` in `cl.tax` (parcel centroid). 1% building sample, 2016
(`code/analysis/local_juris/R/01_coord_validation.R` → `output/analysis/local_juris/coord_validation.txt`):
- **Fill 94.5%**, ≥ 5 decimals (≈ 1 m) in 99%. 94.6% of points unique; 4.7% shared only by condo units of the same building;
  0.7% shared across 2+ buildings (likely street/ZIP centroids). Missing is concentrated where towns tax: NH 41%, VT 36%,
  LA 34%, SD 29%, ME 28%, ND 25%, CT 21%, RI 20%, AR 15%, MA 14%, KY/MS 12%, MI 9%; ≤ 5% in most other states.
  (Other roll years or Data Axle P-geocodes, which agree with these to a median 1 m (§4.7), could fill some gaps.)
- **County point-in-polygon (cb 2019 500k) vs roll fips: 98.9% same.** Nearly all of the rest are roll codes, not
  coordinates: Miami-Dade under its pre-1997 FIPS 12025 (= 12086; 9.1k of 9.6k) and VA independent cities coded to the
  surrounding county (Manassas, Fairfax City, Falls Church, Poquoson, Bedford). Genuine misplacements ≈ 0.03%.
- **Incorporated places (cb 2017, LSAD ≠ 57) vs `municipality_name`**: 47% inside a place with matching name, 11.5% inside
  a place with a different name, 41% outside any place. Mismatches are mostly what the roll stores, not location errors:
  Chicago's townships (Lake View, Jefferson), NYC neighbourhoods, DC assessment neighbourhoods, DE hundreds, IN townships
  inside Indianapolis/Carmel, postal city names in CA. Name match ≥ 95% in MA, NH, RI, ME, CT, TX, PA, NJ, MD, MI, NV, WI.
- **Unified school districts vs `school_district_name`**: 87% name match overall (≥ 96% TX, FL, PA, NC, VA, GA, CO, AZ, OK,
  WV, UT, NV); remaining failures are abbreviations (NYC DEPT OF ED, Springfield R-XII) and VT/ME supervisory unions.
  LA 2.5% (roll field holds something else).
- **Boundary proximity** (50k points, 1:500k place boundaries): 0.3% within 10 m, 1.2% within 25 m, 3.0% within 50 m,
  6.5% within 100 m. The 1:500k generalisation error is of the order of tens of metres, so use full-resolution TIGER/Line
  for assignment; points within ~25 m of a boundary (≈ 1%) remain ambiguous.

### 4.11 Recovering nominal rates: median regression bill ~ a + r·AV within tax-area cells (2026-10-20)
Test on the 1% sample (2016 residential, cells = fips × tac with ≥ 30 parcels: 5,438 cells, 669k parcels), slope vs the
published `total_property_tax_rate_percent` (units: percent in NC, NJ, IN, IL, MO, DC; mills elsewhere; VA field unusable)
(`code/analysis/local_juris/R/02_nominal_rate_test.R` → `output/analysis/local_juris/nominal_rate_test.txt`):
- **Recovers the published rate exactly** (median slope/published = 1.000; 87–100% of cells within 5%) in NJ, CT, NH, ME,
  NC, DC, MO, WA. Intercepts are negative in about half the cells (lump-sum exemptions) and positive elsewhere (fixed charges).
- **Cook County: slope/published = 2.803 = the 2016 state equaliser** (CoreLogic AV is pre-equalisation; the statutory
  rate applies to EAV). Other IL counties 0.93–1.00. So the slope is the rate per dollar of *CoreLogic AV*, which is what we
  want for bills, but it is not the statutory rate where an equaliser/fractional step sits between AV and the taxable base.
- **Proportional relief biases the slope down**: OH 0.883 ≈ 1 − 0.125 (10% + 2.5% owner-occupied rollbacks); IN 0.46
  (60% homestead deduction plus 1% circuit-breaker caps).
- **Mixtures**: FL pooled 0.83, but non-homestead cells 1.00 and homestead cells 0.91 — the homestead share rises with AV,
  so a single intercept does not absorb the exemption. Estimate within exemption status (or add the flag as an intercept
  shift) where the homestead flag or a bill discount identifies it.
- **Cells that are not rate areas fail**: MA, RI (no tac → cell = county; published town rate varies within the cell in
  86–100% of cells), PA (published rate varies in 50% of cells).
- Most states have few qualifying cells at 1%; the full roll gives enough parcels per tac.

### 4.12 Which states give a clean flat-rate fit, without a published rate (2026-10-20)
Same median regression, cells = fips × tax area code (fips × `municipality_name` where there is no code: MA, RI, MI).
Test: share of parcels whose bill is within 1% of the cell's fitted line. In the states validated against the published
rate (NJ, CT, NH, ME) this is 91–99%. 1% sample, 2016, cells ≥ 30 parcels
(`code/analysis/local_juris/R/03_rate_fit_all_states.R` → `output/analysis/local_juris/rate_fit_all_states.txt`).
- **≥ 80% on the line (flat rate recovered):** ME 99, NH 99, RI 98, MA 96 (town cells), VA 93, NJ 92, CT 91, WY 88,
  TN 83, CO 82, UT 80.
- **60–79% (mostly works; coarse cells or an exemption mixture):** VT, NC, WA, NE, KS, OR, PA, MO, KY.
- **< 50% (no single line):** CA 49 (84% within 5%: parcel-specific direct assessments on the bill), WI, LA, OH (proportional
  rollbacks), HI, IL (classification, exemptions), MN (class rates), AL, DE, NM, AK, MT, IA, MD, OK, SC (4%/6% ratios), SD,
  MS, TX 25 (percentage homestead exemptions, coarse codes), NY 23 (classes, STAR), AR, ND, GA, AZ, ID, IN (deductions,
  caps), FL 14 (homestead exemption and SOH mixture), MI 14 (taxable value ≠ SEV, PRE exemption), NV 9 (abatement caps).
- A low share does not mean the slope is wrong for every parcel (e.g. FL non-homesteads are on the line); it means one
  line does not describe the cell. Next step: split cells by exemption/homestead status or the bill discount.
- **Split by exemption status** (`04_rate_fit_split.R` → `rate_fit_split.txt`; cell × homestead flag, × occupancy code O,
  or both): large gains in OK 33→81, PA 71→83, IL 45→70, CA 49→67, OH 46→66, MS 27→67, AL 43→70, WV 51→79, VT 74→97,
  NM 40→62, GA 20→44, FL 14→32. Still < 50%: TX, NY, AZ, IN, MI, NV, ND, FL, MD, DE, MT, SD, ID, AR. Within-cell slope
  homestead / non-homestead (proportional relief): HI 0.52, IN 0.61, MI 0.75, ID 0.88, MD 0.91, FL 0.98; = 1 elsewhere.

### 4.13 Where does nominal-rate variation sit? State / county / place / unified district (2026-10-20)
1% sample, 2016 (`code/analysis/local_juris/R/05_rate_variance_decomp.R` → `output/analysis/local_juris/rate_variance_decomp.{txt,csv}`;
parcel layers in `data/local_juris/parcel_layers_1pct.parquet`).
- **Rate:** per tax-area cell, median-regression slope on non-homesteads (≥ 30 parcels, ≥ 50% of bills within 1% of the line),
  else the pooled slope if it passes. Market-value rate = rate on AV × median(AV/MV) × sales ratio `ratio_c` (CoreLogic MV
  = AV in IL, LA, NY, MI, so MV alone is not market value). 750 of 3,654 fitted cells dropped (market-value rate outside
  0.05–6% or no sales ratio). Kept: median 1.25% of market value (p10 0.69, p90 2.64).
- **Coverage (% of parcels):** coordinates 94.5; inside an incorporated place 55.3 (the binding layer); inside a unified
  district 87.2; all four layers 50.9; cell rate 32.7; **rate and all four 19.0** (180k parcels); 28.6 if unincorporated land
  counts as a county-level "place". By state, rate and all four: MA 31, IL 27, MN 27, OH 26, CA 24, NJ 24, CT 16, PA 15,
  FL 7.5, TX 7, NY 3, MI 0.6, HI/AK 0 (no place/unsd maps).
- **Decomposition of the market-value rate (parcel-weighted, nested R² increments):** state 69%, county 23%, below county
  7.6%, residual within place × district 0.3%. Within states only: county 74%, below county 25%.
- **Place vs school district is not separately identified here:** whichever enters first takes almost all of the
  below-county share (place first 7.6 / 0.1; district first 0.4 / 7.3). Shapley shares are ≈ 25% for every layer because
  the layers nest (a place dummy alone also absorbs its county and state), so they are not informative.
- Caveats: (i) the rate is the *total* rate of a tax-area cell, so the decomposition says where total-rate variation sits
  geographically, not which government levies it; (ii) only big tax areas pass the ≥ 30-parcel rule at 1%, which
  understates fine-grained (special-district) variation, so the residual is small partly by construction; (iii) the sample
  leans to states where the flat-rate fit works (TX, FL, NY, MI barely enter). Full roll needed for (ii)–(iii).

### 4.14 Parcels near a state border (2026-10-20)
Distance from parcel points to the nearest boundary shared with another state (lower 48 + DC; coastlines and international
borders excluded; cb 2020 1:500k, EPSG:5070). 1% building sample, 890k parcels with coordinates
(`code/analysis/local_juris/R/07_state_border_distance.R` → `output/analysis/local_juris/state_border_distance.txt`).
Within 5 mi 8.7%, 10 mi 16.0%, 20 mi 28.6%, **30 mi 38.8%**, 50 mi 53.9%, 100 mi 76.6%. Within 30 mi by state: DC, DE, RI
100%, NH 96, MD 96, VT 94, MA 89, NV 87, CT 85, NJ 83, WV 78, MO 74 … NY 58, PA 66, IL 68 … FL 10, TX 8, CO 8, AZ 6, MI 6,
CA 1. Border parcels are concentrated in the Northeast corridor and metros on state lines (NYC–NJ, Philadelphia, DC,
St. Louis, Kansas City, Chicago–NW Indiana, Cincinnati, Louisville, Memphis, Portland–Vancouver; NV's 87% is Reno and
Henderson/Las Vegas, which lie within ~30 mi of the CA and AZ lines).

### 4.15 Border rate gaps with CoreLogic-provided rates, 2013–2021 (2026-10-20)
Parcels within 20 mi of a shared state border (1% building sample, 257k parcels; side polygons per state pair from
`R/08_border_pair_buffers.R`, assignment `R/09_border_parcels_1pct.R`); yearly rows from SOM job 3692148
(`cluster/e_border_1pct.sql`, 80 s). `R/10a_border_cl_gaps.R` → `output/analysis/local_juris/border_rate_gaps_cl.txt`,
`border_pair_gaps_cl.csv`, `figures/border_rate_gaps_cl_box.png`.
- Unit of `total_property_tax_rate_percent` differs by state: **fraction in VA** (0.010 = 1%), percent in NJ, IL, MO, DC,
  mills elsewhere; chosen per state-year as the unit closest to the median bill/AV. VA's value is 1.000% for every parcel
  that has it (≈ 1.7k sample parcels, all years) — likely a placeholder; treat with caution.
- **Coverage: 16.5% of border parcel-years in 2013, 20–23% in 2014–21.** Usable states: CT, MA, RI (2014+), NH, NJ, DC
  (2013–16), ME ≈ 80%, IL ≈ 65%, MO ≈ 38%, OH 7%, WA 2%, PA 1.4%, VA (above).
- **Only 4–6 state pairs per year** have ≥ 30 sample parcels with a CL rate on both sides: CT–MA, MA–RI, MA–NH, CT–RI,
  ME–NH, DC–VA. 2016 gaps: CT–MA 1.77 pp (3.53 vs 1.76%), MA–RI 0.70, MA–NH 0.69, CT–RI 0.67, ME–NH 0.59, DC–VA 0.11; median
  across pairs 0.6–0.75 pp in every year. These are nominal rates per dollar of assessed value (all six states assess near
  full value, so comparable here). Too few pairs for a national picture; the imputed rates are needed for that.

### 4.16 Border gaps in effective rates (bill / sale price), 2013–2021 (2026-10-20)
Border parcels (≤ 20 mi, 1% sample) that sell arm's-length in the year (SALT panel; new construction dropped), effective
rate = bill of the sale year / price, kept if 0.05–10% (`R/11_border_effective_gaps.R` →
`output/analysis/local_juris/border_rate_gaps_effective.txt`, `border_pair_gaps_effective.csv`,
`figures/border_rate_gaps_effective_box.png`).
- Coverage: 3.4% (2013) – 5.0% (2021) of border parcels per year; 75–104 of 212 pair sides have ≥ 30 sample sales.
- **21–41 state pairs per year** (vs 4–6 with CL rates). Median |gap| of side means 0.69 pp (2013), 0.41 (2016), 0.36
  (2021); p75 0.95 → 0.52; max 1.2–1.65. 2016 largest: NY–PA 1.39 (NY 4.0 vs PA 2.7%), NJ–PA 1.34, NJ–NY 1.12, CT–MA 1.00,
  IN–OH 0.99, IL–MO 0.94, MD–PA 0.87, DE–PA 0.85, IL–IN 0.81.
- The decline is partly entry of new pairs and partly levels: in the 16 pairs present every year the median gap goes
  0.74 → 0.40 pp while the mean effective rate falls 1.72 → 1.25% (prices outpace bills); the gap relative to the level is
  0.41 → 0.28 (≈ 0.31–0.39 in 2014–20). Side means rest on 30–500 sample sales, so year-to-year noise is large.
- Bill/price is a sale-based measure: sellers' bills reflect their (often stale) assessment, so in states with assessment
  caps or rare revaluations the effective rate of sold homes is below that of the stock.
- Why only 21–41 pairs (2016 diagnosis): 106 pairs, 101 with sample parcels on both sides; the thinner side of the median
  pair has 497 sample parcels but only 13 usable sales (p25: 1). ≥ 30 sales on both sides: 35 pairs; ≥ 10: 58; ≥ 5: 67.
  Stock-based measures (≥ 30 parcels per side) would reach 92 pairs. Two causes: 1% × ~4% annual turnover, and price
  non-disclosure states (border parcels selling with a usable price in 2016: SD 0%, NM 0.5%, WV 1.4%, MI 1.6%, MS 1.9%,
  TX 2.4%, LA 2.7% vs 3.5–5% elsewhere). Full data / the 10% sample or pooling years fixes the first, not the second.

### 4.17 Border-gap year effects with pair FE, centred on 2017 (2026-10-20)
gap_pt = a_p + b_t, **2017 = 0** (last pre-SALT-cap year), SE clustered by pair (`R/12_border_gap_twfe.R` →
`output/analysis/local_juris/border_gap_twfe.{txt,csv}`, `figures/border_gap_twfe.png`).
- **Effective rate (bill / price), gap in pp; full panel (45 pairs, 300 pair-years, 2017 mean 0.58) / balanced (16 pairs,
  2017 mean 0.61):** 2013 +0.07 / +0.06, 2014 +0.05 / −0.04, 2015 0.00 / −0.01, 2016 −0.02 / −0.10 (SE 0.05), 2018 −0.08
  (SE 0.03) / −0.05, 2019 −0.01 / +0.05, 2020 −0.06 (0.03) / −0.09 (0.04), 2021 −0.11 (0.04) / −0.17 (0.06).
  Pre-2017 coefficients are small and mixed in sign; after 2017 the gap is 0.05–0.17 pp narrower, growing by 2021.
- **Relative gap (gap / mean effective rate, 2017 mean 0.36):** pre −0.06 to +0.04, post −0.03 to +0.07, none significant
  except balanced 2019 (+0.07, SE 0.04, borderline). The pp narrowing after 2017 is the fall in effective-rate levels, not
  relative convergence.
- CoreLogic-reported rates: 7 pairs (2 balanced) — uninformative.
- Few clusters (16–45): cluster-robust SEs approximate.

### 4.18 Border effective-rate gaps from a per-side sample of the full roll (2026-10-20)
On each of 208 pair sides: max(5,000, 1% of the 2016 residential parcels within 20 mi) parcels of the full roll (31.6m
border parcels → 1.01m sampled; SOM job 3692460, `cluster/f_border_sample.sql`, ~3 min). Effective rate as in §4.16.
`R/13_border_sample_twfe.R` → `output/analysis/local_juris/border_sample_twfe.{txt,csv}`, `border_sample_gaps_effective.csv`,
`figures/border_sample_gaps_box.png`, `figures/border_sample_twfe.png`.
- Usable sales per side-year: median 178 (p10 12); 87% of side-years have ≥ 30. **69–87 pairs per year, 88 in total, 54
  in all 9 years** (vs 21–41 / 16 from the 1% sample).
- Median |gap| 0.28–0.40 pp each year (p75 0.56–0.85).
- **TWFE, gap in pp, 2017 = 0 (full 88 pairs / balanced 54; 2017 mean 0.53):** 2013–16 between −0.002 and +0.023 (SE
  0.02–0.04); 2018 −0.002 / +0.020; 2019 −0.004 / +0.006; 2020 −0.051 (0.026) / −0.067 (0.023); 2021 −0.106 (0.023) /
  −0.097 (0.025). Flat pre-period; no change in 2018–19; narrowing only in 2020–21.
- **Relative gap (2017 mean 0.35):** all years within ±0.03, none significant (2018 +0.017 / +0.027, SE 0.013–0.015).
  The 2020–21 narrowing in pp is the pandemic price surge lowering all effective rates (bills lag prices), not relative
  convergence. No sign that border gaps in effective rates moved with the 2018 SALT cap.
- Imputed stock-based rates (§4.12 method, 1% sample, `R/10_border_rate_gaps.R` → `border_rate_gaps.txt`): coverage
  ≈ 69% of border parcels (NY 14%, MI 4%, AZ 3%, SD 1% low); 65–79 pairs per year. Gaps in % of AV are not comparable
  across states (median 1.6–2.0 pp, driven by assessment ratios); in % of market value median 0.34–0.45 pp.
- **Population-weighted mean gap by year** (`R/14_border_gap_weighted.R` → `border_gap_weighted_means.csv`,
  `figures/border_gap_weighted_means.png`; weight = pair's residential parcels within 20 mi in the 1% sample, 2016, fixed;
  largest NJ–PA 6.6%, NJ–NY 6.3%, IL–IN 4.5%, MD–VA 4.4%, NC–SC 3.5%, IL–MO 3.1%, CT–NY 2.9%). Weighted mean gap (all pairs)
  0.59–0.66 pp in 2013–19 (2017: 0.63), 0.54 in 2020, 0.48 in 2021; unweighted 0.51–0.54 → 0.41. Populous pairs have larger
  gaps (+0.1 pp). Relative to the level: weighted 0.36–0.40 in 2013–19, 0.34–0.35 in 2020–21; no break at 2018. Balanced
  panel (54 pairs) is nearly identical.

### 4.19 Coverage by year and the border gaps extended to 2024 (2026-10-20)
- **Tax-roll coverage by year** (full SOM roll, `juris_cells.parquet`, residential parcels as % of 2016; counties with ≥ 80%
  of their 2016 count): 2008 22% / 635; 2009 82% / 1,461; 2010 71% / 1,259; 2011 60% / 1,054; 2012 72% / 1,401; **2013 92% /
  2,462**; 2014–2023 96–109% / 2,620–2,906; **2024 88% / 2,130 (≈ 700 counties not yet delivered in the SOM copy)**; 2025 ≈ 1%.
  2010–12 gaps are whole counties. Near-full coverage starts in 2013 (fully from 2014). The 1% SALT panel (Bouchet copy)
  shows 2024–25 more complete (863k / 799k parcels vs 931k in 2016), so the Bouchet copy may hold a later delivery.
  Arm's-length sales in the 1% panel: 39–48k/yr 2013–21, 40k 2022, 31k 2023–25 (market slowdown + recording lag).
- **Border sample 2013–2024** (same per-side sample; SOM job 3692593 pulled 2010–24 → `data/local_juris/bsample1024/`;
  `BORDER_IN=bsample1024 BORDER_Y0=2013 BORDER_Y1=2024 BORDER_SFX=_2013_2024` for `R/13`, `R/14`; outputs `*_2013_2024.*`):
  69–87 pairs/yr, 59 in 2024 (missing counties); 88 pairs, 41 balanced over all 12 years.
  TWFE gap in pp, 2017 = 0 (full / balanced): 2013–16 0.00–0.01 / 0.01–0.03; 2018–19 ≈ 0 / +0.01–0.02; 2020 −0.05 / −0.07;
  2021 −0.11 / −0.09; 2022 −0.10 / −0.11; 2023 −0.10 / −0.12; 2024 −0.12 (0.04) / −0.11 (0.05). Gap narrows ~0.1 pp in 2020–21
  and stays there. Relative gap: pre ≤ +0.03, 2018–19 +0.01–0.04, 2020–21 ≈ 0, 2022–24 +0.02–0.06 (none significant at 5%).
  Weighted mean gap: 0.59–0.66 pp 2013–19 → 0.54 (2020), 0.48 (2021), 0.46 (2022), 0.48 (2023), 0.39 (2024); mean effective
  rate 1.58% (2017) → 1.13% (2024). The pp narrowing tracks the fall in effective rates; relative gaps stay ≈ 0.33–0.40.
- **Bill vs price** (`R/15_border_bill_price_twfe.R` → `border_bill_price_twfe_2013_2024.{txt,csv}`, figure same name; same
  sales, logs so log eff = log bill − log price exactly; 2017 = 0, SE by pair):
  *Side levels (side FE; full 88 pairs):* log price −0.19 (2013) → +0.20 (2020), +0.33 (2021), +0.40 (2022), +0.49 (2024);
  log bill −0.15 (2013) → +0.10 (2020), +0.11 (2021–22), +0.18 (2024); log effective rate +0.04 (2013), ≈ 0 to 2016,
  −0.03 (2018), −0.10 (2020), −0.23 (2021), −0.29 (2022–23), −0.31 (2024). Before 2018 bills and prices grew together
  (≈ 4%/yr); from 2020 prices jumped while bills kept growing ≈ 2–3%/yr, so the fall in effective rates is a price effect.
  Balanced (41 pairs) the same.
  *Cross-border gaps (abs. log difference, pair FE):* effective rate ≈ 0 throughout (−0.02 to +0.03; balanced +0.07 in 2024,
  SE 0.035); price gap ≈ 0 to −0.03 (n.s.); bill gap ≈ 0 to 2021, then +0.02 / +0.07 (2023 / 2024, full; SE 0.02–0.03) and
  +0.04 / +0.08 / +0.11 (2022–24, balanced). Bills diverge slightly across borders after 2021 (2024 partial); no change around 2018.

### 4.20 Revenue on the lower- vs higher-tax side of state borders (2013–2021)
Script `code/analysis/local_juris/R/16_border_revenue_low_high.R`; output `output/analysis/local_juris/border_revenue_low_high.{txt,csv}`, figure `figures/border_revenue_low_high.png`.
- Revenue = 100 × sum of tax bills per side-year in the 1% random parcel panel within 20 mi (`border1/border_panel_1pct`, 2013–2021 only). To keep roll gaps out of totals, only counties whose parcel count is ≥ 90% of their max in every year (642 of 1,229 counties, 65% of parcel-years).
- Sides ordered once, by the 2013–17 median effective rate (bill / price, per-side sales sample, ≥ 100 sales per side). 60 pairs with revenue on both sides in all years.
- Higher-tax sides collect ~2.3× more: mean $713m vs $316m per side in 2017. They also have more parcels (173k vs 106k) and higher mean bills ($2,710 vs $1,930).
- Growth (2017 = 100): both sides grew in step before 2017 (~3.5–4%/yr). After 2017 the lower-tax side grew faster: 117 vs 112 in 2021. The mean per-pair log(higher / lower revenue) falls from 0.000 (2017) to −0.023 (2020, SE 0.014) and −0.029 (2021, SE 0.016): borderline, not significant at 5%. Parcel counts are flat, so this is bills, not more parcels.
- TWFE (`R/17_border_revenue_twfe.R`, output `border_revenue_twfe.{txt,csv}`, figure `figures/border_revenue_twfe.png`; 2017 = 0, SE by pair). Log revenue = log mean bill + log parcels. All 60 pairs are balanced, so full = balanced.
  - Each side alone (side FE): revenue up 12% (higher-tax) and 15% (lower-tax) by 2021 vs 2017. About −11 to −13% in 2013. All of it is mean bill; log parcels moves < 2%.
  - Lower minus higher (side FE + pair × year FE): pre-period +0.015/+0.006/−0.002/+0.015 (2013–16), post +0.016/+0.014/+0.023/+0.029 (2018–21), SE 0.013–0.030. Nothing significant. The post-2017 drift of ~2–3 log points toward the lower-tax side is about the size of the pre-period noise.

### 4.21 Out-migration near state borders: sales turnover as the proxy (2013–2024)
Bouchet compute was under emergency maintenance on 2026-10-20, so the Data Axle measure couldn't run. Ready but not yet run: `code/analysis/local_juris/bouchet/g_moveout_bkey_1pct.sql` (primary family_id change at 1% building-sample addresses, with coordinates) and `R/18_border_moveout.R` (assign sides, annualise by file cut-offs, same TWFE). Sales stand in for now.
Script `R/19_border_sales_turnover.R`; output `border_sales_turnover{,_twfe}.{txt,csv}`, figure `figures/border_sales_turnover.png`.
- Turnover = arm's-length sales (price > 10k, no new construction, first sale per parcel-year) / sampled parcels in the year's roll, per side (bsample1024; 2016 residential universe). Sides ordered as in §4.20. 84 pairs (full); 49 present in every year (balanced).
- Levels: the higher-tax side turns over more. 2017: 4.04% vs 3.69%. Both sides follow the same cycle: 3.1–3.4% (2013) → 4.6–4.8% (2021) → ~3.0% (2024; partly recording lag).
- TWFE, lower minus higher (pp, 2017 = 0, SE by pair), full panel: pre 2013–16 +0.04/+0.10/+0.07/+0.04 (SE 0.07–0.11); post 2018–24 −0.01, +0.22, +0.15, +0.18, +0.20, +0.27, +0.30 (SE 0.09–0.16). Pooled post − pre: +0.13 (SE 0.08); 2018–22 only +0.10 (0.08). Balanced: +0.05 (0.12).
- So the pre-2018 gap (higher-tax side turns over ~0.3 pp more) narrows by ~0.1–0.2 pp after 2017. That is the opposite sign from extra out-migration on the higher-tax side after SALT. Not significant, and not robust to the balanced panel.
- Caveat: sales measure owner turnover, including in-state moves and investor purchases, not out-migration. Data Axle move-outs (§4.5c) also capture renters and non-sale moves.

### 4.22 Map: local variation in effective tax rates (grid, sales 2017–2019)
SOM extract `code/analysis/local_juris/cluster/h_grid_eff.sql` (42 s on gpunormal). Arm's-length sales of SFR/condos (pic 10/11), price > 10k, no new construction; effective rate = sale-year bill / price, kept 0.05–10%; tax-roll coordinates in EPSG:5070, binned into 5 km cells. 12.3m sales, 128k cells; up to 50 sales kept per cell (hash). Local script `R/20_grid_rate_differences.R` (env GRID_AGG = merge AGG×AGG blocks, GRID_MIN = min sales, GRID_NOISE = pure-noise benchmark). Output `grid_rate_differences_{5km_min20,10km_min50}[_noise].{txt,csv}`, figures `figures/grid_rate_differences_*.png`, `figures/grid_max_diff_{10,25,50}mi_*.png`.
- Cell value = median effective rate. For each cell and radius (10/25/50 mi, centre-to-centre): max and mean |median_i − median_j| over neighbours.
- Precision: robust SE of a cell median (1.2533·IQR/1.349/√n) is 0.06 pp for 5 km cells (≥ 20 sales, 46.7k cells) and 0.04 pp for 10 km (≥ 50 sales, 17.6k cells). The normal-theory SE (sd-based, 0.11) overstates it because of heavy tails.
- Median over cells of the max difference (pp), real vs pure noise (all cells same rate, medians ~ N(0, SE)):

| radius | 5 km real | 5 km noise | 10 km real | 10 km noise |
|---|---|---|---|---|
| 10 mi | 0.47 | 0.19 | 0.26 | 0.08 |
| 25 mi | 0.77 | 0.30 | 0.52 | 0.14 |
| 50 mi | 1.08 | 0.40 | 0.77 | 0.20 |

  Mean differences: 0.17/0.22/0.27 (5 km) and 0.13/0.19/0.23 (10 km) vs noise 0.07–0.08 / 0.04–0.05. Noise is about a quarter of the 10 km max, so use the 10 km map.
- Where: largest local differences in NJ, NY, NH, CT, DE, VT, PA (25 mi median max 1.0–1.5 pp at 10 km), then IL (Chicago vs NW Indiana), TX metros, WI, OH. Smallest across the South-East and Mountain West (cell max < 0.5 pp). Cell medians p10/p50/p90 = 0.52/1.0/2.1%.
- Caveat: the cap of 50 sales per 5 km cell means 10 km medians weight dense and sparse sub-cells equally.

### 4.23 USPS vacancy on the lower- vs higher-tax side of state borders (2013–2024)
SOM `cluster/i_border_vacancy.sql` (same per-side sample as bsample1024); local `R/21_border_vacancy.R`; output `border_vacancy{,_twfe}.{txt,csv}`, figure `figures/border_vacancy.png`.
- Flag: CoreLogic tax `situs_delivery_point_validation_vacant_indicator` (boolean). National 0.01% check: filled 44% (2012) → 70–86% (2014–23), 1.3–3.1% TRUE among all records. In the border sample: filled 4% (2010), 11% (2011), 51% (2012), 82–88% from 2013. Vacancy = TRUE / filled, per side-year; side-years need ≥ 1000 filled and ≥ 50% fill. 82 pairs (full), 41 balanced.
- Levels: vacancy falls everywhere, from 2.4–2.9% (2013) to 1.3–1.5% (2024), mostly from 2019 on (tight housing market). The higher-tax side is ~0.4 pp more vacant before 2018 (2017: 2.52 vs 2.10%).
- TWFE, lower minus higher (pp, 2017 = 0), full: pre −0.05 to +0.11; post +0.02, +0.04, +0.21, +0.14, +0.15, +0.40, +0.40 (SE 0.05–0.27); pooled post +0.15 (0.11). Balanced: post +0.09, +0.25, +0.43, +0.51, +0.56, +0.69, +0.58 (SE 0.07–0.37); pooled +0.38 (0.16). Side levels (balanced): higher-tax side −1.14 pp by 2024 vs lower-tax −0.56.
- So vacancy fell more on the higher-tax side; the gap closes after 2019. Like sales turnover (§4.21), this is the opposite of a post-SALT exodus from the higher-tax side. Possible confounds: catch-up from the higher starting level (in proportional terms −40% vs −28%, balanced), and the 2020–22 housing-demand shock, which may differ between the more urban higher-tax sides and the rest. Not specific to 2018: the gap starts closing in 2019–20.

### 4.24 Map v2: census tracts and drive-time neighbours (sales 2017–2019)
- Drive times: UChicago CSDS tract-to-tract driving matrix (OpenTripPlanner; tracts within 100 km; minutes), from the PySAL `access` docs (https://access.readthedocs.io/en/stable/resources.html), Box link `prapz7ac7vwuz44nnab3dhe10vbg55cz.bz2` (420 MB bz2). Stored in `data/external/csds_ttm/`: raw bz2 + `tract_drive_le90.parquet` (pairs ≤ 90 min: 72.9m pairs, 67,777 origin tracts = 2010 tract vintage). Non-numeric minutes (nulls) dropped.
- Tracts: `cl.tax.census_id` = 6-digit tract + 4-digit block within county (10 chars, filled 93–99% in 2016–2023; national 0.01% check `cluster/j_census_id_check.sql`). Tract GEOID = county FIPS ‖ left(census_id, 6); 2010 vintage in 2017–19, matching the matrix. SOM extract `cluster/j_tract_eff.sql` (1 min on gpunormal): same sales as §4.22, 12.7m sales, 99.8% with a tract, 70.1k tracts, 63.3k with ≥ 30 sales. Local `R/22_tract_drive_differences.R` (env TRACT_MIN, TRACT_NOISE); output `tract_drive_differences_min30[_noise].{txt,csv}`, figures `figures/tract_drive_{max,p90}_diff_min30.png`, `figures/tract_drive_metro_zoom_min30.png` (NY, Chicago).
- Robust SE of a tract median: 0.035 pp (≥ 30 sales). Tract medians p10/p50/p90 = 0.58/1.13/2.38%.
- Median over tracts, real vs noise benchmark (pp):

| drive time | neighbours (median) | max diff | max (noise) | p90 diff | p90 (noise) | mean diff | mean (noise) |
|---|---|---|---|---|---|---|---|
| 15 min | 12 | 0.31 | 0.10 | 0.23 | 0.07 | 0.13 | 0.04 |
| 30 min | 63 | 0.62 | 0.18 | 0.36 | 0.09 | 0.19 | 0.05 |
| 60 min | 301 | 0.98 | 0.30 | 0.49 | 0.10 | 0.25 | 0.05 |

  The 90th-percentile difference is the cleanest measure (noise about a quarter of the real value, flat in T); the max still grows with neighbour count.
- By state (30 min, median max / p90): NJ 2.76 / 1.70, NY 2.14 / 1.29, CT 1.89 / 1.04, PA 1.65 / 0.98, DC 1.45, IL 1.45 / 0.75, DE, MI, OH ≈ 1.1–1.25. Lowest: UT 0.19, MT, AR, WA, NM, TN, CO ≈ 0.2–0.3.
- Zooms: northern NJ / Westchester / Long Island show the largest within-30-min spread. In Chicago the hot spot is south Cook County against NW Indiana (rates ≥ 3% vs ~1%). NYC itself is mostly missing (few SFR/condo sales per tract; co-ops are not in the data).
- **v2b: all tracts, zero-filled** (`TRACT_MIN=1`; outputs `_min1[_noise]`). Every tract with ≥ 1 sale (70.1k) is kept. Tracts in the matrix with no other tract with sales within T minutes get 0 (15/30/60 min: 10,350 / 2,007 / 228 tracts). For n < 10 the IQR in the SE uses the national median IQR. 5,184 tracts (≈ 5% of sales, all states, most in FL) are absent from the CSDS matrix: tract ID not among its 67,777 routable 2010 tracts. They stay missing (not 0), since we can't tell whether they're isolated.
  - Median over 64.9k tracts (pp), real / noise: p90 diff 0.21/0.07 (15 min), 0.39/0.10 (30), 0.53/0.12 (60); max diff 0.29/0.09, 0.71/0.26, 1.22/0.54; mean 0.12/0.04, 0.20/0.05, 0.28/0.06. With tiny tracts the max is noise-heavy (noise p90 of the max 0.6–1.5 pp). The p90 difference stays clean: use it.
  - Remaining grey: tracts with no sales, mainly price non-disclosure states (MT, WY, ID, UT, NM, ND and parts of KS, MO) plus the unmatched IDs.
- **v2c: relative (signed) gap** (same script/outputs; figures `figures/tract_drive_gap_{min,p10}_min1.png`, `figures/tract_drive_gap_metro_zoom_min1.png`). gap_min = own median − lowest neighbour median within T (what moving to the cheapest reachable tract saves; < 0 means own tract is the cheapest); gap_p10 = own − 10th percentile of neighbour medians. 0 if no neighbour. Diverging map, ±2 pp.
  - Median over tracts (pp), real / noise: gap_min 0.14/0.05 (15 min), 0.42/0.18 (30), 0.70/0.42 (60); gap_p10 0.08/0.03, 0.19/0.05, 0.26/0.06. p90 over tracts of gap_p10: 0.54 / 0.81 / 1.05 (noise 0.18–0.19). The minimum is noise-heavy (it picks the luckiest small tract); gap_p10 is clean, so use it.
  - Share of tracts with gap > 0.5 pp: gap_min 21/44/65% (noise 9/23/42%); gap_p10 11/20/27% (noise 1%). Own tract is the cheapest reachable: 10/6/2%.
  - Zooms: Westchester/Rockland, northern NJ, Long Island and Philadelphia are 1–2 pp above the cheap end of their 30-min commute shed. South Cook County is ≥ 2 pp above (NW Indiana within reach).

### 4.25 Local differentials by distance to the nearest state border
Script `R/23_tract_diff_by_border_distance.R`; output `tract_diff_by_border_distance.{txt,csv}`, figure `figures/tract_diff_by_border_distance.png`. Tracts with any sales 2017–19 (as §4.24, v2b), located at the mean coordinates of their sales; distance to the nearest boundary shared with another state (as R/07). Per tract: mean |own − neighbour median| over tracts within 15/30/60 drive minutes (all, same-state, other-state), then means across tracts by 5-mile bins. 64.0k tracts.
- 30 min, all neighbours: 0.49 pp (0–5 mi) → 0.43 (5–10) → 0.37 (10–15) → ~0.33 (20–50) → 0.29 (80–100) → 0.16–0.19 (100+ mi). 60 min: 0.62 → 0.64 → 0.55 → 0.45–0.49 (15–30) → ~0.38–0.40 (30–80) → 0.20 (100+). Noise benchmark flat at 0.07–0.11 throughout.
- The near-border rise is the cross-border pairs. Other-state neighbours differ by ~0.9–1.2 pp (30 min) vs 0.37–0.39 for same-state neighbours of the same tracts. At 0–5 mi, 24% (30 min) / 41% (60 min) of reachable tracts are across a state line. The same-state differential is ~0.05 pp higher near borders than at 20–50 mi.
- The low level beyond 100 mi is composition (interiors of large, uniform-rate states like TX, CA, FL, AZ, and the West), not distance as such.

### 4.26 Border-distance IV: first stage, school access, balance (2026-10-20 night)
Data `R/24_iv_tract_data.R` → `data/local_juris/iv_tract.parquet` (69.2k tracts). Inputs: tract medians (sales 2017–19), drive-time gaps (R/22), distance to the nearest internal state border, school district by tract location (TIGER cb 2019 unified + elementary; 69.2k matched) with SEDA 4.1 pooled cohort-standardised achievement (`data/external/seda41/`, from stacks.stanford.edu druid xv742vh9296; 68.7k), ACS 2013–17 tract covariates (tidycensus, cached `data/external/acs17_tract.csv`; 68.4k).
- `R/25_iv_first_stage_balance.R` (plain distance bins, state or county FE): near the border (0–5 mi), 38% of tracts have a ≥ 0.5 pp cheaper cross-state tract within 30 min (0% beyond 50 mi). But 44% also have a cross-state district ≥ 0.25 SD better in achievement, and border tracts are poorer, older, more rental and lower-achieving (0.2–0.3 SD, state FE). **Plain distance fails balance.**
- `R/26_iv_high_side.R`, **directional design**: each tract is assigned to its nearest state-border pair; the side whose tract medians within 20 mi are higher is "high". Instrument = near × high, with pair × side FE + pair × distance-bin FE, SE by county. 55.9k tracts, 70 pairs.
  - 0–5 mi × high, 30 min: cross-state cheaper option +0.33 (0.04); tax gap (own − p10 reachable) +0.35 pp (0.08). School access: school gap −0.05 SD (0.05); cross-state better school −0.09 (0.05). Own-tract balance (SD units): achievement +0.06 (0.05), SES +0.17 (0.13), log income +0.04, log home value +0.02, BA share +0.02, white −0.06 (0.03), owner share −0.01. Pooled near (< 10 mi) × high, in SD of Y: first stage +0.45 (x_cheap), +0.64 (tax gap); balance ≤ 0.17 SD and mostly insignificant.
  - **So the directional instrument has a strong first stage, no better-school access, and good balance.** Output `iv_high_side.{txt,csv}`, `iv_first_stage_balance.txt`.
- `R/27_school_district_gaps.R`: district median rates (9,760 districts ≥ 20 sales), adjacency, own − cheapest adjacent: p50 0.22, p90 0.82 pp, 23% > 0.5 pp. On state lines (1,495 districts), own − cheapest district across the line: p50 0.12, p90 1.15, 27% > 0.5 pp. The cheapest neighbour's achievement is not lower (+0.02 SD on average, +0.07 when gap > 0.5; corr 0.09). Cheaper adjacent districts are not worse-school districts. Maps `figures/school_district_gaps_{us,zoom}.png`.

### 4.27 (a) SALT exposure and local property-tax revenue: Census of Governments (2012–2022)
`code/analysis/salt_local/a1_revenue_exposure.R`; output `output/analysis/salt_local/a1_*`. Exposure: 2016 SOI county × AGI stub, SALT deduction above $10k per return (stub-average approximation; mean $0.44k, SD $0.94k; top: Manhattan, Marin, San Mateo, Westchester, Santa Clara, Fairfield CT, SF, Nassau). Saved `data/local_juris/salt_exposure_county2016.csv`. Event study log revenue × exposure (SD), county + state × year FE.
- School districts (F-33 census each year, 2,724 counties): unweighted, exposed counties grow ~0.2–0.25%/yr faster per SD before and after 2017, no break (slope change −0.16%/yr, SE 0.10, FY2019 start). **Weighted by returns: pre-slope +0.13–0.19%/yr, then a slope change of −0.26 to −0.30%/yr per SD (SE 0.10).** Exposed large counties' school levies stopped outgrowing the rest after the cap.
- County governments (balanced 1,174): level +0.6–1.3% in 2018–19 then fading. Weighted slope change −0.35 to −0.67%/yr (SE 0.18–0.27).
- All local governments (Census years only): 2012 → 2017 → 2022 per SD: −0.7%, 0, +1.6% unweighted; weighted −1.1%, 0, +0.2%. Weighted, growth slows after 2017.
- Caveats: COVID-era state aid and the 2020–22 housing boom fall in the post period; the exposure measure is county-level and correlated with income and density.

### 4.28 School-district finance panel (Census F-33 district files, FY2012–2023): (a), (b), border DDD
`code/analysis/salt_local/a3_district_panel.R`; data `data/external/elsec/elsecYY.txt` (www2.census.gov school-finances tables; files end lines with a trailing comma, so read positionally; FY2022 txt is truncated (1,630 rows), so `elsec22.xlsx` is used). 13.9k districts, 11.7k balanced; NCESID = LEAID links to SEDA, TIGER and our tract → district map. Output `output/analysis/salt_local/a3_*`.
- (a) District log property tax on county exposure (district + state × year FE): unweighted pre −0.4 to −0.5% (2012–15), post ≈ 0 (SE 0.3%). Enrollment-weighted pre −1.1 to −0.4%, post −0.2 to −0.5%. Pre-trend upward, flat after: same pattern as the county weighted result (§4.27).
- (b) Within-county SD of log property tax per pupil across districts vs exposure: +0.4 to +0.65 pp in 2019–21 (SE 0.4–0.5): not significant. National mean of within-state SDs rises 0.60 (2017) → 0.65 (2023), and p90/p10 1.39 → 1.46: dispersion across districts grows after 2017, but it had been flat before.
- **Border DDD (district near × high, majority of its tracts; pair × side × year + pair × distance-bin × year FE; 6,813 districts, 777 near-high):** log property tax 2012–16 −0.5, −0.1, +0.2, −1.5, −0.6%; **2018–23 −1.6, −1.7, −3.2, −3.1, −2.7, −3.4% (SE 1.1–1.4)**. Robust to near < 20 mi (pre ≈ +0.1 to +1.3, post −0.3 to −3.2%); weaker at < 5 mi. Per pupil: pre −0.9 to −2.1, post −1.2 to −2.9%. **Not larger in high-SALT-exposure counties**: the near × high × exposure interaction is +0.3 to +0.5%/SD, and the high- vs low-exposure split gives −0.9 to −2.3% vs −1.8 to −4.3%. So high-side border districts' levies fell behind after 2017, but the cross-section of exposure does not point to the SALT cap as the reason. Figure `figures/a3_district_border_ddd.png`.
- County-level analogue (`a2_revenue_outside_option.R`; counties with > 25% of population in near-high tracts, 267 of 2,715): county-government property tax, exposure × border-high county: 2019 −1.8% (0.6), 2021 −1.7% (1.0), 2022 −1.9% (1.1), pre −0.3 to −0.8% (noisy). Weighted 2019 −1.3% (0.5). School districts: none (weighted +0.5 to +1.1%).

### 4.29 (c) House prices in the border design: FHFA tract HPI (2012–2025)
`code/analysis/salt_local/b4_hpi_border_ddd.R`; FHFA annual tract HPI (`data/external/fhfa/hpi_at_tract.csv`, from fhfa.gov/hpi/download/annual/hpi_at_tract.csv; 2010 tracts). Balanced 2012–2025: 24.0k tracts in the border sample, 2,410 near-high. Triple difference as R/26 (tract + pair × side × year + pair × distance-bin × year FE, SE by county).
- Near × high, all: 2012–16 −0.3 to +0.6%; 2018–25 +0.1, −0.1, −0.3, −0.4, −0.7, −0.6, −0.7, −0.3% (SE 0.3–1.2): small and insignificant.
- **Near × high × county SALT exposure (per SD): 2012–16 +0.13, +0.21, +0.15, +0.05, +0.02% (flat); 2018 −0.12 (0.08), 2019 −0.31 (0.12), 2020 −0.51 (0.19), 2021 −0.85 (0.24), 2022 −0.98 (0.27), 2023 −0.92, 2024 −0.97, 2025 −1.01% (0.30).** In SALT-exposed areas, prices in tracts with a cheap cross-border option fell ~1% per SD of exposure behind comparable tracts, gradually from 2019. The split by exposure shows the same: high-exposure near-high −1.0 to −1.7% in 2021–24, low-exposure ≈ +1 to +2% (with a level pre-difference).
- Reading: consistent with the cap raising the effective (after-federal-tax) cost of high local taxes for itemizers, which buyers can escape by crossing to the cheaper side, and that being capitalised. The revenue response (§4.28) is not exposure-specific. Next: CoreLogic tract-year prices, effective rates and assessment ratios in the same design (k_panel extract).
- **Correction after the placebo (same day):** near × LOW side × exposure falls just as much (10 mi: 2021–25 −0.9 to −1.2%/SD, vs high side −1.1 to −1.2%), at all cutoffs (5/10/20 mi). The decline is a near-border × exposure pattern on both sides, not the outside-option mechanism. The high-minus-low difference is −0.1 to −0.4%/SD and insignificant. With state-level exposure instead of county, the high-side interaction has a falling pre-trend (+1.5% in 2012 → +0.1% in 2016) and turns positive after 2021. **So the HPI evidence does not support a SALT-specific capitalisation of the cross-border option.** Robustness table in `b4_hpi_border_ddd.txt` / `_robust.csv`.
- Pair-level SALT contrast (own state minus neighbouring state, return-weighted 2016 exposure, SD across pairs; 61% of near-high districts are in the more-exposed state): near × high × contrast 2012–14 −1.1 to −1.2%, 2015–16 ≈ 0, 2018–23 +0.3 to +1.0% (SE 0.5–0.75). A rising trend, not a 2018 break, and the wrong sign for a SALT story. The base near × high effect stays at −1.9 to −3.9% after 2017. **So the relative decline of high-side border districts' levies is real but not explained by SALT exposure, county or state.**

### 4.30 (b) Within-metro dispersion of school-district property tax per pupil vs SALT exposure
`code/analysis/salt_local/a4_cbsa_dispersion.R`; CBSA from the F-33 files, balanced districts with ≥ 100 pupils, CBSAs with ≥ 10 districts. Exposure: return-weighted county excess SALT, SD across CBSAs. CBSA + state × year FE.
- SD of log property tax per pupil (2017 mean 0.47): no clear pattern. Enrollment-weighted SD (mean 0.38): pre 2012–16 0 to +0.3 log points; post 2018–23 −0.2, +0.25, +0.55, +0.71, +0.74, +0.67 (SE 0.22–0.35). p90–p10 (mean 1.01): pre +0.7 to +2.0 (noisy), post −0.8 to +1.3.
- So no compression in exposed metros. If anything, enrollment-weighted dispersion widens slightly (+0.7 log points per SD ≈ 2% of the mean by 2021–23). National average within-CBSA dispersion is flat 2012–23.
- District border DDD, pooled: near × high × post (FY2018+) −2.2% (SE 1.0). Leave-one-pair-out range −1.7 to −2.6%; largest pairs NJ-PA, NJ-NY, IL-IN, KY-OH, NY-PA, CT-NY.
- `a5_adjacent_district_gaps.R`: |log property tax per pupil| gaps between adjacent districts, cross-state (1,566 pairs) vs same-state (27,593), pair + year FE. Cross-state gaps are 0.73–0.77 vs 0.43–0.45 within state. Cross × year vs 2017: 2015–16 +0.02 (SE 0.01), 2018–21 −0.004 to +0.006, 2022–23 +0.03 (0.03). **No post-2017 narrowing of cross-border gaps.** Interaction with the two states' SALT contrast: ≈ 0.
- District 2SLS: first stage, district mean tract gap (30 min) on near × high, +0.31 pp (0.08), F = 14.5. Log property tax on post × gap instrumented by post × near × high: **−6.3% per pp of outside-option gap (SE 3.8)**. Per share of population with a cheaper cross-border tract: −8.2% (4.7).
- Heterogeneity by district SES (SEDA, SD units; SES × year slopes as controls): near × high × SES pre 2012–16 +0.2 to +1.3%, post 2018–23 −0.1 to +0.8% (SE 0.5–0.9). The revenue decline is no larger in high-SES (more itemizing) districts.

### 4.31 (a)/(c) on the CoreLogic 1% parcel panel (SOM queue unavailable overnight)
`code/analysis/salt_local/c2_parcel_panel.R`; `data/salt_cap/panel` (931k parcels with bill and AV, 2013–2025; 455k arm's-length sales). Parcel tract = county FIPS ‖ 6-digit tract. Output `output/analysis/salt_local/c2_parcel_panel.{txt,csv}`, figure `figures/c2_parcel_panel.png`. SOM extract (`cluster/k_panel*`) is queued for the full roll; jobs pushed to early November by fair-share, since several other jobs of the user's were queued the same evening.
- **County exposure (per SD, parcel FE + state × year, SE by county):**
  - log bill 2013–16 −1.6, −0.8, −0.4, −0.2%; 2018–25 +0.2 to +0.4% (SE 0.1–0.3). Faster growth in exposed counties stops after 2017 (≈ 0.4%/yr → ≈ 0), as in the CoG weighted results.
  - log AV: pre +0.5%/yr, post +0.3 to +0.8% to 2020, then fading.
  - log nominal rate (bill/AV): 2019–22 −0.2 to −0.5% (SE 0.2–0.3).
  - log sale price (county FE): pre flat (±0.2%); 2020 −0.7, 2021 −1.6, 2022 −1.3, 2023–25 −2.0% (SE 0.3). Falls behind mostly from 2020 (likely the pandemic re-sorting away from expensive metros, which are the most exposed).
  - log effective rate given price: +1.1 to +2.3% from 2020. Log assessment ratio given price: +1.4 to +2.3% from 2020: assessments don't follow the relative price decline.
  - Regressivity (exposure × log price slope of log AV/P): +0.001 to +0.003 after 2017 vs +0.0005 to +0.001 before. Tiny (slightly less regressive).
- **Border DDD (near × high, constant parcels; 1% sample, so noisy):**
  - log bill 2014–16 +1.1, +1.0, +0.4%; **2018–25 −2.8, −2.0, −0.7, −1.6, −1.9, −2.6, −3.2, −4.4% (SE 1.1–2.2)**. Matches the district revenue DDD (§4.28).
  - log nominal rate 2018–22 −1.9, −3.5, −3.1, −2.4, −2.2% (SE 1.1–2.8); log AV ≈ 0 to +2% until 2022. So the bill decline is mainly rates, not assessments.
  - Sale prices, effective rates and assessment ratios in this design: noisy (SE 3–5%), no pattern.

### 4.32 (a) robustness: property-tax-based exposure
`code/analysis/salt_local/a6_alt_exposure.R`. County share of 1% panel parcels with a 2016 bill > $10k (mean 4.5%) or > $5k (18%); corr with the SOI exposure 0.73. District log property tax (F-33 balanced), district + state × year FE.
- Unweighted: pre-slope +0.05 (sh10) / +0.23 (sh5) %/yr per SD; slope change after FY2019 −0.07 (0.04) / −0.15 (0.08).
- **Enrollment-weighted: pre-slope +0.21 / +0.56%/yr per SD; level +0.7 / +1.5% at FY2019; slope change −0.39 (0.14) / −0.71 (0.15) %/yr.** Event study (sh5, weighted): 2012 −3.0, 2014 −2.5, 2016 −0.9, 2017 0, 2019 +0.7, 2020 +0.8, 2021 +0.6, 2022 0.0, 2023 +0.3%.
- Same picture as with the SOI measure: in large, exposed (high-bill) jurisdictions, levy growth that had outpaced the state flattens after the cap. Unweighted, i.e. across small districts, the break is weak.

### 4.33 Out-migration in the border and exposure designs (Data Axle 1% building sample, Bouchet)
Bouchet `local_juris/bouchet/g_moveout_bkey_1pct.sql` (1 min; the tax 1% sample has no `census_id`, so addresses are placed in 2010 tracts locally with TIGER cb 2019). Local `code/analysis/salt_local/d1_moveout_ddd.R`; output `d1_moveout_ddd.{txt,csv}`. Clean address pairs (one primary family, both files), annualised by file cut-offs; 5.9m address-years, 814k addresses, mean 11.4%/yr; border sample 671k addresses, 62k near-high.
- County exposure (per SD, tract + state × year FE): pre 2013–16 −0.05 to −0.12 pp; post 2018–21 −0.01, +0.01, −0.11, −0.06 (SE 0.03–0.04). Owner-occupied SFR similar. **No change.**
- Border DDD near × high: pre −0.31, +0.04, −0.38, −0.27; post −0.24, −0.33, −0.11, −0.24 (SE 0.3–0.38). **No change.**
- Near × high × exposure: pre −0.02 → −0.16, post −0.15, −0.30, −0.09, −0.12 (SE 0.09–0.10): continues a mild pre-trend, no break.
- So Data Axle shows no extra move-outs from high-side border tracts or exposed counties after the cap (to 2021). This matches the sales-turnover (§4.21) and SALT-study results.
- Randomisation inference (`a7_district_ri.R`): flipping which side of each pair is "high" at random (200 draws). Placebo pooled coefficients have median −0.3%, sd 1.0%, 95% range −2.4 to +1.4%. Actual −2.2%: **RI p = 0.04**.

### 4.34 Full CoreLogic roll panel 2012–2024 (Bouchet, 2026-10-20 night)
Extract `code/analysis/local_juris/bouchet/k_panel_{2012,2019}.sql` (= cluster/k_panel_year.sql per year, run via run_sql.sbatch on Bouchet, 4c/64G, ~4 min per year). Files `data/local_juris/kpanel/{roll_tract,roll_area,sales}_YYYY.parquet` (2.0 GB): residential roll by tract (parcels, bills, AV, MV, median nominal rate), by tax area, and all residential arm's-length sales joined to the sale-year roll. 91–109m residential parcels and 3.0–5.4m sales a year. Build `code/analysis/salt_local/b1_build_panels.R` → `data/local_juris/{tract_year,county_year}.rds`.
- **County event studies** (`b2_county_event_studies.{txt,csv}`, figure; exposure = 2016 SOI excess SALT per return, SD units; counties with stable roll coverage for roll outcomes; county + state × year FE):
  - Residential levy: unweighted, pre −2.2% (2013) → 0 and post +0.4 → +1.7% (the trend continues); weighted, pre ≈ 0, post +0.2 to +0.4% to 2020, fading to 0 by 2024 (mild slowdown).
  - Log nominal rate (bill/AV): unweighted, pre +0.3 to +0.6%, post 2021–23 −0.3, −0.5, −0.9% (SE 0.2–0.3); weighted ≈ 0 to −0.2%.
  - Log median sale price: post −0.6 to −1.8% per SD from 2019 (SE 0.2–0.3); pre flat. Effective rate +0.005 to +0.03 pp (≈ +2.8% of the mean by 2024). Log median assessment ratio +0.4 to +4.0% unweighted, +0.3 to +1.9% weighted (pre −0.3 to −0.7%).
  - Regressivity (slope of log AV/P on log P, mean −0.28): unweighted 2018–19 +0.005, 2023–24 −0.008 (SE 0.004); weighted −0.002 to −0.005 (pre −0.005 to −0.001). PRD: nothing. Essentially no change.
  - (b) Within-county SD of log nominal rates across tax areas (mean 0.12) and SD of tract effective rates (mean 0.21 pp): flat (±0.003).
- **Tract border DDD on the full roll** (`b3_tract_iv_ddd.txt`; 29.6k tracts, 2.9k near-high; roll composition makes tract levels noisy). Tax per parcel: pre −3.7 to +2.6%, post −5.3, −2.1, −0.1, −1.6, −0.9, −5.4, −5.4% (SE 1.0–2.7). Log AV per parcel post −1.1 to −10% (noisy). Median effective rate pre −0.04 to −0.08 pp, post −0.05 to −0.18 pp (SE 0.04–0.11). Log assessment ratio post −1.6 to −12% (SE 1.7–6.6). Prices +0.1 to +2.3% (n.s.). Directionally consistent with lower taxes on the high side near borders, but noisy.
- **ZIP-level exposure** (`a8_zip_exposure.R`: SOI 2016 ZIP × stub, winsorised at p99, mapped to tracts via the sales' modal ZIP; 70.3k tracts). Dose-response in the tract DDD with the low-side placebo in the same model (`b3_tract_iv_ddd_zipexposure.csv`):
  - Median sale price: near × high × exposure, pre +0.2 to +0.8%, post 2018–24 −1.2, −1.5, −1.3, −1.9, −1.2, −0.9, −1.3% (SE 0.5–1.9). Near × low × exposure post +0.8 to +3.9% (pre +2.3, +3.4, −0.4, +1.1). Effective rate: high side +0.04 to +0.09 pp post (pre −0.02 to −0.035), low side ≈ 0 to −0.03.
  - Pattern: prices fall on the high side and rise on the cheap side near the border in exposed ZIPs. This is the capitalisation story, but the low-side pre-period is noisy.
- **FHFA HPI with ZIP exposure** (`b5_hpi_zip_exposure.{txt,csv}`, figure): both sides' near-border × exposure coefficients rise before 2017 (pre-trend). After 2017 the high side flattens (5 mi: −0.3 to +0.3%) while the low side keeps rising (+0.9 to +1.1% by 2023–25). Low minus high: 5 mi ≈ 0 before → +1.2% by 2024; at 10/20 mi already +0.3 to +0.6% before, +0.5 to +1.1% after. **Suggestive at 5 mi, not robust.**
- **Full-data check of §4.33** (subsample → full-data rule; Bouchet `salt_local/bouchet/m_moveout_tract_full.sql`, 14 min, 4c/184G; local `d2_moveout_full.R`). 528m address pairs, 68.4k tracts, mean 11.3%/yr annualised. County exposure (per SD, weighted): pre 2013–16 −0.08 to −0.11 pp, post 2018–21 −0.04 to −0.11 (SE 0.02–0.03). Border DDD near × high: pre −0.07 to +0.06, post −0.07, −0.06, −0.03, −0.01 (SE 0.07–0.12). × exposure: pre +0.01 to −0.02, post +0.01 to −0.05 (SE 0.03–0.05). **The full data confirm no move-out response.**
- District DDD × district ZIP-based exposure (mean of the district's tracts' winsorised ZIP exposure, SD units): event study near × high × exposure pre −0.1 to +0.4%, post +0.3 to +1.1%; near × low × exposure pre −0.3 to +0.2%, post +0.5 to +2.1% (SE 0.3–1.0). Pooled post: near × exposure (both sides) +1.5% (0.7); **extra on the high side −0.9% per SD (SE 0.6, p ≈ 0.15)**; base near × high −2.2% (1.0). Exposed near-border districts gained revenue on both sides, the high side less. Right direction for a SALT dose-response, but not significant.
- County-line placebo (`a9_county_line_placebo.R`): the same design on within-state county pairs (7,825 shared lines; tracts ≥ 20 mi from any state border; 2,679 districts, 1,391 near-high, 579 pairs). Pre 2012–16 +3.0, +3.2, +5.1, +2.7, +1.3%; post 2018–23 +0.8, +1.8, −0.9, −1.1, −0.5, −3.2% (SE 2–5); pooled −3.1% (3.6). **Uninformative:** county lines are dense, so almost all districts are "near", estimates are imprecise and the pre-trend is as large as anything after.
- Leave-one-state-out (all pairs involving a state dropped): pooled near × high × post ranges −1.4% (drop OK) to −2.9% (drop PA); dropping NJ −2.8%, IL −2.6%, TX −1.6%. Not driven by the levy-cap states (NJ, NY, IL).

### 4.35 Repeat sales and cross-border move direction (Bouchet devel, 2026-10-20 23:30)
- **Repeat sales** (`salt_local/bouchet/r_repeat_sales.sql` → `data/local_juris/kpanel/repeat_sales.parquet`: 28.9m arm's-length sales 2012–24 of 12.8m residential parcels with ≥ 2 sales, 2018-roll tract; `b6_repeat_sales.R`, 30% of parcels, parcel FE + pair × side × year + pair × distance-bin × year + ZIP exposure × year).
  - Near × high: pre +0.6 to +2.2%, post +1.5 → +5.6% (SE 1.1–2.3); no decline.
  - Near × high × ZIP exposure (per SD): pre −0.9 to +0.8%; post +0.1, −0.3, −0.3, −0.6, −0.8, −1.9, −2.0% (2018–24, SE 0.6–1.6). Near × low × exposure (placebo): post −0.3 to +0.5%.
  - Pooled: high × exposure post −0.2% (0.8), plus −1.0% (0.7) from 2021; low-side 0. **The direction matches capitalisation of the cross-border option in exposed areas (high side declines gradually, low side flat), but it is not significant.**
- **Direction of cross-border moves** (`salt_local/bouchet/f_family_moves_1pct.sql`: Data Axle 1% family sample, 3.7m families, 47–68k address changes a year with LOR ≤ 2, 15–19% interstate; `d3_family_moves.R`). 4,306 moves with origin and destination within 20 mi of the same pair on opposite sides.
  - Share going high → low: pair FE year effects ≈ 0 (±0.05).
  - Pairs where the high-tax side's state is also more SALT-exposed: share 0.48–0.55 (2013–15) → 0.62–0.65 (2016–21). Relative to other pairs vs 2017: 2013–15 −0.09 to −0.14, 2016 +0.04, 2018–21 +0.08, +0.07, +0.04, +0.11 (SE 0.04–0.06). The shift toward high → low moves in SALT-contrast pairs mostly happens by 2016, so it is not a clean post-2017 break.
- Repeat sales, all parcels: exceeds laptop memory (14–16 GB) at 55% and 100%. **Replication on an independent second 30%** (`RS_OFF=0.3`, `b6_repeat_sales_rep2.txt`): near × high × exposure pre −2.2 to 0% (2016 −1.1), post −1.3 to −2.8%; pooled post −0.5% (0.8), plus −0.8% (0.8) from 2021; low side ≈ 0. Two independent halves give late effects of −1.0 and −0.8% per SD (each SE ≈ 0.75): same sign, borderline jointly. Near × high alone rises in both (+1.5 → +5%, with 2016 already +1.8 to +2.2%).
- **Full-data cross-border moves** (Bouchet devel `salt_local/bouchet/f_family_moves_full.sql`: families in 8,555 ZIPs near state borders, year pairs 2012→13 … 2020→21, 12.0m address changes, 1.36m interstate; `d3_family_moves.R` with `FM_IN='fmfull/*.parquet'`, output `d3_family_moves_full.txt`). 427,738 moves between opposite sides of a pair, both within 20 mi.
  - Share going high → low: 0.40 (non-contrast pairs) and 0.59 (pairs whose high side is the more exposed state), stable.
  - Pair FE year effects: pre +0.0 to +0.9 pp, post −0.5, −1.0, −1.8, −3.5 pp (SE 0.4–1.5): if anything, *fewer* moves toward the cheaper side.
  - SALT-contrast difference: pre −5.7, −3.5, −1.8, +0.2 pp (a pre-trend that ends in 2016), post +0.3, −0.5, +1.7, +4.6 (SE 0.9–2.6).
  - **No post-2017 shift toward the low-tax side.** The 1% sample's pattern was the pre-trend plus noise.
- Pooled repeat sales on 60% of parcels (`b7_repeat_sales_pooled.R`, lean model): near × high × exposure, post −0.4% (0.8), late −0.9% (0.7); low side 0. SEs barely shrink (county clustering dominates). Exposure alone (within pair × side × year): post −3.5% (0.4), late a further −2.1% (0.35) per SD.

### 4.36 (c) Repeat-sales prices and ZIP SALT exposure (all residential parcels)
`code/analysis/salt_local/b8_repeat_sales_exposure.R`; 30% of parcels with ≥ 2 arm's-length sales 2012–24 (7.8m sales, 3.5m parcels), parcel FE, SE by county; exposure = 2016 SOI ZIP SALT above $10k per return, winsorised at p99, SD units. Figure `figures/b8_repeat_sales_exposure.png`.
- **State × year FE: 2012–16 +1.3, +1.2, +1.4, +1.1, +0.9% (flat); 2018 −1.3, 2019 −2.2, 2020 −3.2, 2021 −4.4, 2022 −4.8, 2023 −4.9, 2024 −5.6% (SE 0.2–0.7).** A clear break at the cap, already visible in 2018–19 before the pandemic.
- County × year FE (within-county ZIP variation): 2012–16 +3.3, +2.4, +2.3, +1.6, +1.1% (a downward pre-trend of ≈ −0.55%/yr); 2018–24 −1.7, −1.9, −2.7, −3.5, −3.7, −3.2, −3.7%. Relative to the extrapolated pre-trend, about −1.1% in 2018, −0.8% in 2019, −0.6 to −1.3% in 2020–21, ≈ 0 by 2023–24.
- Reading: high-SALT ZIPs' repeat-sales prices fell ≈ 2–2.5% per SD relative to the rest of their state in 2018–19, growing to ≈ 5–6% by 2021–24, from a flat pre-period. This is consistent with capitalisation of the cap, about 1 pp of which is clearly pre-COVID. Within counties, half or more of it is a continuing pre-trend, so the cross-county/within-state comparison drives the result.
- **Income control** (ZIP shares of 2016 returns with AGI ≥ $200k and $100–200k, each × year; corr(exposure, share ≥ 200k) = 0.85): exposure coefficient 2014–16 ≈ 0 (2012 +3.7, 2013 +1.5); 2018 0.0, 2019 −0.5, 2020 −1.2, 2021 −1.7, 2022 −2.6, 2023 −2.8, 2024 −2.5% per SD (SE 0.5–1.2). The $100–200k share carries a strong secular downward gradient over the whole period (+7.9% in 2012 → −8.0% in 2024). **Conditional on income, a SALT-specific decline of ≈ 2.5% per SD remains, but it builds from 2020 rather than at the cap. The immediate 2018–19 decline in the unconditional version is largely an income gradient.**

### 4.37 (a) Revenue slowdown with an income control
`code/analysis/salt_local/a10_revenue_income_control.R` (CoG, weighted by returns, trend break FY2019+; county share of 2016 returns with AGI ≥ $200k, corr with exposure 0.75).
- School districts by county: without control, exposure slope change −0.30%/yr (0.10). With the income share × trend/post/post × trend, the slope change loads on income (−0.58%/yr, SE 0.24), and exposure goes to +0.10 (0.20). **The school-levy slowdown is a high-income-county slowdown, not SALT-specific conditional on income.**
- County governments: exposure slope change −0.67 (0.27) → **−0.76%/yr (0.40) with the income control** (borderline); income itself +0.13 (0.31). The county-government slowdown is exposure-specific.
- **Decisive placebo: split by pair SALT level** (return-weighted state exposure; "low-SALT pairs" = both states at or below the median of pair maxima: 32 pairs, e.g. OK-TX, KY-OH, IN-OH, AL-GA, LA-TX, OH-PA). **Low-SALT pairs: pooled −4.2% (SE 1.6), event study 2015 −2.5, 2016 −2.7, 2018 −3.4 … 2023 −6.8%. Pairs involving a higher-SALT state (29 pairs, 492 near-high districts): −0.5% (1.4), all years within ±1.6%.** The border revenue decline is concentrated where SALT exposure is low on both sides and starts before 2017. **It is not a SALT-cap effect.** It is a pre-existing relative decline of high-side border districts at low-tax borders (oil-state and Midwestern pairs prominent).
- Strictest version (county × year FE + income shares × year; within-county ZIP variation conditional on income): exposure 2014–16 −0.7, −0.6, −0.6% (flat; 2012 +1.9); 2018 −0.2, 2019 −0.7, 2020 −1.2, 2021 −1.4, 2022 −2.0, 2023 −2.1, 2024 −2.1% per SD (SE 0.6–0.9). Net of the pre-level ≈ −1.4% by 2022–24 (≈ 1.6 SE). The direction survives; the size is roughly halved and borderline.

### 4.38 Follow-ups (2026-10-21 morning)
- **Instrument, one-sided** (`salt_local/c4_iv_signed_distance.R`; figure `figures/c4_iv_signed_distance.png`; `c4_iv_one_sided.txt`). Near (< 10 mi) vs far within one side, pair FE, SD units:
  - High side only: first stage +1.53 / +0.57 (cheaper option / gap), but balance fails: achievement −0.24, SES −0.28, income −0.23, owner −0.20, year built −0.29, population −0.21 (SE 0.04–0.12).
  - Low side only: the same pattern (achievement −0.27, SES −0.32, income −0.20, owner −0.19, year built −0.18).
  - **Border tracts are negatively selected on both sides; there is no reverse selection on the low side.** The directional instrument is a spatial difference-in-differences: it relies on equal near-border selection on the two sides.
  - The plot shows a dip on both sides, but deeper on the low side at 0–5 mi for SES (−0.37 vs −0.09) and achievement (−0.135 vs −0.04). Balance therefore depends on bandwidth: at 0–5 mi the instrument tilts toward the high side being better-off (ach +0.06, SES +0.17, n.s. in R/26); at < 10 mi it balances.
  - Home values dip only on the high side near the border (−0.09). That could be the treatment itself (capitalisation) rather than a covariate imbalance.
- **Buffering reading of the border revenue result** (Q3): high-SALT pairs minus low-SALT pairs, near × high × year: 2012–16 −1.2, +0.7, +0.1, +1.9, +3.7%; 2018–23 +3.2, +3.9, +5.3, +3.2, +5.8, +6.1% (SE 2.3–2.9). Trend-break version: pre-slope +0.48%/yr (0.60), level shift at 2018 +0.7% (1.7), slope change +0.03 (0.66). A smooth divergence from 2014, no break at the cap, so no evidence of buffering.
- **Capitalisation factor** (Q5): ZIP exposure (winsorised) mean $1.29k, SD $2.89k of SALT above $10k per return; top-quintile ZIPs average $5.2k, median 2017 price $414k (overall $217k). One SD ≈ $2.9k lost deduction × ~0.30 marginal rate ≈ $870/yr extra federal tax per return; PV at 5% ≈ $17k. Price effect per SD at a ~$350k home: unconditional −5.6% ≈ $20k (factor ≈ 1.1); income-controlled −2.5% ≈ $9k (≈ 0.5); within-county + income −1.4% ≈ $5k (≈ 0.3). Identification is within state across ZIPs (state × year FE), or within county; not cross-state.
- **Assessment ratios** (Q6): county 2017 → 2022, prices +0.42 log points on average, assessment ratios −0.24. Within state, Δlog ratio on Δlog median price −0.14 (0.02), Δlog AV per parcel on Δlog price +0.10. Exposure effect on Δlog ratio +3.1%/SD, +3.0% after controlling for the price change. The national fall is assessment lag in a boom (valuation dates, multi-year revaluation cycles, assessment caps), but the exposure gradient is not explained by measured price changes.
- **Dispersion vs the earlier cross-border gap result** (Q4): national mean of within-county SD of tract effective rates 0.215 pp (2017) → 0.163 (2024), −24%, exactly as the level (1.19% → 0.90%). The CV is flat at 0.17–0.19. Gaps in pp shrink because all effective rates fell (prices outran bills, §4.19); relative gaps don't change. Consistent with the log-based dispersion results.
- **Predicted-endogenous balance test** (`salt_local/c5_iv_predicted_balance.R`, `c5_iv_predicted_balance.txt`, figure `figures/c5_iv_predicted_balance.png`). Step 1: x_cheap / tax_gap regressed on controls (SEDA achievement + SES, ACS income, BA share, white share, owner share, year built, population; optionally log home value) with pair FE; the prediction is the covariate index. Within-R² is only 0.014 (x_cheap) and 0.053 (tax_gap): the controls predict little of the endogenous variable.
  - Directional near × high: predicted x_cheap +0.002 (0.003) vs actual +0.274 (0.031), i.e. 0.8% of the first stage; with home value +0.007 (0.003), 2.4%. Predicted tax_gap +0.008 (0.012) vs actual +0.360 (0.065), 2%; with home value +0.023 (0.011), 6%.
  - One-sided, high side only: predicted x_cheap +0.011 (0.003), 3% of actual; tax_gap +0.040 (0.010), 13% of actual. Low side only: +0.007 / +0.020, n.s.
  - Plot: predicted access is flat in distance. Predicted tax_gap rises slightly toward the border on *both* sides (+0.04 / +0.02 pp in the 0–5 mi bins), so it differences out in the directional design.
  - **Passes:** whatever selection there is on observables predicts at most 1–6% of the first stage in the directional design. The one-sided (high side only) design has a small but significant predicted imbalance (3–13%). Caveat: the controls explain little of the endogenous variable, so the test has limited power against selection on unobservables.
- **Nonparametric version** (`salt_local/c6_iv_predicted_balance_np.R`, `c6_iv_predicted_balance_np.txt`, figure `figures/c6_iv_predicted_balance_np.png`). Target = endogenous variable demeaned within pair; features = controls raw and pair-demeaned, no location. Linear, GAM (mgcv bam, s(k=8) per feature) and random forest (ranger, 500 trees), all cross-fitted with 5 folds by county. Out-of-sample R²:
  - tax_gap: forest 0.084, linear 0.019, GAM 0.005; with log home value 0.147 / 0.053 / 0.069. (The in-sample linear within-R² of 0.053 in c5 overstated fit.)
  - x_cheap: all negative (−0.06 to 0.00). The controls have no out-of-sample predictive power for having a cheaper tract in reach.
  - Most important forest features: district achievement (within pair), year built, SES, white share.
- Forest-predicted tax_gap on the directional instrument: +0.036 (0.033) vs actual +0.360, i.e. 10%, n.s.; with home value +0.046 (0.027), 13%, p ≈ 0.09. Predicted x_cheap +0.003 (0.016), 1%. High side only: forest-predicted tax_gap +0.051 (0.016), 17% of actual, significant; with home value 18%. Linear and GAM predictions stay at ≤ 5% (directional).
- In the binned plot, the forest prediction of tax_gap steps up at the border on the high side (+0.04 to +0.055 pp at 0–10 mi) and dips on the low side (−0.05 at 5–15 mi). **So flexible prediction recovers up to ~10–13% of the directional first stage from observables (n.s.) and ~17% of the one-sided one (significant).** Observables can account for a modest share of the rate-gap first stage but none of the cheaper-option indicator.
- **Rate-difference range by signed distance** (`salt_local/c7_gap_range_signed_distance.R`, figure `c7_gap_range_signed_distance.png`). Per tract: own median − neighbour medians over tracts within 30 min drive (min / p10 / mean / p90 / max), averaged in 2.5-mi bins, split by all, same-state and cross-state neighbours.
  - Same-state neighbours: the mean is ≈ 0 everywhere and the p10–p90 band is flat across the border. So border proximity does not change within-state options.
  - Cross-state neighbours: the mean jumps from −0.87 pp (low side, < 10 mi) to +0.77 pp (high side). All reachable: the mean steps from −0.11 to +0.14 at the border; the max (own − cheapest) rises from 0.62 to 1.06.
  - The low side's own − most-expensive (min) dips to −2 to −2.7 pp at 0–5 mi, in the same-state panel too. These are extremes of any-sales medians and likely noisy.
- **Pair rate gaps vs state school finance** (`salt_local/c8_pair_gap_school_finance.R`; F-33 FY2016 state aggregates; 59 pairs).
  - In 76% of pairs the high-rate side has the lower state-aid share. In 69% it raises more property tax per pupil.
  - The size of the gap is unrelated to the aid-share difference (R² 0.001, slope −0.001 pp per pp of aid share). Property tax per pupil explains 12%.
  - So school finance goes in the right direction but does not account for the size of the rate gaps.
- **Coverage checks** (`salt_local/c9_border_assign_district_coverage.R`).
  - **Border-pair step.** The current rule (R/26) loses 6,628 of 63,978 tracts.
    - 93 tracts: the nearest boundary line belongs to another pair of states. All are ≥ 10 mi out, median 90 mi.
    - 6,535 tracts: a side has < 20 tracts within 20 mi, which rules out 33 mostly western/Plains pairs.
  - **Alternative rule:** nearest line among the own state's pairs, and rank sides with ≥ 5 tracts within 20 mi.
    - Keeps 60,982 tracts in 90 pairs. 13 sparse western pairs stay unranked (2,996 tracts).
    - No tract kept by both rules changes pair.
    - Within 50 mi: 35,082 vs 33,192.
    - Saved to `data/local_juris/iv_tract_side_ownstate.rds`.
  - **School districts per parcel** (1% residential sample, lower 48 + DC).
    - 94.5% of parcels have coordinates. 99.94% of those fall in a real TIGER cb 2019 district polygon (AZ 98.1%).
    - Unified 93.1%; elementary 6.9%; secondary 6.7%. Elementary + secondary ("dual" systems) cover 6.7%, and the tract-level `leaid` keeps only the elementary layer.
    - CoreLogic's own `school_dist` field covers 64%.
    - In instrument-sample tracts: the parcel's district equals the tract's assigned `leaid` for 95.0% of parcels. 13.1% of tracts have sampled parcels in > 1 district.
  - **Shapefile universe.**
    - TIGER cb 2019 has 10,885 unified, 1,946 elementary and 482 secondary polygons (18 "not defined").
    - Of F-33 FY2016 districts with enrolment (lower 48 + DC), 96.6% have a polygon, covering 99.3% of enrolment and 99.7% of property-tax revenue.
    - Missing ones are mostly non-geographic agencies (county offices, vocational, state-run) plus a few CA city districts (Modesto, Santa Rosa, Petaluma, Santa Cruz city schools). These are probably coded under different elementary/high LEAIDs in TIGER (not verified).
- **District-level data coverage** (`salt_local/c10_district_data_coverage.R`).
  - Universe: TIGER cb 2019 polygons in the lower 48 + DC: 10,808 unified, 1,946 elementary and 482 secondary.
  - **F-33 finance:**
    - Each year 2012–23 covers 98–99% of unified/elementary districts and 99.1–99.6% of parcels. A balanced 12-year panel covers 98.6% of parcels; secondary districts only 79%.
    - Property tax is zero for dependent school systems funded by their parent government (F-33 item T02): CT, MD, MA, NC, VA, DC (100% of districts), TN 90%, RI 89%, VT 98%, ME 63%. Only 84% of parcels lie in a district with a positive F-33 property tax.
  - **SEDA 4.1:** achievement covers 99.4% of parcels, SES 99.2%. Geographic districts only, so there is nothing for the secondary layer. Gaps: VT (31% of parcels), MI (11%).
  - **CoreLogic 1% sample:** has parcels in 89% of unified and 83% of elementary districts. The misses are small districts, which the full roll would cover.
- **F-33 fill rates and property tax vs CoreLogic** (`salt_local/c11_f33_fill_and_ptax_compare.R`).
  - **Trap:** the elsec `STATE` column is the Census state code (alphabetical; 15 = IN, 09 = DC), not FIPS. Use `CONUM` / `NCESID` for FIPS. Earlier scripts used `NCESID`, so they are unaffected.
  - **Fill rates.** 130 fields are present every year 2012–23, with no missing values (0 = zero or not applicable). 46 fields were added from FY2020 (ESSER/COVID relief AR*/AE*, new C/B items).
    - Non-zero in > 95% of records every year: total revenue and expenditure, federal, state and local revenue, current spending, instruction, total salaries (Z32), benefits (Z34), capital outlay.
    - Enrolment (V33): 93.7%; zeros are non-operating or agency records.
    - Property tax (T06): 84.4–85.5% every year; zeros are dependent systems. Parent-government contributions (T02): 7.6%.
    - Long-term debt: about 77%. From cities and counties (D23): about 50%.
    - Full table in `c11_f33_field_fill.csv`.
  - **Property tax 2017 by state, same counties** (CT state-wide, because the CoG 2017 uses planning regions).
    - US totals: CoreLogic bills $515bn; CoG all local governments $490bn; F-33 school T06 $195bn (T06 + T02 $253bn).
    - CoG / CoreLogic is mostly 0.8–1.25. It is about 2 in WV and WY, and 1.4–1.6 in SC and MS: personal, mineral and vehicle property is taxed but not on the parcel roll. It is 0.28 in VT, where the statewide education tax counts as a state tax.
    - School share (T06 / CoreLogic) ranges from 0.2 (KS, MT, NY) to 0.8 (DE, OK, SC). It is about 0 for dependent systems: CT, MA, MD, NC, VA, DC, TN, RI, VT.
- **School share of property tax.** CoG FY2017: school-district property tax $212bn of $508bn for all local governments (42%), before counting dependent systems' share of parent-government levies (F-33 T02 $57bn, part of it property tax). Roughly 45–50% of US property tax funds schools, so F-33 is not undercounting.
- **Covariate coverage in the instrument's tracts** (`salt_local/c12_border_covariate_coverage.R`).
  - ≥ 97.5% of tracts in every side × distance cell have: SEDA achievement, SES and % FRL; F-33 spending per pupil; ACS income, home value, education, race, tenure and year built; CoreLogic rate.
  - The exception is district property tax per pupil (> 0): 79% overall, and 55% on the low side < 10 mi, because dependent systems (VA, NC, MD, TN, MA, CT, RI) are disproportionately on low-tax sides.
- **Side selection in levels** (`salt_local/c13_side_selection.R`, figure `c13_side_selection.png`). High minus low side within pair, tracts < 5 / < 10 mi, and a local-linear jump at 0 within 10 mi; SE by county.
  - Effective rate: +0.75 to +0.81 pp (≈ 1 SD).
  - Tract demographics and housing (ACS income, home value, BA share, white share, owner share, year built, age, population): all |Δ| ≤ 0.13 SD at < 5/10 mi, none significant. Home value is −0.10 SD (n.s.), in the direction of capitalisation.
  - School district: SES +0.27 to +0.35 SD (p ≈ 0.02–0.05); FRL −0.31 to −0.37 SD; test scores +0.20 to +0.31 SD (p ≈ 0.1); spending per pupil −0.05 SD (n.s.).
  - So the high-tax side has somewhat more advantaged school districts, but not more advantaged tracts. FRL is partly a state-policy artefact (eligibility and community-eligibility take-up).
- **Side profiles out to 100 mi** (`salt_local/c14_side_profiles.R`, figure `c14_side_profiles.png`; 5-mi bins, pair FE, SD units).
  - Near (< 10 mi) vs inland (50–100 mi):
    - Tax rate: high side 0.43 vs 0.28; low side −0.61 vs −0.30. The rates diverge towards the line.
    - District SES: high −0.10 vs 0.04; low −0.37 vs 0.00.
    - FRL: high 0.07 vs 0.03; low 0.39 vs −0.10.
    - Test scores: high −0.08 vs −0.03; low −0.28 vs −0.01.
    - Tract income: high −0.11 vs 0.07; low −0.23 vs −0.02.
  - So the school-district gap at the border comes from a dip on the low-tax side near the line, not from an advantage on the high-tax side. The high side near the border looks like its own interior.
  - Other tract characteristics are flat within the wide CIs.
- **Border RD on covariates** (`salt_local/c15_border_rd.R`, figure `c15_border_rd.png`). rdrobust: local linear, triangular kernel, MSE-optimal h (10–20 mi), robust bias-corrected CIs, SE by county; tracts within 100 mi (44,524). Pair heterogeneity handled by demeaning by pair (A) or by pair dummies as covariates (B).
  - Tax rate jump: +0.75 / +0.77 pp (≈ 0.9 SD), p < 0.001.
  - No covariate is significant at 5%. Smallest robust p: test scores 0.09–0.12, SES 0.10–0.14, FRL 0.10–0.12, owner share 0.10–0.13, population 0.10–0.15. Holm-adjusted p = 1 for all 11.
  - Point estimates are close to the c13 side means (SES +0.3 SD, FRL −0.33 SD, test scores +0.3 SD), but robust CIs span about ±0.35 SD. So the RD can rule out school-district jumps above ~0.6 SD, not ~0.3 SD.
- **Border RD by year 2013–2024** (`salt_local/c16_border_rd_by_year.R`, figure `c16_border_rd_by_year.png`). rdrobust, fixed h = 15 mi, pair-demeaned within year, robust CIs, SE by county; sides fixed. The c15 covariate RD was a cross-section mixing years: tract rates from 2017–19 sales, SEDA pooled 2009–19, ACS 2013–17, F-33 FY2016.
  - **Effective rate gap** falls steadily from 0.92 pp (2013) and 0.79 (2017) to 0.70 (2018), 0.60 (2020) and 0.43 pp (2024). Every year is significant. There is no break at 2018; the decline speeds up from 2020.
  - **Log nominal rate gap:** flat at about 0.3 from 2015.
  - **Log bill per parcel gap:** about 0.1, flat and n.s.
  - **Log price gap:** −0.27 (2015–16) to −0.14 (2024), narrowing but n.s.
  - **Log assessment-ratio gap:** 0.26 (2015) to 0.08 (2024), n.s.
  - **Sales per parcel, F-33 spending, property tax per pupil and aid share:** flat and n.s.
  - So the cross-border effective-rate convergence comes from the denominator (prices and assessment ratios converging), not from bills or nominal rates.
  - ACS and SEDA covariates are not yet in yearly form.
