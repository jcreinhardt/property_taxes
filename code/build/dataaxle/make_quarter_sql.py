#!/usr/bin/env python3
"""Generate the DuckDB SQL that converts one quarterly Data Axle file (US_Consumer_5_File_YYYY_Month.csv.gz) to typed,
sorted zstd parquet on Bouchet scratch. Usage: python3 make_quarter_sql.py 2019 June > convert_2019_06.sql
Pass 1 streams the gzip CSV once (all VARCHAR, names hashed, raw names never stored) to a raw parquet.
Pass 2 reads the raw parquet (fast, parallel): per-column conversion-failure report, then the typed parquet.
Types follow the annual table dataaxle_annual; differences: booleans keep NULL when blank (annual turned NULL into false)."""
import sys
year, month = sys.argv[1], sys.argv[2]
FROM_RAW = "--from-raw" in sys.argv   # skip pass 1 (raw parquet already exists)
# 2024 files keep the 2023 header but the person fields changed content (verified 2026-10-16 on 2024 March vs 2023 December,
# 178k common FamilyIDs): person 1: title_code -> gender (M/F/U), Gender_1 -> age decade code 1-9 (= age // 10), Age_1 blank,
# Ethnicity_Code_1 -> new 2-digit numeric code. Persons 2-5 (8 header fields): IndividualID_i -> first name, First_Name_i -> middle
# initial, Middle_Name_i -> last-name initial, Last_Name_i blank, Suffix_Code_i -> gender, Gender_i -> age decade, Age_i blank,
# Ethnicity_Code_i -> numeric code. No individual ids or full last names for persons 2-5.
LAYOUT_2024 = int(year) >= 2024
mnum = {"March": "03", "June": "06", "September": "09", "December": "12"}[month]
RAW = "/nfs/roberts/pi/pi_csl53/corelogic/raw_data/dataaxle/quarterly/US_Consumer_5_File_%s_%s.csv.gz" % (year, month)
OUT = "/nfs/roberts/scratch/pi_csl53/jr2728/da_quarterly"
RAWP = f"{OUT}/raw/da_{year}_{mnum}_raw.parquet"
TYPED = f"{OUT}/fy={year}/fm={mnum}"   # hive-partitioned by state inside; read with read_parquet('.../fy=*/fm=*/*/*.parquet', hive_partitioning=true)
REP = f"/home/jr2728/ptax_explore/hh_strat/out/daq_{year}_{mnum}"

# (raw header name, output name, kind) kind: v=varchar, i=integer type, m=money x1000, b=bool '1'/'0', z=zero->NULL, d=double zero->NULL
cols = [
 ("FamilyID","family_id","BIGINT"), ("DOWNGRADE_REASON_CODE","downgrade_reason_code","nn"), ("DOWNGRADE_DATE","downgrade_date","z"),
 ("RECENCY_DATE","recency_date","v"), ("LOCATION_TYPE","location_type","v"), ("PRIMARY_FAMILY_IND","primary_family_ind","b"),
 ("HOUSEHOLDSTATUS","household_status","v"), ("TRADELINE_COUNT","tradeline_count","USMALLINT"), ("HEAD_HH_AGE_CODE","head_hh_age_code","v"),
 ("LENGTH_OF_RESIDENCE","length_of_residence","USMALLINT"), ("CHILDRENHHCOUNT","children_hh_count","USMALLINT"), ("CHILDREN_IND","children_ind","b"),
 ("ADDRESSTYPE","address_type","v"), ("MAILABILITY_SCORE","mailability_score","USMALLINT"), ("WEALTH_FINDER_SCORE","wealth_finder_score","UBIGINT"),
 ("FIND_DIV_1000","find","m"), ("OWNER_RENTER_STATUS","owner_renter_status","USMALLINT"), ("ESTMTD_HOME_VAL_DIV_1000","estmtd_home_val","m"),
 ("MARITAL_STATUS","marital_status","USMALLINT"), ("PPI_DIV_1000","ppi","m"),
 ("CBSACODE","cbsa_code","UINTEGER"), ("CBSATYPE","cbsa_type","USMALLINT"), ("CSACODE","csa_code","UINTEGER"), ("LOCATIONID","location_id","UBIGINT"),
 ("HOUSE_NUM","house_num","v"), ("HOUSE_NUM_FRACTION","house_num_fraction","v"), ("STREET_PRE_DIR","street_pre_dir","v"), ("STREET_NAME","street_name","v"),
 ("STREET_POST_DIR","street_post_dir","v"), ("STREET_SUFFIX","street_suffix","v"), ("UNIT_TYPE","unit_type","v"), ("UNIT_NUM","unit_num","v"),
 ("BOX_TYPE","box_type","v"), ("BOX_NUM","box_num","v"), ("ROUTE_TYPE","route_type","v"), ("ROUTE_NUM","route_num","v"),
 ("CITY","city","v"), ("STATE","state","v"), ("ZIP","zip","UINTEGER"), ("ZIP4","zip4","UINTEGER"), ("DPBC","dpbc","v"),
 ("VACANT","vacant","b"), ("USPSNOSTATS","usps_nostats","b"), ("GE_LATITUDE_2010","ge_latitude_2010","d"), ("GE_LONGITUDE_2010","ge_longitude_2010","d"),
 ("GE_CENSUS_LEVEL_2010","ge_census_level_2010","v"), ("GE_CENSUS_STATE_2010","ge_census_state_2010","USMALLINT"),
 ("GE_ALS_COUNTY_CODE_2010","ge_als_county_code_2010","USMALLINT"), ("GE_ALS_CENSUS_TRACT_2010","ge_als_census_tract_2010","zi"), ("GE_ALS_CENSUS_BG_2010","ge_als_census_bg_2010","USMALLINT"),
 ("title_code","title_code","USMALLINT"),
]
if not LAYOUT_2024:
    for i in range(1, 6):
        cols += [(f"IndividualID_{i}", f"individual_id_{i}", "UBIGINT"), (f"Suffix_Code_{i}", f"suffix_code_{i}", "v"), (f"Gender_{i}", f"gender_{i}", "v"),
                 (f"Age_{i}", f"age_{i}", "USMALLINT"), (f"Ethnicity_Code_{i}", f"ethnicity_code_{i}", "v")]
else:
    cols = [c for c in cols if c[1] != "title_code"]
    cols += [("IndividualID_1", "individual_id_1", "UBIGINT"), ("Suffix_Code_1", "suffix_code_1", "v"), ("title_code", "gender_1", "v"),
             ("Gender_1", "age_decade_1", "USMALLINT"), ("Ethnicity_Code_1", "ethnicity_code_1", "v")]
    for i in range(2, 6):
        cols += [(f"Middle_Name_{i}", f"ln_initial_{i}", "v"), (f"Suffix_Code_{i}", f"gender_{i}", "v"), (f"Gender_{i}", f"age_decade_{i}", "USMALLINT"),
                 (f"Ethnicity_Code_{i}", f"ethnicity_code_{i}", "v")]
# dropped: MSA2000_*, CSA2000_CODE (empty after 2010), GE_CENSUS_COUNTY/TRACT/BG (old vintage), Middle_Name_i; First/Last names hashed.
names = [f"{p}_Name_{i}" for i in range(1, 6) for p in ("First", "Middle", "Last")]
if LAYOUT_2024:
    names += [f"IndividualID_{i}" for i in range(2, 6)]            # these hold first names in 2024
    names = [n for n in names if n not in [f"Middle_Name_{i}" for i in range(2, 6)]]   # these hold last-name initials in 2024: keep

def nb(c): return f"nullif(trim(\"{c}\"), '')"
def conv(c, o, k):
    x = nb(c)
    if k == "v": return f"{x} AS {o}"
    if k == "b": return f"CASE WHEN {x} = '1' THEN true WHEN {x} = '0' THEN false END AS {o}"
    if k == "nn": return f"({x} IS NOT NULL) AS {o}"
    if k == "z": return f"nullif({x}, '000000') AS {o}"
    if k == "m": return f"try_cast({x} AS UBIGINT) * 1000 AS {o}"
    if k == "d": return f"nullif(try_cast({x} AS DOUBLE), 0) AS {o}"
    if k == "zi": return f"nullif(try_cast({x} AS UBIGINT), 0) AS {o}"
    return f"try_cast({x} AS {k}) AS {o}"
def failexpr(c, o, k):
    x = nb(c)
    t = {"m": "UBIGINT", "d": "DOUBLE", "zi": "UBIGINT"}.get(k, k)
    if k in ("v", "nn", "z"): return None
    if k == "b": return f"({x} IS NOT NULL AND {x} NOT IN ('0','1'))"
    return f"({x} IS NOT NULL AND try_cast({x} AS {t}) IS NULL)"

sql = [f"""SET preserve_insertion_order=false; USE memory; SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
-- Quarterly Data Axle {year} {month}: gzip CSV -> raw parquet (names hashed) -> conversion report -> typed parquet.
CREATE OR REPLACE MACRO hn(x) AS CASE WHEN coalesce(trim(x), '') <> '' THEN hash(upper(trim(x))) END;
.timer on
{"-- Pass 1 skipped (--from-raw)" if FROM_RAW else "-- Pass 1: single streaming read of the gzip"}
{"/*" if FROM_RAW else ""}COPY (
  SELECT * EXCLUDE ({", ".join(names)}),
         {("hn(First_Name_1) fn_1, hn(Last_Name_1) ln_1, " + ", ".join(f"hn(IndividualID_{i}) fn_{i}, NULL::UBIGINT ln_{i}" for i in range(2, 6))) if LAYOUT_2024 else ", ".join(f"hn(First_Name_{i}) fn_{i}, hn(Last_Name_{i}) ln_{i}" for i in range(1, 6))}
  FROM read_csv('{RAW}', all_varchar = true, header = true, ignore_errors = true, store_rejects = true, rejects_limit = 1000000)
) TO '{RAWP}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 1000000);
COPY (SELECT * FROM reject_errors) TO '{REP}_rejects.csv' (HEADER);
COPY (SELECT * FROM reject_scans) TO '{REP}_reject_scans.csv' (HEADER);{"*/" if FROM_RAW else ""}
-- Pass 2a: conversion-failure report (non-blank values that do not cast), with examples
CREATE OR REPLACE TABLE raw AS SELECT * FROM read_parquet('{RAWP}');
COPY (SELECT count(*) n_rows, count(DISTINCT FamilyID) n_family_ids FROM raw) TO '{REP}_rows.csv' (HEADER);
COPY (
  SELECT * FROM (VALUES
    {",\n    ".join(f"('{o}', (SELECT count(*) FROM raw WHERE {nb(c)} IS NOT NULL), (SELECT count(*) FROM raw WHERE {failexpr(c,o,k)}), (SELECT string_agg(DISTINCT left(v, 20), ' | ') FROM (SELECT {nb(c)} v FROM raw WHERE {failexpr(c,o,k)} LIMIT 5)))" for c,o,k in cols if failexpr(c,o,k))}
  ) t(col, n_nonblank, n_fail, examples)
) TO '{REP}_convert_report.csv' (HEADER);
-- Pass 2b: typed, sorted parquet
COPY (
  SELECT {", ".join(conv(c,o,k) for c,o,k in cols)},
         {", ".join(f"fn_{i}, ln_{i}" for i in range(1, 6))},
         CASE WHEN nullif(trim(ADDRESSTYPE),'') = 'S' AND nullif(trim(HOUSE_NUM),'') IS NOT NULL AND nullif(trim(STREET_NAME),'') IS NOT NULL AND try_cast(ZIP AS UINTEGER) IS NOT NULL
              THEN hash(lpad(try_cast(ZIP AS UINTEGER)::VARCHAR, 5, '0'), upper(trim(STREET_NAME)), upper(trim(HOUSE_NUM))) END AS bkey,
         {year}::USMALLINT AS file_year, {int(mnum)}::UTINYINT AS file_month, DATE '{year}-{mnum}-01' AS file_date
  FROM raw
) TO '{TYPED}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 1000000, PARTITION_BY (state), OVERWRITE_OR_IGNORE true);
-- no global ORDER BY: sorting 183m wide rows took > 1 h and spilled; one file per state gives the same pruning by region
COPY (SELECT count(*) n_rows, count(DISTINCT family_id) n_family_ids, avg((address_type='S')::INT) share_street, avg((primary_family_ind)::INT) share_primary,
             count(bkey) n_bkey, min(recency_date) min_recency, max(recency_date) max_recency, avg(length_of_residence) mean_lor, {"avg(age_decade_1) mean_age_decade_1" if LAYOUT_2024 else "avg(age_1) mean_age_1"}
      FROM read_parquet('{TYPED}/*/*.parquet', hive_partitioning = true)) TO '{REP}_typed_summary.csv' (HEADER);
"""]
print("\n".join(sql))
