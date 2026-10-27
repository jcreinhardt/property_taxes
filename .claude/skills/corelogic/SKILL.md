---
name: corelogic
description: How to use the CoreLogic tables (tax, ownertransfer deeds, mortgage) in corelogic.db / corelogic_public.db — keys, residential and owner-occupancy definitions and their accuracy, sale definitions, coverage gaps in the tax roll, bill/exemption fields, and the 1% samples. Use whenever a task touches cl.tax, cl.ownertransfer, parcels, sales or owner-occupancy.
---

# CoreLogic property data

Cluster/SQL rules: `bouchet`, `duckdb` skills. Findings: `text/data_exploration/dataaxle.md` §4.5c and §4.7.
Two copies: Bouchet `corelogic.db` (42 tables, also Data Axle) and SOM `corelogic_public.db` (17 tables, no Data Axle). Both read-only.

## Tables and keys
- `cl.tax`: one row per parcel × `tax_year` (2006–2023), 237 columns. Parcel id `clip` (qualify it in joins);
  `fips_code` (VARCHAR — compare as string, casting kills pushdown), `apn_parcel_number_unformatted`, situs address fields
  (`situs_house_number`, `situs_street_name`, `situs_unit_number`, `situs_zip_code`, `situs_city`, `situs_state`), owner
  names (hash them), mailing address fields, `property_indicator_code` (pic), `land_use_code`, values, `year_built`,
  `total_living_area_square_feet_all_buildings`, last-sale fields, and bill fields (below). `~/ptax_explore/cols.sh tax`.
- `cl.ownertransfer` (deeds): `clip`, `deed_situs_*`, `sale_derived_date`, `sale_amount`, `primary_category_code`
  (A = arm's-length, B/C = inter-family/trust/quitclaim, mostly unpriced), `interfamily_related_indicator`,
  buyer/seller names (hash). `cl.mortgage`, `llma_*`, `liens_*`, `permits*`, `raw_mls` also exist.
- Building key shared with Data Axle: `hash(left(situs_zip_code,5), upper(trim(situs_street_name)), upper(trim(situs_house_number)))`.
- **1% samples** (`~/ptax_explore/samples/`): `tax_bld1pct.parquet` (~50 cols, all years), `tax_bill_bld1pct.parquet`
  (bill, exemption, tax-area columns, 2012–21), `deeds_bld1pct.parquet`. Same 1% of buildings as the Data Axle samples.

## Standard definitions used in this project
- **Residential with structure**: pic in (10 SFR, 11 condo, 21 duplex, 22 apartment) and `calculated_improvement_value > 0`
  or living area > 0. **Michigan never fills `calculated_improvement_value`** — use living area / year built there.
- **Arm's-length sale**: `primary_category_code = 'A' AND NOT interfamily_related_indicator AND sale_amount > 0`.
  Owner-occupied SFR sell 4.4–4.8%/yr; tax owner surname changes 5.6%/yr (88% have a deed; the rest are non-arm's-length
  transfers, order swaps, parse differences and ~10% deed-less). The roll shows a new owner one roll later for most sales.
- **Owner-occupied** (`owner_occupancy_code`): this is a **deterministic rule, not an observation**: O ⇔ mailing address =
  situs address **or** `homestead_exempt_indicator`; A ⇔ mail elsewhere and no flag; blank ⇔ PO box. Bill-based check in
  states where owner-occupants pay a structurally lower effective rate (SC, IN, AL, MS, WV, ID, MI): 91–98% of
  bill-discounted homes are coded O (good recall), but 38–57% of **non**-discounted homes are also coded O and 9–35% of
  homes coded A receive the owner-occupant discount. Treat O as "bill mailed here", not as proof of occupancy; prefer the
  bill discount where available, or combine with a Data Axle primary family and the homestead flag.
- `homestead_exempt_indicator` is populated only where counties deliver it (MN 77%, FL 63%, TX 57%, SC 16%, UT 1%, zero in
  non-homestead states). `total_tax_amount`, `assessed_total_value`, `calculated_total_value`, `tax_area_code` /
  `tax_rate_area_code`, `total_tax_exemption_amount` (filled only CA, FL), `calculated_total_tax_exemption_amount`
  (CA, FL, OK, NM, NY, WV, …). MA, RI, MI have no tax-area code (cells = county, so town rate differences look like discounts).

## Coverage gaps in the tax roll (don't read them as exits)
- Between first and last appearance a parcel misses 0 years in 81.5%, 1 in 15.5%, 2+ in 3%. 85% of gaps are whole
  county-years with no rows delivered (740 county-years, concentrated 2013–15 and 2019: e.g. Suffolk NY 2019, Franklin OH
  2018, CT 2014, MD 2013/15, TX 2015) → `dropped_county_years_full.csv`.
- New England rolls are **by town**: in the 2013/14 transition each MA/CT/ME/NH/VT town appears under only one tax year
  (MA towns are either 2014-onwards or 2013-then-2015; CT skips 2014; ME/NH/VT skip 2013). Flag town-years
  (fips, situs_city, tax_year) with far fewer rows than usual as not delivered and carry parcels across.
- Isolated parcel gaps are 0.26% of address-years; 30% exist under another CLIP/address or coded non-residential that year.
- Other reasons for gaps: CAMA/vendor changes, tax-year labelling, blank situs, splits/merges, new construction,
  address re-standardisation, property recoding.

## Practical
- Narrow the column list in any full-table pull (237 columns > 1 h; 50 columns 30 min at 8c/96G).
- Deeds and tax use different situs formatting occasionally; match by `clip` first, building key second.
- Names/addresses of people are never written to the repo; hash in-query.
