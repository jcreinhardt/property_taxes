# designs/exposure_did: exposure × year event studies

Units (county, ZIP, state, district, parcel) with higher pre-period SALT exposure vs lower, before and after 2017:
`y ~ i(year, exposure, ref = 2017) | unit + state × year`, SE clustered by county, weighted by 2016 returns. Exposure from
`build/exposure/`. Outputs in `output/designs/exposure_did/` (file names keep the old script prefixes).

| Script | Outcome |
|---|---|
| `01_revenue.R` | county property-tax revenue (CoG): school districts by county, county governments, county area; trend-break version |
| `02_revenue_income_control.R` | the same with an income control |
| `03_cbsa_dispersion.R` | within-CBSA dispersion of district property tax per pupil |
| `04_county_outcomes.R` | county sales medians, bills, assessment ratios, nominal rates from the CoreLogic panel |
| `05_prices_by_component.R` | **current price result**: log median price on the three lost-subsidy components per owner, county / no-income-tax states / state / ZIP |
| `06_zip_pretrend_diagnostics.R` | why the ZIP design has a pre-trend (price tiers, not sample selection) |
| `07_repeat_sales_zip_exposure.R` | repeat-sales prices on ZIP exposure |
| `08_soi_migration_flows.R` | SOI county flows: out-migration rates and cross-border dyads |
| `09_soi_mover_agi.R` | AGI of movers vs stayers |
| `10_enrolment.R` | pupils per district (also the district border DDD) |
| `11_parcel_panel_outcomes.R` | parcel-level bills, AV, rates on the 1% panel (also the border DDD) |
| `12_revenue_outside_option.R` | county revenue × share of tracts with a cheaper cross-border option |

Notebook: `notebook/2026-10-21-capitalisation-by-component`, `notebook/2026-10-21-migration-nulls`. Superseded versions
(`c25`, `c27`, `b5`, `a6`) are in `code/archive/2026-10-superseded/`.
