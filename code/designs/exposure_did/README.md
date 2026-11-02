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
| `13_soi_state_six.R` | SOI state files: out-migration from the six Diop & Green "high-SALT" states (CA NY CT PA MA IL) vs the rest, state + year FE (the variation the other scripts absorb) |
| `14_simulated_instrument.R` | prices, revenue (school districts, county governments) and SOI migration on the simulated instrument (`build/exposure/06`): Z1, Z2, observed SOI loss (OLS) and SOI instrumented by Z1; county variation, ACS-identified counties and all counties with PUMA fill-in |
| `18_rents_altos.R` (`ALTOS_INDEX=repeat` → `_repeat` outputs on the repeat-listing index of `build/altos/03`, ZIP specs 1, 2, 6) | **asking rents** (Altos listings, `build/altos`): log rent at a fixed type x bedroom mix on the lost subsidy per owner (total, and the three components), ZIP and county, with / without the 2012 rent level x year |
| `15_simulated_instrument_migration.R` | the same for SOI migration counts (returns and people), in-migration and net migration at county level, and state-level out-of-state / in-from-other-state / net interstate rates (counties summed, state + year FE, population weights) |
| `17_housing_stock.R` | housing stock: log residential parcels and new-construction sales (roll, `build/corelogic/10`); log units, vacancy (all, for rent, for sale, seasonal), owner share, median year built (ACS 1-year and 5-year county panels, `build/acs/07`) |
| `17_state_did_big_four.R` | state-level exposure × year event studies (net, out, in rates; net to the big four / to the other 46 from the dyads) for all 50 states, CA NY FL TX only, the other 46, and all 50 with big-four × year FE; exposure per return, per owner, SALT/AGI 2017, CA/NY dummy |
| `18_state_did_big_four_dataaxle.R` | the same on Data Axle same-family_id moves (1% family sample, `output/validation/dataaxle/soi_vs_dataaxle/da_state_pairs_1pct.csv`), pp of linked families, 2012-2021 |
| `19_net_rate_change_dyads.R` | treatment = 2016→18 change in the net-of-federal-deduction state income tax rate (STC collections / AGI minus m × deducted income taxes / AGI; also TAXSIM cap burden): state-to-state dyads with dyad + year FE and the state event studies, all / big four / mixed / other 46 |

Numbers 17 and 18 were taken twice by sessions running in parallel: `17_housing_stock` / `18_rents_altos` (outputs `17_housing_stock*`, `d18_*`)
and `17_state_did_big_four` / `18_state_did_big_four_dataaxle` (outputs `c17_*`, `c18_*`). File names are distinct.

`16_diop_green_table7.R` (Diop & Green Table 7 / Table 6 on state-to-state dyads without their controls) was removed on
2026-10-28; results and figures in `notebook/2026-10-27-diop-green-table7-replication`, code in commit 73f3a8e
(`build/soi/10-11`, removed with it, are back because `17-19` read their panels).
Notebook: `notebook/2026-10-21-capitalisation-by-component`, `notebook/2026-10-21-migration-nulls`,
`notebook/2026-10-27-diop-green-reconciliation`, `notebook/2026-10-27-diop-green-table7-replication`, `notebook/2026-10-29-state-did-big-four`, `notebook/2026-10-29-net-rate-change-treatment`,
`notebook/2026-10-27-simulated-instrument-outcomes`, `notebook/2026-10-28-housing-stock`. Superseded versions
(`c25`, `c27`, `b5`, `a6`) are in `code/archive/2026-10-superseded/`.
