# build/exposure: SALT-cap exposure measures

All from the SOI tables in `data/database.db` (`build/soi`); outputs in `data/built/exposure/`.

| Script | Measure | Output |
|---|---|---|
| `01_county_stub_2016.R` | SALT above $10k per return, TY2016, stub by stub (`excess_pp`, `ex_z` in SD units) | `salt_exposure_county2016.csv` |
| `02_zip_stub_2016.R` | the same at ZIP level, mapped to tracts | `salt_exposure_tract_zip2016.csv` |
| `04_delta_deduction_value.R` | observed TY2016 → TY2018 change in the federal value of deductions per return (`d_salt`, `cap_bite`, `d_mid`), with assumed MFJ marginal rates by stub | `salt_exposure_county_delta.csv` |
| `05_components_county_state_zip.R` | the change split into property-tax, income/sales-tax and mortgage-interest parts, per return and per owner household (ACS owner counts), at county / state / ZIP; ZIP `clean` flag for uncensored cells | `exposure_components_{county,state,zip}.csv` |
| `06_simulated_instrument.R` | simulated instrument (Currie–Gruber): 2016 ACS owner-occupier heads through TAXSIM 35 under 2017 law, 2018 law and 2018 law with the cap removed; `dcap` = cap burden per owner household. Z1 = households at their own location (county / PUMA / state); Z2 = one national sample assigned to every state and a grid of effective rates (property tax = rate × own house value), interpolated at each county's / PUMA's 2016 CoreLogic rate. Needs `data/database.db` (ACS), `corelogic/county_year.rds`, `corelogic/tract/tract_eff.parquet`, `data/external/puma_rel/` | `simulated_instrument_{county,puma,state}.csv`, `simulated_z2_grid.csv`; caches `taxsim_z{1,2}.parquet`; comparison with `01` in `output/build/exposure/06_simulated_instrument.{txt,png}` |
| `07_state_income_tax_rates.R` | effective state income tax rates by state × tax year 2012–22: collections / SOI AGI (Census STC, `data/external/stc/`), TAXSIM on each year's ACS households, and law-only (fixed 2016 national sample through every state-year's law; also top-10% households) | `state_income_tax_rates.csv` (TAXSIM cache `taxsim_state_law.csv`) |

The current price designs (`designs/exposure_did/05`) use the per-owner components of `05`; `01` is the exposure of the
revenue, dispersion and SOI-migration designs. Regressions on these measures are weighted by 2016 returns. `06` is the
simulated alternative to `01`/`05`: same object (cap burden per owner), but from a frozen 2016 sample and the tax code only,
so it carries no 2017+ behavioural response (Z1) and no local composition (Z2). ACS top-codes property tax at $10,000 before
2018; `06` replaces top-coded values by max($10k, PUMA effective rate × house value). Finest geography: county where the ACS
identifies it (about 500 of 2,900 counties, most large ones), PUMA otherwise.
