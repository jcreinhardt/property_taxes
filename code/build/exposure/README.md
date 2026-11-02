# build/exposure: SALT-cap exposure measures

All from the SOI tables in `data/database.db` (`build/soi`); outputs in `data/built/exposure/`.

| Script | Measure | Output |
|---|---|---|
| `01_county_stub_2016.R` | SALT above $10k per return, TY2016, stub by stub (`excess_pp`, `ex_z` in SD units) | `salt_exposure_county2016.csv` |
| `02_zip_stub_2016.R` | the same at ZIP level, mapped to tracts | `salt_exposure_tract_zip2016.csv` |
| `04_delta_deduction_value.R` | observed TY2016 → TY2018 change in the federal value of deductions per return (`d_salt`, `cap_bite`, `d_mid`), with assumed MFJ marginal rates by stub | `salt_exposure_county_delta.csv` |
| `05_components_county_state_zip.R` | the change split into property-tax, income/sales-tax and mortgage-interest parts, per return and per owner household (ACS owner counts), at county / state / ZIP; ZIP `clean` flag for uncensored cells | `exposure_components_{county,state,zip}.csv` |
| `07_state_income_tax_rates.R` | effective state income tax rates by state × tax year 2012–22: collections / SOI AGI (Census STC, `data/external/stc/`), TAXSIM on each year's ACS households, and law-only (fixed 2016 national sample through every state-year's law; also top-10% households) | `state_income_tax_rates.csv` (TAXSIM cache `taxsim_state_law.csv`) |

The current price designs (`designs/exposure_did/05`) use the per-owner components of `05`; `01` is the exposure of the
revenue, dispersion and SOI-migration designs. Regressions on these measures are weighted by 2016 returns.
