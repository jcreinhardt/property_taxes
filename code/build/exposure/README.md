# build/exposure: SALT-cap exposure measures

All from the SOI tables in `data/database.db` (`build/soi`); outputs in `data/built/exposure/`.

| Script | Measure | Output |
|---|---|---|
| `01_county_stub_2016.R` | SALT above $10k per return, TY2016, stub by stub (`excess_pp`, `ex_z` in SD units) | `salt_exposure_county2016.csv` |
| `02_zip_stub_2016.R` | the same at ZIP level, mapped to tracts | `salt_exposure_tract_zip2016.csv` |
| `04_delta_deduction_value.R` | observed TY2016 → TY2018 change in the federal value of deductions per return (`d_salt`, `cap_bite`, `d_mid`), with assumed MFJ marginal rates by stub | `salt_exposure_county_delta.csv` |
| `05_components_county_state_zip.R` | the change split into property-tax, income/sales-tax and mortgage-interest parts, per return and per owner household (ACS owner counts), at county / state / ZIP; ZIP `clean` flag for uncensored cells | `exposure_components_{county,state,zip}.csv` |

The current price designs (`designs/exposure_did/05`) use the per-owner components of `05`; `01` is the exposure of the
revenue, dispersion and SOI-migration designs. Regressions on these measures are weighted by 2016 returns.
