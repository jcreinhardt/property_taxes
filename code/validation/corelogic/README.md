# validation/corelogic: checks on the CoreLogic tax roll

| Script | What |
|---|---|
| `01_coordinates.R` | parcel coordinates vs Census boundaries (1% sample, 2016) |
| `02_nominal_rate_test.R`, `03_rate_fit_all_states.R`, `04_rate_fit_split.R` | can a median regression bill ~ a + r·AV within a tax-area cell recover the nominal rate; which states fit a flat rate; split by exemption status |
| `05_rate_variance_decomp.R` | variance of nominal rates across state / county / place / district layers |
| `06_zip_nesting.R` | how cleanly ZIPs nest in tracts and school districts |
| `07_grid_rate_differences.R` | map of local effective-rate variation on a 5 km grid |

Findings are logged in `text/data_exploration/dataaxle.md` §4.9–4.13 and §4.22.
