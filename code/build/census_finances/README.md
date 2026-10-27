# build/census_finances: Census of Governments and school-district finances

| Script | What |
|---|---|
| `01_download.sh` | downloads the Annual Survey of Local Government Finances individual-unit files to `data/census_gov_finances/raw/` |
| `02_import_cog.R` | loads them into `data/census_finances_test.db` |
| `03_clean_cog.R` | county aggregates: `govt_finances_sd_by_county`, `govt_finances_county`, `govt_finances_county_area` |
| `04_f33_district_panel.R` | F-33 district files `data/external/elsec/elsecYY.*` FY2012–2023 → `data/built/census_finances/district_finance.rds` |

Notes on the data: dependent school systems report `T06 = 0` and get property tax through the parent government; field
fill rates and the F-33 vs CoreLogic comparison are in `validation/coverage/02`. Connecticut's 2017 CoG uses planning
regions, so CT is collapsed to the state level in the revenue designs.
