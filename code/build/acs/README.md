# build/acs: ACS microdata imputations and tract/district yearly panels

| Script | What |
|---|---|
| `01_microdata_import.R` … `04_tcja_burden.R` | IPUMS ACS extract `data/ACS_microdata/usa_00004.csv` → `data/database.db`; imputes SALT deductions, itemisation and the TCJA burden per household (used by `paper/`) |
| `05_acs5_tract_download.R` | ACS 5-year tract tables 2013–2023 via tidycensus (`CENSUS_API_KEY` in `~/.Renviron`) → `data/external/acs/acs5_tract_YYYY.csv` |
| `06_tract_year_panel.R` | ACS releases 2020+ mapped from 2020 to 2010 tracts by land-area share (`data/external/tract_rel/`), median year built recoded (0 → 1939 from the 2018 release); SEDA 4.1 long files → `data/built/acs/acs_tract_year.rds`, `seda_district_year.rds` |

The yearly border RD (`designs/border_rd/04`) reads the two panels.
