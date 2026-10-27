# build/soi: IRS Statistics of Income into `data/database.db`

Imports the public SOI tables into the local DuckDB file `data/database.db` (tables `returns_state`, `returns_county`,
`returns_county_agi`, `returns_zipcode`, `returns_zipcode_agi`, `migration_state`, `migration_county`, plus the HUD
ZIP-to-county crosswalk). Run in order, from the repository root:

| Script | Reads | Writes |
|---|---|---|
| `01_importing_state_returns.R` | `data/SOI/state_returns/` | `returns_state` |
| `02_importing_county_returns.R` | `data/SOI/county_returns/` | `returns_county` |
| `03_importing_county_returns_w_agi.R` | `data/SOI/county_returns_w_agi/` | `returns_county_agi` (county × AGI stub; 2016 state rows carry `COUNTYFIPS` "0", later years "000") |
| `04_importing_hud_zip_to_county.R` | `data/hud/zip_to_county/` | `hud_zip_county` |
| `05_importing_zipcode_returns.R`, `06_importing_zipcode_returns_w_agi.R` | `data/SOI/zipcode_returns*/` | `returns_zipcode*` (counts < 20 are zero in the source) |
| `07_importing_state_migration.R`, `08_importing_county_migration.R` | `data/SOI/migration_*/` | `migration_state`, `migration_county` (dyads < 20 suppressed) |

The exposure measures in `build/exposure/` and the SOI designs in `designs/exposure_did/08-09` read these tables.
