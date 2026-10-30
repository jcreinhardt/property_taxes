# build/corelogic: CoreLogic extracts (clusters) and the panels built from them

CoreLogic is read only on the clusters (`.claude/skills/bouchet`, `corelogic`, `duckdb`); the SQL here runs there and
its parquet output is copied to `data/built/corelogic/` (see `data/built/MANIFEST.md`).

- `som/`: SOM HPC jobs (`run_small.sbatch`, `*.sbatch` + `*.sql`): `a_extract16` / `b_layers` (2016 roll with Census
  layers), `e_border_*` (border parcels 1%), `f_border_sample` (per-side samples), `h_grid_eff`, `j_tract_eff` (grid and
  tract effective rates), `i_border_vacancy`, `k_panel*` (full-roll jurisdiction/tract panel 2012–2024), `juris_counts`,
  `l_hedonic_sales.sql` + `l_price_indices.R` + `l_price_indices.sbatch` (2026-10-30: all residential arm's-length sales
  2012–2024 with parcel attributes, then three price indices per tract / county / school district × year: median log price,
  repeat-sales index with parcel FE, hedonic index with state-specific attribute coefficients → `data/built/corelogic/price_indices/`).
- `bouchet/`: the same panel split in two windows for Bouchet (`k_panel_2012_2018.sql`, `k_panel_2019_2024.sql`),
  repeat sales (`r_repeat_sales.sql`), and the 1% / 10% building-sample parcel panels (`p_*.sql`) that the archived
  household study built and that `designs/exposure_did/11` and `build/geo/01, 03` still read.
- `10_county_tract_year_panels.R`: `kpanel/` → `county_year.rds`, `tract_year.rds` (sales medians, bills, assessment ratios by year).
- `12_nominal_rates_by_cell.R`: median-regression nominal rate per tax-area cell from the SOM fit sample (`full/`, not on the laptop).
