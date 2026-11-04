# build/corelogic: CoreLogic extracts (clusters) and the panels built from them

CoreLogic is read only on the clusters (`.claude/skills/bouchet`, `corelogic`, `duckdb`); the SQL here runs there and
its parquet output is copied to `data/built/corelogic/` (see `data/built/MANIFEST.md`).

- `som/`: SOM HPC jobs (`run_small.sbatch`, `*.sbatch` + `*.sql`): `a_extract16` / `b_layers` (2016 roll with Census
  layers), `e_border_*` (border parcels 1%), `f_border_sample` (per-side samples), `h_grid_eff`, `j_tract_eff` (grid and
  tract effective rates), `i_border_vacancy`, `k_panel*` (full-roll jurisdiction/tract panel 2012–2024), `l_yearbuilt*` (year-built and roll-entry counts per
  county × tract × year on the residential roll; written 2026-10-28, not yet run — `10` picks the files up if present), `juris_counts`,
  `l_hedonic_sales.sql` + `l_price_indices.R` + `l_price_indices.sbatch` (2026-10-30: all residential arm's-length sales
  2012–2024 with parcel attributes, then three price indices per tract / county / school district × year: median log price,
  repeat-sales index with parcel FE, hedonic index with state-specific attribute coefficients → `data/built/corelogic/price_indices/`).
- `som/n_levy_regime.sbatch` (`n_levy_regime_head.sql`, `n_levy_regime_year.sql`; 2026-11-04): residential levy and AV per tax
  area × year 2012–2024, for all parcels and for the same parcels in t-1 and t → `data/built/corelogic/levyreg/`.
- `bouchet/`: the same panel split in two windows for Bouchet (`k_panel_2012_2018.sql`, `k_panel_2019_2024.sql`),
  repeat sales (`r_repeat_sales.sql`), and the 1% / 10% building-sample parcel panels (`p_*.sql`) that the archived
  household study built and that `designs/exposure_did/11` and `build/geo/01, 03` still read.
- `10_county_tract_year_panels.R`: `kpanel/` → `county_year.rds`, `tract_year.rds` (sales medians, bills, assessment ratios, new-construction
  sale counts by year; year-built counts if `kpanel/roll_built_*.parquet` exist).
- `12_nominal_rates_by_cell.R`: median-regression nominal rate per tax-area cell from the SOM fit sample (`full/`, not on the laptop).
- `13_levy_regime.R`: budget-first vs rate-first tax areas: slope of same-parcel levy growth on AV growth per area and pooled by
  state → `data/built/corelogic/levy_regime_area.parquet`, `output/build/corelogic/levy_regime_*` (notebook 2026-11-04).
