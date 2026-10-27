# code/: how the SALT-cap study is organised

The question: what did the 2017 SALT cap do to local jurisdictions — revenue, the dispersion of tax levels, property
values and assessment, migration. Results are summarised in `notebook/INDEX.md` (read it first); the step-by-step log is
`text/data_exploration/dataaxle.md`. This folder was restructured on 2026-10-27 (`RESTRUCTURE.md` has the old → new map).

```
code/
├── R/            paths.R (ROOT, DATA, OUTPUT, out_dir; loads db.R and theme.R), db.R (read-only DuckDB), theme.R
├── build/        (a) data import, cleaning and derived inputs -> data/built/   [README in each folder]
│   ├── soi/              IRS SOI tables -> data/database.db
│   ├── census_finances/  Census of Governments; F-33 district panel
│   ├── acs/              ACS microdata imputations; ACS 5-year tract panel 2013-23; SEDA
│   ├── corelogic/        cluster SQL (som/, bouchet/) and the county/tract/parcel panels
│   ├── dataaxle/         quarterly-file conversion; move-out and family-move extracts (Bouchet)
│   ├── geo/              state borders, tract/parcel -> border pair and side, drive-time neighbours
│   └── exposure/         SALT exposure measures (stub, delta, components, per owner)
├── designs/      (b) one folder per identification strategy; outcomes (prices, revenue, migration, enrolment) live inside
│   ├── exposure_did/     exposure x year event studies at county / ZIP / state / district / parcel
│   ├── border_rd/        RD at state borders, yearly RDs, difference in discontinuities (varies the rate)
│   ├── border_iv/        near x high-side instrument for a cheaper cross-border option (varies the choice set)
│   ├── border_twfe/      pair x year gap panels
│   └── district_border/  adjacent school districts within states, placebo, randomisation inference
├── validation/   data checks that belong to no design: dataaxle/, corelogic/, coverage/
├── paper/        figures for text/
└── archive/      retired code by date: 2026-10-gen1 (2025 designs), 2026-10-household-salt-cap, 2026-10-superseded
```

## Conventions

- **Run from the repository root:** `Rscript code/designs/border_rd/03_covariate_rd.R`. Every script starts with
  `source("code/R/paths.R")`; `ROOT` is the git root (or `PTAX_ROOT` on a cluster copy), `DATA = data/`, `OUTPUT = output/`,
  `OUT <- out_dir("designs/border_rd")` creates and returns the script's output folder. DuckDB files are opened read-only
  with `db_open("database.db")`; parquet queries use `db_mem()` / `dq()`.
- **Data:** `data/` is git-ignored. Raw downloads and cluster exports stay in `data/<source>/`; everything derived is in
  `data/built/<area>/`, and `data/built/MANIFEST.md` names the script or cluster job that makes each file.
- **Outputs:** `output/` mirrors this tree (`output/designs/border_rd/...`); figures and small tables are tracked, parts,
  logs, parquet and large CSVs are ignored. File names were kept when scripts were renamed (`c15_border_rd.csv` is written by
  `designs/border_rd/03_covariate_rd.R`); each renamed script says `# Formerly <old path>` on its first line.
- **Estimation:** written out in the scripts (fixest, rdrobust); no shared wrappers. Exposure × year regressions are
  weighted by 2016 returns; year effects are centred on 2017.
- **Clusters:** CoreLogic and Data Axle are read only on Bouchet / SOM (`CLAUDE.md`, `.claude/skills/`); the SQL in
  `build/corelogic/{som,bouchet}` and `build/dataaxle` runs there and its output is copied into `data/built/`.
- **Results worth keeping** get a notebook entry (`.claude/skills/notebook`); scripts and outputs alone are not the record.

## Run order

1. `build/soi` → `build/census_finances` → `build/acs` (laptop; DuckDB files and panels).
2. Cluster extracts in `build/corelogic/som`, `build/corelogic/bouchet`, `build/dataaxle`; copy to `data/built/`.
3. `build/corelogic/10`, `build/geo/01–07` (with `designs/border_iv/01` between `geo/05` and `geo/06`), `build/exposure/01–05`.
4. Any design folder, scripts in numeric order where one reads another's output (noted in the folder README).
