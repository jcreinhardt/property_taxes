# Proposal: restructuring `code/` around data building and research designs

Date: 2026-10-27. Status: phase 1 (cleanup, archive) committed as 196099c; phase 2 (code/R helpers, paths) committed; phases 3-4 pending. Revised the same day: no estimation wrapper in `code/R/`;
migration and repeat sales are outcomes of existing designs, not designs; the household SALT-cap study is archived.
This file becomes `code/README.md` once the moves are done. (`text/` is for paper drafts, proposals and slides only.)

## 1. What is there now

83 commits; 22 in July 2025, a handful through May 2026, 44 since 2026-10-16. The code has three generations that
were never reconciled, so `code/analysis/` has eight sibling folders that are really sessions, not topics.

| Folder | Files | Last commit | What it is | Verdict |
|---|---|---|---|---|
| `data_prep/build_database/` | 14 | 2026-05 | SOI (state/county/ZIP × AGI, migration), HUD ZIP–county, ACS microdata + SALT/itemisation imputation, Census of Governments → `data/database.db` | **keep**, still read by 20+ scripts |
| `data_prep/data_cleaning/` | 5 | 2026-05 | Columbia-era Slurm job (`--account=lapoint`, 1 TB) that winsorises a local `data/corelogic.db`; uplot sketches | **drop** (verify first that nothing reads the winsorised flag) |
| `data_prep/dataaxle_quarterly/` | 60 (untracked) | – | Bouchet conversion of quarterly Data Axle CSVs to parquet; 51 generated SQL files | **keep**, commit generator + sbatch, drop generated SQL |
| `analysis/filings/`, `migration/`, `transactions/`, `local_govt/` | 21 | 2026-05 | Generation 1 (2025): SOI state-to-state migration, income-tax savings, county revenue, parcel border RD on transactions, paper figures | **archive**; only what `text/` still needs moves to `paper/` |
| `archive_legacy/2026-05-26/` | 55 | 2026-05 | A copy of generation 1 plus the 2025 `archive/`; already deleted in the working tree, deletion not committed | **commit the deletion** |
| `analysis/salt_cap/` | 27 | 2026-10-17 | Generation 2: pre-registered 10-strategy household study, self-contained (`00_setup.R`, part files, decision log) | **keep as one design**, rename only |
| `analysis/local_juris/` | 58 | 2026-10-20 | Generation 3a: jurisdiction rates (R/01–06), border geometry (07–09), border TWFE (10–17), border outcomes (18–23), IV (24–27); cluster SQL for Bouchet and SOM | **split**: half is data building |
| `analysis/salt_local/` | 55 | 2026-10-21 | Generation 3b: the a/b/c/d series. Letters are overnight batches (a = revenue/dispersion, b = panels/prices, c = follow-ups, d = moves), not designs | **split by design** |
| `analysis/dataaxle/` | 14 (untracked) | – | Data Axle validation against ACS and CoreLogic (tenure, households, address universes) | **keep** under validation |
| `paper/` | 3 (untracked) | – | fig1 (SALT refund change), fig2_acs (mover rates); `fig2.R` is a wrapper | **keep**, drop wrapper |

What makes the current tree hard to use:

- **Data building is mixed into analysis.** `b1_build_panels.R`, `a3_district_panel.R`, `c17/c18` (ACS, SEDA),
  `c24/c26` (exposure), `local_juris/R/24, 26` (`iv_tract.parquet`, `iv_tract_side.rds`) and all the cluster SQL
  produce the inputs everything else reads. `data/local_juris/` is a dumping ground for 30+ built files with no
  record of which script made which (`iv_tract_side.rds` is read by 34 scripts).
- **Cross-folder dependencies by number.** `salt_local/c*` reads `local_juris/R/26` output; the notebook recipes say
  "local_juris R/26" because there is no other name for that step.
- **Three path conventions:** `"../../../data/..."` (gen 1, run from the script folder), `"data/..."` (gen 3, run from
  the repo root), `SALT_ROOT` env + `00_setup.R` (gen 2).
- **Superseded scripts sit next to current ones** with no marker (`c25` → `c26/c27/c29`; `10a` vs `10`; `a6`;
  `b5`; `k_panel_2012 2018.sql` with a space in the name next to `k_panel_2012.sql`).
- **`output/` is tracked wholesale:** 1,044 files, 701 of them `salt_cap` part files and logs.
- Root clutter: `Rplots.pdf`, `texput.log`, `list1_Sep_2018.xls`, `nj_reval_2017_2022.csv`, `model/`, `temp/`,
  `.DS_Store` in several folders.

## 2. Proposed layout

```
code/
├── R/                        shared helpers, sourced by every script
│   ├── paths.R               ROOT (here::here()), DATA_RAW, DATA_BUILT, OUT; one place for SALT_ROOT / cluster overrides
│   ├── db.R                  read-only DuckDB connections (laptop database.db; cl on Bouchet/SOM), dq()/pq() helpers
│   └── theme.R               ggplot theme, figure sizes
│
├── build/                    (a) data import, cleaning and derived inputs. One folder per source; writes data/built/
│   ├── soi/                  returns × AGI stub (state/county/ZIP), migration flows, HUD crosswalk      [build_database/SOI]
│   ├── census_finances/      Census of Governments, F-33 district panel, fill-rate checks        [census_gov_finances, clean_census_finances, a3, c11]
│   ├── acs/                  ACS 5-yr tract 2013–23 + 2020→2010 crosswalk; ACS microdata imputations  [c17, c18, build_database/ACS_microdata]
│   ├── seda/                 SEDA 4.1 district panel                                             [c18]
│   ├── corelogic/            cluster SQL (bouchet/, som/) and the local panel builders: county/tract/parcel panels, repeat-sales pairs  [local_juris/cluster, local_juris/bouchet, salt_local/bouchet, b1, c2, R/06]
│   ├── dataaxle/             quarterly conversion; move-out and family-move extracts (the migration outcomes)  [dataaxle_quarterly, g_/m_/f_ SQL]
│   ├── enrolment/            F-33 pupils per district panel                                     [c19]
│   ├── geo/                  state-border lines and buffers, tract/parcel → border pair and side, drive-time neighbours, school-district shapes  [R/07–09, 22, 24, 26, c9]
│   └── exposure/             every SALT exposure measure: stub-average, Δ deduction value, components, per owner, at county/ZIP/state  [a1, a8, a6, c24, c26]
│
├── designs/                  (b) one folder per identification strategy; scripts numbered within the folder.
│   │                         Migration, repeat-sales prices, enrolment, revenue etc. are outcomes inside a design, not designs.
│   ├── exposure_did/         exposure × year event studies at county/ZIP/state: revenue, prices (medians and repeat sales), assessment, dispersion, SOI migration and mover AGI  [a1, a2, a4, a10, b2, b5, b8, c22, c23, c25, c27, c28, c29, local_govt]
│   ├── border_rd/            state-border RD, yearly RDs, difference in discontinuities, balance, profiles; enrolment as an outcome  [c7, c13–c16, c20]
│   ├── border_iv/            the choice-set instrument: first stage, balance, reduced forms (HPI, repeat sales, Data Axle move-outs, move direction), predicted-endogenous tests  [R/25–27, b3, b4, b6, b7, c1, c4–c6, c8, c12, d1–d3]
│   ├── border_twfe/          pair × year gap TWFE; revenue, turnover, vacancy and Data Axle move-outs by side  [R/10–19, 21]
│   └── district_border/      adjacent school-district DDD, county-line placebo, randomisation inference  [a5, a7, a9]
│
├── validation/               data checks that belong to no design
│   ├── dataaxle/             vs ACS and CoreLogic  [analysis/dataaxle]
│   ├── corelogic/            coordinates, nominal-rate recovery, variance of rates across layers  [R/01–05], ZIP nesting [c21]
│   └── coverage/             district data coverage, F-33 vs CoreLogic property tax  [c10, c11]
│
├── paper/                    figures and tables referenced from text/  [paper/, filings/fig1, whatever text/ still needs from gen 1]
├── archive/                  dated folders of retired code: generation 1, the household SALT-cap study, superseded scripts
└── README.md                 the map: run order build → designs, which design answers which question, link to notebook/INDEX.md
```

Every `build/*` and `designs/*` folder gets a short `README.md`: question, scripts in run order, inputs
(`data/built/...` with the producing script), outputs, and a pointer to the notebook entry that reports the result.

### Conventions that go with it

- **Run from the repo root, always.** `source("code/R/paths.R")` at the top of each script; no `../../../`.
  Estimation stays in the scripts (fixest / rdrobust calls written out); `code/R/` holds paths, connections and the
  plot theme only.
- **`data/raw/<source>/` vs `data/built/<area>/`.** Raw downloads and cluster exports are never modified in place;
  built files carry the producing script in `data/built/MANIFEST.md` (file, script, date, upstream cluster job).
  `data/local_juris/` is dissolved into `data/built/{geo,corelogic,exposure,acs,...}`. `data/salt_cap/panel/`
  (the 1% parcel panel, still read by `c2`) moves to `data/built/corelogic/parcel_1pct/`; the rest of
  `data/salt_cap/` stays on disk, untracked, with the archived study.
- **Numbered within a folder, named by what they do**, e.g. `designs/border_rd/03_diff_in_disc_by_year.R`. The
  a/b/c/d letters and the cross-folder "R/26" references disappear.
- **Superseded scripts move to `code/archive/<date>/` with one line in the folder README saying what replaced them.**
  Git keeps the history; nothing is deleted outright except generated files.
- **`output/`:** mirror the code tree (`output/designs/<design>/`, `output/build/`, `output/validation/`). Track
  figures and small CSV/markdown tables; add `output/**/parts/`, `*.log`, `Rplots.pdf` and sample sub-folders
  (`bld10pct/`) to `.gitignore`. `output/analysis/salt_cap/` (701 tracked files) goes to `output/archive/` with
  the study; keep `report.md`, `decision_log.md` and `estimates.csv`, untrack the parts and logs.

## 3. Mapping (old → new)

Build:

| Old | New |
|---|---|
| `data_prep/build_database/SOI/01–08` | `build/soi/01–08` (same names) |
| `data_prep/build_database/census_gov_finances/*`, `data_cleaning/clean_census_finances.R`, `salt_local/a3_district_panel.R` | `build/census_finances/01_download.sh`, `02_import_cog.R`, `03_clean_cog.R`, `04_f33_district_panel.R` |
| `salt_local/c11_f33_fill_and_ptax_compare.R` | `validation/coverage/02_f33_fill_vs_corelogic.R` |
| `data_prep/build_database/ACS_microdata/01–04`, `salt_local/c17`, `c18` | `build/acs/01_microdata_import.R` … `04_tcja_burden.R`, `05_acs5_tract_download.R`, `06_tract_year_panel.R`; `build/seda/01_district_year.R` |
| `local_juris/cluster/*`, `local_juris/bouchet/*`, `salt_local/bouchet/r_repeat_sales.sql`, `salt_cap/sql/00_extract_10pct.sql`, `01_build_panel*.sql` (the 1% parcel panel `c2` reads) | `build/corelogic/som/`, `build/corelogic/bouchet/` (keep file names, drop `k_panel_2012 2018.sql` duplicates) |
| `salt_local/b1_build_panels.R`, `c2_parcel_panel.R` (build part), `local_juris/R/06_fit_cells_full.R` | `build/corelogic/10_county_tract_year_panels.R`, `11_parcel_panel_1pct.R`, `12_nominal_rates_by_cell.R` |
| `data_prep/dataaxle_quarterly/{make_quarter_sql.py, run_quarter*.sbatch, quarters*.txt}`, `local_juris/bouchet/g_moveout_bkey_1pct.sql`, `salt_local/bouchet/{m_,f_}*.sql` | `build/dataaxle/` |
| `salt_local/c19_enrolment.R` (panel part) | `build/enrolment/01_f33_pupils_district_year.R` |
| `local_juris/R/07, 08, 09, 22, 24, 26`, `salt_local/c9` | `build/geo/01_border_distance.R`, `02_border_pair_buffers.R`, `03_border_parcels_1pct.R`, `04_tract_drive_neighbours.R`, `05_iv_tract_data.R`, `06_tract_border_side.R`, `07_tract_side_ownstate.R` |
| `salt_local/a1` (exposure part), `a8`, `a6`, `c24`, `c26` | `build/exposure/01_county_stub_2016.R`, `02_zip_stub_2016.R`, `03_county_proptax_share.R`, `04_delta_deduction_value.R`, `05_components_county_state_zip.R` |

Designs:

| Old | New |
|---|---|
| `salt_local/a1, a2, a10` (estimation parts), `a4`, `b2`, `b8`, `c22`, `c23`, `c28`, `c29`, `local_govt/*` | `designs/exposure_did/01_revenue.R`, `02_revenue_income_control.R`, `03_cbsa_dispersion.R`, `04_county_outcomes.R`, `05_prices_by_component.R` (c29, current), `06_zip_pretrend_diagnostics.R`, `07_repeat_sales_zip_exposure.R`, `08_soi_migration_flows.R`, `09_soi_mover_agi.R`; `c25`, `c27`, `b5`, `local_govt` → `archive/` |
| `salt_local/c7, c13, c14, c15, c16, c20` | `designs/border_rd/01_side_selection_levels.R`, `02_profiles_by_distance.R`, `03_covariate_rd.R`, `04_rd_by_year_and_diff_in_disc.R`, `05_enrolment_rd.R`, `06_gap_range_signed_distance.R` |
| `local_juris/R/25, 27`, `salt_local/b3, b4, b6, b7, c1, c4, c5, c6, c8, c12, d1, d2, d3` | `designs/border_iv/01_first_stage_balance.R`, `02_school_district_map.R`, `03_tract_ddd.R`, `04_hpi_ddd.R`, `05_repeat_sales_event_study.R`, `06_repeat_sales_pooled.R`, `07_moveout_1pct_ddd.R`, `08_moveout_full.R`, `09_family_move_direction.R`, `10_figure.R`, `11_signed_distance_validity.R`, `12_predicted_balance.R` (+ `_np`), `13_pair_gap_vs_school_finance.R`, `14_covariate_coverage.R` |
| `local_juris/R/10–19, 21` | `designs/border_twfe/01_rate_gaps.R` … `09_moveout_by_side.R`, `10_vacancy_by_side.R`; `10a` → `archive/` |
| `salt_local/a5, a7, a9` | `designs/district_border/01_adjacent_gaps_ddd.R`, `02_randomisation_inference.R`, `03_county_line_placebo.R` |
| `salt_cap/R/*`, `salt_cap/sql/00_mortgage_bld1pct.sql`, `00_tax_bill_bld1pct_all.sql`, `run_r_10pct.sbatch` | `archive/2026-10-household-salt-cap/` (whole study, with its report and decision log in `output/archive/`); the `salt-cap-pipeline` memory note is retired |
| `salt_local/c3_key_figure.R` | `paper/key_figure_overnight.R` or `archive/` |

Validation and the rest:

| Old | New |
|---|---|
| `analysis/dataaxle/*` | `validation/dataaxle/*` |
| `local_juris/R/01–05`, `salt_local/c21`, `c10` | `validation/corelogic/01_coordinates.R`, `02_nominal_rate_test.R`, `03_rate_fit_all_states.R`, `04_rate_fit_split.R`, `05_rate_variance_decomp.R`, `06_zip_nesting.R`; `validation/coverage/01_district_data_coverage.R` |
| `paper/fig1.R`, `fig2_acs.R`; `filings/fig1.R` | `paper/fig1_salt_refund_change.R`, `fig2_mover_rates_acs.R`; `paper/fig2.R` wrapper dropped |
| `filings/`, `migration/`, `transactions/`, `data_cleaning/` | `archive/2026-10-gen1/` (or delete, see decisions) |
| `salt_cap/` | `archive/2026-10-household-salt-cap/` |
| `archive_legacy/2026-05-26/` | deletion committed |
| root `Rplots.pdf`, `texput.log`, `.DS_Store`, `model/`, `temp/`, `list1_Sep_2018.xls`, `nj_reval_2017_2022.csv` | delete / `data/raw/misc/`; add patterns to `.gitignore` |

## 4. How to do it without breaking anything

Four commits, each leaving the repo runnable:

1. **Cleanup commit.** Commit the `archive_legacy` deletion; remove `Rplots.pdf`, `texput.log`, `.DS_Store`, the
   wrapper `fig2.R`, the `k_panel_* *.sql` duplicates; extend `.gitignore` (`*.DS_Store`, `Rplots.pdf`, `*.log`,
   `output/**/parts/`, `output/**/bld10pct/`); commit the untracked `dataaxle/`, `dataaxle_quarterly/` (generator
   only), `paper/`, `migration_predictors.R` where they are so the moves have history; move `salt_cap/` and
   generation 1 to `code/archive/` and their outputs to `output/archive/`.
2. **Shared helpers + path normalisation.** Add `code/R/paths.R`, `db.R`, `theme.R`; replace the three path
   conventions in place (sed, one pattern per convention); run the four notebook recipes and check their numbers
   (0.747 pp, −0.331 pp, −0.080 dyad coefficient, the c29 county coefficients). No file moves yet.
3. **`git mv` into the new tree**, build and validation first, then designs; `data/local_juris` → `data/built/*`
   with a manifest; `output/` mirrored. Update every literal path (the sed list comes from the mapping table).
   Rerun the same four recipes.
4. **Docs.** Folder READMEs, `code/README.md`, fix script paths in the four notebook entries (links only, answers
   untouched), update `CLAUDE.md` and the `bouchet`/`corelogic`/`dataaxle` skills, add a `salt-cap-pipeline`
   memory note with the new paths.

Cluster copies (the SOM `k_panel` jobs, Bouchet `run_sql.sbatch` inputs) read their own SQL paths; the sbatch files
move but their contents do not change, so only the `--chdir`/`SQL=` lines need checking. The Bouchet copy of the
household study (`~/ptax_explore/salt/`) is left alone.

## 5. Decisions

Agreed 2026-10-27 (recommended answers taken):

1. Generation 1 (`filings`, `migration`, `transactions`, `local_govt`, `data_cleaning`) goes to `code/archive/`
   rather than being deleted outright; only `fig1`/`fig2_acs` stay in `paper/`. Before the move, check which of the
   30 figures in `text/figures/` are still cited by `main.tex`/`presentation.tex` and which script makes each.
2. The a/b/c/d scripts are renamed per the mapping; the old name goes in the first comment line of each moved script
   so `dataaxle.md` §4.9–4.39 and the notebook recipes can still be followed.
3. `output/` tracks figures and small tables only.
4. The household SALT-cap study (`salt_cap/`) is archived whole, not kept as a design.
5. Superseded exposure scripts (`c25`, `c27`, `b5`, `a6`) are archived.
6. Migration, repeat sales and enrolment are outcomes inside `exposure_did`, `border_iv`, `border_rd` and
   `border_twfe`, not designs of their own.
