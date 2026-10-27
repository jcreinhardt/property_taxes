# Project: SALT cap (2017) and migration / property taxes

## Bouchet cluster (mandatory rules — see `.claude/skills/bouchet/SKILL.md`)
- Access Bouchet via `ssh bouchet1` (laptop alias: login1 via `ProxyJump bouchet`, reuses the Duo-authenticated `bouchet` master; start that with `ssh -fNM bouchet` in the user's terminal) and work inside the tmux session `ptax` on **login1**; never in a bare SSH shell. (`bouchet` round-robins login1/login2, so tmux there may be on the wrong node.)
- Compute runs in an interactive allocation: `salloc --partition=devel --time=06:00:00 --cpus-per-task=4 --mem=60G` (6h, 60G, 4 cores). No heavy work on login nodes.
- `/nfs/roberts/pi/pi_csl53/corelogic/database/corelogic.db` (DuckDB, group-writable!) is **read-only, always**: open with `read_only = TRUE` / `-readonly` / `mode=ro`. Never write to, modify, copy, move, or delete it.
- **Never write anything under `/nfs/roberts/pi/pi_csl53/`** (no outputs, temp/spill files, logs, `slurm-*.out`, installs). Our files go in `~`, our project, or scratch. Use `~/bin/duckdb` (current version) for queries.
- Bouchet login node = 1 core / 1 GB per user: nothing beyond ls/git/sbatch there. `devel` QOS: 6h max, 4 CPUs + 60G per user total, 2 jobs max — so the one 4-core job is the whole budget; run everything inside it (OK to interrupt the `ptax` pane).
- **Exploration queries must take < 20 s**: start with `USING SAMPLE 0.01% (system)`, time it, then scale to 1% only if fast. No bernoulli sampling.
- **Batch job size:** background `day` jobs use the largest allocation that starts without waiting, capped at 16 CPUs / 320G (measured: jobs used ≤ 300G and ≤ 48% CPU; more cores barely help, I/O-bound); else 4c/64G queued. Raise the cap (`MAXC=40 MAXG=640 ~/ptax_explore/jobsize.sh ...`) only for queries that need it, e.g. all-years panel joins. Use `sbatch $(~/ptax_explore/jobsize.sh HH:MM:SS) ... run_sql.sbatch file.sql`.
- **Subsample → full-data rule:** any test/exploration run on a subsample also gets a background subagent that runs the same test on all years of the full data as a `day` batch job and reports back.
- Record findings as we go in the data exploration doc `text/data_exploration/dataaxle.md`.
- Data and SQL know-how: skills `duckdb` (CLI, preamble, traps, 2.0 agent mode), `dataaxle` (families, file timing, how to code a move), `corelogic` (tax/deeds keys, owner-occupancy accuracy, roll gaps) in `.claude/skills/`.

## SOM HPC cluster (second copy of the CoreLogic DB; follow the yale-som-hpc plugin skills)
- Access via `ssh som` (hpc.som.yale.edu, key auth, ControlMaster) and work inside tmux session `ptax` on the login node (currently `hpc-ln01`).
- DB: `/gpfs/project/corelogic_database/corelogic_public.db` (DuckDB, 1.6 TB, 17 tables: tax, mortgage, ownertransfer, llma_*, liens_*, permits*, raw_mls — **no Data Axle**). **Read-only, always** (ATTACH ... (READ_ONLY)); never write anything under `/gpfs/project/corelogic_database/` (team deletes foreign files; its `temp/` and `.db.tmp` dirs are not ours). Set `temp_directory` to `~/ptax_explore/tmp`.
- Use `~/.local/bin/duckdb` (v1.4.2). Nothing heavy on the login node: quick checks via `srun -p default_queue` (4h max); data pulls as batch jobs (team README: ≤32 CPUs, ≤150 GB; prefer `cpunormal`/`gpunormal` without GPUs for long jobs). The same 20 s exploration rule and subsample → full-data rule apply.

## Code layout (restructured 2026-10-27; map in `code/README.md`, old → new in `code/RESTRUCTURE.md`)
- `code/build/` (data import and derived inputs → `data/built/`, producers in `data/built/MANIFEST.md`), `code/designs/`
  (one folder per identification strategy: `exposure_did`, `border_rd`, `border_iv`, `border_twfe`, `district_border`),
  `code/validation/`, `code/paper/`, `code/archive/` (retired code by date; the 2025 designs and the household SALT-cap study).
- Run scripts from the repository root; every script starts with `source("code/R/paths.R")` (ROOT, DATA, OUTPUT,
  `out_dir()`, `db_open()` read-only). No `setwd()`, no `../../` paths, no hard-coded laptop paths.
- `output/` mirrors the code tree; track figures and small tables only. Each folder has a README with run order and inputs;
  update it when adding a script. Results worth keeping get a `notebook/` entry (skill `notebook`).
- Exposure × year regressions are weighted by 2016 returns; year effects centred on 2017.
