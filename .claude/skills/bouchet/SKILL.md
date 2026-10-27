---
name: bouchet
description: Rules for working on the Yale YCRC Bouchet cluster for this project — SSH + tmux session management, 6h interactive jobs (60G, 4 cores), and strictly read-only access to corelogic.db (CoreLogic data in Cameron LaPoint's csl53 folders). Use whenever touching Bouchet, CoreLogic data, or corelogic.db.
---

# Working on Bouchet (YCRC)

These rules are mandatory for this project.

## 1. Connection: SSH + tmux, always

- SSH alias: `bouchet` (in `~/.ssh/config`, host `bouchet.ycrc.yale.edu`, user `jr2728`).
  Requires Yale VPN or campus network, plus Duo MFA on login.
- `~/.ssh/config` uses `ControlMaster auto` / `ControlPersist 12h`, so the user authenticates (Duo) **once**
  in their own terminal (`ssh bouchet`). Every later `ssh bouchet '<cmd>'` from Claude reuses that socket
  without prompting. If a command hangs/fails on auth, ask the user to re-run `ssh bouchet` and approve Duo.
- **All interactive work happens inside a tmux session named `ptax` on the login node.**
  Never run work in a bare SSH shell that dies when the laptop sleeps.
- Login nodes are round-robin (`bouchet` lands on login1 or login2). The `ptax` session lives on **login1**:
  use the laptop alias `bouchet1` (`HostName login1.bouchet.ycrc.yale.edu`, `ProxyJump bouchet`, own
  ControlMaster). It rides on the `bouchet` master, so no extra Duo; login1's host key was added to
  `~/.ssh/known_hosts` from the cluster's `/etc/ssh/ssh_known_hosts`. Start the master first
  (`ssh -fNM bouchet`, user approves Duo), then use `ssh bouchet1 '<cmd>'` for everything below.

Create / reattach (user, interactive):
```bash
ssh -t bouchet1 'tmux attach -t ptax || tmux new -s ptax'
# detach: Ctrl-b then d      list: tmux ls      kill when done: tmux kill-session -t ptax
```

Claude drives the session from the laptop with send-keys / capture-pane:
```bash
ssh bouchet1 "tmux send-keys -t ptax 'some command' Enter"
ssh bouchet1 "tmux capture-pane -p -t ptax -S -100"
```

## 2. Compute: 6h interactive job, 60G RAM, 4 cores

The login node is a shell, not a compute host. Anything heavier than `ls`/`git`/editing
(queries on corelogic.db, R/Python analysis, big `find`s) runs inside the allocation.

Inside tmux pane `ptax`:
```bash
salloc --partition=devel --time=06:00:00 --cpus-per-task=4 --mem=60G
```

**Bouchet limits (verified 2026-10-08):**
- **Login node: 1 core and 1 GB RAM per user** (hard cgroup limit). Only `ls`, `git`, editing, `squeue`/`sbatch`.
  Even opening corelogic.db in DuckDB can be OOM-killed there. Everything else needs an allocation.
- **`devel` partition (QOS `part_devel`):** max wall **6h** (default 1h if `--time` omitted); per user
  **at most 4 CPUs and 60G memory summed over all your devel jobs**; max 2 submitted jobs; default 5G/CPU.
  Nodes: a1114u33n01, a1130u05n[01-04].
- The 4-core interactive job therefore uses the whole per-user devel CPU budget — a second devel job,
  or an `srun --overlap` step needing memory, will hang pending. Run everything inside the one allocation
  (the `ptax` pane); it is fine to interrupt/overwrite whatever is running in that pane.
- Need >6h or >4 cores: `sbatch` on `day` (24h max) or another partition.
- **Batch queries alongside the interactive job:** devel is full, so batch goes to `day`
  (QOS `part_day`: 24h max, generous per-user limits). Template: `~/ptax_explore/run_sql.sbatch`
  — `cd ~/ptax_explore && sbatch $(~/ptax_explore/jobsize.sh HH:MM:SS) --job-name=NAME --time=HH:MM:SS run_sql.sbatch file.sql`.
  It ATTACHes corelogic.db READ_ONLY, sets DuckDB threads = allocated CPUs and memory_limit = 80% of allocated memory,
  logs to `~/ptax_explore/logs/NAME_JOBID.out`
  (`--chdir`/`--output` keep slurm files out of the pi folder). Write outputs as `*.tmp`, rename when the job succeeded.
- **Background/batch job sizing rule (user instruction, 2026-10-14; cap lowered same day):** launch every background batch job at the
  **largest size that is allocatable right now without waiting**, capped at **16 CPUs, 320G**. Measured on the 2019 jobs at up to
  40c/640G: peak memory 60–300G, CPU efficiency 2–48% (I/O-bound reads of the DB; extra cores barely shorten jobs). Raise the cap
  with `MAXC=40 MAXG=640 ~/ptax_explore/jobsize.sh ...` only for queries that need it (e.g. all-years panel joins).
  If nothing larger starts without waiting, use **4 cores / 64G** and let it queue. `~/ptax_explore/jobsize.sh HH:MM:SS`
  implements this: it picks the `day` node with the most free CPUs and memory (idle/mixed, including `mixed-`), caps at 16/320G,
  and accepts the size if `sbatch --test-only` estimates a start no later than the 4c/64G baseline (+2 min); otherwise
  it halves the request, falling back to `--cpus-per-task=4 --mem=64G`. (`--test-only` adds a constant ~40 min to every
  estimate, so only the comparison with the baseline is meaningful.) QOS `part_day` per-user caps: 1500 CPUs, 20000G, 24h.
  Free memory, not CPUs, is usually the binding constraint on `day` nodes.
- **Storage (verified 2026-10-13):** home `/home/jr2728` 125 GiB personal, backed up → default for our outputs
  (`~/ptax_explore/samples/`). `/nfs/roberts/project/pi_csl53/jr2728` exists but group project quota is at 92% — avoid.
  `/nfs/roberts/scratch/pi_csl53/jr2728` is purged after 30 days.
- DuckDB 1.5 deprecates `c -> expr` lambdas; write `lambda c: expr` (e.g. `COLUMNS(lambda c: NOT regexp_matches(c, '...'))`).
- **Saved samples** (`~/ptax_explore/samples/`, names dropped): `dataaxle_annual_system1pct.parquet`
  (1% block sample, cross-section only; 26.3m rows, 88 cols, built in 26 min on 2026-10-13) and `dataaxle_annual_hh1pct.parquet` (all years for families with
  `md5_number(family_id::VARCHAR) % 100 = 0`, sorted by family_id, record_year — use this for panel/migration work).
 After the allocation starts,
the shell in `ptax` is on the compute node — confirm with `hostname`. When the 6h expire,
request a new allocation the same way; do not request longer interactive jobs.
Longer/batch work goes through `sbatch`. Set thread counts to the allocation
(`SLURM_CPUS_PER_TASK`, e.g. DuckDB `SET threads=4`, `OMP_NUM_THREADS=4`).
`exit` the allocation when finished so cores aren't held idle.

## 3. Never write anything in the pi folders

Nothing — no files, scripts, outputs, temp/spill files, logs, Slurm `.out` files, installs, symlinks —
is ever written anywhere under `/nfs/roberts/pi/pi_csl53/` (or other `/nfs/roberts/pi/*` folders).
Treat the whole tree as read-only, even though group permissions would allow writes. Check `cwd`
before running anything that writes (Slurm writes `slurm-*.out` to the submit directory!).
Our own files go in `~` (`/home/jr2728`), our project dir, or our scratch dir.

Our own tools: the DuckDB CLI lives in `~/bin` (home folder, NOT the pi folder): `~/bin/duckdb` → `duckdb2`
(**2.0.0-alpha preview**, installed 2026-10-16, has agent mode; see the `duckdb` skill) with `~/bin/duckdb-1.5.6` as the
stable fallback. Prefer them over the old bundled `/nfs/roberts/pi/pi_csl53/corelogic/database/duckdb` (v0.10.4).

## 4. corelogic.db: READ-ONLY, always

Located under Cameron LaPoint's (netid `csl53`) group storage.
It is shared data owned by someone else. **Never write, modify, vacuum, create tables/views in, or
attach it in read-write mode.** Never `cp`/`mv`/`rm` it, never `chmod` it. Write derived data to our own
project/scratch directories instead.

Open it read-only every time:
- DuckDB CLI: `duckdb -readonly /path/corelogic.db`
- DuckDB R: `dbConnect(duckdb::duckdb(), dbdir = path, read_only = TRUE)`
- DuckDB Python: `duckdb.connect(path, read_only=True)`
- DuckDB attach: `ATTACH '/path/corelogic.db' AS cl (READ_ONLY);`
- If it is SQLite: `sqlite3 -readonly path`, R `dbConnect(RSQLite::SQLite(), path, flags = RSQLite::SQLITE_RO)`,
  Python `sqlite3.connect(f"file:{path}?mode=ro", uri=True)`

Path: `/nfs/roberts/pi/pi_csl53/corelogic/database/corelogic.db`

- **DuckDB** file (~6.5 TB), owned by `alr228`, permissions `rwxrwx---` group `pi_csl53` — i.e. it is
  *group-writable*, so the filesystem will NOT stop an accidental write. Read-only is enforced only by us.
- Other people's files sit in the same directory (`database.db`, `corelogic.db.tmp`, scripts, parquet
  exports). Do not touch or write anything in `/nfs/roberts/pi/pi_csl53/corelogic/database/`.
- Bundled CLI: `/nfs/roberts/pi/pi_csl53/corelogic/database/duckdb` (v0.10.4-dev). Verified 2026-10-08:
  `$D/duckdb -readonly $D/corelogic.db -c "SHOW TABLES;"` works (42 tables, incl. `tax`, `ownertransfer`,
  `mortgage`, `raw_mls`, `dataaxle_annual`, `raw_L2`, `permits`; `*_old` tables are older vintages).
- **DuckDB's default spill dir for a file `X.db` is `X.db.tmp` next to the db — i.e. inside the pi folder.**
  Always `SET temp_directory` to our own dir before any query. The wrapper `~/ptax_explore/q.sh file.sql`
  does this (plus threads=4, memory_limit=48GB) and writes `file.out` next to the sql in `~/ptax_explore`.
  It starts DuckDB as an in-memory session and runs `ATTACH '<db>' AS cl (READ_ONLY); USE cl;`, so scratch
  tables can be created in RAM (`CREATE TABLE memory.main.x AS ...`) while corelogic.db stays read-only
  (a `-readonly` session cannot create even in-memory tables).
  Pipe SQL in from the laptop with `ssh bouchet 'cat > ~/ptax_explore/x.sql' < x.sql` (avoids quote mangling),
  then run it in tmux: `ssh bouchet1 'tmux send-keys -t ptax "~/ptax_explore/q.sh ~/ptax_explore/x.sql" Enter'`.
- **Exploration queries should take < 20 seconds.** Start every new query on a sample and scale up only
  if it is fast: first `USING SAMPLE 0.01% (system)`, check the timing (`.timer on`), then 1% if it makes
  sense; full-table scans only when needed and when the columns are already cached. Use `system` (block)
  sampling, not `bernoulli` — this is quick exploration, true randomness is not needed (bernoulli still reads every row).
  Speed is dominated by first-touch I/O from GPFS/NFS: measured 2026-10-08 on `dataaxle_annual` (2.66bn rows),
  one column cold 1% system sample = 100s, the same column fully scanned once cached = 2s, bernoulli 1% = 275s.
  Every new column touched pays the cold-read cost once per job — touch few columns at a time.
- **Subsample → full-data rule:** whenever a test or exploration is run on a subsample (one state, one year,
  the 1% samples, ...), also launch a **background subagent** that runs the same test on the full data for
  all years as a batch job (`run_sql.sbatch` on `day`) and reports the results back to the main agent.
  The main agent keeps working interactively on the subsample meanwhile.
- Findings go into the running data exploration doc `text/data_exploration/dataaxle.md` as we go.
- Run queries only from the compute-node allocation, with `SET threads=4;` and a modest
  `SET memory_limit='48GB';`. Use `SET temp_directory` pointing at our own scratch dir, never at the db folder.
