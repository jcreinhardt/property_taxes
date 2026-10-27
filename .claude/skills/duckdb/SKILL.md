---
name: duckdb
description: How we run DuckDB in this project — session preamble, reserved-word and parser traps we have hit, panel SQL idioms, cheap introspection, sizing, and DuckDB 2.0 agent mode (installed on Bouchet 2026-10-16). Use whenever writing or debugging DuckDB SQL for CoreLogic / Data Axle work, on Bouchet, SOM HPC or the laptop.
---

# DuckDB in this project

Cluster rules (read-only DB, no writes under the pi folder, job sizing) live in the `bouchet` skill; data-specific
notes in the `corelogic` and `dataaxle` skills. This skill is about the SQL and the CLI.

## 1. Binaries

| Where | Binary | Version |
|---|---|---|
| Bouchet | `~/bin/duckdb` → symlink to `~/bin/duckdb2` | **2.0.0-alpha (Cyanoptera) preview**, installed 2026-10-16 from `artifacts.duckdb.org/staged/v2.0-cyanoptera/duckdb-cli-linux-amd64.tar.gz` |
| Bouchet fallback | `~/bin/duckdb-1.5.6` | 1.5.6 stable; use it if the preview misbehaves on a big job (`sed -i` the sbatch template or call it explicitly) |
| SOM HPC | `~/.local/bin/duckdb` | 1.4.2 |
| Laptop | `/opt/homebrew/bin/duckdb`, R `duckdb` package | 1.5.5 |

The preview reads the 1.5-era `corelogic.db` and our parquet samples (tested read-only, 2026-10-16). Never let a newer
binary open the shared DB read-write: a newer version could rewrite the file format. `(READ_ONLY)` / `-readonly` always.

## 2. Preamble for every SQL file

```sql
SET preserve_insertion_order=false;          -- lets COPY/aggregations stream
USE memory;                                  -- scratch tables live in RAM, never in the attached DB
SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';   -- default spill dir is next to the DB (pi folder) or in $HOME (125 GiB quota)
-- optional for exploration: SET max_execution_time = 120000;  (ms)
```
The sbatch template `~/ptax_explore/run_sql.sbatch` already runs `ATTACH ... AS cl (READ_ONLY)` and sets threads /
memory_limit; a second `ATTACH` in the file fails with "database with name cl already exists" and marks the job FAILED
even though later statements ran. For `srun`/sample runs on parquet, no ATTACH is needed.

Write results with `COPY (...) TO '/home/jr2728/ptax_explore/hh_strat/out/NAME.csv' (HEADER);` and `scp` them to the
project's `output/` folder. Don't print large results to stdout.

## 3. Traps we have hit (each cost a resubmitted job)

- **Reserved words as aliases** fail at parse time: `rows, matched, key, mode, last, sample, type, ref, strict, scope,
  both, limit, offset, order, filter, window, from, to, user, current`. Use `nrow, mtch, bkey, mde, lst, smpl, ptype,
  rage, strct, scp, o_and_mail, ...`.
- **Ambiguous columns in joins**: qualify `clip` (`t.clip`), `st`, and anything selected from a self-join (`a.st`).
- **Case-insensitive column clash**: `RELATED` and `related` in one SELECT are the same column.
- **Struct expansion** `x.a.*` is not supported inside expressions; list the fields.
- **Correlated LEFT JOIN after a comma join** ("cannot reference outer query") → build a `grid` table first
  (`CROSS JOIN (SELECT unnest(range(2012, 2022)) y)`) and join to it.
- **Type mismatches**: `coalesce(bool_col, '')` fails (boolean vs string); `homestead_exempt_indicator` is BOOLEAN,
  compare with `IS TRUE`, not `'Y'`. Unsigned subtraction overflows (`UINT16 - UINT16`): cast to INT first.
- **Casts in WHERE defeat predicate pushdown**: `fips_code::INT = 25017` scanned the whole table (26 min) where
  `fips_code = '25017'` took 2 min. Compare in the stored type.
- **Lambdas**: 1.5+ wants `lambda c: expr`, not `c -> expr`.
- **`USING SAMPLE n% (system)`** samples blocks: fast, fine for exploration, not for anything needing i.i.d. rows.
  `bernoulli` reads every row. For persistent samples hash a key: `hash(family_id) % 100 = 0`.
- Column order in `read_csv`-built tables is arbitrary; never rely on `SELECT *` positions.

## 4. Idioms that work

```sql
-- latest attributes per key
arg_max(struct_pack(pic := property_indicator_code, occ := owner_occupancy_code), tax_year) a   -- then a.pic, a.occ
-- backfill a yearly panel (carry last observed value forward)
last_value(x IGNORE NULLS) OVER (PARTITION BY bkey ORDER BY y ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
-- modal value within a group
row_number() OVER (PARTITION BY bkey, y ORDER BY n DESC, ln) = 1
-- within-cell benchmark (e.g. full tax rate = 90th percentile in county x tax area x year)
quantile_cont(r, 0.9) ... GROUP BY fips_code, tac, y HAVING count(*) >= 20
-- names: never exported in clear text
hash(upper(trim(last_name_1)))  -- or md5(...)
-- building key shared by Data Axle, tax and deeds
hash(lpad(zip::VARCHAR,5,'0'), upper(trim(street_name)), upper(trim(house_num)))
-- reusable predicate
CREATE OR REPLACE MACRO hn(x) AS CASE WHEN coalesce(trim(x), '') <> '' THEN hash(upper(trim(x))) END;
```

## 5. Introspect before you scan

`DESCRIBE SELECT * FROM read_parquet('…')` (columns and types, no scan); `SUMMARIZE (SELECT … USING SAMPLE 0.01% (system))`;
`EXPLAIN ANALYZE` when something is slow; `~/ptax_explore/cols.sh TABLE` lists the DB's columns;
`duckdb_functions()` for function docs. Touch few columns: first-touch I/O from NFS dominates (one cold column of
`dataaxle_annual` ≈ 100 s even for a 1% sample; a 237-column pull took > 1 h where 50 columns took 30 min).

## 6. Sizing (measured)

- Queries on the 1% parquet samples: `srun -p day -c 4 --mem=32G -t 00:15:00 ~/bin/duckdb < file.sql` — seconds to minutes.
  Not `devel` (its 4-CPU/user cap makes a second job queue).
- Full-table pulls: `sbatch -p day -c 8 --mem=96G --time=01:30:00 run_sql.sbatch file.sql` with a narrow column list and
  a `WHERE` on the hashed key; all-years panel joins: `MAXC=40 MAXG=1000 jobsize.sh`, `SET memory_limit='300GB'`.
- Spill: a 300G job writes hundreds of GB to `temp_directory`; home filled once. Always point it at scratch.

## 7. DuckDB 2.0 agent mode (preview on Bouchet)

Agent mode (`.help agent`) switches on automatically when an agent env var such as `CLAUDECODE` is set *and* stdout is
not a terminal *and* no `-csv/-json/-markdown` flag is given. Over `ssh`/`srun` the env var is not forwarded, so on the
cluster it stays **off unless you pass `-agent`** (or `DUCKDB_AGENT_MODE=1`). When on:
- Output is a compact markdown table with types in the header; results above 1,000 rows / 10,000 bytes show the first
  and last rows plus a footer with the true count — **read the footer before trusting a printed result**; `.maxrows -1`
  / `.maxbytes 0` only when the full print is wanted.
- Errors are JSON on stderr; an `estimate:` line precedes queries reading ≥ 1m rows; `progress:` lines every 5 s.
- Once the row cap is hit the query is stopped after 100k more rows and the count is a lower bound — use `COPY` for
  anything you need completely.
- The footer hash is order-independent: compare two runs by hash instead of printing both.
- Use `-agent` for interactive checks we read ourselves; keep `COPY … TO csv` for results that feed R.
