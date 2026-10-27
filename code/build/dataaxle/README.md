# build/dataaxle: Data Axle conversions and migration extracts (Bouchet)

Everything here runs on Bouchet against `corelogic.db` (read-only) or the raw quarterly files; nothing is written under
`/nfs/roberts/pi/`. See `.claude/skills/dataaxle` for what a family and a move are.

- Quarterly files → typed parquet on scratch: `make_quarter_sql.py YEAR Month > sql/convert_YYYY_MM.sql` (generated SQL is
  ignored by git; the two 2019 June files are hand-edited examples), run with `run_quarter_array.sbatch` over
  `quarters.txt` (`_2024` variants for the 2024 layout). `annual_2022_from_quarters.sql` replaces the truncated 2022 annual file.
- Move-outs and family moves for the border designs: `g_moveout_bkey_1pct.sql` (address-level, 1% building sample),
  `m_moveout_tract_1pct.sql` / `m_moveout_tract_full.sql` (tract × year), `f_family_moves_1pct.sql` /
  `f_family_moves_full.sql` (origin–destination of cross-border moves). Output goes to `data/built/dataaxle/border_moveout/`.
