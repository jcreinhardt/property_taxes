# build/altos: Altos Research rental listings -> rent panels

Altos Research "Rental Intel" national weekly listing files (`us_national_rentals_classic_YYYY-MM-DD.csv.gz`, 782 files,
2011-01-07 to 2025-12-26, ~0.9–1.4m unit listings per week with asking rent, type, beds, baths, sqft, ZIP and coordinates)
sit on **Bouchet** in `/nfs/roberts/pi/pi_csl53/corelogic/raw_data/altos/` (read only; codebook in
`.../docs/codebooks/altos/`). Not on the SOM cluster.

| Script | Where | What |
|---|---|---|
| `01_altos_unit_month.sql` | Bouchet, `day` batch job (`~/ptax_explore/altos_01_unit_month.sql`, see header) | unit × month table; tract of every listing coordinate (DuckDB spatial, 2019 cartographic tracts); ZIP × month, tract × quarter, tract × year cells by type × beds; repeat-listing rent changes. Outputs in `~/ptax_explore/altos/`, copied to `data/altos/` |
| `02_rent_panels.R` | laptop | `data/built/altos/{zip,county,tract}_year.rds`: `lr_adj` (log rent at fixed type × bedroom mix), `lr_raw`, `p50_apt2`, `n`; `output/build/altos/02_rent_panels.txt` coverage by year |
| `03_repeat_index.R` | laptop | `data/built/altos/{zip,tract}_year_repeat.rds`: repeat-listing (same unit across years) log rent index, Bailey-Muth-Nourse per ZIP / tract from `altos_repeat_unit.parquet`, 2017 = 0, NA where a year is not chained to 2017: `lr_rep` (exact-zero changes dropped: listings straddling a year end), `lr_rep0` (kept), `n_rep` pairs involving the year, `n_end` pairs ending in it; `output/build/altos/03_repeat_index.txt` and `figures/03_repeat_index.png` national chained index vs the fixed-mix index |

Used by `designs/exposure_did/18_rents_altos.R`, `designs/border_rd/09_rent_diff_in_disc.R`, `designs/border_iv/17_rent_exposure_near_far.R`
(fixed-mix index by default; `ALTOS_INDEX=repeat Rscript ...` runs each on the repeat index with `_repeat` outputs).

Caveats of the repeat index: the unit ids change in 2018 (no pair links ≤ 2017 to ≥ 2019 directly; chains cross the cap through 2018 only);
35–83% of consecutive-year pairs are exact zeros (the same listing spell priced in both years), so `lr_rep` drops them; the 2019–22
feed is thin (median 2–3 linked pairs per tract-year in 2019, 2020, 2022).
