# build/altos: Altos Research rental listings -> rent panels

Altos Research "Rental Intel" national weekly listing files (`us_national_rentals_classic_YYYY-MM-DD.csv.gz`, 782 files,
2011-01-07 to 2025-12-26, ~0.9–1.4m unit listings per week with asking rent, type, beds, baths, sqft, ZIP and coordinates)
sit on **Bouchet** in `/nfs/roberts/pi/pi_csl53/corelogic/raw_data/altos/` (read only; codebook in
`.../docs/codebooks/altos/`). Not on the SOM cluster.

| Script | Where | What |
|---|---|---|
| `01_altos_unit_month.sql` | Bouchet, `day` batch job (`~/ptax_explore/altos_01_unit_month.sql`, see header) | unit × month table; tract of every listing coordinate (DuckDB spatial, 2019 cartographic tracts); ZIP × month, tract × quarter, tract × year cells by type × beds; repeat-listing rent changes. Outputs in `~/ptax_explore/altos/`, copied to `data/altos/` |
| `02_rent_panels.R` | laptop | `data/built/altos/{zip,county,tract}_year.rds`: `lr_adj` (log rent at fixed type × bedroom mix), `lr_raw`, `p50_apt2`, `n`; `output/build/altos/02_rent_panels.txt` coverage by year |

Used by `designs/exposure_did/17_rents_altos.R`, `designs/border_rd/08_rent_diff_in_disc.R`, `designs/border_iv/17_rent_exposure_near_far.R`.
