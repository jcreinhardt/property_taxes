# build/geo: state borders, border assignment of parcels and tracts, drive-time neighbours

Run in order (all read the 1% parcel panel `data/built/corelogic/parcel_1pct/` and TIGER files cached by `tigris`):

| Script | Writes |
|---|---|
| `01_border_distance.R` | distance of each 1% parcel to the nearest shared state border |
| `02_border_pair_buffers.R` | `data/built/geo/border20_sides.geojson`: 20-mile buffers per (state, neighbour) side |
| `03_border_parcels_1pct.R` | `border_parcels_1pct.parquet`, `sample_clips_1pct.parquet` |
| `04_tract_drive_neighbours.R` | tract medians vs tracts reachable by car (`data/external/csds_ttm/`) → `output/build/geo/tract_drive_differences_*.csv` and maps |
| `05_iv_tract_data.R` | `iv_tract.parquet`: tract medians, SEDA district scores, ACS 2017 covariates |
| `06_tract_border_side.R` | `iv_tract_side.rds`: nearest ranked border pair, high/low side (median of tract medians within 20 mi), near (< 10 mi). Reads `iv_tract_nb.rds`, which `designs/border_iv/01` writes |
| `07_tract_side_ownstate.R` | `iv_tract_side_ownstate.rds`: the own-state nearest-line assignment (60,982 tracts, 90 pairs), plus school-district coverage checks |

`iv_tract_side.rds` is the input of every border design (`designs/border_rd`, `border_iv`, `district_border`).
