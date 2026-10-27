# validation/dataaxle: Data Axle against ACS and CoreLogic

Household counts, tenure, generations and surnames vs the ACS; street-address universes vs the CoreLogic tax roll
(2019 and pooled 2012–2021), unit numbers, NYC near-miss shares. Inputs are cluster aggregates in
`output/validation/dataaxle/<topic>/` and `output/dataaxle_exploration/full*/` (Bouchet batch jobs; the SQL sits next to
the CSVs). `pums_mv.sh` builds the ACS PUMS tenure counts. Findings: `text/data_exploration/dataaxle.md` §4.1–4.8 and the
`notebook/2026-10-16-dataaxle-*` entries.
