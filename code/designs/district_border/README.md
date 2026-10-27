# designs/district_border: adjacent school districts within states

Gaps in property tax per pupil between adjacent school districts (TIGER unified + elementary district shapes, F-33
finances) and their change after 2017; the state-border versions of the district DDD live in `designs/border_iv/03`.

| Script | What |
|---|---|
| `01_adjacent_gaps_ddd.R` | adjacent-district pairs, high vs low side, DDD |
| `02_randomisation_inference.R` | permute which side of each pair is "high" |
| `03_county_line_placebo.R` | the same construction at county lines within states |
