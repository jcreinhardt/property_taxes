# SALT cap and local jurisdictions: overnight report (2026-10-20/21)

Questions: (a) did the 2018 SALT cap change local property-tax revenue; (b) did it change dispersion of tax levels across
jurisdictions; (c) did it change property values, assessment ratios, assessment regressivity and nominal rates. Priority
(per your note): the border-distance instrument. Everything below is exploratory; nothing is pre-registered. Detailed notes
and every number: `text/data_exploration/dataaxle.md` §4.25–4.31. Code: `code/analysis/salt_local/` (new) and
`code/analysis/local_juris/R/24–27`. Outputs and figures: `output/analysis/salt_local/`.

## 1. Bottom line

__SUMMARY__

## 2. Data assembled tonight

| Source | What | Where |
|---|---|---|
| CoreLogic tract medians, sales 2017–19 + CSDS drive-time matrix | outside-option gaps by tract (15/30/60 min) | `data/local_juris/tract/`, `data/external/csds_ttm/` |
| SEDA 4.1 (Stanford) | district achievement and SES, pooled | `data/external/seda41/` |
| ACS 2013–17 (tidycensus) | tract income, home value, education, race, tenure, age of stock | `data/external/acs17_tract.csv` |
| TIGER cb 2019 school districts | tract → district; district adjacency | tigris cache |
| IRS SOI county 2016 | SALT exposure: SALT deduction above $10k per return (stub approximation) | `data/local_juris/salt_exposure_county2016.csv` |
| Census of Governments (county aggregates, FY2012–22) | county / school / county-area property tax | `data/census_finances_test.db` (existing) |
| Census F-33 district files FY2012–23 | property tax, enrollment, spending by district (NCES ID) | `data/external/elsec/` |
| FHFA annual tract HPI | house prices 2012–2025 | `data/external/fhfa/` |
| CoreLogic tract/area/county-year panel 2012–24 (SOM) | roll levy, AV, nominal rates, sales prices, assessment ratios | `data/local_juris/kpanel/` |

## 3. The border instrument

__IV__

## 4. (a) Revenue

__A__

## 5. (b) Dispersion of tax levels

__B__

## 6. (c) Values, assessment, regressivity, nominal rates

__C__

## 7. Map: outside options across school-district lines

__MAP__

## 8. Caveats and what I would do next

__NEXT__
