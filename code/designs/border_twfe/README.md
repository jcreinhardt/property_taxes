# designs/border_twfe: cross-border gaps over time

Pair × year panels of the gap between the two sides of each state border (effective rate, bills, prices, revenue,
turnover, vacancy, move-outs) with pair and year FE, year effects centred on 2017. Earlier than the RD
(`designs/border_rd`), parcel-based (1% building sample within 20 mi; per-side samples from `build/corelogic/som/f_border_sample.sql`).

| Script | What |
|---|---|
| `01_rate_gaps.R`, `02_effective_gaps.R` | CoreLogic-reported and effective (bill / price) rate gaps 2013–2021 |
| `03_gap_twfe.R`, `04_sample_twfe.R`, `05_gap_weighted.R` | pair × year TWFE; per-side sample version (to 2024 with `BORDER_IN=bsample1024`); population-weighted means |
| `06_bill_price_twfe.R` | bill vs price decomposition of the gap change |
| `07_revenue_low_high.R`, `08_revenue_twfe.R` | revenue by side (writes `data/built/corelogic/border_revenue_side_year.csv`) |
| `09_sales_turnover.R`, `10_moveout_by_side.R`, `11_vacancy_by_side.R` | turnover, Data Axle move-outs, USPS vacancy by side |

Outputs in `output/designs/border_twfe/` (names as before: `border_gap_twfe.csv`, ...).
