---
date: 2026-10-27
title: What we know about the SALT cap's local effects, and how much to trust each design
question: Summarise the results so far, how much to trust the designs, and whether the border design varies the choice set or the tax rate.
status: current
code: property_taxes ae98cf7 (local main, not pushed)
tags: synthesis, SALT cap, design assessment
---

## Answer
- **Firm:** no large behavioural response in the first two years:
  - no migration ([migration nulls](../2026-10-21-migration-nulls/README.md));
  - no change in nominal rates;
  - no compression of rate dispersion across jurisdictions;
  - no clearly SALT-specific local revenue response. Exposed counties' revenue grew 0.3–0.7%/yr more slowly per SD, but for school districts that is explained by income; county governments −0.76%/yr is borderline.
- **Less firm:** prices fell somewhat in exposed places. Only the property-tax part behaves like capitalisation
  ([by component](../2026-10-21-capitalisation-by-component/README.md)). Within states the decline loads on the income-tax part and tracks price level.
- **Assessment:** ratios rose ≈ 3% per SD; regressivity flat.
- **At borders:** the rate gap narrowed by about a third, through prices and assessment ([border](../2026-10-21-border-rate-gaps-over-time/README.md)).

| Design | Main threat | Trust |
|---|---|---|
| County exposure × year | exposure ≈ income and price level; COVID-era repricing | moderate that prices fell; low on size |
| ZIP exposure × year | price-tier pre-trends | low as is |
| Cross-state / no-income-tax states | few clusters, noisy pre-trends | suggestive |
| Border RD / difference in discontinuities | low-side school-district dip near the line | good, descriptive |
| Border instrument | near-border selection assumed equal on both sides; weak SALT treatment | strong first stage, no reduced-form effect |
| Migration nulls | power, Data Axle measurement | high for "no large effect" |

**Common weakness: timing.** Effects build from 2020. The clean 2018–19 window shows small effects.

**Border design:** the RD varies the rate itself (≈ 0.8 pp jump); the instrument varies the choice set (a cheaper option
within 30 min), and that option is worth little after the cap. The households hit hardest escape by long-distance moves to
no-income-tax states, not across the nearest line.

## How
This is a synthesis of the entries linked above plus earlier results in `text/data_exploration/dataaxle.md` §4.20–4.39.
The longer write-up is `output/reports/synthesis_log_2026-10-27.md`. No new runs.

## Open
- ZIP design on repeat sales, with trend-break / Rambachan–Roth bounds.
- Price-independent exposure variation.
- Expected cap duration in the capitalisation factor.
- Rerun the instrument with the own-state border assignment.
