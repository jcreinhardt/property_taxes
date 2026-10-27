# SALT cap and local property taxes: synthesis log (2026-10-27)

Status of the evidence after the overnight run (2026-10-20/21) and the follow-ups through c29. Numbers and scripts are in
`text/data_exploration/dataaxle.md` §4.20–4.39; scripts are in `code/analysis/salt_local/` (a*, b*, c*).

## Bottom line

- **Firm:** no large behavioural response in the first two years after the cap:
  - no migration response;
  - no change in nominal tax rates;
  - no compression of rate dispersion;
  - no clearly SALT-specific local revenue response.
- **Less firm:** house prices fell somewhat in high-exposure places. Only the property-tax part behaves like capitalisation, and its size and timing are uncertain. Exposed places were on different price trajectories for other reasons, and most estimated effects build from 2020 (COVID, remote work, price-tier reversal).

## What we know, by outcome

### 1. Capitalisation into prices: probably some, size uncertain

**Exposure measures** (SOI county/ZIP × AGI stub):
- The original measure: SALT above $10k per return, TY2016.
- The preferred measure (c24/c26): the observed loss in the federal value of deductions TY2016 → TY2018, per owner household.
  - It uses assumed marginal rates by stub and an AMT adjustment.
  - It is split into a real-estate-tax part (pro-rata allocation of the capped amount), an income/sales-tax part, and lost mortgage interest.

**Main results** (return-weighted throughout; % price change per $100/yr of lost subsidy per owner):
- **County** (county + state × year FE): pre-period flat; 2019 −0.32, 2021 −0.64, 2024 −0.65. Capitalisation factor 0.45 (2019) → 0.9, at 5% and assuming a permanent loss.
- **Within states,** the decline loads on the **income/sales-tax part** (2019 −0.40 → 2024 −1.45). That part is unavoidable by moving within the state, so it should not capitalise across locations. More likely a demand effect among high-income buyers, or confounding.
- **Exposure tracks price level** (correlation ≈ 0.6 with the 2012 price level at both ZIP and county level). Controlling for the 2012 price × year removes the effect, but that control absorbs much of the treatment (bad control). So the result is unresolved, not refuted.
- **ZIP level:** a pre-trend. High-SALT ZIPs were already appreciating more slowly within counties in 2013–17 (post-crash catch-up of cheaper areas). It is not sample selection: the ZIP sample with county exposure has a flat pre-period. Not credible in its current form.
- **Most theory-consistent signals (property-tax part):**
  - Across states: 2018 −0.71, 2019 −1.50 (significant; 48 states).
  - No-income-tax states (FL, NV, SD, TX, WA, WY, NH, TN): about −1% from 2019, a factor of ≈ 1. Noisy, with a pre-trend.
- **Mortgage-interest loss:** prices *rose* where it was larger. That is a confound marker (mortgage-heavy suburbs boomed after 2020), not capitalisation.
- **Duration:** a factor near 1 assumes a permanent loss. The cap was legislated to expire after 2025, so the expected loss was shorter than that and a factor near 1 is if anything too large.

### 2. Migration: no response

- **Data Axle:** move-outs flat; cross-border family moves don't change direction.
- **IRS county-to-county flows** (finest public SOI flow geography):
  - High→low cross-border county flows were flat in 2018–19 and fell in 2020–22, the opposite of escape.
  - Out-migration rates were trending up before the cap, with no break.
  - Leavers are richer than stayers only from 2020; the 2018–19 rise in leavers' AGI is general income growth.
- **Pupils per district:** no change at borders (district RD, difference in discontinuities).
- **Sales turnover:** no change.
- **The dip in ZIP return counts** (−1% per SD in 2020) is mostly filing composition (stimulus filers), not migration.

### 3. Local revenue: at most a modest slowdown

- Property-tax revenue in exposed counties grew 0.3–0.7%/yr more slowly per SD of exposure.
- School districts: explained by high-income counties generally.
- County governments: −0.76%/yr, exposure-specific but borderline.
- Border district revenue decline: driven by low-SALT pairs and predates 2017. Not SALT.

### 4. Rates, assessment, dispersion

- Nominal rates: flat.
- Dispersion between jurisdictions: no compression (percentage-point gaps fall with rate levels; the coefficient of variation is flat).
- Assessment ratios: +3% per SD, not explained by measured prices. Assessment regressivity: flat.
- At state borders, the effective-rate gap narrowed by about one third (0.79 pp in 2017 → about 0.45 pp by 2023–24), through prices and assessment ratios, not bills or nominal rates.

## How much we trust each design

| Design | Identifying variation | Main threat | Trust |
|---|---|---|---|
| County exposure × year | Rich vs less-rich counties within a state | Exposure ≈ income and price level; COVID-era repricing; income-tax part shouldn't capitalise within a state | Moderate that prices fell; low on magnitude |
| ZIP exposure × year | Neighbourhoods within a state or county | Pre-existing price-tier trends; only bad controls fix it | Low as is |
| Cross-state / no-income-tax states | Property-tax burden across states | Few clusters, noisy pre-trends | Suggestive |
| Border RD / difference in discontinuities | Rate jump at state lines, over time | Low-side school-district dip near the line; 5-year ACS smoothing | Good, descriptive |
| Border instrument (near × high side) | Access to a cheaper cross-state option | Near-border selection on both sides, assumed equal | Strong first stage, balance mostly OK, no reduced-form effect |
| Migration nulls | Many independent sources | Power, Data Axle measurement | High that there's no large effect |

The common weakness is timing. The cap starts in 2018, but most effects build from 2020. The clean window is 2018–19, and the effects there are small.

## What variation the border design produces

**Rate level (border RD / difference in discontinuities):** the effective rate jumps 0.75–0.9 pp (≈ 1 SD) at state lines while tract demographics are balanced. This is variation in the tax rate itself, driven by state regimes (school finance, levy limits, assessment). The SALT question there is whether that gap changed after 2018. It narrowed, through prices and assessment, not rates.

**Choice set (border instrument):** pair × side fixed effects remove side-level rate differences. Near × high side shifts access to a cheaper cross-state tract within 30 minutes (first stage +0.33). Combined with the cap, the test is whether high-side tracts with a nearby cheap option respond more.

**Limits of the choice-set version:**
- After the cap, crossing is worth only a few hundred dollars a year to an itemizer, and most pairs have few households over the cap. So it carries a weak treatment.
- Near-border tracts are negatively selected on both sides. Identification needs that selection to be equal, and the low-side school-district dip near the line suggests it isn't quite.

**Takeaway:** the border is a good laboratory for showing that rate gaps are large, stable and not competed away. It is a weak one for detecting SALT effects: the most-hit households (high earners in high-income-tax states) escape by moving long distances, not across the nearest line.

## Open items

- **ZIP design:** use repeat sales instead of medians (b8 had a flat pre-period); add a trend-break test and Rambachan–Roth bounds instead of price-level controls.
- **Exposure that varies independently of price:** tax-rate differences across municipal or school-district lines within a county.
- **Run the same checks on the cross-state and no-income-tax-state property-tax results.**
- **Duration:** model the expected cap duration (expiry after 2025; later legislation) for the capitalisation factor.
- **Border instrument:** adopt the own-state nearest-border assignment (c9: 60,982 tracts vs 57,350) and rerun.
