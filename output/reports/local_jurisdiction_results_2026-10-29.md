# Local-jurisdiction results: inventory, reliability, and the border sign (2026-10-29)

What the repository has on the SALT cap's effects on local jurisdictions, which of it holds up, and what the 2026-10-28
border check changed. Numbers come from the saved outputs under `output/designs/` and the notebook entries linked below;
the only new estimation is `designs/border_rd/08_pair_exposure_split.R`.

## 1. Designs

Two families and a small third one. All event studies are centred on 2017 and return-weighted unless noted.

- **Exposure × year** (`designs/exposure_did`): units with higher 2016 SALT exposure vs lower, unit + state × year FE.
  Levels: county (main), ZIP, state, school district, parcel (1% panel). Exposure measures: SALT above $10k per return
  (SD units); lost federal subsidy per owner household split into property-tax, income/sales-tax and mortgage-interest
  parts; the simulated ACS/TAXSIM instrument Z1 (own location) and Z2 (national sample at the county rate); SOI
  instrumented by Z1. Cross-state variants: no-income-tax states only; six Diop & Green states vs the rest.
- **State borders**, one tract-to-pair-and-side assignment (`build/geo/06`), three estimators:
  `border_rd` (RD on signed distance, yearly RDs, difference in discontinuities; varies the rate itself),
  `border_iv` (near × high-side instrument for a cheaper tract within 30 min; varies the choice set),
  `border_twfe` (pair × year gap panels from per-side parcel samples).
- **Adjacent school districts within states** (`district_border`): DDD, randomisation inference, county-line placebo.

## 2. Results by outcome and design

| Outcome | Exposure × year | Border RD / diff-in-disc | Border IV | Border pair panel |
|---|---|---|---|---|
| Prices | county −0.3% (2019) to −0.65% per $100 lost subsidy; −0.4% to −1% on Z1; state −1.5% on the property part; ZIP has price-tier pre-trends | high-rate side +7 log points by 2024, n.s. | no reduced-form effect | bill vs price decomposition |
| Revenue | exposed counties grow 0.3–0.7%/yr slower per SD, explained by income for school districts; county governments −0.76%/yr borderline; Z2 −1.5 to −2.6% from 2020, unresolved | district tax per pupil on the high-rate side −11% by 2023, with a pre-trend | district DDD −3% where a cheaper district is within reach | revenue by side |
| Rates, dispersion, assessment | nominal rates flat; no CBSA compression (within-CBSA dispersion +2% per SD from 2020); assessment ratios +3% per SD | rate gap +0.75 pp at the line, narrowed 0.33 pp by 2023–24 via prices and assessment | first stage +0.33 | effective gaps 2013–24, −0.10 to −0.12 pp from 2021 |
| Migration | nothing pre-COVID in SOI rates, counts, net flows, mover AGI; state-level drift from high-SALT states is a pre-trend plus a COVID step | enrolment: nothing | Data Axle move-outs, direction: nothing | turnover, move-outs, vacancy: flat |

Sources: `notebook/2026-10-27-salt-cap-synthesis`, `2026-10-21-capitalisation-by-component`, `2026-10-21-migration-nulls`,
`2026-10-21-border-rate-gaps-over-time`, `2026-10-27-simulated-instrument-outcomes`.

## 3. Which results have flat pre-trends and a post-2017 effect

Every saved event-study series (374 series in 40 tables under `output/designs/`) was scored with one rule: weighted
pre-2017 slope |t| < 2 and no pre coefficient with |t| > 2.5; at least two post-2019 coefficients significant with the
same sign (`notebook/2026-10-28-design-reliability-scan`). 27 series pass mechanically. Reading the paths:

| Result | Pre 2013–16 | Post | Status |
|---|---|---|---|
| Border diff-in-disc, effective rate (high minus low side) | within ±0.1 pp | −0.09 pp (2019) → −0.33 pp (2023–24) | passes; same in the per-side parcel panel (0 through 2019, −0.10 to −0.12 pp 2021–24) |
| County exposure, effective rate bill/price (CoreLogic) | within ±0.006 | +0.005 (2018) → +0.033 (2024) per SD | passes; the mirror of the price decline |
| School districts near a cheaper cross-state district, high side (border IV DDD) | within ±1.5% n.s. | −3.1 to −3.4% from 2020 | passes at 10 and 20 mi, RI p = 0.04; county-line placebo −3.1% (SE 3.6); exposure interaction wrong sign |
| County prices per $100 cap burden (Z1, SOI, IV) | +0.4% in 2013 drifting to 0 | −0.4% (2019) → −1.0% (2021–24) | near pass: the drift is a quarter of the effect, opposite sign |
| Mortgage-interest component, county prices | flat | +0.7% → +3.1% from 2020 | passes mechanically, **wrong sign**: the post-2020 boom in mortgage-heavy suburbs |
| Z2 county-government revenue; Z2 migration | flat | −1.6 to −2.6% from 2020; migration 2020–21 only | unresolved vs mean reversion; COVID |

Everything on migration fails (pre-trends, 2020 steps, or null). School-district revenue on exposure is null with a
flat post-period. Enrolment, the parcel panel, ZIP repeat sales and the ZIP property-tax component have pre-trends.
Every surviving effect starts in 2019–20 and builds to 2022–24, and the mortgage-interest series shows that a flat
pre-period plus a 2020 effect is also what a COVID-era confound looks like.

## 4. The border sign, and the 2026-10-28 check

All border numbers are high-property-tax-rate side minus low-rate side. The effective-rate gap narrowed with bills and
nominal rates flat, so prices on the high-rate side rose relative to the low side (+7 log points, n.s.) and the
assessment ratio fell (−13). Capitalisation of the cap predicts the opposite: the side with larger bills loses value.
An earlier reading of "one fact seen three ways" was wrong on this sign: the county exposure design and the border
design point in opposite directions on prices.

One rescue was that the high-property-tax side of many pairs is a no-income-tax state, hence the low total-exposure
side. `designs/border_rd/08` ranks each pair's sides by lost subsidy per owner household as well: 30 of 59 pairs
"agree" (high-rate side is also high-exposure), 29 "disagree"; 13 high-rate sides are no-income-tax states, nearly all
in the disagree group. Rerunning the difference in discontinuities by group (`notebook/2026-10-28-border-sides-by-exposure`):

| 2023, high minus low side | agree (30 pairs) | agree excl. NJ-NY | disagree (29 pairs) |
|---|---|---|---|
| effective rate, pp | −0.49 | −0.58 | −0.13 (−0.19 in 2022) |
| log sale price | +0.11 | +0.09 | +0.02 |
| log assessment ratio | −0.19 | −0.23 | 0.00 |
| district tax per pupil | −0.12 | −0.12 | −0.11 |
| district spending per pupil | −0.02 | −0.03 | −0.09 |

The gap narrows in both groups and three to four times more where the high-rate side is the exposed side, with flat
pre-periods. So the rescue fails: the exposed side of a border (NJ vs NY and PA, CT vs MA, DE vs PA, IL vs IN) gained
value on its cross-state neighbour after 2019, while the county design shows the same exposed places losing value
relative to less-exposed counties in their own state. Different counterfactuals, both post-2019, both consistent with
the 2020–22 boom favouring suburbs of large exposed metros. Neither is clean evidence of capitalisation. The per-pupil
declines on the high-rate side are the same in both groups, so they track the property-tax rank, not exposure.

## 5. School districts

Independent districts' property tax grew from $212bn (FY2017) to $256bn (FY2022), about 4%/yr nominal. Relative to
less-exposed places, nothing moves after 2017: the county-aggregated return-weighted series is flat to −0.9% n.s.;
the pre-2017 catch-up trend (−1% in 2012 rising to 0) simply ends, and the trend-break slow-down of −0.30%/yr (SE
0.10) goes to +0.10 (SE 0.20) with a county income × trend control. District-level (11,713 balanced districts): 0 ± 0.6%
through 2023, per pupil flat, state aid and spending flat, within-county dispersion flat. The panel covers independent
districts only: dependent systems (1,542 districts, 17% of pupils; all of CT, MD, MA, VA, NC, DC, AK, HI, NYC; TN 98%,
VT 96%, RI 93%) report zero property tax, so much of the exposed universe is missing from the district design and
appears instead in county and municipal T01.

## 6. Data notes: Census of Governments (`notebook/2026-10-28-census-of-governments-benchmarks`)

- The individual-unit file is the universe only in 2012, 2017 and 2022 (about 89,000 units); survey years hold only the
  sampled units (about 24,000), so sums over them are not national totals. School districts are near-complete every
  year, county governments a large sample (2,000; 1,706 from 2019), municipalities, townships and special districts thin.
- 2017 local property tax: our sum $508.2bn; Census Table 2 $508.1bn, matched to $0.1bn in every type-of-government
  cell. BEA local property tax receipts $522.2bn (calendar year); Census QTAX state + local $567.3bn.
- FY2017 shares of local property tax: school districts 42%, municipalities 24%, counties 24%, townships 7%, special
  districts 4%. Property tax as a share of general revenue: counties 28%, municipalities 24%, townships 62%, special
  districts 11%, school districts 37%; of own taxes 71 / 50 / 92 / 62 / 96%.
- The database's `other_own_source` holds only U-coded miscellaneous revenue; A-coded current charges (hospitals,
  sewerage, airports) were treated as enterprise revenue, so any general-revenue share computed from it is overstated
  (36% vs 28% for counties). No design uses general revenue as an outcome.
- Every local government is coded to exactly one county area (no null county codes in 2017), so multi-county districts
  are attributed wholly to one county; county areas sum to the national total but describe the geography approximately.

## 7. Open

- Equal pair weights and a continuous exposure-difference interaction in the border split; the same split for the
  per-side parcel panel and the choice-set instrument.
- Fill the dependent-system states into the school-district revenue design from parent-government T01 or F-33 T02.
- Fix the general-revenue columns in `build/census_finances/02` (A-coded current charges).
- Z2 county-revenue decline against the nominal-rate panel; ZIP design on repeat sales with trend-break bounds.
