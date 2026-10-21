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

**Idea.** A tract near a state border can reach jurisdictions in another state, which often tax at a very different rate.
If proximity only changes the *tax* menu and not other amenities, it is an instrument for having a cheap outside option.

**Plain distance fails** (`iv_first_stage_balance.txt`, `local_juris/R/25`). Within 5 miles of a border, 38% of tracts have
a tract ≥ 0.5 pp cheaper in another state within 30 minutes' drive (0% beyond 50 miles). But border proximity *also*
gives access to better schools across the line (44% have a cross-state district ≥ 0.25 SD better in SEDA achievement).
Border tracts are also poorer, older, more rental and lower-achieving (0.2–0.3 SD with state fixed effects).

**Directional version works** (`local_juris/R/26`, figure `figures/c1_iv_first_stage_balance.png`). Each tract is
assigned to its nearest state-border pair. Only the higher-tax side gains a cheaper outside option, while both sides share
whatever makes border areas different. So the instrument is *near (< 10 mi) × high-tax side*, with pair × side and pair ×
distance-bin fixed effects (55.9k tracts, 70 pairs, SE by county):
- First stage: cheaper cross-border option +0.33 at 0–5 mi (SE 0.04), or +1.0 SD pooled < 10 mi; tax gap (own minus 10th
  percentile reachable) +0.35 pp (0.08). District level: +0.31 pp, F = 14.5.
- School access: *no* better schools in reach (school gap −0.05 SD, better cross-line district −0.09; both ≈ 0).
- Balance: own-tract achievement, SES, income, home value, education, tenure, age of stock, population all within
  ±0.17 SD and insignificant; share white −0.17 SD is borderline.
- Cheaper adjacent districts are not worse-school districts in general: across 9.8k districts, the cheapest adjacent
  district's achievement is +0.02 SD relative to own (+0.07 when it is ≥ 0.5 pp cheaper).

So the directional border instrument has a strong first stage, does not bring better schools, and is balanced on
observables. It is usable for (a)–(c).

## 4. (a) Revenue

**Exposure designs (IRS SALT exposure, 2016).**
- Census of Governments, school districts aggregated by county, weighted by returns: exposed counties' school levies had
  been growing 0.13–0.19%/yr faster per SD of exposure. After FY2019 that slope falls by **0.26–0.30 %/yr per SD
  (SE 0.10)**. County governments: −0.35 to −0.67 %/yr per SD (SE 0.18–0.27). Unweighted: no significant break.
  (`a1_revenue_event_study.txt`, figure `a1_revenue_event_study.png`)
- F-33 district panel (11.7k balanced districts, FY2012–23) with a property-tax-based exposure (county share of
  parcels with a 2016 bill > $5k, from CoreLogic), enrollment-weighted: pre-slope +0.56 %/yr per SD, **slope change
  −0.71 %/yr (0.15)**; with the > $10k share, −0.39 (0.14). (`a6_alt_exposure.txt`)
- CoreLogic 1% parcel panel (constant parcels, parcel FE): bills in exposed counties stop outgrowing the rest after 2017
  (pre ≈ +0.4 %/yr per SD, post ≈ 0). Nominal rates drift down 0.2–0.5% per SD in 2019–22. (`c2_parcel_panel.txt`)
- Reading: where the cap bit (large, high-income, high-tax jurisdictions), levy growth slowed relative to the rest of the
  state, by roughly 0.3–0.7% a year per SD of exposure. Caveat: the slowdown coincides with COVID-era aid and the
  2020–22 housing cycle, and the identifying variation is mostly a handful of large metro counties.

**Border design (directional instrument).**
- F-33 districts, triple difference (district + pair × side × year + pair × distance-bin × year FE): high-side
  border districts' property-tax revenue fell behind after 2017. Pre 2012–16: −0.5, −0.1, +0.2, −1.5, −0.6%. Post 2018–23:
  **−1.6, −1.7, −3.2, −3.1, −2.7, −3.4% (SE 1.1–1.4)**. Pooled −2.2% (1.0); leave-one-pair-out −1.7 to −2.6%; robust at
  < 20 mi. 2SLS: **−6.3% revenue per pp of outside-option gap (SE 3.8)**. (`a3_district_panel.txt`, figure
  `a3_district_border_ddd.png`)
- CoreLogic parcels in the same design: bills on high-side near-border parcels −2.8% (2018) to −4.4% (2025) relative,
  mostly through **nominal rates** (−1.9 to −3.5% in 2018–22), not assessed values.
- **But it is not SALT-specific in the cross-section.** The decline is not larger where county SALT exposure is higher
  (interaction +0.3 to +0.5% per SD), not larger where the high-side state is more exposed than its neighbour, and not
  larger in high-SES districts. County governments show a negative exposure × border-high interaction (2019 −1.8%,
  SE 0.6), but school districts do not.
- Reading: after 2017, jurisdictions with a cheaper option across a nearby state line cut rates and fell behind in
  levies, a timing consistent with the cap raising the after-tax price of high local taxes. But the lack of a dose-response in
  exposure means I can't attribute it to SALT rather than to a general post-2017 tightening of cross-border competition
  (remote work, migration from high-tax states).

## 5. (b) Dispersion of tax levels

- Within metros (CBSA, school districts with ≥ 100 pupils): no compression in exposed metros. Enrollment-weighted SD of
  log property tax per pupil *rises* by 0.55–0.74 log points per SD of exposure in 2020–23 (SE 0.3; mean SD 0.38).
  (`a4_cbsa_dispersion.txt`)
- Within counties: within-county SD vs exposure +0.4 to +0.65 pp in 2019–21, not significant. (`a3_district_panel.txt`)
- Across state lines: |gap| in log property tax per pupil between adjacent districts in different states (1,566 pairs;
  mean 0.73 vs 0.44 within states) shows no post-2017 narrowing relative to same-state pairs, and no relation to the two
  states' SALT contrast. (`a5_adjacent_district_gaps.txt`)
- Nationally, within-state dispersion across districts drifts up after 2017 (mean state SD 0.60 → 0.65, 2017–23).
- Answer to (b): no evidence that the cap compressed differences in tax levels; if anything slight widening in exposed
  metros. The border result in (a) (high-side border districts falling behind) narrows cross-border gaps for the
  ~800 districts concerned, but that is too small to show up in pair-level gaps.

## 6. (c) Values, assessment, regressivity, nominal rates

- **House prices.** FHFA tract HPI 2012–25: near × high-side tracts flat (±0.7%, n.s.). A near × high × exposure
  interaction falls ~1% per SD after 2019, but the **low-side placebo falls equally**. So it is a near-border × exposure
  pattern on both sides, not capitalisation of the cross-border option. (`b4_hpi_border_ddd.txt`) County exposure in the
  CoreLogic 1% sales: exposed counties' prices fall behind 1.3–2.0% per SD from 2021 (pandemic re-sorting away from
  expensive metros is the obvious confound).
- **Assessment ratios.** Given price, log AV/price rises 1.4–2.3% per SD of exposure from 2020: assessments do not
  follow the relative price decline, so effective rates (bill/price) rise 1.1–2.3% per SD. (`c2_parcel_panel.txt`)
- **Regressivity.** The slope of log(AV/price) on log price changes by +0.001 to +0.003 per SD of exposure after 2017:
  negligible (if anything, slightly less regressive).
- **Nominal rates.** Exposure: −0.2 to −0.5% per SD (2019–22). Border design: −2 to −3.5% on high-side near-border
  parcels; this is the margin through which their levies fell.
- __KPANEL__

## 7. Map: outside options across school-district lines

`local_juris/R/27`, figures `output/analysis/local_juris/figures/school_district_gaps_us.png` and
`school_district_gaps_zoom.png` (NY, Chicago, Philadelphia-NJ, Kansas City, Cincinnati, Portland). For each of 9.8k
school districts: median effective rate (2017–19 sales), own minus the cheapest adjacent district, and, for the 1,495
districts on a state line, own minus the cheapest adjacent district in the other state. Median gap to the cheapest
neighbour is 0.22 pp (p90 0.82); across state lines the median is 0.12 but the p90 is 1.15 pp, and 27% of state-line
districts face a ≥ 0.5 pp cheaper district across the line. The largest outside options are on the NJ side of NJ-PA and
NJ-NY, IL against IN/WI/MO, NY against PA, and TX against OK/LA/NM. Tract-level versions with 15/30/60-minute
drive-time neighbourhoods are in `local_juris/figures/tract_drive_gap_*` (§4.24).

## 8. Caveats and what I would do next

- **Identification limits.** Exposure designs rest on variation across a few dozen large exposed counties within states and
  coincide with the COVID period. The border design is clean on observables, but its revenue effect has no SALT
  dose-response.
- **Cluster access.** SOM's queues gave us only lowest priority all night, and its estimated start for the full-roll
  panel extract moved to November. I ran the same extract on Bouchet (__BOUCHET__). The SOM jobs were cancelled.
- **Next steps:**
  1. Event-study the full CoreLogic tract-year panel, decomposing levies into AV and rates, with assessment ratios and regressivity by county-year.
  2. Measure exposure at the parcel or ZIP level (SOI ZIP 2016), so it matches the border tracts rather than county averages.
  3. Data Axle in-migration by origin state, from the family-level sample (did movers into low-side border tracts come from the high side?).
  4. Placebo borders (pseudo-sides) and randomisation inference for the district DDD.
  5. A 2SLS of migration on the outside-option gap once Data Axle tract move-outs are in.
