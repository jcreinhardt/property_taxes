# SALT cap and local jurisdictions: overnight report (2026-10-20/21)

Questions: **(a)** did the 2018 SALT cap change local property-tax revenue; **(b)** did it change dispersion of tax
levels across jurisdictions; **(c)** did it change property values, assessment ratios, assessment regressivity and nominal
rates. Priority (your note): the border-distance instrument. Everything is exploratory and not pre-registered. Full numbers
and file pointers: `text/data_exploration/dataaxle.md` §4.25–4.34. Code: `code/analysis/salt_local/` (new; `a*` revenue
and dispersion, `b*` CoreLogic panel and prices, `c*` figures, `d*` migration, `bouchet/` SQL) and
`code/analysis/local_juris/R/24–27` (instrument, school districts). Outputs: `output/analysis/salt_local/`. Key figure:
`figures/c3_key_results.png`.

## 1. Bottom line

1. **The directional border instrument is usable.** The instrument is being within 10 miles of a state border on its
   *higher-tax* side, compared with the low side of the same border and with tracts farther away. It raises the chance
   that a ≥ 0.5 pp cheaper tract is within 30 minutes' drive across the state line by 33 pp (SE 4), and the outside-option
   gap by 0.35 pp. It does *not* bring better schools within reach, and it is balanced on own-tract achievement, SES,
   income, home values, education, tenure and housing age. Plain distance to the border fails: border areas differ, and
   they also offer better schools across the line.
2. **(a) Revenue.** Two findings, which don't line up as one clean SALT story:
   - **Exposure designs:** in large, SALT-exposed jurisdictions, property-tax growth that had outpaced the rest of the
     state flattens after the cap, by about 0.3–0.7% a year per SD of exposure (Census of Governments and F-33 district
     finance, weighted; robust to an IRS-based or a property-tax-based exposure measure). Unweighted, across small
     jurisdictions, there is no clear break.
   - **Border design:** school districts with a cheaper district across a nearby state line lost **2.2% (SE 1.0)** of
     property-tax revenue relative after 2017, rising to about 3% by 2020–23. Pre-trends are flat, the
     randomisation-inference p is 0.04, and dropping any single border pair leaves it between −1.7% and −2.6%. The margin
     is nominal rates, not assessed values (CoreLogic parcels). 2SLS: about −6% revenue per pp of outside-option gap.
     But the effect has **no clear SALT dose-response**: it is not larger with county, state, ZIP or SES-based exposure.
     The finest (ZIP) test points the right way, −0.9% per SD on the high side (SE 0.6), but is not significant.
3. **(b) Dispersion.** No compression of tax levels: within metros, within counties, across tax areas, across tracts, or
   between adjacent districts on either side of state lines. If anything, dispersion across school districts widens
   slightly in exposed metros (+2% of the mean SD).
4. **(c) Values and assessment.**
   - **Prices:** sale prices in exposed counties fall 0.6–1.8% per SD behind after 2019, most likely pandemic-era
     re-sorting away from expensive metros.
   - **Assessment ratios:** assessments don't follow those relative declines, so assessment ratios (+1 to +4%) and
     effective rates (+2 to +3%) rise.
   - **Regressivity:** essentially unchanged.
   - **Nominal rates:** drift down 0.3–0.9% per SD in exposed counties, and 2–3.5% on the high side near borders.
   - **Border capitalisation:** a cross-border capitalisation pattern appears with ZIP-level exposure: prices fall on the
     high side and rise on the cheap side near borders in exposed ZIPs. But it is not robust in FHFA tract indices, and
     with county exposure it fails a low-side placebo.
5. **Migration.** No move-out response at all (Data Axle full data: 528m address pairs, 68k tracts) in either the border
   or the exposure design. This matches the earlier sales-turnover result and the household-level SALT study.

## 2. Data assembled tonight

| Source | What | Where |
|---|---|---|
| CoreLogic tract medians, sales 2017–19, + UChicago CSDS drive-time matrix | outside-option gaps by tract (15/30/60 min) | `data/local_juris/tract/`, `data/external/csds_ttm/` |
| SEDA 4.1 (Stanford) | district achievement and SES | `data/external/seda41/` |
| ACS 2013–17 (tidycensus) | tract covariates | `data/external/acs17_tract.csv` |
| TIGER cb 2019 school districts | tract → district; adjacency | tigris cache |
| IRS SOI 2016, county and ZIP | SALT above $10k per return (stub approximation) | `data/local_juris/salt_exposure_county2016.csv`, `salt_exposure_tract_zip2016.csv` |
| Census of Governments (county aggregates, FY2012–22) | county / school / county-area property tax | `data/census_finances_test.db` (existing) |
| Census F-33 district files FY2012–23 | property tax, enrollment, spending by NCES district | `data/external/elsec/` (FY2022 from xlsx; the txt is truncated) |
| FHFA annual tract HPI | prices 2012–2025 | `data/external/fhfa/` |
| **CoreLogic full residential roll + sales 2012–24 (Bouchet)** | tract / tax-area / county-year levy, AV, nominal rates; 3–5m sales a year | `data/local_juris/kpanel/` → `tract_year.rds`, `county_year.rds` |
| Data Axle (Bouchet), 1% and full | address move-outs by tract-year 2013–21 | `data/local_juris/border_moveout/` |

## 3. The border instrument

Figure `figures/c1_iv_first_stage_balance.png`; tables `output/analysis/local_juris/iv_first_stage_balance.txt` and `iv_high_side.txt`.

- **Plain distance fails** (R/25). Within 5 mi, 38% of tracts have a ≥ 0.5 pp cheaper cross-state tract within 30 minutes
  (0% beyond 50 mi). But 44% also have a cross-state district ≥ 0.25 SD better in achievement. Border tracts are poorer,
  older, more rental and lower-achieving (0.2–0.3 SD, state FE).
- **Directional version** (R/26). Each tract is assigned to its nearest state-border pair. The "high" side is the one with
  higher median tract rates within 20 mi. Instrument: near (< 10 mi) × high, with pair × side and pair × distance-bin fixed
  effects (55.9k tracts, 70 pairs, SE by county).
  - **First stage:** +0.33 (0.04) cheaper cross-border option; +0.35 pp (0.08) tax gap. District level: +0.31 pp, F = 14.5.
  - **School access:** p90 reachable achievement minus own −0.05 SD (0.05); better district across the line −0.09 (0.05).
  - **Balance:** all within ±0.17 SD and insignificant (share white −0.17 SD is borderline).
- **Cheaper adjacent districts are not worse-school districts** (R/27, 9.8k districts): the cheapest adjacent district's
  achievement is +0.02 SD relative to own (+0.07 when it is ≥ 0.5 pp cheaper).

## 4. (a) Revenue

**Exposure designs.** County SALT exposure (2016 SOI) or property-tax exposure (share of parcels with a 2016 bill above
$5k or $10k); county or district + state × year FE.
- **Census of Governments, weighted by returns:** school districts by county, slope change after FY2019 −0.26 to −0.30%/yr
  per SD (SE 0.10). County governments −0.35 to −0.67%/yr. Unweighted: no significant break. (`a1_revenue_event_study.txt`)
- **F-33 districts, enrollment-weighted, property-tax exposure:** slope change −0.71%/yr (0.15) with the $5k share,
  −0.39 (0.14) with the $10k share. (`a6_alt_exposure.txt`)
- **CoreLogic full roll, residential levy:**
  - Unweighted, faster growth in exposed counties continues (+1.7% per SD by 2024 vs 2017).
  - Weighted, a mild slowdown after 2020.
  - Nominal rates fall 0.3–0.9% per SD in 2021–23 (unweighted). (`b2_county_event_studies.txt`)
- **Caveats:** the identifying variation is a few dozen large exposed counties within their states, and the post period
  overlaps COVID aid and the 2020–22 housing cycle.

**Border design** (figure `a3_district_border_ddd.png`; `a3_district_panel.txt`, `a7_district_ri.txt`).
- **F-33 districts, triple difference** (district + pair × side × year + pair × distance-bin × year FE; 6.8k districts,
  777 near-high):
  - Pre 2012–16: −0.5, −0.1, +0.2, −1.5, −0.6%.
  - Post 2018–23: −1.6, −1.7, −3.2, −3.1, −2.7, −3.4% (SE 1.1–1.4).
  - Pooled −2.2% (1.0); RI p = 0.04; leave-one-pair-out −1.7 to −2.6%; leave-one-state-out −1.4 to −2.9% (not driven
    by the levy-cap states NJ/NY/IL); similar at < 20 mi, weaker at < 5 mi.
  - A county-line placebo within states is uninformative: county lines are dense, so estimates are imprecise
    (SE 3–4%) and pre-trends are large.
  - 2SLS −6.3% per pp of gap (3.8).
- **CoreLogic 1% constant parcels:** bills −2.8% (2018) to −4.4% (2025), via nominal rates (−1.9 to −3.5%), not AV
  (`c2_parcel_panel.txt`). The full-roll tract version points the same way but is noisy (`b3_tract_iv_ddd.txt`).
- **SALT dose-response:** none with county exposure (+0.3 to +0.5%/SD), with the state contrast across the pair, or
  with district SES. With district ZIP exposure, the high side has −0.9%/SD (0.6) relative to the low side.
- **County-government version:** exposure × border-high county 2019 −1.8% (0.6), 2021–22 −1.7 to −1.9% (≈ 1.0)
  (`a2_revenue_outside_option.txt`).

**Reading.** After 2017, jurisdictions with a cheaper option across a nearby state line cut rates and fell behind in
levies. The timing fits a higher after-tax cost of high local taxes, but the weak dose-response means I can't separate SALT
from a general post-2017 increase in cross-border competition (remote work, outflows from high-tax states).

## 5. (b) Dispersion of tax levels

- **Within metros** (CBSA, school districts): enrollment-weighted SD of log property tax per pupil +0.55 to +0.74 log
  points per SD of exposure in 2020–23 (SE 0.3; mean 0.38). Widening, not compression. (`a4_cbsa_dispersion.txt`)
- **Within counties:** across districts, +0.4 to +0.65 (n.s.). Across tax areas (SD of log nominal rates, CoreLogic) and
  across tracts (SD of effective rates): flat. (`a3_district_panel.txt`, `b2_county_event_studies.txt`)
- **Across state lines:** adjacent-district gaps (1,566 cross-state pairs; mean 0.73 vs 0.44 within states) show no
  post-2017 narrowing and no relation to the states' SALT contrast. (`a5_adjacent_district_gaps.txt`)
- **Nationally:** within-state dispersion drifts up after 2017 (mean state SD 0.60 → 0.65).

## 6. (c) Values, assessment, regressivity, nominal rates

- **Prices** (county exposure, CoreLogic full sales): −0.6 to −1.8% per SD from 2019, pre flat.
- **Assessment ratios** (log median AV/price): +0.4 to +4.0% per SD (weighted +0.3 to +1.9%).
- **Effective rates:** about +2.8% of the mean by 2024.
- **Regressivity:** the slope of log(AV/P) on log P (mean −0.28) moves by at most ±0.008, essentially flat; PRD flat.
  (`b2_county_event_studies.txt`, figure `b2_county_event_studies.png`)
- **Nominal rates:** −0.3 to −0.9% per SD in exposed counties (2021–23); −2 to −3.5% on the high side near borders.
- **Border capitalisation:**
  - FHFA HPI with county exposure: a near × exposure decline appears on *both* sides, so the low-side placebo fails.
    (`b4_hpi_border_ddd.txt`)
  - CoreLogic tract median prices with ZIP exposure: high side −1.2 to −1.9% per SD after 2017, low side +0.8 to +3.9%
    (noisy pre-period). (`b3_tract_iv_ddd_zipexposure.csv`)
  - FHFA with ZIP exposure: within 5 mi the low-minus-high gap opens from ≈ 0 to +1.2% by 2024, but at 10/20 mi it already
    existed before 2017. (`b5_hpi_zip_exposure.txt`, figure)
  - **Repeat sales** (28.9m sales, parcel FE, 30% of parcels): high-side near-border × ZIP exposure declines gradually,
    0 before 2017 to −1.9/−2.0% per SD in 2023–24 (SE 1.3–1.6). The low-side placebo is flat. Pooled −0.2% (0.8) for 2018+,
    plus −1.0% (0.7) from 2021. (`b6_repeat_sales.txt`, figure)
  - **Suggestive, not established**: every version points toward capitalisation of the cross-border option in exposed
    areas, none is significant on its own.

## 7. Migration

Data Axle address move-outs (primary family changes; annualised by file cut-offs). 1% sample, then the full data (528m
address pairs). County exposure: flat (pre −0.08 to −0.11 pp, post −0.04 to −0.11). Border DDD: pre −0.07 to +0.06, post
−0.07 to −0.01 (SE 0.07–0.12). Interaction with exposure: none. (`d1_moveout_ddd.txt`, `d2_moveout_full.txt`)

**Direction of cross-border moves** (Data Axle 1% families; 4,306 moves between opposite sides within 20 mi of the same
border pair; `d3_family_moves.txt`). Overall, the share going high → low is flat. Where the high-tax side's state is also
more SALT-exposed, that share rises from about 0.5 (2013–15) to about 0.63 (2016–21). Relative to other pairs the post-2017
coefficients are +0.04 to +0.11 (SE 0.04–0.06), but the shift is already there in 2016, so it is not a clean break.

## 8. Map: outside options across school-district lines

Figures `output/analysis/local_juris/figures/school_district_gaps_us.png` and `school_district_gaps_zoom.png` (NY,
Chicago, Philadelphia-NJ, Kansas City, Cincinnati, Portland). Each of the 9.8k districts shows its median effective rate,
own minus the cheapest adjacent district, and, for state-line districts, own minus the cheapest district across the line.
- **All districts:** the median gap is 0.22 pp (p90 0.82).
- **Across state lines:** median 0.12, p90 1.15 pp; 27% of state-line districts have a ≥ 0.5 pp cheaper neighbour across
  the line.
- **Largest outside options:** NJ against PA/NY, IL against IN/WI/MO, NY against PA, and TX against OK/LA/NM.
- **Drive-time versions:** tract-level maps with 15/30/60-minute neighbourhoods are in
  `local_juris/figures/tract_drive_gap_*`.

## 9. Caveats, process notes, next steps

- **Identification.** Exposure designs rely on few large counties and overlap COVID. The border design is clean on
  observables, but its revenue effect has no significant SALT dose-response.
- **Measurement.** Tract-level CoreLogic levies are noisy because roll composition changes (filtered to tracts with
  stable parcel counts). The SOI exposure is a stub-average approximation.
- **Compute.** SOM gave us only lowest priority all night, and its estimates for the full-roll extract slipped to
  November. The extract and both Data Axle jobs ran on Bouchet instead (about 30 min), and the SOM jobs were cancelled.
  Rule saved: CPU-only jobs on SOM GPU partitions must be cancellable.
- **Next steps:**
  1. Repeat sales on all parcels, not 30% (needs the cluster: the laptop's memory limits it), to tighten the capitalisation test.
  2. Dose-response with the 2018 SOI "limited SALT" amounts (actual binding) by ZIP.
  3. Scale up the cross-border move-direction test to the full Data Axle (the 1% family sample has ~480 such moves a year).
  4. Synthetic placebo borders (county lines within states) for the district DDD.
  5. Separate the school-district result by state school-finance regime (states that cap levies, e.g. NJ 2% cap, NY tax
     cap, IL PTELL), since levy caps could produce the high-side slowdown independently of SALT.
