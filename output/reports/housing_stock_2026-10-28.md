# Does the housing stock respond to the SALT cap? (2026-10-28)

Question: is there evidence that the housing stock changes in response to the cap — vacancy, new construction, ownership
rates? Scripts: `code/designs/exposure_did/17_housing_stock.R`, `code/designs/border_rd/04_rd_by_year_and_diff_in_disc.R`;
data builds `code/build/acs/07_housing_stock.R`, `06_tract_year_panel.R`, `code/build/corelogic/10_county_tract_year_panels.R`.
Notebook: `notebook/2026-10-28-housing-stock`. Running notes: `text/data_exploration/dataaxle.md` §4.41–4.42.

## Bottom line

No. Every stock margin we can measure is flat or moves by tenths of a percent per SD of exposure, and the two larger
movements (vacancy, new-construction sales) either arrive with COVID or continue a pre-2017 trend. In 2018–19, the
clean window, nothing looks like a supply or tenure response.

## What existed before today

- **USPS vacancy at state borders** (`designs/border_twfe/11`, §4.23): vacancy fell everywhere after 2019 and fell
  *more* on the higher-tax side; the lower-minus-higher gap rose +0.15 pp (full) / +0.38 pp (balanced) pooled post,
  significant only from 2020. The opposite sign to an exodus; read as catch-up plus the COVID demand shock.
- **ACS owner share and population as border covariates** (`designs/border_rd/04`, 2026-10-21 entry): owner share on
  the high-tax side −0.2 pp (2018) and −0.4 pp (2019), population −0.5% and −0.9%, then insignificant. Tiny.
- Nothing on units, construction or tenure in the county exposure design; the new-construction flag was only a sample filter.

## New results

### County exposure × year

`y ~ i(year, ex_z, ref = 2017) | county + state × year`, SE by county, weighted by 2016 returns; ex_z = 2016 SALT
above $10k per return in SD units. Coefficients per SD, SE in parentheses.

| Outcome (2017 weighted mean) | 2014 | 2016 | 2018 | 2019 | 2021 | 2023 |
|---|---|---|---|---|---|---|
| log residential parcels, roll (1,633 counties) | 0.001 (0.001) | 0.001 (0.000) | 0.000 (0.000) | −0.000 (0.001) | −0.001 (0.001) | −0.001 (0.001) |
| new-construction sales per 1,000 parcels, roll (3.35) | +0.09 (0.04) | +0.03 (0.03) | −0.05 (0.03) | −0.09 (0.03) | −0.08 (0.04) | −0.12 (0.05) |
| new-construction share of sales, % (6.3) | −0.05 (0.07) | +0.01 (0.04) | −0.21 (0.11) | −0.17 (0.05) | −0.16 (0.04) | −0.36 (0.10) |
| log housing units, ACS 1-yr (844 counties) | −0.001 (0.000) | −0.000 (0.000) | 0.000 (0.000) | 0.000 (0.000) | +0.001 (0.000) | +0.001 (0.000) |
| vacant per 100 units, ACS 1-yr (10.2) | −0.03 (0.03) | +0.01 (0.02) | +0.05 (0.03) | +0.05 (0.02) | +0.33 (0.06) | +0.26 (0.04) |
| vacant for rent per 100, ACS 1-yr (2.15) | −0.04 (0.02) | −0.00 (0.02) | −0.02 (0.01) | +0.00 (0.01) | +0.12 (0.02) | +0.03 (0.03) |
| vacant for sale per 100, ACS 1-yr (0.86) | −0.01 (0.01) | −0.00 (0.01) | +0.01 (0.01) | +0.00 (0.01) | +0.03 (0.01) | +0.02 (0.01) |
| seasonal vacant per 100, ACS 1-yr (2.69) | −0.01 (0.02) | +0.00 (0.01) | +0.01 (0.01) | −0.02 (0.01) | +0.04 (0.01) | +0.07 (0.02) |
| owner share of occupied units, %, ACS 1-yr (62.3) | +0.01 (0.04) | −0.01 (0.03) | −0.05 (0.03) | −0.07 (0.03) | −0.07 (0.04) | −0.13 (0.08) |
| median year built, ACS 1-yr (1976) | −0.03 (0.02) | −0.01 (0.02) | −0.02 (0.02) | +0.04 (0.02) | −0.04 (0.04) | −0.05 (0.04) |
| log housing units, ACS 5-yr (3,108 counties) | −0.001 (0.000) | −0.000 (0.000) | 0.000 (0.000) | +0.001 (0.000) | +0.002 (0.001) | +0.003 (0.001) |
| vacant per 100 units, ACS 5-yr (11.2) | +0.03 (0.02) | +0.01 (0.01) | +0.00 (0.01) | +0.02 (0.01) | +0.13 (0.03) | +0.22 (0.03) |
| vacant for sale per 100, ACS 5-yr (0.96) | +0.00 (0.00) | −0.00 (0.00) | +0.00 (0.00) | +0.01 (0.00) | +0.02 (0.00) | +0.03 (0.00) |
| owner share, %, ACS 5-yr (63.7) | −0.05 (0.01) | −0.02 (0.01) | −0.01 (0.01) | −0.03 (0.02) | −0.08 (0.04) | −0.12 (0.06) |
| median year built, ACS 5-yr (1975) | −0.06 (0.03) | −0.02 (0.01) | −0.00 (0.01) | −0.01 (0.02) | −0.01 (0.01) | −0.04 (0.02) |

Reading, outcome by outcome:

- **Stock (parcels, units).** Nothing. The roll count is within ±0.1% per SD throughout; ACS units are within
  ±0.1% through 2020 and +0.2–0.3% by 2021–23 in the 5-year data (which by then averages 2017–23 windows). A SD of
  exposure is a large difference in cap burden, so a 0.2% unit response over five years is no response.
- **New construction.** New-build sales per 1,000 parcels fall 1.5–4% of the mean per SD from 2018, significant from
  2019. But 2013 and 2014 sit at +0.07 and +0.09 (SE 0.04): exposed counties were already on a relative decline in
  new-build sales before the cap, so the post-period continues a trend. The share-of-sales version is noisier and has
  a −0.24 outlier in 2015. Median year built does not move. The direct count of parcels built in the year on the full
  roll needs a cluster job (`code/build/corelogic/som/l_yearbuilt.sbatch`, written, not run: clusters unreachable).
- **Vacancy.** +0.05 vacant units per 100 per SD in 2018 and 2019 (0.5% of the mean, borderline, 1-year data), then
  +0.2–0.3 from 2021 in both surveys. The 2021+ rise is the COVID-era pattern in dense, expensive counties, which are
  the exposed ones; the types split shows it in for-rent units in 2021 (+0.12) and in seasonal units by 2023, with a
  small steady rise in for-sale vacancies (+3% of their mean from 2021). Not a 2018 break.
- **Ownership.** The owner share of occupied units falls by about 0.1 pp per SD by 2023 on a base of 62–64%, with
  2019 significant in the 1-year data (−0.07, SE 0.03). In the 5-year data the pre-period runs from −0.05 (2013) up to
  0 (2017), so the post-period decline is a reversal of a pre-trend rather than a clean break. A tenth of a
  percentage point either way.

### Border difference in discontinuities

RD of the within-tract change since 2017 at state borders (rdrobust, h = 15 mi, pair-demeaned, SE by county; high-tax
minus low-tax side). New outcomes added today:

| Outcome | 2015 | 2016 | 2018 | 2019 | 2020 | 2023 |
|---|---|---|---|---|---|---|
| tract log residential parcels (roll) | +0.008 | +0.009 | −0.018 [−0.03, 0.02] | −0.076 [−0.28, 0.11] | +0.011 | −0.055 |
| new-construction sales per 1,000 parcels (roll) | +0.60 [−0.17, 2.20] | +0.02 | +0.63 [−0.01, 2.03] | +0.87 [0.40, 2.64] | +0.53 [0.16, 2.49] | +0.79 [−0.29, 2.42] |
| tract log housing units (ACS 5-yr) | +0.004 | +0.002 | −0.004 [−0.010, 0.000] | −0.008 [−0.018, −0.003] | −0.008 [−0.020, 0.008] | −0.029 [−0.051, 0.005] |
| tract vacancy share (ACS 5-yr) | −0.001 | −0.001 | −0.000 [−0.005, 0.002] | −0.000 [−0.008, 0.003] | −0.001 | −0.002 |
| tract owner share (ACS 5-yr, from 2026-10-21) | +0.002 | +0.002 | −0.002 [−0.007, −0.001] | −0.004 [−0.010, −0.002] | −0.002 | −0.006 [−0.019, −0.001] |

- Roll parcel counts: nothing, with ±20% confidence intervals (county-year roll gaps make tract counts jumpy).
- ACS units fall 0.4% and 0.8% on the high-tax side in 2018–19, matching population (−0.5%, −0.9%), but the series
  slopes down across the whole window (+1% in 2013 → −3% in 2023): a linear trend, not a break.
- Vacancy share: flat every year.
- New-construction sales per 1,000 parcels are *higher* on the high-tax side after 2017 (+0.6 to +0.9, about a fifth
  of the county mean), with 2015 already at +0.6. The opposite of supply flight, and not well dated.
- The 21 outcomes already in the script reproduce exactly after the rerun.

## Caveats

- ACS 5-year release y = survey years y−4..y, so 2018–2021 windows straddle the cap; the 1-year series is cleaner but
  covers only counties ≥ 65k and skips 2020. Both are shown.
- The county exposure measure correlates with price level and density, so anything that happened to expensive urban
  counties in 2020–22 loads on it. That is the vacancy story.
- The 1% parcel sample is a fixed 2016 universe and cannot see post-2016 construction; the roll count and the
  new-construction sale flag are the only construction measures available on the laptop.
- Roll counts have delivery gaps; the county design keeps counties whose residential count is ≥ 80% of its maximum in
  every year (1,633 of about 3,000 counties).

## What would sharpen this

1. Run `som/l_yearbuilt.sbatch` on SOM: parcels with year built = y and roll entries per county × tract × year 2013–24;
   copy `roll_built_*.parquet` into `data/built/corelogic/kpanel/` and rerun `build/corelogic/10` and
   `designs/exposure_did/17` (both pick the files up).
2. The SOM `permits*` tables (untested) for permitted units, which lead construction by a year or two.
3. Tract-level vacancy by type (B25004) in the border design, if the county for-sale vacancy result is worth following.

## Data notes raised alongside (not results)

- **Age in Data Axle:** persons 1–5 carry an age (adults only; missing for 47% of persons in 2006, 18% in 2017; decade
  codes only in the 2024 file), and `head_hh_age_code` is 100% filled in bands (A–I under 65 in 5-year bands, J 65+
  inferred, K–M 65–69/70–74/75+ reported). The 1% parcel panel already carries the primary family's age (`age_prim`,
  `age16`). Name + age is not a reliable national person key (53% unique; 78% with state).
- **First-time buyers:** no flag anywhere. Approximations: (i) buyer full-name hash never seen as an earlier owner or
  grantor in the same state (deeds + roll; needs a cluster lookup; common names get misclassified as repeat buyers);
  (ii) Data Axle trajectory: the new primary family's previous address renter-coded or multi-unit and never
  owner-coded (but only ~half of sellers and a quarter of leavers are tracked, and the lost records are the thin
  single-person renter profiles that first-time buyers have; tenure is modelled except code 9); (iii) proxies on the
  1% panel: FHA flag in `mortgages.parquet`, loan-to-price > 95%, `age_prim` < 35. A workable definition is (i) plus
  (iii), validated against the national first-time share (about a third of purchases in 2017–19).
