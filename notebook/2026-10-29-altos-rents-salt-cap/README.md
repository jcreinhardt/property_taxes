---
date: 2026-10-29
title: Asking rents and the SALT cap: exposure event studies, border diff-in-disc, near vs far from the border (Altos listings)
question: Compute the average SALT subsidy lost at ZIP / county level and run (a) exposure difference-in-differences on rents, (b) border differences-in-discontinuities of the rent jump on the cross-border difference in subsidy lost, (c) a border-region design comparing differently exposed areas near a state border with those further away.
status: current
code: property_taxes 9d83b5f + uncommitted scripts on branch claude/altos-data-hpc-location-77c629 (worktree; see Scripts)
tags: rents, Altos, exposure, border RD, DDD, SALT
---

## Answer
Within state, asking rents in more exposed ZIPs fell relative to less exposed ones after 2018, by about the same amount
as sale prices did, but the pattern is not clean: there is a pre-trend of the same sign in 2013–14, a 2012-rent-level
× year control removes the post-2018 effect, nothing happens at state borders, and the gradient is absent within
10 miles of a border. The exposure measure is the lost federal subsidy per owner household (`build/exposure/05`,
`d_salt_own`, TY2016 → TY2018, per $100/yr, winsorised p1/p99); the rent index is log asking rent at a fixed type ×
bedroom mix (`build/altos`); all weighted by 2016 returns, SE by county, 2017 = 0.

**(a) Exposure × year, ZIP (9,486 uncensored SOI ZIPs with ≥ 10 listings every year 2012–24; zip + state × year FE), % per $100/yr:**

| | 2013 | 2014 | 2015 | 2016 | 2018 | 2019 | 2020 | 2021 | 2022 | 2024 |
|---|---|---|---|---|---|---|---|---|---|---|
| total lost subsidy | −0.29 (0.07) | −0.16 (0.06) | 0.03 (0.05) | 0.04 (0.04) | −0.03 (0.02) | −0.13 (0.05) | −0.26 (0.05) | −0.43 (0.07) | −0.42 (0.09) | −0.66 (0.08) |
| + 2012 rent level × year | −0.41 (0.08) | −0.26 (0.05) | −0.16 (0.04) | −0.16 (0.03) | −0.00 (0.03) | −0.05 (0.06) | −0.01 (0.07) | −0.06 (0.08) | −0.07 (0.09) | −0.16 (0.09) |

Three components jointly (ZIP): the property-tax part is −0.3 to −0.5 after 2018 but −0.6 in 2013 and −0.26 in 2016;
the income/sales-tax part goes from +0.26 (2016) to −0.9 (2024); mortgage interest from −0.47 (2016) to +1.3 (2024).
Raw mean log rent and the 2-bed-apartment median give the same shape (2024: −0.75 and −0.55). The **county** panel
(1,542 counties) is unusable: −1.4% in 2013 rising to +0.8% in 2019 and back to −0.5% in 2024, worse with the 2012
control; the no-income-tax states look the same (−3.0 in 2013).

**(b) Border diff-in-disc on the exposure gap** (59 pairs, 39,727 tracts within 100 mi with ≥ 5 listings; high-exposure
side = larger return-weighted mean exposure within 20 mi; gap median $186/yr per owner, IQR $87–269; it coincides with
the high-tax side in 54% of pairs). Pooled local-linear (h = 15 mi, pair × year FE with pair-specific slopes on each
side, triangular kernel): change in the rent discontinuity per $100 gap is −0.006 (0.004) in 2018, +0.005 (0.005) 2019,
−0.002 (0.008) 2021, −0.009 (0.010) 2024; pre-period within ±0.014. Series of per-pair rdrobust diff-in-discs regressed
on the gap: post-2018 pooled slope −0.011 (0.006) with 42 pairs, pre-period 0.001 (0.009); yearly slopes −0.007 to
−0.022 (SE 0.007–0.010). The unscaled high-tax-side diff-in-disc is −0.01 to −0.02 log points in 2018–22 (CIs ±0.03),
−0.02 already in 2016. **There is no discontinuity in exposure at the line:** pair-demeaned exposure is −2.0 ($100) at
5–10 mi on the low side, +1.2 at 0–5 mi on the low side and +0.7 at 0–5 mi on the high side (rdrobust −1.1
[−9.8, 3.9]); the "gap" is a 20-mile-average difference, not a jump, so (b) has no first stage at the border.

**(c) Near vs far** (28,833 tracts within 50 mi of a ranked pair, 70 pairs; tract + pair × side × year + pair × distance-bin
× year FE): the exposure gradient away from the line (c_t) is −0.06 (0.02) in 2018, −0.16 (0.04) 2019, −0.25 (0.04)
2021, −0.39 (0.04) 2024, with −0.26 / −0.24 in 2013–14 and ≈ 0 in 2015–16; the extra gradient within 10 mi (b_t) is
+0.14 to +0.21 (SE 0.06–0.11) after 2018 and +0.27 in 2013–14, so near the border the gradient is about zero
(near-only event study: 0.01 (0.07) in 2019, 0.00 (0.12) 2022, −0.23 (0.06) 2024; far: −0.16, −0.25, −0.39). The
exposure response, where it exists, is a within-state, away-from-border phenomenon.

What it means: the sign and size of (a) match the price result (c29: −0.4% in 2019, −1% by 2021–22 per $100), and
the timing is the same (effects from 2019–20, growing). But the 2013–14 pre-trend, the sensitivity to the 2012 rent
level, the county instability and the absence near borders and at the line say this is a ZIP-level rent trend
correlated with exposure (high-income, high-rent ZIPs within a state) rather than an identified cap effect. Rents
are also not the margin the cap should move first (renters do not lose the deduction; landlords in high-exposure ZIPs
lose it only on second homes), so a rent fall of the same size as the price fall is itself a sign of a common trend.

Data caveat: the Altos feed changes in 2018–20: coordinate coverage falls from 100% to 66% (2018), 50% (2019) and
47% (2020) before recovering to 93–97% from 2022; the apartment share jumps from 0.75 to 0.83–0.90 in 2019–21; the
number of ZIPs with ≥ 10 listings drops from 16.6k (2018) to 13.1k (2020); the median 2-bed apartment rent jumps
$923 → $1,095 in 2019. The balanced ZIP panel is selected on surviving this; the tract panels lose a third of listings
in 2019–20. State × year FE absorb a national break but not one that differs by exposure.

## How
- **Outline:** Altos weekly CSVs (Bouchet) → unit × month median rent → tract by point-in-polygon (2019 cartographic
  tracts) → ZIP × month and tract × year cells by type × beds → geography × year FE of cell mean log rent (`lr_adj`)
  → (a) `lr ~ i(y, E) | zip + st^y`, (b) within-tract change since 2017 on signed distance by pair, (c) tract DDD.
- **Choices:** rent filter $200–20,000/month; unit = Altos `unit_id` (address + unit hash); unit-month = median over the
  weeks listed; ZIP cells count unit-months, tract cells unit-years (median of months); index = geography × year FE
  from `mlp ~ 1 | geo x year + type x beds` weighted by n (type: apartment / house / other; beds −1, 0–4+); county =
  ZIP's main HUD county; (a) balanced on ≥ 10 listings every year 2012–24, uncensored SOI ZIPs (`clean`), weights
  returns16; (b) tract-years ≥ 5 listings, tract exposure = ZIP measure where uncensored (87–90% of tracts) else county,
  tract weight = ZIP returns × parcel share, sides and distances from `iv_tract_side.rds` (T = 30), high-exposure side by
  20-mi return-weighted mean, h = 10 / 15 / 25, per-pair RDs only for pair-years with ≥ 20 tracts within h on each
  side, series by WLS 1/se²; (c) tracts < 50 mi seen in ≥ 6 of 12 years, near = < 10 mi, bins as in `iv_tract_side`.
- **Inputs:** Bouchet `/nfs/roberts/pi/pi_csl53/corelogic/raw_data/altos/` (782 files; read only); Bouchet jobs
  31309924 (`altos_01_unit_month.sql`, 7 min on 16c/320G, peak 320G) and 31311040 (`altos_01b_cells.sql`); exports in
  `data/altos/` (not in git: `altos_{zip_month,tract_year,tract_quarter,repeat_unit,file_stats}.parquet`;
  `altos_unit_month.parquet`, 11.6 GB, stays in `~/ptax_explore/altos/` on Bouchet); `data/built/altos/*.rds`;
  `data/built/exposure/exposure_components_{zip,county}.csv`, `salt_exposure_tract_zip2016.csv`;
  `data/built/geo/iv_tract_side.rds`; `data/hud_zip_to_county_crosswalk.xlsx`.
- **Scripts:** `code/build/altos/01_altos_unit_month.sql`, `01b_altos_cells.sql`, `02_rent_panels.R`;
  `code/designs/exposure_did/17_rents_altos.R`, `code/designs/border_rd/08_rent_diff_in_disc.R`,
  `code/designs/border_iv/17_rent_exposure_near_far.R`; outputs `output/designs/exposure_did/d17_*`,
  `output/designs/border_rd/d08_*`, `output/designs/border_iv/d17_*`, `output/build/altos/02_rent_panels.txt`.
- **Check:** 237,392,005 unit-months, 58,174,731 units, 21,643,196 of 21,645,482 coordinates in a tract; ZIP spec 1:
  2024 −0.66 (0.08), 2013 −0.29 (0.07); (b) post-2018 series slope −0.011 (0.006), 42 pairs; (c) c_2024 −0.39 (0.04),
  b_2024 +0.15 (0.07).

## Open
- A repeat-listing index (same unit across years, `altos_repeat_unit.parquet`) would separate the feed's composition
  break from rent changes; the 2012-level control suggests a price-tier trend, as in the ZIP price design (06).
- Monthly ZIP cells exist (`altos_zip_month.parquet`) for an event study around the December 2017 enactment.
- Both border designs use the SOI-exposure gap; a rate-based gap (as in `border_rd/04`) is the other natural scaling.
