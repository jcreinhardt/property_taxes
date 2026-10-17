# Decision log — effects of the TCJA SALT cap on homeowners

All entries are dated. Specifications are written **before** the corresponding estimation is run. Changes after seeing
results are appended as dated amendments with a reason, never edited in place.

Code: `code/analysis/salt_cap/` (`sql/` runs on Bouchet against the CoreLogic/Data Axle samples, `R/` runs on the laptop on
the exported panel). Outputs: `output/analysis/salt_cap/` (this log, `assumption_ledger.csv`, `estimates.csv`, `figures/`,
`report.md`). Intermediate data: `data/salt_cap/` (git-ignored).

## 2026-10-16 — Inputs, scope and data decisions (before building anything)

### Inputs (Section 1 of the instructions)

| Source | Where | Notes |
|---|---|---|
| Data Axle consumer file | `cl.dataaxle_annual` on Bouchet; 1% building sample `~/ptax_explore/samples/da_bld1pct.parquet` | 2006–2023, **2022 file is truncated (30% of rows) and is treated as missing**. Surnames are stored as hashes only. |
| Deeds (transfers) | `cl.ownertransfer`; sample `deeds_bld1pct.parquet` | arm's-length flag = `primary_category_code = 'A'`, not inter-family, price > 0. Dates to 2026. |
| Mortgages | `cl.mortgage`; sample `mortgage_bld1pct.parquet` (built today, job 29755414) | `mortgage_type_code` P/R/J (purchase, refinance, junior); `mortgage_purpose_code` A = cash-out first. |
| Tax rolls | `cl.tax`; samples `tax_bld1pct.parquet` (all years, owner hashes, mailing, lat/lon) and `tax_bill_bld1pct_all.parquet` (bill, values, exemptions, tax area, CBSA, municipality, school district; built today, job 29755485) | 2006–2025. Bill = `total_tax_amount`; value = `calculated_total_value` (source: assessed/market/appraised per county). |
| IRS SOI ZIP data 2011–2022 | local `data/database.db`, table `returns_zipcode_agi` (6 AGI bins) | SALT amounts by bin (`taxes_paid_*` = N18300/A18300, `property_taxes_*` = N18500/A18500, from 2018 `limited_state_and_local_taxes_*` = N18460/A18460). |
| Jurisdiction boundaries | `tigris` (TIGER 2017 places, county subdivisions, unified school districts) | downloaded at estimation time for Strategy 5. |
| TAXSIM | NBER TAXSIM 35 via R package `usincometaxes` 0.7.1 | |
| Freddie Mac PMMS 30-year rate | FRED `MORTGAGE30US` weekly, `data/salt_cap/external/MORTGAGE30US.csv` | averaged to origination month. |
| County → CBSA | Census delineation file Sept 2018, `data/salt_cap/external/list1_Sep_2018.xls` | fallback when the tax roll's own CBSA field is empty. |

### Sample scale (decision)
The pipeline is built and all pre-specified estimates are first produced on the **1% building sample** (every address
whose hashed building key falls in 1% of the key space; identical sample across tax, deeds, mortgage and Data Axle; about
870k single-family and 88k condo parcels on the 2016 roll). This is the sample reported in `estimates.csv` unless a row's
`sample` field says otherwise. Following the project rule, the same build is then submitted as a background job on the
full data (all years) and the full-data estimates are appended with `sample = full` when they finish. Strategies whose
cells are too thin on 1% (boundary DiD, kink, CT/NJ revaluations) are flagged as such in the report and rerun on the full
data first.

### Fixed definitions (Section 2), as implemented
- Event time: reference year **2016**; 2017 shown but excluded from pre/post tests; main window 2012–2019; extended
  2020–2024 flagged COVID; reversal 2025+.
- `bill16` = `total_tax_amount` on the 2016 roll (2015 if the 2016 row is missing or the bill is 0/NULL; never 2017+).
- `value16` = `calculated_total_value` on the same roll row, **divided by a county sales ratio** when the county's
  calculated value is not at market: ratio_c = median(calculated value / arm's-length price) over 2015–2017 sales in county
  c with ≥ 30 sales; if ratio_c ∈ [0.85, 1.15] no adjustment, otherwise value16 = calculated value / ratio_c. Counties with
  < 30 sales keep the unadjusted value and are flagged (`value_adj = NA`). Parcels with a 2014–2016 arm's-length sale
  also carry `price1416` as a check. Reason: published assessment ratios are not available uniformly; the sales ratio is
  the standard substitute.
- `rate16` = bill16 / value16; parcels outside 0.05%–10% effective rate are flagged `rate_ok = FALSE` and excluded from
  rate-based estimation (data errors, partial-year bills, exempt parcels).
- `above16` = 1[bill16 > 10,000].
- Base sample: `property_indicator_code` 10 (single family) or 11 (condo), non-corporate owner, owner-occupied in 2016 by
  the Section 3 rule, in a state whose crosswalk match rate is ≥ 70%.
- Outcomes at parcel × year (year = calendar year of the event for deeds/mortgages, tax year for roll variables, file year
  for Data Axle):
  - `exit`: Section 3 step 3 (Data Axle surnames).
  - `sale`: arm's-length deed in year t. `log_price`: log price of that deed (first sale in the year if several).
  - `refi`: a first-lien refinance (`mortgage_type_code = 'R'`, or `refinance_loan_indicator`) recorded in year t.
    `cashout`: refi whose amount ≥ 1.05 × the imputed remaining balance of the previous first-lien loan (30-year fixed
    amortisation at the PMMS rate of the previous loan's origination month). CoreLogic's own cash-out code (purpose A) is
    reported alongside as a check.
  - `to_rental`: owner surname unchanged from t−1 to t and (homestead flag TRUE in t−1 and FALSE in t, or mailing address
    at the parcel in t−1 and elsewhere in t). Defined only where the signal exists (homestead states / non-missing mailing).
  - `appeal_proxy`: assessed value falls by more than 3% from t−1 to t, in a cell (county × tax area × year) where fewer
    than 20% of parcels change assessed value by more than 3% in either direction (i.e. not a revaluation or a
    jurisdiction-wide change). CA's 2% Prop 13 factor is below the 3% threshold by construction.

### Data build (Section 3), pre-specified
1. Crosswalk: address key = hash(ZIP5, street name, house number, unit) with upper-cased, trimmed components in both
   sources (the project's `bkey` plus the unit). Match = a Data Axle street-address record at the key in the same year.
   Match rates reported by state × year for 2012–2021 and 2023; states with a 2016 match rate < 70% among the base
   sample are dropped and listed. Condos are matched at unit level; a condo without a unit-level match keeps the deed
   outcomes but has `exit = NA`.
2. Owner-occupancy 2016: `oo16 = hs16 OR mh16 OR nm16` (homestead flag; mailing = situs; owner surname 1 or 2 in the Data
   Axle surnames at the address in the 2016 file). `inv16 = NOT oo16 AND (corporate owner OR mailing address elsewhere and
   not missing)`. Agreement of the three signals is tabulated. Classification is fixed at 2016 for all years.
3. `exit_t = 1` if none of the surnames at the address in file t appears in file t+1 or t+2; `NA` if the address has no
   Data Axle record in t, or in neither t+1 nor t+2. With 2022 missing, t = 2020 uses 2021 and 2023, t = 2021 uses 2023
   only. Validation on the 1% sample: (a) P(exit_{t−1} = 1 or exit_t = 1 | arm's-length sale in calendar year t), using
   t−1 because the file for year t has a mid-year cut-off and often already shows the buyer; (b) false-positive rate =
   P(exit_t = 1 | no sale in t−1…t+2 and owner name on the roll identical in t, t+1, t+2). Threshold: if (a) < 60% `sale`
   becomes the primary outcome and `exit` secondary.
4. Mortgage stock: first-lien loans (type P or R) from 2000; the latest with date ≤ 2016-12-31 is amortised as a 30-year
   fixed loan at the PMMS average of its origination month; `mortint16` = interest due during calendar 2016 (0 if no loan
   or loan older than 30 years). Interest-rate field used instead of PMMS when populated and between 1% and 15%.
5. Co-owner class on the 2016 roll row (two owners recorded):
   - married: `owner_relationship_type_code` in HW, MA, MM, MW, or the latest pre-2017 deed vesting in TY (tenants by
     entirety) or CP (community property);
   - likely married: not married, owners share a surname hash;
   - unmarried: not married, surnames differ, and (relationship code in SI, SM, SW, UM, UW, UN, or deed vesting in JT,
     TC, RS, UI with no spousal code);
   - other (trusts, estates, corporations, unclear): excluded from Strategy 8.
6. Imputed 2016 AGI: within ZIP, a household income index = first principal component of log value16, log inflated
   purchase price (BLS CPI-U to 2016 dollars) and log original mortgage amount / 0.28 (lender DTI rule), missing parts
   handled by using the available components; households are ranked on the index within ZIP and assigned to the SOI AGI
   bins of their ZIP (2016, all returns, N1 by `agi_stub`) by quantile matching, so the ZIP distribution of imputed bins
   equals the SOI distribution by construction; the point estimate is the bin's ZIP-level mean AGI (A00100/N1) scaled by
   the household's within-bin rank (linear between bin bounds). Known bias: homeowners are richer than all filers in
   their ZIP; a sensitivity that matches to the distribution of returns claiming the real-estate-tax deduction (N18500)
   is run for Strategy 6's validation only.
7. Descriptives: counts by state and year, distributions of bill16 and rate16, share above16 by state.

### Inference
Standard errors clustered at the taxing jurisdiction: county × tax area code (`tax_area_code`, fallback
`tax_rate_area_code`, fallback municipality, fallback county). County clustering reported as robustness.

### Strategy pre-specifications (Sections 4–13)
Each strategy is run exactly as below; the `spec_id` in `estimates.csv` refers to these labels.

**S1 (area DiD, ZIP level).** `E_z` = 2016 share of returns whose SALT exceeds $10k, approximated from AGI-bin averages:
within each ZIP × bin, SALT per return claiming taxes paid = A18300/N18300; the share above $10k in the bin is taken from a
log-normal with that mean and the national within-bin log-SD of SALT estimated from the ACS-based imputation in
`data/database.db` (`acs_salt`); weighted by N18300/N1. Outcomes: ZIP exit rate, sale rate, median log price (parcel
sample collapsed to ZIP × year, base sample only). Spec `Y_zt = α_z + γ_{state×t} + Σ_k β_k E_z 1[t=k] + ε`, weights =
ZIP parcel counts, cluster county. Diagnostics: pre-period joint F; add ZIP mean AGI × year and report Δβ. `spec_id`: S1_base, S1_agi.

**S2 (household DiD on bill16).** `Y_it = α_i + γ_{county×t} + Σ_k β_k above16_i 1[t=k] + ε`, and with log(bill16).
Diagnostic: add log(value16) × year, report %Δβ (post-period average). `spec_id`: S2_above, S2_logbill, S2_above_val, S2_logbill_val.

**S3 (core).** `Y_it = α_i + γ_{CBSA×valuebin16×t} + δ_{county×t} + Σ_k β_k T_i 1[t=k] + ε`, T = rate16 (×100, i.e.
percentage points) and above16; valuebin16 = 50 quantile bins of value16 within CBSA (parcels outside a CBSA use the
state as the CBSA). Augmented: + imputed-AGI bin × year + mortint16 decile × year. Diagnostics: pre-trends; covariate
balance across rate16 terciles within value bins; Δβ under augmentation; value16 replaced by price1416 in the subsample
with 2014–2016 sales. `spec_id`: S3_rate, S3_above, S3_rate_aug, S3_above_aug, S3_rate_price, S3_above_price.

**S4 (owner vs investor DDD).** S3 specification on owner-occupied ∪ investor parcels, with `T × Owner × Post` and all
lower-order terms, fixed effects interacted with Owner. Outcomes sale, log_price, refi, to_rental (not exit). Diagnostics:
pre-period DDD event study; only parcels where the three occupancy signals agree. `spec_id`: S4_rate, S4_above, S4_agree.

**S5 (boundary DiD).** Parcels within 0.5 (and 0.25) miles of a municipal (TIGER place / county subdivision) or unified
school district boundary where the median rate16 differs across the border by ≥ 10% (relative). Border segment = the pair
of jurisdictions. S3 spec with segment × valuebin × year replacing CBSA × valuebin × year; cluster by segment.
Diagnostics: covariate balance and pre-trends across each border (pooled); household size and surname turnover across the
border in Data Axle. `spec_id`: S5_rate_050, S5_rate_025.

**S6 (simulated tax price).** TAXSIM 2017 vs 2018 law with 2016 inputs (imputed AGI as wages, filing status married if
Section 3 class = married/likely married else single, state, bill16, mortint16, state tax computed by TAXSIM);
dprice = ∂tax/∂proptax under 2018 law minus under 2017 law (finite difference of $100). Validation vs SOI 2016 and 2018 at
ZIP × AGI bin (itemisation share, SALT > $10k share); weak-identification flag if the ZIP itemisation correlation < 0.6.
Spec: S3 with dprice × Post, controls: AGI bin × year, mortint16 decile × year, filing status × year, state × year,
bill16 decile × year. Decomposition: 2018 law without the cap. Coarse/fine control sensitivity. `spec_id`: S6_base, S6_coarse, S6_fine, S6_nocap.

**S7 (kink at $10k, no-income-tax states).** TX, FL, WA, NV, SD, WY (TN, NH robustness); subsample with mortint16 ≥
$6,000 (likely itemiser under 2018 law with bill ≈ $10k) and full. Outcome change in slope at $10k: local linear
`rdrobust` with `deriv = 1` on bill16, MSE-optimal bandwidth, half and double; 2018–19 pooled minus 2014–16 pooled.
Diagnostics: pre-period kink ≈ 0; covariate kinks; density of the bill at $10k 2016 vs 2019 (bunching is reported as an
outcome); threshold shifted down by the IRS optional sales-tax table amount for the imputed AGI. `spec_id`: S7_diff, S7_h05, S7_h2, S7_salesadj.

**S8 (married vs unmarried co-owners DDD).** Two-owner parcels; `Y_it = α_i + γ_{CBSA×valuebin×t} + β 1[10k < bill16 ≤
20k] × Married × Post + lower-order terms`; comparison bills ≤ 10k and > 20k. Run with likely-married pooled with married
and excluded. Vesting changes of the same parcel reported as an outcome. `spec_id`: S8_pooled, S8_strict.

**S9a (CT/NJ revaluations).** Town revaluation years from the CT OPM schedule and NJ Division of Taxation lists; treatment
= revaluation in 2018–2020 moving the bill across $10k; comparison = towns revaluing in other years. S3 spec with
Reval × Post. **S9b (Prop 13).** CA parcels by purchase-year cohort within block group vs the same cohorts in comparison
states (WA, OR, NV, AZ, CO), triple difference. `spec_id`: S9a_base, S9b_base.

**S10 (2025 reversal).** Parameters verified against IRS guidance before running (recorded below when done). S3 and S7
with threshold $40k and post = 2025, if a full post year of outcomes exists (deeds through 2025 exist; Data Axle does not).

**S13 (capitalisation).** S3 with log_price; PV conversion at 4–7%; incumbents (bought ≤ 2015) vs post-2018 buyers,
descriptive. Conditioning on value16 does not remove capitalisation (stated in the report).


## 2026-10-16 — Data build results and two amendments made before any estimation

**Build run** (`sql/01_build_panel.sql`, job 29756342, 1% sample, 1 min): 1,160,857 single-family/condo parcels on the
2010–2025 rolls; 949,455 with a 2016 (or 2015) roll row and a positive bill; owner-occupied by the three-signal rule
737,680 (729,011 non-corporate); investor-owned 211,772.

**Crosswalk.** 2016 match rates among non-corporate parcels (building+unit key, same file year): single family 82%, condo
52% (unit numbers are missing or formatted differently for half of the condo records). All states ≥ 70% except HI (39%
unit level) and GU/VI; HI is dropped (listed in `table_match_rate_by_state_2016.csv`).

**Occupancy signals** (non-corporate parcels): all three present 230k; homestead absent but mailing = situs and surname
match 283k (non-homestead states); none of the three 163k (of which 92k have no Data Axle record at all in 2016).

**Exit validation (pre-specified definition, all families' surnames).** P(exit in t−1 or t | arm's-length sale in t):
58–63% for 2012–2015 but **45–52% for 2016–2020**; P(exit_t | sale) falls from ~50% to 22–35% after 2015. False-positive
rate 1–2% (single family). Reason (from the Data Axle exploration, §4.2/4.5c): from the 2016/2017 files onward Data Axle
carries many stale secondary family records, so a seller's surname keeps "appearing" at the address after the sale.
**Verdict: fails the 60% threshold in the post period → `sale` is the primary outcome and `exit` is secondary, as
pre-specified.** One labelled alternative is added (not a replacement): `exit_prim`, the same rule applied to
primary-family surnames only; its validation is reported next to `exit` and it is used only in clearly labelled rows.

**Amendment 1 (value16 ratio cell).** CoreLogic's calculated value is the assessed value in many jurisdictions (median
value/price 0.09 LA, 0.29 DE, 0.30 IL, 0.44 MI, 0.56 WV, 0.68 CT, 0.71 CA/AZ, 0.73 NY). In NY, IL, CT and others the
assessment ratio is set by town/tax area, not county, so the sales ratio is now computed at county × tax-area level when
the cell has ≥ 30 sales in 2015–2017, falling back to the county, then to no adjustment (`ratio_level` records which).
Thresholds unchanged. On the 1% sample many small counties have < 30 sales (`no_ratio` share in `diag_by_state.csv`);
this resolves on the full data. In California the calculated value is the Prop 13 assessed value, which understates
market value for long-tenured owners by construction; the price1416 robustness (S3_*_price) is the check, and Strategy 9b
uses this feature deliberately.

**Amendment 2 (exit_prim).** As above; added to the panel as a separate column.

**Co-owner classes** (owner-occupied, non-corporate): single owner 51.0%, likely married (shared surname) 27.5%, married
(vesting/relationship code) 14.3%, other 4.7%, unmarried 2.6%.

## 2026-10-16 — Estimation details fixed before running Strategies 1–3

- **log_price specification.** With parcel fixed effects, `log_price` is identified only from repeat arm's-length sales,
  which are too few on the 1% sample (most base parcels sell at most once in 2012–2019). Pre-specified implementation for
  all strategies: for `log_price` the parcel fixed effect is replaced by hedonic controls — log(value16), year built,
  log living area, log lot size (missing values in their own category), condo indicator — with the same group × year fixed
  effects as the outcome in question. A repeat-sales version with parcel fixed effects is reported as a robustness row
  (`spec_id` suffix `_rs`).
- **Treatment scaling.** `rate16_pp` = 100 × rate16 (effect per percentage point of effective tax rate); `above16` in
  {0,1}; `lbill16` = log(bill16).
- **Post-period summary.** Each event-study row in `estimates.csv` is the average of the 2018 and 2019 coefficients (the
  main window), with the delta-method SE; the extended window (2020–2024) is a separate row with suffix `_ext`. Year-by-year
  coefficients are saved next to each figure as CSV.
- **Pre-trend test.** Joint Wald test that the 2012–2015 coefficients are zero (2016 reference, 2017 excluded); verdict
  pass if p ≥ 0.05, fail if p < 0.05 and the pre-coefficients are economically non-trivial (> 25% of the post effect),
  inconclusive otherwise.
- **Balance (S3).** rate16 terciles within CBSA × value bin; each 2016 covariate is regressed on tercile indicators with
  bin fixed effects; the T3 − T1 difference is reported in standard-deviation units; verdict pass if all |diff| < 0.1 SD.
- **Clusters.** `juris` = county × tax area code (fallback municipality code, then county alone).
- **Sample filters at estimation.** `rate_ok` (0.05%–10% effective rate), value bin available, base sample, states passing
  the match screen. Parcel-years off the roll keep deed outcomes (sale, price, refi) but have roll outcomes missing.

## 2026-10-16 — Exit validation after the rebuild (`table_exit_validation.csv`)

| Definition | P(exit in t−1 or t | sale in t), single family | false-positive rate | annual exit rate |
|---|---|---|---|
| `exit` (pre-specified: all families' surnames) | 58–63% (2012–15), 45–51% (2016–20) | 1.0–2.3% | 3.2–7.4% |
| `exit_prim` (labelled alternative: primary family only) | 80–87% every year | 4.1–8.7% | 8.0–17% |

`exit` stays the pre-specified secondary outcome; `exit_prim` is reported in rows labelled `exit_prim`. Its annual rate
(8–11% for 2012–2020) is above owner turnover in the roll (5–6%), consistent with the Data Axle re-keying documented in the
exploration notes (§4.5c): some "exits" are record churn, which is why the false-positive rate is 4–5%.

## 2026-10-16 — Strategy 10 parameters (verified before running)

Public Law 119-21 (One Big Beautiful Bill Act, §70120), IRC §164(b)(7), confirmed against the Congressional Research
Service report R48611 and the IRS "Working Families Tax Cuts" guidance transcript and Rev. Proc. 2025-32:
- Tax year 2025: SALT cap $40,000 ($20,000 married filing separately); reduced by 30% of modified AGI above $500,000
  ($250,000 MFS), not below $10,000; fully phased down to $10,000 at MAGI ≥ $600,000.
- Tax year 2026: $40,400 cap and $505,000 threshold (1% indexing), floor $10,000; 2027–2029 indexed likewise; reverts to
  $10,000 in 2030.
- Implementation: S3 rerun with post = 2025 and the main 2016 exposure variables unchanged (incumbent owners), plus a
  version using the 2024 bill (`bill24`) with `above24_40k` = 1[bill24 > 40,000] as the reversal treatment; S7 kink moved
  to $40,000 in TX/FL/WA/NV/SD/WY using 2025 outcomes; MAGI phase-down as an additional threshold at imputed AGI $500k.
- Outcomes available for 2025: sale and log_price from deeds (through 2026-Q1 in the sample), refi/cash-out from
  mortgages, roll outcomes for tax year 2025. Data Axle ends in 2023, so exit is not available.

Sources: congress.gov CRS R48611; irs.gov newsroom transcript "Understanding the Working Families Tax Cuts"; IRS
Rev. Proc. 2025-32 (p6079).

## 2026-10-16 — Bug fix in the data build (before Strategy 3 estimation)

`mortint16` was computed with an amortisation helper whose `ifelse()` truncated the 12 monthly balances to one month
(median 2016 interest among parcels with a loan came out at $540). Fixed in `R/01_prepare.R`; the Strategy 3 run that had
started with the wrong deciles was discarded and restarted. Strategies 1 and 2 do not use `mortint16` and stand.
Mortgage coverage note: 63% of base parcels have a first-lien loan recorded since 2000, but coverage is far lower in VT (0%),
SD (1%), MS (12%), WV (14%), CT (19%) and RI (24%), where CoreLogic's mortgage feed is thin; `mortint16 = 0` there mostly
means "unobserved", which the augmented S3 specification absorbs only as a decile-0 × year effect.

## 2026-10-16 — Diagnostic failure in the rate16 designs and the one labelled alternative (written before running it)

Strategies 3 and 5 with the continuous treatment `rate16` fail pre-trends with the same inverted-V pattern around the
2016 reference year (sale coefficients rise to +0.6/+1.4 pp in 2015 and fall to −1.5/−2.5 pp in 2018–19), and both fail
balance on tenure and owner age (high-rate homes have owners 0.2–0.3 SD younger with 0.2–0.3 SD shorter tenure). The
mechanism is mechanical: where assessments reset at sale (CA by law; many other jurisdictions in practice), a home that
sold in 2015–16 carries a higher 2016 bill relative to its calculated value than an identical long-held home, so `rate16`
partly indexes "recently bought"; recently bought homes have a sale in the pre-period by construction and a low sale
hazard afterwards. The pre-specified estimates are kept and reported with their failed verdicts.
**Labelled alternative (the single one allowed): `S3_rate_inc` / `S3_above_inc`** — the Strategy 3 base specification on
incumbents only, defined as base parcels with no arm's-length purchase recorded between 2013-01-01 and 2016-12-31
(`buy_year` < 2013 or no purchase deed since 2000). Outcomes sale, exit_prim, refi, log_price. Everything else unchanged.
If the alternative still fails pre-trends, the rate16 results are reported as not identified.

**Result of the labelled alternative (same day, after running it).** Among incumbents the rate16 sale coefficient flips
sign (+0.0038 (0.0005) per pp, vs −0.0158 in the full sample), exit_prim is −0.0031 (0.0010) and refi −0.0020
(0.0005); pre-trend tests still reject (sale: mean |pre| 0.0025 vs post 0.0038 → fail). But the alternative is partly
degenerate: conditioning on no 2013–16 purchase removes every 2013–16 arm's-length sale, so the 2013–15 sale
coefficients are zero by construction and the price outcome cannot be estimated (dropped). The alternative is therefore
reported only as evidence that the pre-specified rate16 estimate is contaminated by sale-based reassessment, not as a
clean estimate; the rate16 designs are reported as not identified on this sample. The binary above16 treatment, whose
pre-trends pass, is the Strategy 3 headline. No further alternatives are run (rule 4).

## 2026-10-16 — Strategy 6 decomposition: code correction (not a specification change)

The decomposition into "cap" and "larger standard deduction" originally built the no-cap 2018 counterfactual price as
1 − federal marginal rate × 1[itemises with uncapped SALT], dropping the state-tax component that TAXSIM includes in
the 2017 and 2018 prices; the state part therefore appeared in the cap term with a negative sign (negative "cap" effect
in AGI bins 1–4). Corrected in `R/s6_decomp_fix.R`: price18_nocap = 1 + Δsiitax(2018)/100 − mtr_fed(2018) × 1[itemises
with uncapped SALT]; d_std = price18_nocap − price17, d_cap = price18 − price18_nocap. Only the decomposition table and the
two `S6_d_cap` / `S6_d_std` rows are replaced; all other Strategy 6 outputs are unchanged.
