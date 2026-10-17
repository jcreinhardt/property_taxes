# The 2018 SALT cap and homeowners: evidence from parcel-level tax rolls, deeds and household records

*Pre-registered analysis; 1% building sample of the CoreLogic/Data Axle linkage (725k owner-occupied parcels), with a
10% replication reported in Section 8. All code, the decision log and the full ledger are in `output/analysis/salt_cap/`.*

## 1. Summary

**Headline.** Across the pre-specified designs, we find no robust evidence that the $10,000 SALT cap caused exposed
homeowners to sell, move, refinance or convert their homes to rentals in 2018–19, and no evidence of a price
(capitalisation) effect in repeat sales. The one estimate that survives its own diagnostics in the core design points
the "wrong" way: homes with a 2016 bill above $10,000 were **0.76 pp (SE 0.26) less likely to sell** in 2018–19 than
equally valued homes in the same metro area (base rate ≈ 5%), with flat pre-trends on the 1% sample — but that estimate drops to +0.10 pp (0.27) once
imputed-income and mortgage-interest bins × year are added, and on the 10% sample it shrinks to −0.28 pp (0.08) with
pre-period coefficients of +0.2 to +0.7 pp that fail the magnitude rule (2016 is the peak of a rising pre-trend). It is
therefore not robust either. The preferred strategy is Strategy 3 with the binary treatment (bill above $10k within CBSA × value
bin), supported by Strategy 4 (investor-owned homes as untreated controls: owner-specific effects are imprecise zeros,
sale −0.6 pp (0.9)) and Strategy 8 (married vs unmarried co-owners: +2.7 pp (2.1), zero within noise). Confidence that the *true*
sale/exit response in 2018–19 was small (less than about 1 pp in absolute value, i.e. under 20% of the base rate) is
moderate and rests mainly on the binary contrasts of Strategies 3, 4 and 8 on the 10% sample; confidence in any specific
non-zero number is low.

**Why the continuous designs fail.** Every design that uses the 2016 effective tax rate as the treatment (Strategies
3, 4, 5, 6 and the rate version of 10) fails its pre-trend test with the same signature: an inverted V around the 2016
reference year and a 0.2–0.3 SD imbalance in owner tenure and age across rate terciles. Where assessments reset at sale,
a home bought in 2015–16 carries a higher bill relative to its calculated value than an identical long-held home, so the
rate variable partly indexes "recently bought"; recent buyers have a sale in the pre-period by construction and a low
re-sale hazard afterwards. The single labelled alternative (incumbents only) flips the rate coefficient on sales from
−1.6 pp to +0.4 pp per percentage point of rate, confirming the contamination, but is itself degenerate for sales (it
removes all 2013–16 sales) and still fails the pre-trend rule. We therefore report the rate-based estimates as **not
identified** in this data, and the binary-exposure estimates as the evidence.

**Other outcomes.** The kink at $10k in states without an income tax (Strategy 7) is zero with wide intervals on 1%
(sale slope change +0.014 (0.035) per $1k) and there is no bunching of bills below $10k in 2019. Revaluation-driven
bill shocks in CT/NJ (Strategy 9a) affect 192 parcels on 1% and are uninformative. California Prop 13 cohorts (9b)
give a +2.7 pp (0.7) sale effect for recent cohorts, but with a failing pre-trend. The 2025 increase of the cap to
$40k (Strategy 10) shows no sign of a reversal in 2025: the above16 sale coefficient in 2025 (−0.84 pp (0.29)) equals
the 2018–19 one, which argues against reading the 2018–19 sale decline as a cap response at all.

## 2. Data and sample

**Sources.** CoreLogic tax rolls (2010–2025), deeds (arm's-length transfers to 2026-Q1) and mortgages, linked by parcel id
(`clip`); Data Axle consumer file 2010–2023 (2022 file truncated and dropped) linked by a standardized address key
(ZIP5 + street name + house number + unit); IRS SOI ZIP statistics 2011–2022; Census TIGER 2017 places and unified school
districts; NBER TAXSIM 35; Freddie Mac PMMS. All results in this report use the **1% building sample** (every address whose
hashed building key falls in 1% of the key space; the same buildings in all four CoreLogic/Data Axle sources). A 10%
sample was built on the cluster with identical code and most strategies were replicated on it; see Section 8.

**Crosswalk.** Among non-corporate single-family and condo parcels on the 2016 roll, a Data Axle record exists at the
same address in the 2016 file for 82% of single-family parcels (building level) and 52% of condos (unit level; unit
numbers are missing or formatted differently for half of the condo records). Match rates by state and year are in
`table_match_rate_by_state_year.csv`. Measured on the base sample (owner-occupied by the two non-Data-Axle signals), all
states are at or above 79% except Hawaii (40%), which is dropped together with GU and VI. The all-parcel rate (including
absentee owners) is 62–69% in AK and VT and 71–73% in MT, WV, NM, WY, MS.

**Owner-occupancy in 2016.** The three signals (homestead flag; mailing address = parcel; owner surname among the Data
Axle surnames at the address) agree on "occupied" for 230k parcels (26% of non-corporate parcels), and mailing + surname
agree without a homestead flag for a further 283k (32%; homestead flags are delivered only in some states). 10% of
parcels show none of the three signals and have no Data Axle record at all. Base sample: **725,369 owner-occupied**
parcels (single family 92%, condo 8%); **161,153 investor-owned** parcels for Strategy 4.

**Exit validation.** The pre-specified exit measure (no surname at the address in file t reappears in t+1 or t+2)
flags 58–63% of arm's-length sales in 2012–15 but only 45–51% from 2016 on, below the 60% threshold: Data Axle carries
stale secondary family records from the 2016/17 files onward, so a seller's surname persists after the sale. Its
false-positive rate (exits with no sale and an unchanged owner on the roll) is 1–2%. **`sale` is therefore the primary
outcome and `exit` secondary.** One labelled alternative, `exit_prim` (primary-family surnames only), flags 80–87% of sales
in every year with a 4–6% false-positive rate and an annual rate of 8–11%, above roll owner turnover (5–6%); the gap is
Data Axle re-keying, documented in the project's Data Axle notes.

**Exposure.** Median 2016 bill $2,334 (p10 $527, p90 $7,286, p99 $17,784); median effective rate 1.22% of market value
(p10 0.59%, p90 2.78%); 4.8% of base parcels have a bill above $10,000 (NJ 29%, NY 20%, CT 12.5%, IL 9.5%, CA 8.5%,
TX 7.7%, MA/NH 7%). 98% of parcels pass the 0.05–10% rate screen. The county/tax-area sales ratio adjusts the calculated
value for 47% of parcels (assessed values at fractional ratios in NY, IL, CT, LA, DE, MI, WV and others). Imputed 2016 AGI
has median $44k and reproduces the SOI all-returns distribution of its ZIP by construction (bin shares 36/24/14/9/13/5%),
so it understates homeowner incomes; 63% of parcels have a first-lien mortgage recorded since 2000 (median 2016 interest
$5,800 among them; coverage is near zero in VT and SD and under 25% in MS, WV, CT, RI). Two-owner parcels: 27.6% likely
married (shared surname), 14.4% married by vesting or relationship code, 2.6% unmarried, 4.7% other.

**Panel.** 15.2m parcel-years (2010–2025); the main window 2012–2019 has 5.7m owner-occupied parcel-years from 712k
parcels after the rate screen. Parcel-years on the roll by year run from 620k (2012) to 714k (2016) and 621k (2025).

## 3. Results by strategy

### Strategy 1 — ZIP-level exposure DiD (descriptive benchmark)
Exposure E_z is the 2016 share of returns with SALT above $10k, approximated within AGI bin from the SOI mean SALT per
return and a log-normal within-bin dispersion estimated from the ACS imputation (σ = 0.76–1.36 by bin). 10,367 ZIPs with
≥ 20 base parcels, 82,936 ZIP-years, ZIP and state × year fixed effects, parcel-count weights, county clusters. The
post-period (2018–19) coefficients are −0.013 (0.006) for the sale rate, 0.000 (0.004) for the exit rate and −0.31
(0.07) for the median log price per unit of E_z. **Identifying assumption:** parallel trends between high- and
low-exposure ZIPs. **Diagnostic:** the joint test of the 2012–2015 coefficients rejects for all three outcomes
(p ≤ 0.003), and adding ZIP mean AGI × year flips the sign of every estimate. Verdict: fail; the strategy is reported as
a benchmark only. Figure: `figures/es_S1_sale_rate_S1_base.png`.

### Strategy 2 — Household DiD on the 2016 bill
Parcel and county × year fixed effects; 5.70M parcel-years. With `above16` the post-period effects are +0.0017 (0.0016)
on sale, −0.0001 (0.0013) on exit, +0.14 (0.02) on log price, −0.012 (0.002) on refinancing, −0.005 (0.001) on cash-out,
−0.002 (0.001) on conversion to rental and +0.0025 (0.0010) on the appeal proxy. **Assumptions:** parallel trends within
county and a bill that does not proxy for wealth. **Diagnostics:** pre-trends reject for every outcome except
`to_rental` (p = 0.15); adding log(value16) × year moves the sale coefficient by −99% and the refinancing and cash-out
coefficients by 40–60%. Verdict: fail on the wealth-proxy assumption, which is the motivation for Strategy 3. The 10%
sample gives the same picture (sale +0.0032 (0.0006), −74% with value × year).

### Strategy 3 — Bill conditional on home value (core design)
CBSA × 50 value-bin × year and county × year fixed effects, parcel fixed effects, jurisdiction clusters (48,932);
5.64M parcel-years. Two treatments: the 2016 effective rate in percentage points and the indicator for a bill above
$10k. Post-period (2018–19) effects:

| Outcome | rate16 (per pp) | augmented | above16 | augmented |
|---|---|---|---|---|
| sale | −0.0158 (0.0011) | −0.0103 (0.0011) | −0.0076 (0.0026) | +0.0010 (0.0027) |
| exit (pre-specified) | −0.0034 (0.0007) | −0.0021 (0.0007) | −0.0028 (0.0022) | −0.0003 (0.0022) |
| exit_prim (alternative) | −0.0067 (0.0010) | −0.0049 (0.0011) | −0.0033 (0.0031) | −0.0002 (0.0031) |
| log price (hedonic) | +0.052 (0.008) | +0.049 (0.008) | +0.095 (0.017) | +0.087 (0.017) |
| log price (repeat sales) | −0.022 (0.039) | | +0.010 (0.060) | |
| refi | −0.0016 (0.0005) | +0.0014 (0.0006) | +0.0023 (0.0029) | +0.0008 (0.0029) |
| cash-out | +0.0002 (0.0003) | +0.0008 (0.0003) | +0.0004 (0.0018) | +0.0014 (0.0018) |
| to rental | −0.0080 (0.0007) | −0.0079 (0.0007) | −0.0015 (0.0012) | −0.0013 (0.0013) |
| appeal proxy | +0.0004 (0.0005) | +0.0006 (0.0005) | +0.0004 (0.0012) | +0.0005 (0.0012) |

The 10% sample reproduces the sale coefficient on rate16 (−0.0162 (0.0010), augmented −0.0097). County clustering
roughly doubles the rate16 standard errors (sale: 0.0020) and leaves the above16 ones unchanged.
**Assumptions and diagnostics.** (1) Given value, millage is unrelated to post-2018 trends: the rate16 event studies
show large and erratic pre-period coefficients (sale: −1.2, −0.8, −0.5, +0.6 pp in 2012–15 against −1.5/−1.6 pp in
2018–19), so the joint test rejects (p < 0.001) and the mean absolute pre-coefficient is about half the post effect →
fail. The above16 event studies are flat before 2017 (sale: +0.3 to +0.4 pp, p = 0.75) → pass for sale, exit, refi,
cash-out and to_rental; fail for log price (pre-coefficients +0.03 to +0.06, i.e. 2016 is a low reference year and
there is no post change relative to 2012–15) and appeal proxy. Covariate balance across rate16 terciles within value
bins fails: higher-rate homes have younger owners (−0.27 SD) with shorter tenure (−0.27 SD), slightly larger living
area (+0.18 SD) and higher imputed AGI (+0.14 SD); lot size and mortgage interest are balanced. (2) Value bins absorb
the other TCJA provisions: adding imputed-AGI bin × year and mortgage-interest decile × year moves the rate16 sale and
exit coefficients by 28–38% and the above16 sale and exit coefficients by 88–113% (to zero) → fail for sale, exit and
refi, pass for price and to_rental. (3) Local public goods: untestable. (4) value16 is measured well: in the subsample
with a 2014–16 arm's-length sale (683k parcel-years) the estimates change sign when value bins are formed from the sale
price instead of the assessor value (above16 on sale: −0.011 (0.014) vs +0.192 (0.016)); this subsample is itself
selected (recent buyers), but the sensitivity means the assessor-value bins do not pin down "same value" → fail.
**Reading.** The only Strategy 3 result that survives its own diagnostics is the above16 effect on sales under the base
controls: homes with a 2016 bill above $10k were 0.76 pp (about 15% of a 5% base rate) less likely to sell in 2018–19,
starting in the 2017 transition year — and that estimate disappears once income and mortgage-interest bins × year are
added. No price effect is found in repeat sales. Extended window (2020–24, COVID-confounded): sale −0.0181 (0.0011) per
pp and −0.0074 (0.0023) for above16; log price +0.065 (0.006) and +0.10 (0.01) in the hedonic version.

### Strategy 7 — Kink at $10k in states without an income tax
TX, FL, WA, NV, SD, WY; local-linear slope change at a $10k bill (`rdrobust`, deriv = 1, MSE-optimal bandwidth
≈ $2–3k), 2018–19 minus 2014–16; about 44k parcel-years within the bandwidth on the 1% sample. Difference in kinks per
$1,000 of bill: sale +0.014 (0.035), log price −0.30 (0.56), exit_prim −0.001 (0.054), refi −0.003 (0.029); half and
double bandwidths, the likely-itemiser subsample (mortint16 ≥ $6k), adding TN/NH, the full sample, and the sales-tax-
adjusted threshold all give estimates indistinguishable from zero. **Diagnostics:** pre-period kinks are zero for sale,
exit and refi but +0.61 (0.28) for log price → fail (price only); covariate kinks pass; the bill density shows no excess
mass below $10k in 2016 or 2019 (excess −0.03 and −0.04, no bunching) → no evidence of appeals-driven manipulation.
The design has no power on 1%; it is a full-data item.

### Strategy 9a — Connecticut and New Jersey revaluations
Revaluation years from the NJ Division of Taxation approved lists 2017–22 (204 of 551 NJ roll municipalities matched
by county and name) and, for CT, from the roll itself (town-years in which ≥ 20% of parcels change assessed value by
more than 3%), because the published CT schedule post-dates the 2022 re-zoning. Treated: 99 towns revalued in 2018–20
(5,370 base parcels with 2016 bills of $5–20k), of which only 192 parcels crossed $10k at the revaluation; comparison:
114 towns revaluing at other times. Town-level revaluation × post: sale −0.002 (0.005), log price +0.04 (0.04),
exit_prim −0.007 (0.006), refi +0.009 (0.007), all with passing pre-trends. The crossing estimates (sale +0.001 (0.018))
are uninformative at this sample size. **Assumption:** revaluation timing exogenous to 2018: NJ list years agree with
roll-based revaluation years for 171 town-years, with 151 roll-only and 56 list-only → inconclusive (list names and
roll names only partly match; CT cannot be checked against a pre-2017 schedule).

### Strategy 9b — California Prop 13 cohorts
Purchase-year cohorts (pre-2001, 2001–07, 2008–12, 2013–16) within block group, CA vs WA/OR/NV/AZ/CO, 465k
parcel-years. Note that the roll's calculated value in CA is the Prop 13 assessed value, so the bill/value ratio does
not vary by cohort (0.86–0.88% in every cohort); the cohort contrast is the treatment. Recent cohorts (2008–16) × CA ×
post: sale +0.027 (0.007), refi −0.024 (0.008); the price version is not identified (singleton cells). **Diagnostic:**
pre-period cohort trends differ between CA and the control states (sale p = 0.002, with a 2012 coefficient as large as
the post effect; refi p = 0.049) → fail.

### Strategy 4 — Owner-occupied vs investor-owned (triple difference)
712k owner-occupied and 159k investor-owned parcels (6.97M parcel-years); Strategy 3 fixed effects interacted with
owner status; coefficient = T × Owner × Post. With above16: sale −0.60 pp (0.95), log price +0.03 (0.06), refi −0.9 pp
(0.8), to_rental −0.4 pp (0.9); all four pre-trend tests pass (p = 0.26, 0.07, 0.86, 0.74). With rate16: sale −1.9 pp
(0.2), refi −0.3 pp (0.1), to_rental −0.6 pp (0.1), log price −0.01 (0.02); pre-trends fail for sale, refi and
to_rental (the recency pattern again). **Assumptions:** (1) the bill gradient would have moved alike for owners and
investors absent the cap — pass for the binary treatment, fail for the continuous one; (2) occupancy misclassification
unrelated to the bill — restricting owners to parcels where all three occupancy signals agree (223k parcels) leaves the
above16 estimates within two SE (sale +0.4 pp (1.0)), while the rate16 sale estimate shrinks to −0.2 pp (0.2) → fail
for rate, pass for above16; (3) classification fixed at 2016, with `to_rental` as an outcome: no differential
conversion. **Reading:** the investor comparison removes the Strategy 3 sale decline for high-bill homes (−0.76 pp
becomes −0.60 pp with a 0.95 pp SE), i.e. investor-owned high-bill homes sold less too; what remains is an imprecise
zero.

### Strategy 5 — Boundary DiD
135k parcels within 0.5 miles of a municipal or unified-school-district border where the median effective rate differs
by ≥ 10% (8,662 segments; 75k parcels and 7,499 segments at 0.25 miles); segment × value-decile × year fixed effects,
segment clusters, rate16 treatment. Sale −2.5 pp (0.3) per pp at both bands, exit_prim −1.6 pp (0.3), refi −0.5 pp
(0.2), log price +0.06 (0.02). **Diagnostics:** pre-trends fail for sale, exit and refi (p < 0.01; sale coefficients
−1.7, −1.1, −0.4, +1.4 pp in 2012–15, the inverted V); covariate balance across the border fails on tenure (−0.20 SD)
and owner age (−0.21 SD) while year built, size, lot, value and imputed AGI are balanced (|diff| ≤ 0.07 SD); household
size and surname turnover gaps across the border move by < 0.02 pre vs post (inconclusive); services and spillovers
untestable. The boundary design shares the recency contamination of the rate treatment and is not identified here.

### Strategy 6 — Simulated tax-price change (TAXSIM)
TAXSIM 35 run for 724k households under 2017 and 2018 law with 2016 inputs (imputed AGI as wages, filing status from
the co-owner class, state, bill16, mortint16). The after-tax price of a marginal dollar of property tax rises by 0.066
on average; 39% of households see a rise above 0.01 (by AGI bin: 0.01 for AGI < $25k, 0.19 for $100–200k, 0.13 above
$200k where the AMT already limited the deduction). **Validation:** predicted 2016 itemisation shares correlate 0.80
with SOI across ZIPs (0.80 in 2018); the mean error is +0.11 in 2016 and +0.04 in 2018, as expected when comparing
homeowners with all filers → pass; the strategy is not flagged as weakly identified. **Estimates** (S3 spec with
dprice × year and AGI-bin, mortgage-decile, filing-status and bill-decile × year controls): sale −4.8 pp (0.4) per unit
of dprice (i.e. −0.3 pp for the mean change of 0.066), exit_prim −2.0 pp (0.5), refi −0.2 pp (0.4), log price +0.01
(0.02). Coarse and fine control sets give sale −6.9 and −3.9 pp, so the functional-form check fails for sale and passes
for price and exit. **Diagnostics:** pre-trends fail for sale, exit and price (p ≤ 0.006) and pass for refi; imputation
error untestable. Because dprice is a nonlinear function of the bill, the same recency contamination applies; the sale
coefficients are −4.6, −4.3, −4.1 and −3.0 pp in 2012–15 against −5.2 and −4.4 pp in 2018–19, so the "effect" is a
2016 level anomaly rather than a 2018 break. **Decomposition** (corrected, see the decision log): below $100k of AGI
the tax-price increase comes almost entirely from the larger standard deduction (0.01–0.07 of the 0.01–0.09 total; the
cap adds 0.001–0.014); at $100–200k the two parts are comparable (0.10 and 0.09); above $200k the cap adds 0.27 while
AMT relief subtracts 0.14. In the regression both parts carry the same (contaminated) negative sale coefficient (cap
−6.9 pp (0.5), standard deduction −4.6 pp (0.4) per unit of price change) and neither moves prices (+0.02 (0.02) and
+0.01 (0.02)).

### Strategy 8 — Married vs unmarried co-owners
Two-owner parcels: 299k married or likely married and 18.7k unmarried (pooled), 103k and 18.7k (strict); 15.2k with
bills in ($10k, $20k]. Coefficient = mid-bill × married × post, CBSA × value-bin × married × year fixed effects. Pooled:
sale +2.7 pp (2.1), log price +0.26 (0.11), exit_prim +2.6 pp (3.0), refi −1.1 pp (2.2); strict: +2.9 (2.2), +0.23
(0.12), +1.8 (3.0), −0.4 (2.3). All eight pre-trend tests pass (p ≥ 0.29). **Diagnostics:** balance fails — married
co-owners are 0.3–0.5 SD older and 0.5–0.6 SD longer-tenured than unmarried ones within value bin × bill group (value and
imputed AGI balanced); vesting changes of the same parcel (roll relationship code changing without an owner change) fell
from 15% to 10% a year for unmarried mid-bill co-owners after 2017, but by the same amount for unmarried co-owners with
bills below $10k (16% to 11%), so recorded marital status did not respond differentially; misclassification
inconclusive; the lower-bound caveat (whether unmarried co-owners actually split the deduction) is untestable.
**Reading:** the design has power for effects of about ±4 pp on sales; nothing of that size is there. The price
coefficient is a noisy composition effect on 58k sales with a 0.11 SE.

### Strategy 10 — The 2025 reversal (cap raised to $40k)
Deeds cover 2025 fully (30,136 arm's-length sales in the sample vs 30,421 in 2024 → pass). With the 2016 exposure
variables and post = 2025: above16 sale −0.84 pp (0.29) with passing pre-trends (p = 0.75), refi −0.04 pp (0.28),
log price +0.12 (0.03, failing pre-trends); rate16 sale −2.0 pp (0.1), failing pre-trends. Using the 2024 bill, the
newly deductible range ($10k–$40k) shows sale −2.5 pp (0.3) with failing pre-trends; bills above $40k (1,380 parcels)
show sale −1.6 pp (0.9), refi +0.1 pp (0.9). The kink at $40k in no-income-tax states has 1,459 parcel-years within
the bandwidth: slope change −0.05 (0.09) per $1k; price not estimable. Imputed-AGI ≥ $500k owners with bills above
$10k sold at 3.5% in 2025 vs 3.0% for $400–500k owners (2.9% vs 3.5% in 2023). **Reading:** none of the 2025
coefficients moves toward zero relative to 2018–19; if the 2018–19 sale decline for above-$10k homes were a cap
response, the $40k cap should have begun to reverse it. Symmetry and the absence of other 2025 shocks are untestable.

## 4. Comparison table

Headline specification per strategy (1% sample). Cells: post-period coefficient (SE); for Strategies 1–3 and 10 the coefficient is the 2018–19 (2025) average effect per unit of treatment; S4/S8/S9b are triple-difference coefficients; S7 is the change in slope per $1,000 of bill. Verdict counts summarise the ledger rows of each strategy.

| strategy | spec_id | sale | log_price | exit | exit_prim | refi | cashout | to_rental | appeal_proxy | exit_rate | med_log_price | sale_rate | N | clusters | verdicts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| S1 | S1_base |  |  |  |  |  |  |  |  | 0.0003736 (0.0045) | -0.3118 (0.072) | -0.01329 (0.0056) | 82,936 |  1963 | 0 pass / 3 fail / 0 inconcl. / 4 untestable |
| S10 | S10_rate16_pp | -0.02021 (0.0012) | 0.06537 (0.011) |  |  | -0.0028 (0.00056) | 7.017e-05 (0.00036) |  |  |  |  |  | 9,876,524 | 48932 | 1 pass / 0 fail / 0 inconcl. / 2 untestable |
| S13 | S13_incumbent | 0.00249 (0.00063) |  |  |  | 0.00173 (0.0011) |  |  |  |  |  |  | 3,209,650 | 33961 | 0 pass / 0 fail / 0 inconcl. / 1 untestable |
| S2 | S2_above | 0.001709 (0.0016) | 0.1398 (0.019) | -0.0001227 (0.0013) | 0.0001606 (0.002) | -0.01245 (0.0019) | -0.004775 (0.0012) | -0.002019 (0.0009) | 0.002467 (0.00099) |  |  |  | 5,698,936 | 48932 | 5 pass / 27 fail / 0 inconcl. / 0 untestable |
| S3 | S3_above | -0.007607 (0.0026) | 0.09545 (0.017) | -0.002782 (0.0022) | -0.003268 (0.0031) | 0.002298 (0.0029) | 0.0004117 (0.0018) | -0.001484 (0.0012) | 0.000434 (0.0012) |  |  |  | 5,643,728 | 48932 | 17 pass / 30 fail / 0 inconcl. / 1 untestable |
| S3 | S3_rate | -0.01581 (0.0011) | 0.05228 (0.0079) | -0.003435 (0.00067) | -0.006749 (0.001) | -0.001638 (0.00051) | 0.0001552 (0.00032) | -0.008023 (0.00066) | 0.0004109 (0.00049) |  |  |  | 5,643,728 | 48932 | 17 pass / 30 fail / 0 inconcl. / 1 untestable |
| S4 | S4_rate | -0.01947 (0.002) | -0.01398 (0.02) |  |  | -0.003091 (0.0009) |  | -0.005535 (0.0014) |  |  |  |  | 6,821,464 | 53198 | 12 pass / 5 fail / 0 inconcl. / 0 untestable |
| S5 | S5_rate_050 | -0.02476 (0.0026) | 0.05642 (0.024) |  | -0.01603 (0.0033) | -0.005279 (0.0017) |  |  |  |  |  |  | 959,072 |  8662 | 1 pass / 4 fail / 1 inconcl. / 2 untestable |
| S6 | S6_base | -0.04805 (0.0043) | 0.009655 (0.02) |  | -0.02018 (0.0052) | -0.002297 (0.004) |  |  |  |  |  |  | 5,631,544 | 48728 | 4 pass / 5 fail / 0 inconcl. / 1 untestable |
| S7 | S7_notax | 0.01422 (0.035) | -0.3036 (0.56) |  | -0.001027 (0.054) | -0.002698 (0.029) |  |  |  |  |  |  | 49,601 | NA | 1 pass / 2 fail / 1 inconcl. / 0 untestable |
| S8 | S8_pooled | 0.02701 (0.021) | 0.2579 (0.11) |  | 0.02619 (0.03) | -0.01126 (0.022) |  |  |  |  |  |  | 2,417,392 | 37679 | 8 pass / 2 fail / 2 inconcl. / 1 untestable |
| S9a | S9a_cross | 0.0006714 (0.018) | 0.2271 (0.13) |  | -0.006193 (0.021) | 0.01652 (0.017) |  |  |  |  |  |  | 121,891 |   213 | 2 pass / 2 fail / 1 inconcl. / 0 untestable |
| S9b | S9b_base | 0.02716 (0.0066) |  |  |  | -0.02411 (0.0084) |  |  |  |  |  |  | 464,528 | 11207 | 0 pass / 2 fail / 0 inconcl. / 1 untestable |

**Do the strategies agree?** On the sale margin they split into two groups. The continuous-exposure designs (S3 rate,
S4 rate, S5, S6, S10 rate, S1) all show large negative sale effects — −1.6 to −4.8 pp — and all fail their pre-trend
tests with the same inverted-V shape; they also share the same imbalance in owner tenure and age. The binary or
group-contrast designs (S3 above16, S4 above16, S8, S9a town-level) show small effects (−0.8 to +2.9 pp) with passing
pre-trends and standard errors of 0.3–2 pp. We read the first group as one measurement problem (the 2016 rate indexes
recent purchases) expressed five times, not as five confirmations. Within the second group the point estimates differ
in sign but overlap within two SE, so they are consistent with a common effect near zero; the populations also differ
(S3 is national, S8 is two-owner households, S9a is CT/NJ towns with bills of $5–20k, S4 adds investors). On prices,
the hedonic regressions find positive coefficients that are present in every pre-year (a 2016 reference artefact), and
the repeat-sales and S4/S5 estimates are zero within noise. On refinancing, the S2/S3 continuous designs find small
negative effects that reverse sign under augmented controls; the binary designs find zero. Conversion to rental is
negative in every design that measures it (S2, S3, S4) but fails pre-trends in the continuous versions and is within
noise in the binary ones; its construction from the homestead flag and mailing address may also pick up owners
protecting their exemption rather than fewer conversions. The appeal proxy shows nothing in any design and there is no
bunching below $10k.

## 5. Assumption ledger

| strategy | assumption | test | result | verdict |
|---|---|---|---|---|
| S1 | Parallel trends high vs low exposure ZIPs [exit_rate] | Joint Wald test of 2012-2015 coefficients | F=4.71 p=0.001; post 0.0003736 (0.004453); mean|pre|=0.0152; mean|pre|=0.0152; mean|pre|=0.0152; mean|pre|=0.0 | fail |
| S1 | No other TCJA provision correlated with E_z [exit_rate] | Add ZIP mean AGI x year | beta 0.0003736 -> 0.009393 (2414%) | untestable |
| S1 | Parallel trends high vs low exposure ZIPs [sale_rate] | Joint Wald test of 2012-2015 coefficients | F=11.56 p=0.000; post -0.01329 (0.005561); mean|pre|=0.024; mean|pre|=0.024; mean|pre|=0.024; mean|pre|=0.024; | fail |
| S1 | No other TCJA provision correlated with E_z [sale_rate] | Add ZIP mean AGI x year | beta -0.01329 -> 0.01555 (217%) | untestable |
| S1 | Parallel trends high vs low exposure ZIPs [med_log_price] | Joint Wald test of 2012-2015 coefficients | F=4.09 p=0.003; post -0.3118 (0.07174); mean|pre|=0.266; mean|pre|=0.266; mean|pre|=0.266; mean|pre|=0.266; me | fail |
| S1 | No other TCJA provision correlated with E_z [med_log_price] | Add ZIP mean AGI x year | beta -0.3118 -> 0.06241 (120%) | untestable |
| S1 | No spillovers across ZIPs (SUTVA) | none | untestable; movers may relocate to nearby ZIPs | untestable |
| S2 | Parallel trends within county [sale, above16] | Joint Wald test of 2012-2015 coefficients | F=29.36 p=0.000; post 0.001709 (0.001568); mean|pre|=0.0224; mean|pre|=0.0224; mean|pre|=0.0224; mean|pre|=0.0 | fail |
| S2 | Bill does not proxy for wealth [sale, above16] | Add log(value16) x year; % change in beta | beta 0.001709 -> 1.404e-05 (-99%) | fail |
| S2 | Parallel trends within county [exit, above16] | Joint Wald test of 2012-2015 coefficients | F=6.08 p=0.000; post -0.0001227 (0.001346); mean|pre|=0.0069; mean|pre|=0.0069; mean|pre|=0.0069; mean|pre|=0. | fail |
| S2 | Bill does not proxy for wealth [exit, above16] | Add log(value16) x year; % change in beta | beta -0.0001227 -> 4.257e-05 (135%) | fail |
| S2 | Parallel trends within county [exit_prim, above16] | Joint Wald test of 2012-2015 coefficients | F=6.28 p=0.000; post 0.0001606 (0.001989); mean|pre|=0.0088; mean|pre|=0.0088; mean|pre|=0.0088; mean|pre|=0.0 | fail |
| S2 | Bill does not proxy for wealth [exit_prim, above16] | Add log(value16) x year; % change in beta | beta 0.0001606 -> 0.000149 (-7%) | pass |
| S2 | Parallel trends within county [log_price, above16] | Joint Wald test of 2012-2015 coefficients | F=97.79 p=0.000; post 0.1398 (0.01898); mean|pre|=0.292; mean|pre|=0.292; mean|pre|=0.292; mean|pre|=0.292; me | fail |
| S2 | Bill does not proxy for wealth [log_price, above16] | Add log(value16) x year; % change in beta | beta 0.1398 -> 0.1336 (-4%) | pass |
| S2 | Parallel trends within county [refi, above16] | Joint Wald test of 2012-2015 coefficients | F=60.65 p=0.000; post -0.01245 (0.001909); mean|pre|=0.0147; mean|pre|=0.0147; mean|pre|=0.0147; mean|pre|=0.0 | fail |
| S2 | Bill does not proxy for wealth [refi, above16] | Add log(value16) x year; % change in beta | beta -0.01245 -> -0.004765 (62%) | fail |
| S2 | Parallel trends within county [cashout, above16] | Joint Wald test of 2012-2015 coefficients | F=6.07 p=0.000; post -0.004775 (0.001185); mean|pre|=0.00274; mean|pre|=0.00274; mean|pre|=0.00274; mean|pre|= | fail |
| S2 | Bill does not proxy for wealth [cashout, above16] | Add log(value16) x year; % change in beta | beta -0.004775 -> -0.002115 (56%) | fail |
| S2 | Parallel trends within county [to_rental, above16] | Joint Wald test of 2012-2015 coefficients | F=1.70 p=0.146; post -0.002019 (0.0009023); mean|pre|=0.00146; mean|pre|=0.00146; mean|pre|=0.00146; mean|pre| | pass |
| S2 | Bill does not proxy for wealth [to_rental, above16] | Add log(value16) x year; % change in beta | beta -0.002019 -> -0.000793 (61%) | fail |
| S2 | Parallel trends within county [appeal_proxy, above16] | Joint Wald test of 2012-2015 coefficients | F=2.48 p=0.042; post 0.002467 (0.0009854); mean|pre|=0.00257; mean|pre|=0.00257; mean|pre|=0.00257; mean|pre|= | fail |
| S2 | Bill does not proxy for wealth [appeal_proxy, above16] | Add log(value16) x year; % change in beta | beta 0.002467 -> 0.0009697 (-61%) | fail |
| S2 | Parallel trends within county [sale, lbill16] | Joint Wald test of 2012-2015 coefficients | F=149.58 p=0.000; post 0.0007003 (0.0004206); mean|pre|=0.0101; mean|pre|=0.0101; mean|pre|=0.0101; mean|pre|= | fail |
| S2 | Bill does not proxy for wealth [sale, lbill16] | Add log(value16) x year; % change in beta | beta 0.0007003 -> 0.0022 (214%) | fail |
| S2 | Parallel trends within county [exit, lbill16] | Joint Wald test of 2012-2015 coefficients | F=31.16 p=0.000; post -0.001421 (0.0006688); mean|pre|=0.00461; mean|pre|=0.00461; mean|pre|=0.00461; mean|pre | fail |
| S2 | Bill does not proxy for wealth [exit, lbill16] | Add log(value16) x year; % change in beta | beta -0.001421 -> -0.0004424 (69%) | fail |
| S2 | Parallel trends within county [exit_prim, lbill16] | Joint Wald test of 2012-2015 coefficients | F=21.17 p=0.000; post -0.002282 (0.0007828); mean|pre|=0.00448; mean|pre|=0.00448; mean|pre|=0.00448; mean|pre | fail |
| S2 | Bill does not proxy for wealth [exit_prim, lbill16] | Add log(value16) x year; % change in beta | beta -0.002282 -> -0.0003973 (83%) | fail |
| S2 | Parallel trends within county [log_price, lbill16] | Joint Wald test of 2012-2015 coefficients | F=138.13 p=0.000; post 0.05404 (0.008554); mean|pre|=0.2; mean|pre|=0.2; mean|pre|=0.2; mean|pre|=0.2; mean|pr | fail |
| S2 | Bill does not proxy for wealth [log_price, lbill16] | Add log(value16) x year; % change in beta | beta 0.05404 -> 0.04407 (-18%) | pass |
| S2 | Parallel trends within county [refi, lbill16] | Joint Wald test of 2012-2015 coefficients | F=514.23 p=0.000; post -0.008551 (0.0003333); mean|pre|=0.00735; mean|pre|=0.00735; mean|pre|=0.00735; mean|pr | fail |
| S2 | Bill does not proxy for wealth [refi, lbill16] | Add log(value16) x year; % change in beta | beta -0.008551 -> -0.004831 (44%) | fail |
| S2 | Parallel trends within county [cashout, lbill16] | Joint Wald test of 2012-2015 coefficients | F=44.41 p=0.000; post -0.00161 (0.0002031); mean|pre|=0.00084; mean|pre|=0.00084; mean|pre|=0.00084; mean|pre| | fail |
| S2 | Bill does not proxy for wealth [cashout, lbill16] | Add log(value16) x year; % change in beta | beta -0.00161 -> -0.0009856 (39%) | fail |
| S2 | Parallel trends within county [to_rental, lbill16] | Joint Wald test of 2012-2015 coefficients | F=2.13 p=0.074; post -0.003123 (0.0002708); mean|pre|=0.000392; mean|pre|=0.000392; mean|pre|=0.000392; mean|p | pass |
| S2 | Bill does not proxy for wealth [to_rental, lbill16] | Add log(value16) x year; % change in beta | beta -0.003123 -> -0.0008967 (71%) | fail |
| S2 | Parallel trends within county [appeal_proxy, lbill16] | Joint Wald test of 2012-2015 coefficients | F=8.58 p=0.000; post 0.001675 (0.0002334); mean|pre|=0.00134; mean|pre|=0.00134; mean|pre|=0.00134; mean|pre|= | fail |
| S2 | Bill does not proxy for wealth [appeal_proxy, lbill16] | Add log(value16) x year; % change in beta | beta 0.001675 -> 0.0008981 (-46%) | fail |
| S3 | Parallel trends given value bin [sale, rate16_pp] | Joint Wald test of 2012-2015 coefficients | F=70.15 p=0.000 (df 4); post avg -0.01581 (SE 0.001108); mean|pre|=0.00785; mean|pre|=0.00785; mean|pre|=0.007 | fail |
| S3 | Value bins absorb other TCJA provisions [sale, rate16_pp] | Add imputed-AGI bin x year and mortint16 decile x year | beta -0.01581 -> -0.01025 (35% change) | fail |
| S3 | Parallel trends given value bin [exit, rate16_pp] | Joint Wald test of 2012-2015 coefficients | F=55.48 p=0.000 (df 4); post avg -0.003435 (SE 0.0006675); mean|pre|=0.01; mean|pre|=0.01; mean|pre|=0.01; mea | fail |
| S3 | Value bins absorb other TCJA provisions [exit, rate16_pp] | Add imputed-AGI bin x year and mortint16 decile x year | beta -0.003435 -> -0.00212 (38% change) | fail |
| S3 | Parallel trends given value bin [exit_prim, rate16_pp] | Joint Wald test of 2012-2015 coefficients | F=56.72 p=0.000 (df 4); post avg -0.006749 (SE 0.001032); mean|pre|=0.0105; mean|pre|=0.0105; mean|pre|=0.0105 | fail |
| S3 | Value bins absorb other TCJA provisions [exit_prim, rate16_pp] | Add imputed-AGI bin x year and mortint16 decile x year | beta -0.006749 -> -0.004883 (28% change) | fail |
| S3 | Parallel trends given value bin [log_price, rate16_pp] | Joint Wald test of 2012-2015 coefficients | F=4.77 p=0.001 (df 4); post avg 0.05228 (SE 0.00792); mean|pre|=0.0436; mean|pre|=0.0436; mean|pre|=0.0436; me | fail |
| S3 | Value bins absorb other TCJA provisions [log_price, rate16_pp] | Add imputed-AGI bin x year and mortint16 decile x year | beta 0.05228 -> 0.04858 (-7% change) | pass |
| S3 | Parallel trends given value bin [refi, rate16_pp] | Joint Wald test of 2012-2015 coefficients | F=5.69 p=0.000 (df 4); post avg -0.001638 (SE 0.0005063); mean|pre|=0.00149; mean|pre|=0.00149; mean|pre|=0.00 | fail |
| S3 | Value bins absorb other TCJA provisions [refi, rate16_pp] | Add imputed-AGI bin x year and mortint16 decile x year | beta -0.001638 -> 0.001381 (184% change) | fail |
| S3 | Parallel trends given value bin [cashout, rate16_pp] | Joint Wald test of 2012-2015 coefficients | F=1.32 p=0.258 (df 4); post avg 0.0001552 (SE 0.0003194); mean|pre|=0.000602; mean|pre|=0.000602; mean|pre|=0. | pass |
| S3 | Value bins absorb other TCJA provisions [cashout, rate16_pp] | Add imputed-AGI bin x year and mortint16 decile x year | beta 0.0001552 -> 0.0007882 (408% change) | fail |
| S3 | Parallel trends given value bin [to_rental, rate16_pp] | Joint Wald test of 2012-2015 coefficients | F=6.30 p=0.000 (df 4); post avg -0.008023 (SE 0.0006605); mean|pre|=0.00201; mean|pre|=0.00201; mean|pre|=0.00 | fail |
| S3 | Value bins absorb other TCJA provisions [to_rental, rate16_pp] | Add imputed-AGI bin x year and mortint16 decile x year | beta -0.008023 -> -0.007919 (1% change) | pass |
| S3 | Parallel trends given value bin [appeal_proxy, rate16_pp] | Joint Wald test of 2012-2015 coefficients | F=4.96 p=0.001 (df 4); post avg 0.0004109 (SE 0.0004855); mean|pre|=0.000805; mean|pre|=0.000805; mean|pre|=0. | fail |
| S3 | Value bins absorb other TCJA provisions [appeal_proxy, rate16_pp] | Add imputed-AGI bin x year and mortint16 decile x year | beta 0.0004109 -> 0.000607 (48% change) | fail |
| S3 | Parallel trends given value bin [sale, above16] | Joint Wald test of 2012-2015 coefficients | F=0.48 p=0.753 (df 4); post avg -0.007607 (SE 0.002569); mean|pre|=0.00354; mean|pre|=0.00354; mean|pre|=0.003 | pass |
| S3 | Value bins absorb other TCJA provisions [sale, above16] | Add imputed-AGI bin x year and mortint16 decile x year | beta -0.007607 -> 0.0009728 (113% change) | fail |
| S3 | Parallel trends given value bin [exit, above16] | Joint Wald test of 2012-2015 coefficients | F=1.27 p=0.278 (df 4); post avg -0.002782 (SE 0.002186); mean|pre|=0.00275; mean|pre|=0.00275; mean|pre|=0.002 | pass |
| S3 | Value bins absorb other TCJA provisions [exit, above16] | Add imputed-AGI bin x year and mortint16 decile x year | beta -0.002782 -> -0.0003411 (88% change) | fail |
| S3 | Parallel trends given value bin [exit_prim, above16] | Joint Wald test of 2012-2015 coefficients | F=1.02 p=0.397 (df 4); post avg -0.003268 (SE 0.003092); mean|pre|=0.00377; mean|pre|=0.00377; mean|pre|=0.003 | pass |
| S3 | Value bins absorb other TCJA provisions [exit_prim, above16] | Add imputed-AGI bin x year and mortint16 decile x year | beta -0.003268 -> -0.0001531 (95% change) | fail |
| S3 | Parallel trends given value bin [log_price, above16] | Joint Wald test of 2012-2015 coefficients | F=20.84 p=0.000 (df 4); post avg 0.09545 (SE 0.01745); mean|pre|=0.11; mean|pre|=0.11; mean|pre|=0.11; mean|pr | fail |
| S3 | Value bins absorb other TCJA provisions [log_price, above16] | Add imputed-AGI bin x year and mortint16 decile x year | beta 0.09545 -> 0.08725 (-9% change) | pass |
| S3 | Parallel trends given value bin [refi, above16] | Joint Wald test of 2012-2015 coefficients | F=0.86 p=0.487 (df 4); post avg 0.002298 (SE 0.002871); mean|pre|=0.00332; mean|pre|=0.00332; mean|pre|=0.0033 | pass |
| S3 | Value bins absorb other TCJA provisions [refi, above16] | Add imputed-AGI bin x year and mortint16 decile x year | beta 0.002298 -> 0.0007794 (-66% change) | fail |
| S3 | Parallel trends given value bin [cashout, above16] | Joint Wald test of 2012-2015 coefficients | F=0.60 p=0.662 (df 4); post avg 0.0004117 (SE 0.001802); mean|pre|=0.000976; mean|pre|=0.000976; mean|pre|=0.0 | pass |
| S3 | Value bins absorb other TCJA provisions [cashout, above16] | Add imputed-AGI bin x year and mortint16 decile x year | beta 0.0004117 -> 0.001448 (252% change) | fail |
| S3 | Parallel trends given value bin [to_rental, above16] | Joint Wald test of 2012-2015 coefficients | F=2.12 p=0.076 (df 4); post avg -0.001484 (SE 0.001226); mean|pre|=0.0014; mean|pre|=0.0014; mean|pre|=0.0014; | pass |
| S3 | Value bins absorb other TCJA provisions [to_rental, above16] | Add imputed-AGI bin x year and mortint16 decile x year | beta -0.001484 -> -0.001346 (9% change) | pass |
| S3 | Parallel trends given value bin [appeal_proxy, above16] | Joint Wald test of 2012-2015 coefficients | F=3.12 p=0.014 (df 4); post avg 0.000434 (SE 0.001224); mean|pre|=0.00276; mean|pre|=0.00276; mean|pre|=0.0027 | fail |
| S3 | Value bins absorb other TCJA provisions [appeal_proxy, above16] | Add imputed-AGI bin x year and mortint16 decile x year | beta 0.000434 -> 0.0004514 (4% change) | pass |
| S3 | value16 measured well [sale, rate16_pp] | Replace value16 bins by 2014-16 sale-price bins (same subsample) | beta value16 0.00772 (0.005447) vs price -0.02788 (0.005148), N=683288 | fail |
| S3 | value16 measured well [exit, rate16_pp] | Replace value16 bins by 2014-16 sale-price bins (same subsample) | beta value16 0.01338 (0.003988) vs price 0.01166 (0.003856), N=518328 | pass |
| S3 | value16 measured well [refi, rate16_pp] | Replace value16 bins by 2014-16 sale-price bins (same subsample) | beta value16 0.002206 (0.002132) vs price 0.003735 (0.001995), N=683288 | pass |
| S3 | value16 measured well [sale, above16] | Replace value16 bins by 2014-16 sale-price bins (same subsample) | beta value16 -0.01064 (0.0139) vs price 0.1922 (0.01608), N=683288 | fail |
| S3 | value16 measured well [exit, above16] | Replace value16 bins by 2014-16 sale-price bins (same subsample) | beta value16 -0.0111 (0.009997) vs price 0.04231 (0.01004), N=518328 | fail |
| S3 | value16 measured well [refi, above16] | Replace value16 bins by 2014-16 sale-price bins (same subsample) | beta value16 0.009421 (0.009407) vs price -0.02081 (0.008528), N=683288 | fail |
| S3 | Given value, millage unrelated to post-2018 trends (balance) | 2016 covariates by rate16 tercile within CBSA x value bin (T3-T1, SD units) | year_built=0.10; living_area=0.18; lot_sqft=-0.01; lor16=-0.27; agi_imp=0.14; age16=-0.27; mortint16=0.02 | fail |
| S3 | Local public goods differences do not change after 2018 | none | untestable; noted | untestable |
| S10 | At least one full post-year of outcomes exists | 2025 arm's-length sales vs 2024 in the sample | 2024: 30421, 2025: 30136 | pass |
| S10 | Responses symmetric (or asymmetry is the object) | Compare S10_rate16_pp 2025 coefficient with S3 2018-19 coefficient | see estimates S3_rate vs S10_rate16_pp | untestable |
| S10 | No other 2025 shock correlated with exposure | none | untestable (rates, insurance costs rose with value in 2023-25) | untestable |
| S13 | Post-2018 buyers paid a capitalised price (descriptive; buyers select into bills) | Incumbent vs 2018-19 buyer response 2020-24 | see S13 rows | untestable |
| S3 | Parallel trends, incumbents-only alternative [sale, rate16_pp] | Joint Wald test of 2012-2015 coefficients | F=49.98 p=0.000, mean|pre|=0.00251 vs post 0.003805 (SE 0.0004552); mean|pre|=0.00785 | fail |
| S3 | Parallel trends, incumbents-only alternative [exit_prim, rate16_pp] | Joint Wald test of 2012-2015 coefficients | F=15.40 p=0.000, mean|pre|=0.00355 vs post -0.003059 (SE 0.001023); mean|pre|=0.0105 | fail |
| S3 | Parallel trends, incumbents-only alternative [refi, rate16_pp] | Joint Wald test of 2012-2015 coefficients | F=5.14 p=0.000, mean|pre|=0.000754 vs post -0.002035 (SE 0.0005244); mean|pre|=0.00149 | fail |
| S3 | Parallel trends, incumbents-only alternative [to_rental, rate16_pp] | Joint Wald test of 2012-2015 coefficients | F=4.71 p=0.001, mean|pre|=0.00164 vs post -0.006444 (SE 0.0006711); mean|pre|=0.00201 | fail |
| S3 | Parallel trends, incumbents-only alternative [sale, above16] | Joint Wald test of 2012-2015 coefficients | F=5.41 p=0.000, mean|pre|=0.00299 vs post 0.001144 (SE 0.001544); mean|pre|=0.00354 | fail |
| S3 | Parallel trends, incumbents-only alternative [exit_prim, above16] | Joint Wald test of 2012-2015 coefficients | F=0.47 p=0.757, mean|pre|=0.00154 vs post -0.0009932 (SE 0.003112); mean|pre|=0.00377 | pass |
| S3 | Parallel trends, incumbents-only alternative [refi, above16] | Joint Wald test of 2012-2015 coefficients | F=1.58 p=0.176, mean|pre|=0.0044 vs post 0.001859 (SE 0.003046); mean|pre|=0.00332 | pass |
| S3 | Parallel trends, incumbents-only alternative [to_rental, above16] | Joint Wald test of 2012-2015 coefficients | F=0.81 p=0.520, mean|pre|=0.00137 vs post -0.0008701 (SE 0.00121); mean|pre|=0.0014 | pass |
| S4 | Bill gradient would have changed alike for owners and investors [sale, rate16_pp] | Pre-period DDD event study (2012-2015 joint test) | F=39.00 p=0.000; post DDD -0.01947 (0.00203) | fail |
| S4 | Occupancy misclassification unrelated to bill [sale, rate16_pp] | Owners restricted to parcels where all three occupancy signals agree | DDD -0.002446 (0.001833) vs base -0.01947 (0.00203) | fail |
| S4 | Bill gradient would have changed alike for owners and investors [log_price, rate16_pp] | Pre-period DDD event study (2012-2015 joint test) | F=0.70 p=0.591; post DDD -0.01398 (0.01976) | pass |
| S4 | Occupancy misclassification unrelated to bill [log_price, rate16_pp] | Owners restricted to parcels where all three occupancy signals agree | DDD -0.04509 (0.02572) vs base -0.01398 (0.01976) | pass |
| S4 | Bill gradient would have changed alike for owners and investors [refi, rate16_pp] | Pre-period DDD event study (2012-2015 joint test) | F=5.91 p=0.000; post DDD -0.003091 (0.0009025) | fail |
| S4 | Occupancy misclassification unrelated to bill [refi, rate16_pp] | Owners restricted to parcels where all three occupancy signals agree | DDD -0.004925 (0.001241) vs base -0.003091 (0.0009025) | pass |
| S4 | Bill gradient would have changed alike for owners and investors [to_rental, rate16_pp] | Pre-period DDD event study (2012-2015 joint test) | F=2.55 p=0.037; post DDD -0.005535 (0.001364) | fail |
| S4 | Occupancy misclassification unrelated to bill [to_rental, rate16_pp] | Owners restricted to parcels where all three occupancy signals agree | DDD -0.0001492 (0.001486) vs base -0.005535 (0.001364) | fail |
| S4 | Bill gradient would have changed alike for owners and investors [sale, above16] | Pre-period DDD event study (2012-2015 joint test) | F=1.32 p=0.261; post DDD -0.006013 (0.009462) | pass |
| S4 | Occupancy misclassification unrelated to bill [sale, above16] | Owners restricted to parcels where all three occupancy signals agree | DDD 0.00426 (0.009751) vs base -0.006013 (0.009462) | pass |
| S4 | Bill gradient would have changed alike for owners and investors [log_price, above16] | Pre-period DDD event study (2012-2015 joint test) | F=2.20 p=0.066; post DDD 0.02823 (0.05777) | pass |
| S4 | Occupancy misclassification unrelated to bill [log_price, above16] | Owners restricted to parcels where all three occupancy signals agree | DDD -0.09511 (0.06558) vs base 0.02823 (0.05777) | pass |
| S4 | Bill gradient would have changed alike for owners and investors [refi, above16] | Pre-period DDD event study (2012-2015 joint test) | F=0.32 p=0.862; post DDD -0.009467 (0.007863) | pass |
| S4 | Occupancy misclassification unrelated to bill [refi, above16] | Owners restricted to parcels where all three occupancy signals agree | DDD -0.01596 (0.009198) vs base -0.009467 (0.007863) | pass |
| S4 | Bill gradient would have changed alike for owners and investors [to_rental, above16] | Pre-period DDD event study (2012-2015 joint test) | F=0.50 p=0.735; post DDD -0.004475 (0.009197) | pass |
| S4 | Occupancy misclassification unrelated to bill [to_rental, above16] | Owners restricted to parcels where all three occupancy signals agree | DDD -0.004802 (0.009568) vs base -0.004475 (0.009197) | pass |
| S4 | Classification does not respond to treatment | Occupancy fixed at 2016; to_rental reported as an outcome | see to_rental rows | pass |
| S5 | Shocks continuous at the border [sale] | Pre-trends (joint test 2012-2015) | F=33.42 p=0.000; post -0.02476 (0.00262) | fail |
| S5 | Shocks continuous at the border [log_price] | Pre-trends (joint test 2012-2015) | F=0.77 p=0.545; post 0.05642 (0.02383) | pass |
| S5 | Shocks continuous at the border [exit_prim] | Pre-trends (joint test 2012-2015) | F=29.36 p=0.000; post -0.01603 (0.003289) | fail |
| S5 | Shocks continuous at the border [refi] | Pre-trends (joint test 2012-2015) | F=3.49 p=0.007; post -0.005279 (0.00167) | fail |
| S5 | Covariate balance across borders | High- vs low-rate side within segment (SD units) | year_built=0.02; living_area=0.05; lot_sqft=-0.00; lor16=-0.20; agi_imp=0.00; age16=-0.21; value16=-0.07 | fail |
| S5 | Who lives on each side does not change because of TCJA | Household size and surname turnover gap across the border, pre vs post | persons gap pre 0.000 post 0.022; turnover gap pre 0.022 post 0.012 | inconclusive |
| S5 | Services across the border do not change after 2018 | none | untestable; school consolidations not screened | untestable |
| S5 | No spillovers across the border | none | untestable; movers may relocate across the line | untestable |
| S6 | TAXSIM simulation reproduces itemisation (validation vs SOI) | ZIP itemisation share correlation 2016/2018, mean error (homeowners vs all filers) | cor16=0.80 cor18=0.80, mean err 0.108/0.043 | pass |
| S6 | Given controls, dprice affects outcomes only through the tax price [sale] | Pre-trends of dprice x year | F=25.68 p=0.000; post -0.04805 (0.004295) | fail |
| S6 | Given controls, dprice affects outcomes only through the tax price [log_price] | Pre-trends of dprice x year | F=3.61 p=0.006; post 0.009655 (0.01993) | fail |
| S6 | Given controls, dprice affects outcomes only through the tax price [exit_prim] | Pre-trends of dprice x year | F=8.38 p=0.000; post -0.02018 (0.005237) | fail |
| S6 | Given controls, dprice affects outcomes only through the tax price [refi] | Pre-trends of dprice x year | F=1.44 p=0.216; post -0.002297 (0.004) | pass |
| S6 | Imputation error unrelated to potential outcomes | none | untestable | untestable |
| S6 | Functional form of controls [sale] | beta under coarse / base / fine controls | S6_base=-0.04805; S6_coarse=-0.06865; S6_fine=-0.03891 | fail |
| S6 | Functional form of controls [log_price] | beta under coarse / base / fine controls | S6_base=0.009655; S6_coarse=0.05179; S6_fine=0.01112 | pass |
| S6 | Functional form of controls [exit_prim] | beta under coarse / base / fine controls | S6_base=-0.02018; S6_coarse=-0.01369; S6_fine=-0.01783 | pass |
| S6 | Functional form of controls [refi] | beta under coarse / base / fine controls | S6_base=-0.002297; S6_coarse=0.004893; S6_fine=0.01129 | fail |
| S7 | No other kink at $10k | Pre-period (2014-16) kink in each outcome, no-income-tax states | sale: 0.00505 (0.0195); log_price: 0.607 (0.281); exit_prim: 0.0208 (0.0218); refi: -0.00422 (0.0147) | fail |
| S7 | Outcomes otherwise smooth in the bill | Covariate kinks at $10k (slope change per $1k, SD units) | year_built=0.047(0.193); living_area=0.007(0.142); value16=-0.029(0.264); agi_imp=0.220(0.291); lor16=0.020(0. | pass |
| S7 | No manipulation of the bill around $10k | Excess mass in [$9k,$10k) 2019 vs 2016 | excess 2016 -0.0312, 2019 -0.0412 | fail |
| S7 | Sales-tax deduction does not blur the threshold | Threshold shifted by IRS optional sales-tax table amount | see S7_notax_salesadj rows | inconclusive |
| S8 | Married and unmarried co-owners parallel [sale, S8_pooled] | Pre-period DDD joint test | F=1.23 p=0.294; post 0.02701 (0.02114) | pass |
| S8 | Married and unmarried co-owners parallel [log_price, S8_pooled] | Pre-period DDD joint test | F=0.49 p=0.741; post 0.2579 (0.1125) | pass |
| S8 | Married and unmarried co-owners parallel [exit_prim, S8_pooled] | Pre-period DDD joint test | F=0.11 p=0.979; post 0.02619 (0.02977) | pass |
| S8 | Married and unmarried co-owners parallel [refi, S8_pooled] | Pre-period DDD joint test | F=0.95 p=0.433; post -0.01126 (0.0215) | pass |
| S8 | Balance married vs unmarried on age, tenure, value [S8_pooled] | Difference in SD units within value bin x bill group | age16=0.50; lor16=0.59; value16=0.00; agi_imp=0.03 | fail |
| S8 | Married and unmarried co-owners parallel [sale, S8_strict] | Pre-period DDD joint test | F=0.75 p=0.556; post 0.02859 (0.02149) | pass |
| S8 | Married and unmarried co-owners parallel [log_price, S8_strict] | Pre-period DDD joint test | F=0.90 p=0.462; post 0.2316 (0.1241) | pass |
| S8 | Married and unmarried co-owners parallel [exit_prim, S8_strict] | Pre-period DDD joint test | F=0.09 p=0.985; post 0.01777 (0.03031) | pass |
| S8 | Married and unmarried co-owners parallel [refi, S8_strict] | Pre-period DDD joint test | F=0.65 p=0.623; post -0.004116 (0.02249) | pass |
| S8 | Balance married vs unmarried on age, tenure, value [S8_strict] | Difference in SD units within value bin x bill group | age16=0.33; lor16=0.48; value16=0.00; agi_imp=0.03 | fail |
| S8 | Marriage/divorce do not respond to the cap | Vesting changes of the same parcel (roll relationship code) reported as an outcome | see table_s8_vesting_change.csv | inconclusive |
| S8 | Misclassification unrelated to the bill | strict vs pooled definition compared | see S8_strict vs S8_pooled rows | inconclusive |
| S8 | Unmarried co-owners actually split the deduction | none | untestable; estimate is a lower bound | untestable |
| S9a | Revaluation timing exogenous to 2018 (schedules set before 2017) | Published NJ lists 2017-22 / CT statutory cycle vs roll-based revaluation years; unscheduled revaluations | CT: 5 matches, 130 unscheduled (roll only), 121 published-only; NJ: 171 matches, 151 unscheduled (roll only),  | inconclusive |
| S9a | Parallel trends reval vs other-time towns [sale] | Pre-trends 2012-2015 | F=1.45 p=0.214 | pass |
| S9a | Parallel trends reval vs other-time towns [log_price] | Pre-trends 2012-2015 | F=3.14 p=0.014 | fail |
| S9a | Parallel trends reval vs other-time towns [exit_prim] | Pre-trends 2012-2015 | F=2.68 p=0.030 | fail |
| S9a | Parallel trends reval vs other-time towns [refi] | Pre-trends 2012-2015 | F=1.83 p=0.119 | pass |
| S9b | Cohort trends same in CA and control states [sale] | Pre-period DDD joint test | F=4.19 p=0.002; post 0.02716 (0.006602) | fail |
| S9b | Cohort trends same in CA and control states [log_price] | Pre-period DDD joint test | not identified on this sample: 5 DDD terms collinear within block group x year (N=12743) | untestable |
| S9b | Cohort trends same in CA and control states [refi] | Pre-period DDD joint test | F=2.39 p=0.049; post -0.02411 (0.008443) | fail |

## 6. Capitalisation and interpretation

**Direct estimate.** Strategy 3 with log price on arm's-length sales gives +0.052 (0.008) per percentage point of
rate16 and +0.095 (0.017) for above16 under hedonic controls, but the year-by-year coefficients are +0.03 to +0.06 in
every year from 2012 on: 2016 is a low reference year and there is no post-2017 change relative to 2012–15. The
repeat-sales version with parcel fixed effects (44k repeat sales) gives −0.022 (0.039) per pp and +0.010 (0.060) for
above16. Full capitalisation of the lost deduction for an itemising owner at a 24% marginal rate would be a log-price
change of −0.034 (7% discount rate) to −0.060 (4%) per percentage point of rate (`table_s13_pv_comparison.csv`).
Against the hedonic coefficient the implied capitalised share is negative; against the repeat-sales coefficient it is
0.4–0.6 with a confidence interval covering zero and one. Strategy 6's valuation of the deductibility change (mean
tax-price increase of 0.066, i.e. 6.6 cents per dollar of property tax, about $160–$1,200 a year for bills of $2.5k–
$18k) is small relative to house values, so even full capitalisation would be 1–3% of value for typical exposed homes
and within the noise of these price regressions.

**Incumbents vs new buyers.** Owners who bought before 2016 have a 2020–24 sale response to rate16 of +0.25 pp (0.06)
per pp; owners who bought in 2018–19 respond 2.9 pp (0.4) less. This difference is dominated by the low re-sale hazard
of recent buyers and by selection of buyers into high- or low-bill homes, as pre-specified; it is descriptive and does
not measure capitalisation.

**Wealth vs flow-price effect.** Conditioning on value16 is pre-treatment and does not remove capitalisation, which
happens after 2016; the main estimates therefore combine any flow-price effect (the higher annual after-tax cost of a
high-tax home) with any wealth effect from capitalisation. Since neither the repeat-sales price regressions nor the
exposure designs find a response that survives diagnostics, the data do not let us apportion the (small) response
between the two channels. What the sale results do show is a decline in transactions of high-bill homes from 2017 that
is shared by investor-owned homes and does not reverse in 2025, i.e. a market-wide pattern for high-tax homes rather
than an owner-occupier deduction response.

## 7. Limitations

- **No observed itemisation status.** Treatment intensity is imputed (TAXSIM on imputed AGI, filing status from vesting
  codes, mortgage interest from recorded loans). The SOI validation of itemisation shares is good at the ZIP level
  (correlation 0.80) but household-level error is unknown; imputed AGI reproduces the all-filers distribution and so
  understates homeowner incomes.
- **No destinations for movers.** Data Axle ends in 2023, misses 2022, and its surname-based exit measure fails the
  60% validation; the primary-surname alternative validates at 80–87% but includes record churn (4–6% false positives).
  We cannot distinguish an out-of-state move from a local one.
- **Mechanical link between recent sales and the rate variable.** The 2016 effective rate is contaminated by
  sale-based reassessment, which drives the failed pre-trends and the tenure/age imbalance in every continuous design.
  A clean continuous design needs tenure × year controls or exposure measured before the last sale; this was not
  pre-specified and was not done.
- **Value measurement.** The calculated value is the assessed value in many jurisdictions; the county/tax-area sales
  ratio corrects the level but not within-jurisdiction dispersion. Results in the subsample with a 2014–16 sale change
  sign when value bins are formed from the sale price, so "same value" is imperfectly controlled.
- **COVID.** The extended window (2020–24) is reported separately; 2020–21 contain the pandemic sales boom and 2022–24
  the rate shock.
- **State workarounds.** Pass-through-entity taxes (from 2018 in CT, 2019–21 elsewhere) and charitable-credit schemes
  affect business owners' SALT exposure and are not modelled; they attenuate the treatment for some high-income owners.
- **Mortgage coverage** is near zero in VT and SD and under 25% in MS, WV, CT and RI, so `mortint16 = 0` there means
  unobserved; Strategy 6 and the augmented controls treat it as zero.
- **Sample.** All estimates are from a 1% building sample; the thin designs (7, 9a, 10's $40k kink) need the full data.
  The 10% replication of Strategies 1–4 and 7–10 is in Section 8.
- **Multiple outcomes and specifications.** 254 estimates are reported; the pre-specification and the single
  labelled alternative limit, but do not remove, the risk of reading noise.

## 8. Replication on the 10% sample (subsample → full-data rule)

Ledger verdicts on the 10% sample (`bld10pct/assumption_ledger.csv`, re-derived with the magnitude rule): S3 8 pass /
30 fail / 1 inconclusive; S4 8 pass / 9 fail; S7 3 pass / 1 inconclusive; S8 8 pass / 2 fail / 2 inconclusive; S9a 3
pass / 1 fail / 1 inconclusive; S9b 1 pass / 1 fail / 1 inconclusive. The only Strategy 3 pre-trend tests that pass at
10% are refinancing and cash-out with the binary treatment, both with zero effects (−0.03 pp (0.10) and −0.01 pp
(0.06)).

The same build and scripts were run on Bouchet on a 10% building sample (7.15M owner-occupied parcels, 57.2M
parcel-years in the main window; `bld10pct` rows in `estimates.csv`, tables and figures in `bld10pct/`). Strategies 5
and 6 were not replicated (they need TIGER downloads and TAXSIM from compute nodes). Post-period coefficients (SE):

| Strategy, spec | Outcome | 1% | 10% |
|---|---|---|---|
| S2 above16 | sale | +0.0017 (0.0016) | +0.0032 (0.0006) |
| S3 rate16 (per pp) | sale | −0.0158 (0.0011) | −0.0162 (0.0010) |
| S3 rate16 | exit_prim | −0.0067 (0.0010) | −0.0081 (0.0004) |
| S3 rate16 | log price | +0.052 (0.008) | +0.030 (0.004) |
| S3 above16 | sale | −0.0076 (0.0026) | −0.0028 (0.0008) |
| S3 rate16, repeat sales | log price | −0.022 (0.039) | −0.003 (0.007) |
| S3 above16, repeat sales | log price | +0.010 (0.060) | −0.017 (0.016) |
| S3 above16 | exit | −0.0028 (0.0022) | +0.0006 (0.0008) |
| S4 above16 (DDD) | sale | −0.0060 (0.0095) | −0.0033 (0.0034) |
| S4 above16 | refi | −0.0095 (0.0079) | −0.0048 (0.0023) |
| S4 rate16 | sale | −0.0195 (0.0020) | −0.0217 (0.0012) |
| S7 no-income-tax states | sale kink per $1k | +0.014 (0.035) | +0.014 (0.013) |
| S7 likely itemisers | exit_prim kink | +0.023 (0.066) | +0.053 (0.026) |
| S7 | bunching 2019 vs 2016 | −0.010 | −0.003 |
| S8 pooled (DDD) | sale | +0.027 (0.021) | +0.004 (0.006) |
| S8 pooled | log price | +0.26 (0.11) | +0.003 (0.026) |
| S9a crossing $10k at revaluation | sale | +0.001 (0.018) | +0.007 (0.005) |
| S9a crossing | log price | +0.23 (0.13) | +0.20 (0.07) |
| S9a crossing | refi | +0.017 (0.017) | −0.014 (0.007) |
| S9b recent cohort × CA | sale | +0.027 (0.007) | +0.019 (0.002) |
| S10 above16, post = 2025 | sale | −0.0084 (0.0029) | −0.0057 (0.0009) |
| S10 rate16, post = 2025 | sale | −0.0202 (0.0012) | −0.0215 (0.0011) |

The rate-based estimates replicate almost exactly, as expected for a mechanical artefact. The binary and group
contrasts shrink toward zero with ten times the data: the Strategy 3 above16 sale effect falls from −0.76 to −0.28 pp,
its year-by-year pre-period coefficients are +0.2, +0.5, +0.5 and +0.7 pp (a rising pre-trend peaking in the 2016
reference year; mean |pre| 0.46 pp vs a 0.28 pp post effect → fail by the magnitude rule; augmented estimate +0.5 pp), the Strategy 8 price coefficient
goes from +0.26 to +0.003, the Strategy 4 owner-specific sale effect is −0.33 pp (0.34), and the repeat-sales price
effects become precise zeros (−0.003 (0.007) per pp of rate; −0.017 (0.016) for above16; 2.5M repeat-sale observations). Strategy 7 remains a
precise zero on sales (+0.014 (0.013) per $1k) with no bunching; its only non-zero reading is a +5 pp (2.6) exit kink
among likely itemisers, not seen on sales or in the full no-income-tax sample. Strategy 9a's revaluation design gains
480 towns: crossing $10k at a 2018–20 revaluation does not change sales (+0.7 pp (0.5)) but is associated with higher
subsequent sale prices (+0.20 (0.07)) and fewer refinancings (−1.4 pp (0.7)); its pre-trend and schedule checks are in
`bld10pct/assumption_ledger.csv`. The 10% figures for 2025 again show no reversal (above16 sale −0.57 pp (0.09)).

## 9. Next steps

1. **Fix the exposure measure.** Re-define the continuous treatment from a pre-sale bill (the bill in the year before
   the last purchase, carried forward) or add tenure × year controls, and re-run Strategies 3–6 as a new pre-registered
   round; the inverted-V pre-trend is the first thing to check.
2. **Full data for the thin designs.** Strategy 7 (kink at $10k and $40k), 9a (CT/NJ revaluations; 192 crossing
   parcels on 1%) and the $40k reversal need the 100% roll; the 10% build and job scripts are in place.
3. **Itemisation status.** Link to a data source with observed itemisation (e.g. IRS SOI public-use microdata by
   state × AGI for calibration, or a lender dataset with tax transcripts) to replace imputed treatment intensity.
4. **Destinations.** Use the Data Axle family panel (person-level re-linking by name and age, documented in the
   project's Data Axle notes) to classify exits as in-state vs out-of-state moves, where the cap's predicted margin is.
5. **Price.** A repeat-sales design on the full data, and a border-pair hedonic design using only post-2016 first
   sales, would give the capitalisation estimate the power it lacks here.
6. **Workarounds.** Add the state pass-through-entity-tax adoption dates as a treatment modifier for parcels owned via
   entities or by likely business owners.

## Appendix A. All estimates (`estimates.csv`)

| strategy | outcome | sample | coef | se | N | clusters | spec_id |
|---|---|---|---|---|---|---|---|
| S1 | exit_rate | bld1pct |  3.736e-04 | 4.45e-03 |     82936 |  1963 | S1_base |
| S1 | exit_rate | bld1pct |  9.393e-03 | 5.14e-03 |     82936 |  1963 | S1_agi |
| S1 | sale_rate | bld1pct | -1.329e-02 | 5.56e-03 |     82936 |  1963 | S1_base |
| S1 | sale_rate | bld1pct |  1.555e-02 | 4.46e-03 |     82936 |  1963 | S1_agi |
| S1 | med_log_price | bld1pct | -3.118e-01 | 7.17e-02 |     65690 |  1862 | S1_base |
| S1 | med_log_price | bld1pct |  6.241e-02 | 6.38e-02 |     65690 |  1862 | S1_agi |
| S2 | sale | bld1pct |  1.709e-03 | 1.57e-03 |   5698936 | 48932 | S2_above |
| S2 | sale | bld1pct |  1.404e-05 | 7.77e-04 |   5698936 | 48932 | S2_above_val |
| S2 | sale | bld1pct |  1.709e-03 | 1.65e-03 |   5698936 |  2879 | S2_above_cl_county |
| S2 | exit | bld1pct | -1.227e-04 | 1.35e-03 |   4876625 | 47669 | S2_above |
| S2 | exit | bld1pct |  4.257e-05 | 6.72e-04 |   4876625 | 47669 | S2_above_val |
| S2 | exit | bld1pct | -1.227e-04 | 1.28e-03 |   4876625 |  2862 | S2_above_cl_county |
| S2 | exit_prim | bld1pct |  1.606e-04 | 1.99e-03 |   4876625 | 47669 | S2_above |
| S2 | exit_prim | bld1pct |  1.490e-04 | 9.92e-04 |   4876625 | 47669 | S2_above_val |
| S2 | exit_prim | bld1pct |  1.606e-04 | 1.84e-03 |   4876625 |  2862 | S2_above_cl_county |
| S2 | log_price | bld1pct |  1.398e-01 | 1.90e-02 |    245661 | 28138 | S2_above |
| S2 | log_price | bld1pct |  1.336e-01 | 9.43e-03 |    245661 | 28138 | S2_above_val |
| S2 | log_price | bld1pct |  1.398e-01 | 2.72e-02 |    245661 |  2501 | S2_above_cl_county |
| S2 | refi | bld1pct | -1.245e-02 | 1.91e-03 |   5698936 | 48932 | S2_above |
| S2 | refi | bld1pct | -4.765e-03 | 9.38e-04 |   5698936 | 48932 | S2_above_val |
| S2 | refi | bld1pct | -1.245e-02 | 2.72e-03 |   5698936 |  2879 | S2_above_cl_county |
| S2 | cashout | bld1pct | -4.775e-03 | 1.18e-03 |   5698936 | 48932 | S2_above |
| S2 | cashout | bld1pct | -2.115e-03 | 5.93e-04 |   5698936 | 48932 | S2_above_val |
| S2 | cashout | bld1pct | -4.775e-03 | 1.26e-03 |   5698936 |  2879 | S2_above_cl_county |
| S2 | to_rental | bld1pct | -2.019e-03 | 9.02e-04 |   4473126 | 48920 | S2_above |
| S2 | to_rental | bld1pct | -7.930e-04 | 4.55e-04 |   4473126 | 48920 | S2_above_val |
| S2 | to_rental | bld1pct | -2.019e-03 | 8.93e-04 |   4473126 |  2879 | S2_above_cl_county |
| S2 | appeal_proxy | bld1pct |  2.467e-03 | 9.85e-04 |   4984818 | 48831 | S2_above |
| S2 | appeal_proxy | bld1pct |  9.697e-04 | 4.92e-04 |   4984818 | 48831 | S2_above_val |
| S2 | appeal_proxy | bld1pct |  2.467e-03 | 1.28e-03 |   4984818 |  2870 | S2_above_cl_county |
| S2 | sale | bld1pct |  7.003e-04 | 4.21e-04 |   5698936 | 48932 | S2_logbill |
| S2 | sale | bld1pct |  2.200e-03 | 2.31e-04 |   5698936 | 48932 | S2_logbill_val |
| S2 | sale | bld1pct |  7.003e-04 | 5.96e-04 |   5698936 |  2879 | S2_logbill_cl_county |
| S2 | exit | bld1pct | -1.421e-03 | 6.69e-04 |   4876625 | 47669 | S2_logbill |
| S2 | exit | bld1pct | -4.424e-04 | 4.07e-04 |   4876625 | 47669 | S2_logbill_val |
| S2 | exit | bld1pct | -1.421e-03 | 7.27e-04 |   4876625 |  2862 | S2_logbill_cl_county |
| S2 | exit_prim | bld1pct | -2.282e-03 | 7.83e-04 |   4876625 | 47669 | S2_logbill |
| S2 | exit_prim | bld1pct | -3.973e-04 | 4.58e-04 |   4876625 | 47669 | S2_logbill_val |
| S2 | exit_prim | bld1pct | -2.282e-03 | 8.54e-04 |   4876625 |  2862 | S2_logbill_cl_county |
| S2 | log_price | bld1pct |  5.404e-02 | 8.55e-03 |    245661 | 28138 | S2_logbill |
| S2 | log_price | bld1pct |  4.407e-02 | 6.13e-03 |    245661 | 28138 | S2_logbill_val |
| S2 | log_price | bld1pct |  5.404e-02 | 1.46e-02 |    245661 |  2501 | S2_logbill_cl_county |
| S2 | refi | bld1pct | -8.551e-03 | 3.33e-04 |   5698936 | 48932 | S2_logbill |
| S2 | refi | bld1pct | -4.831e-03 | 1.81e-04 |   5698936 | 48932 | S2_logbill_val |
| S2 | refi | bld1pct | -8.551e-03 | 6.84e-04 |   5698936 |  2879 | S2_logbill_cl_county |
| S2 | cashout | bld1pct | -1.610e-03 | 2.03e-04 |   5698936 | 48932 | S2_logbill |
| S2 | cashout | bld1pct | -9.856e-04 | 1.09e-04 |   5698936 | 48932 | S2_logbill_val |
| S2 | cashout | bld1pct | -1.610e-03 | 2.32e-04 |   5698936 |  2879 | S2_logbill_cl_county |
| S2 | to_rental | bld1pct | -3.123e-03 | 2.71e-04 |   4473126 | 48920 | S2_logbill |
| S2 | to_rental | bld1pct | -8.967e-04 | 1.36e-04 |   4473126 | 48920 | S2_logbill_val |
| S2 | to_rental | bld1pct | -3.123e-03 | 3.45e-04 |   4473126 |  2879 | S2_logbill_cl_county |
| S2 | appeal_proxy | bld1pct |  1.675e-03 | 2.33e-04 |   4984818 | 48831 | S2_logbill |
| S2 | appeal_proxy | bld1pct |  8.981e-04 | 1.25e-04 |   4984818 | 48831 | S2_logbill_val |
| S2 | appeal_proxy | bld1pct |  1.675e-03 | 3.90e-04 |   4984818 |  2870 | S2_logbill_cl_county |
| S3 | sale | bld1pct | -1.581e-02 | 1.11e-03 |   5643728 | 48932 | S3_rate |
| S3 | sale | bld1pct | -1.025e-02 | 1.12e-03 |   5631544 | 48932 | S3_rate_aug |
| S3 | sale | bld1pct | -1.581e-02 | 2.03e-03 |   5643728 |  2879 | S3_rate_cl_county |
| S3 | exit | bld1pct | -3.435e-03 | 6.68e-04 |   4811135 | 47669 | S3_rate |
| S3 | exit | bld1pct | -2.120e-03 | 6.83e-04 |   4804167 | 47669 | S3_rate_aug |
| S3 | exit | bld1pct | -3.435e-03 | 8.61e-04 |   4811135 |  2862 | S3_rate_cl_county |
| S3 | exit_prim | bld1pct | -6.749e-03 | 1.03e-03 |   4811135 | 47669 | S3_rate |
| S3 | exit_prim | bld1pct | -4.883e-03 | 1.06e-03 |   4804167 | 47669 | S3_rate_aug |
| S3 | exit_prim | bld1pct | -6.749e-03 | 1.38e-03 |   4811135 |  2862 | S3_rate_cl_county |
| S3 | log_price | bld1pct |  5.228e-02 | 7.92e-03 |    196038 | 28138 | S3_rate |
| S3 | log_price | bld1pct |  4.858e-02 | 7.95e-03 |    195614 | 28138 | S3_rate_aug |
| S3 | log_price | bld1pct |  5.228e-02 | 1.25e-02 |    196038 |  2501 | S3_rate_cl_county |
| S3 | refi | bld1pct | -1.638e-03 | 5.06e-04 |   5643728 | 48932 | S3_rate |
| S3 | refi | bld1pct |  1.381e-03 | 5.68e-04 |   5631544 | 48932 | S3_rate_aug |
| S3 | refi | bld1pct | -1.638e-03 | 5.23e-04 |   5643728 |  2879 | S3_rate_cl_county |
| S3 | cashout | bld1pct |  1.552e-04 | 3.19e-04 |   5643728 | 48932 | S3_rate |
| S3 | cashout | bld1pct |  7.882e-04 | 3.42e-04 |   5631544 | 48932 | S3_rate_aug |
| S3 | cashout | bld1pct |  1.552e-04 | 3.41e-04 |   5643728 |  2879 | S3_rate_cl_county |
| S3 | to_rental | bld1pct | -8.022e-03 | 6.61e-04 |   4412640 | 48920 | S3_rate |
| S3 | to_rental | bld1pct | -7.919e-03 | 6.74e-04 |   4404442 | 48920 | S3_rate_aug |
| S3 | to_rental | bld1pct | -8.022e-03 | 9.47e-04 |   4412640 |  2879 | S3_rate_cl_county |
| S3 | appeal_proxy | bld1pct |  4.109e-04 | 4.86e-04 |   4934215 | 48831 | S3_rate |
| S3 | appeal_proxy | bld1pct |  6.070e-04 | 4.82e-04 |   4925797 | 48831 | S3_rate_aug |
| S3 | appeal_proxy | bld1pct |  4.109e-04 | 1.02e-03 |   4934215 |  2870 | S3_rate_cl_county |
| S3 | sale | bld1pct | -7.607e-03 | 2.57e-03 |   5643728 | 48932 | S3_above |
| S3 | sale | bld1pct |  9.728e-04 | 2.67e-03 |   5631544 | 48932 | S3_above_aug |
| S3 | sale | bld1pct | -7.607e-03 | 2.62e-03 |   5643728 |  2879 | S3_above_cl_county |
| S3 | exit | bld1pct | -2.782e-03 | 2.19e-03 |   4811135 | 47669 | S3_above |
| S3 | exit | bld1pct | -3.411e-04 | 2.23e-03 |   4804167 | 47669 | S3_above_aug |
| S3 | exit | bld1pct | -2.782e-03 | 2.31e-03 |   4811135 |  2862 | S3_above_cl_county |
| S3 | exit_prim | bld1pct | -3.268e-03 | 3.09e-03 |   4811135 | 47669 | S3_above |
| S3 | exit_prim | bld1pct | -1.531e-04 | 3.13e-03 |   4804167 | 47669 | S3_above_aug |
| S3 | exit_prim | bld1pct | -3.268e-03 | 2.81e-03 |   4811135 |  2862 | S3_above_cl_county |
| S3 | log_price | bld1pct |  9.545e-02 | 1.75e-02 |    196038 | 28138 | S3_above |
| S3 | log_price | bld1pct |  8.725e-02 | 1.73e-02 |    195614 | 28138 | S3_above_aug |
| S3 | log_price | bld1pct |  9.545e-02 | 2.87e-02 |    196038 |  2501 | S3_above_cl_county |
| S3 | refi | bld1pct |  2.298e-03 | 2.87e-03 |   5643728 | 48932 | S3_above |
| S3 | refi | bld1pct |  7.794e-04 | 2.85e-03 |   5631544 | 48932 | S3_above_aug |
| S3 | refi | bld1pct |  2.298e-03 | 2.78e-03 |   5643728 |  2879 | S3_above_cl_county |
| S3 | cashout | bld1pct |  4.117e-04 | 1.80e-03 |   5643728 | 48932 | S3_above |
| S3 | cashout | bld1pct |  1.448e-03 | 1.81e-03 |   5631544 | 48932 | S3_above_aug |
| S3 | cashout | bld1pct |  4.117e-04 | 1.80e-03 |   5643728 |  2879 | S3_above_cl_county |
| S3 | to_rental | bld1pct | -1.484e-03 | 1.23e-03 |   4412640 | 48920 | S3_above |
| S3 | to_rental | bld1pct | -1.346e-03 | 1.25e-03 |   4404442 | 48920 | S3_above_aug |
| S3 | to_rental | bld1pct | -1.484e-03 | 1.25e-03 |   4412640 |  2879 | S3_above_cl_county |
| S3 | appeal_proxy | bld1pct |  4.340e-04 | 1.22e-03 |   4934215 | 48831 | S3_above |
| S3 | appeal_proxy | bld1pct |  4.514e-04 | 1.23e-03 |   4925797 | 48831 | S3_above_aug |
| S3 | appeal_proxy | bld1pct |  4.340e-04 | 9.73e-04 |   4934215 |  2870 | S3_above_cl_county |
| S3 | log_price | bld1pct | -2.174e-02 | 3.87e-02 |     43960 | 28138 | S3_rate_rs |
| S3 | log_price | bld1pct |  9.620e-03 | 6.02e-02 |     43960 | 28138 | S3_above_rs |
| S3 | sale | bld1pct |  7.720e-03 | 5.45e-03 |    708576 | 20013 | S3_rate_pricesub |
| S3 | sale | bld1pct | -2.788e-02 | 5.15e-03 |    683288 | 20013 | S3_rate_price |
| S3 | exit | bld1pct |  1.338e-02 | 3.99e-03 |    538437 | 19618 | S3_rate_pricesub |
| S3 | exit | bld1pct |  1.166e-02 | 3.86e-03 |    518328 | 19618 | S3_rate_price |
| S3 | refi | bld1pct |  2.206e-03 | 2.13e-03 |    708576 | 20013 | S3_rate_pricesub |
| S3 | refi | bld1pct |  3.735e-03 | 1.99e-03 |    683288 | 20013 | S3_rate_price |
| S3 | sale | bld1pct | -1.064e-02 | 1.39e-02 |    708576 | 20013 | S3_above_pricesub |
| S3 | sale | bld1pct |  1.922e-01 | 1.61e-02 |    683288 | 20013 | S3_above_price |
| S3 | exit | bld1pct | -1.110e-02 | 1.00e-02 |    538437 | 19618 | S3_above_pricesub |
| S3 | exit | bld1pct |  4.231e-02 | 1.00e-02 |    518328 | 19618 | S3_above_price |
| S3 | refi | bld1pct |  9.421e-03 | 9.41e-03 |    708576 | 20013 | S3_above_pricesub |
| S3 | refi | bld1pct | -2.081e-02 | 8.53e-03 |    683288 | 20013 | S3_above_price |
| S3 | sale | bld1pct | -1.814e-02 | 1.10e-03 |   9171058 | 48932 | S3_rate_ext |
| S3 | log_price | bld1pct |  6.546e-02 | 6.15e-03 |    301812 | 32601 | S3_rate_ext |
| S3 | refi | bld1pct |  1.469e-03 | 4.78e-04 |   9171058 | 48932 | S3_rate_ext |
| S3 | sale | bld1pct | -7.404e-03 | 2.28e-03 |   9171058 | 48932 | S3_above_ext |
| S3 | log_price | bld1pct |  1.009e-01 | 1.35e-02 |    301812 | 32601 | S3_above_ext |
| S3 | refi | bld1pct |  2.233e-03 | 2.56e-03 |   9171058 | 48932 | S3_above_ext |
| S1 | exit_rate | bld10pct |  1.406e-03 | 2.03e-03 |    175910 |  2818 | S1_base |
| S1 | exit_rate | bld10pct |  2.943e-03 | 1.83e-03 |    175633 |  2818 | S1_agi |
| S1 | sale_rate | bld10pct | -1.803e-02 | 3.15e-03 |    176032 |  2818 | S1_base |
| S1 | sale_rate | bld10pct |  5.789e-03 | 2.26e-03 |    175752 |  2818 | S1_agi |
| S1 | med_log_price | bld10pct | -2.494e-01 | 3.87e-02 |    147469 |  2669 | S1_base |
| S1 | med_log_price | bld10pct |  4.886e-02 | 3.13e-02 |    147389 |  2669 | S1_agi |
| S2 | sale | bld10pct |  3.207e-03 | 6.34e-04 |  57211392 | 80554 | S2_above |
| S2 | sale | bld10pct |  8.261e-04 | 3.20e-04 |  57211392 | 80554 | S2_above_val |
| S2 | sale | bld10pct |  3.207e-03 | 8.00e-04 |  57211392 |  2923 | S2_above_cl_county |
| S2 | exit | bld10pct |  2.662e-03 | 4.68e-04 |  49060809 | 78995 | S2_above |
| S2 | exit | bld10pct |  1.231e-03 | 2.36e-04 |  49060809 | 78995 | S2_above_val |
| S2 | exit | bld10pct |  2.662e-03 | 5.32e-04 |  49060809 |  2917 | S2_above_cl_county |
| S2 | exit_prim | bld10pct |  2.034e-03 | 6.96e-04 |  49060809 | 78995 | S2_above |
| S2 | exit_prim | bld10pct |  8.736e-04 | 3.44e-04 |  49060809 | 78995 | S2_above_val |
| S2 | exit_prim | bld10pct |  2.034e-03 | 7.89e-04 |  49060809 |  2917 | S2_above_cl_county |
| S2 | log_price | bld10pct |  1.687e-01 | 1.13e-02 |   2537498 | 58668 | S2_above |
| S2 | log_price | bld10pct |  1.420e-01 | 5.82e-03 |   2537498 | 58668 | S2_above_val |
| S2 | log_price | bld10pct |  1.687e-01 | 2.07e-02 |   2537498 |  2700 | S2_above_cl_county |
| S2 | refi | bld10pct | -1.396e-02 | 7.87e-04 |  57211392 | 80554 | S2_above |
| S2 | refi | bld10pct | -5.447e-03 | 3.85e-04 |  57211392 | 80554 | S2_above_val |
| S2 | refi | bld10pct | -1.396e-02 | 1.85e-03 |  57211392 |  2923 | S2_above_cl_county |
| S2 | cashout | bld10pct | -4.291e-03 | 4.28e-04 |  57211392 | 80554 | S2_above |
| S2 | cashout | bld10pct | -1.814e-03 | 2.11e-04 |  57211392 | 80554 | S2_above_val |
| S2 | cashout | bld10pct | -4.291e-03 | 5.56e-04 |  57211392 |  2923 | S2_above_cl_county |
| S2 | to_rental | bld10pct | -1.821e-03 | 3.82e-04 |  44968943 | 80537 | S2_above |
| S2 | to_rental | bld10pct | -6.238e-04 | 1.90e-04 |  44968943 | 80537 | S2_above_val |
| S2 | to_rental | bld10pct | -1.821e-03 | 4.13e-04 |  44968943 |  2923 | S2_above_cl_county |
| S2 | appeal_proxy | bld10pct |  3.548e-03 | 7.00e-04 |  50098446 | 80412 | S2_above |
| S2 | appeal_proxy | bld10pct |  1.489e-03 | 3.49e-04 |  50098446 | 80412 | S2_above_val |
| S2 | appeal_proxy | bld10pct |  3.548e-03 | 1.24e-03 |  50098446 |  2917 | S2_above_cl_county |
| S2 | sale | bld10pct |  4.467e-04 | 2.43e-04 |  57211392 | 80554 | S2_logbill |
| S2 | sale | bld10pct |  1.992e-03 | 1.35e-04 |  57211392 | 80554 | S2_logbill_val |
| S2 | sale | bld10pct |  4.467e-04 | 5.18e-04 |  57211392 |  2923 | S2_logbill_cl_county |
| S2 | exit | bld10pct | -2.718e-04 | 2.08e-04 |  49060809 | 78995 | S2_logbill |
| S2 | exit | bld10pct |  3.474e-04 | 1.19e-04 |  49060809 | 78995 | S2_logbill_val |
| S2 | exit | bld10pct | -2.718e-04 | 1.74e-04 |  49060809 |  2917 | S2_logbill_cl_county |
| S2 | exit_prim | bld10pct | -1.207e-03 | 2.58e-04 |  49060809 | 78995 | S2_logbill |
| S2 | exit_prim | bld10pct |  3.605e-04 | 1.39e-04 |  49060809 | 78995 | S2_logbill_val |
| S2 | exit_prim | bld10pct | -1.207e-03 | 2.59e-04 |  49060809 |  2917 | S2_logbill_cl_county |
| S2 | log_price | bld10pct |  7.804e-02 | 6.02e-03 |   2537498 | 58668 | S2_logbill |
| S2 | log_price | bld10pct |  5.224e-02 | 3.47e-03 |   2537498 | 58668 | S2_logbill_val |
| S2 | log_price | bld10pct |  7.804e-02 | 1.52e-02 |   2537498 |  2700 | S2_logbill_cl_county |
| S2 | refi | bld10pct | -8.948e-03 | 1.67e-04 |  57211392 | 80554 | S2_logbill |
| S2 | refi | bld10pct | -4.996e-03 | 9.32e-05 |  57211392 | 80554 | S2_logbill_val |
| S2 | refi | bld10pct | -8.948e-03 | 6.14e-04 |  57211392 |  2923 | S2_logbill_cl_county |
| S2 | cashout | bld10pct | -1.828e-03 | 8.05e-05 |  57211392 | 80554 | S2_logbill |
| S2 | cashout | bld10pct | -1.110e-03 | 4.45e-05 |  57211392 | 80554 | S2_logbill_val |
| S2 | cashout | bld10pct | -1.828e-03 | 1.46e-04 |  57211392 |  2923 | S2_logbill_cl_county |
| S2 | to_rental | bld10pct | -3.248e-03 | 2.09e-04 |  44968943 | 80537 | S2_logbill |
| S2 | to_rental | bld10pct | -1.074e-03 | 1.04e-04 |  44968943 | 80537 | S2_logbill_val |
| S2 | to_rental | bld10pct | -3.248e-03 | 2.94e-04 |  44968943 |  2923 | S2_logbill_cl_county |
| S2 | appeal_proxy | bld10pct |  1.671e-03 | 1.74e-04 |  50098446 | 80412 | S2_logbill |
| S2 | appeal_proxy | bld10pct |  9.727e-04 | 9.68e-05 |  50098446 | 80412 | S2_logbill_val |
| S2 | appeal_proxy | bld10pct |  1.671e-03 | 3.49e-04 |  50098446 |  2917 | S2_logbill_cl_county |
| S3 | sale | bld10pct | -1.623e-02 | 9.59e-04 |  57211392 | 80554 | S3_rate |
| S3 | sale | bld10pct | -9.717e-03 | 9.76e-04 |  57091048 | 80554 | S3_rate_aug |
| S3 | sale | bld10pct | -1.623e-02 | 1.93e-03 |  57211392 |  2923 | S3_rate_cl_county |
| S3 | exit | bld10pct | -3.634e-03 | 2.63e-04 |  49060809 | 78995 | S3_rate |
| S3 | exit | bld10pct | -2.104e-03 | 2.77e-04 |  48991177 | 78995 | S3_rate_aug |
| S3 | exit | bld10pct | -3.634e-03 | 5.06e-04 |  49060809 |  2917 | S3_rate_cl_county |
| S3 | exit_prim | bld10pct | -8.078e-03 | 4.49e-04 |  49060809 | 78995 | S3_rate |
| S3 | exit_prim | bld10pct | -6.076e-03 | 4.72e-04 |  48991177 | 78995 | S3_rate_aug |
| S3 | exit_prim | bld10pct | -8.078e-03 | 9.30e-04 |  49060809 |  2917 | S3_rate_cl_county |
| S3 | log_price | bld10pct |  2.966e-02 | 3.69e-03 |   2537498 | 58668 | S3_rate |
| S3 | log_price | bld10pct |  2.507e-02 | 3.73e-03 |   2531220 | 58668 | S3_rate_aug |
| S3 | log_price | bld10pct |  2.966e-02 | 8.48e-03 |   2537498 |  2700 | S3_rate_cl_county |
| S3 | refi | bld10pct | -1.392e-03 | 2.28e-04 |  57211392 | 80554 | S3_rate |
| S3 | refi | bld10pct |  2.109e-03 | 3.35e-04 |  57091048 | 80554 | S3_rate_aug |
| S3 | refi | bld10pct | -1.392e-03 | 2.83e-04 |  57211392 |  2923 | S3_rate_cl_county |
| S3 | cashout | bld10pct |  4.963e-04 | 1.12e-04 |  57211392 | 80554 | S3_rate |
| S3 | cashout | bld10pct |  1.270e-03 | 1.51e-04 |  57091048 | 80554 | S3_rate_aug |
| S3 | cashout | bld10pct |  4.963e-04 | 1.21e-04 |  57211392 |  2923 | S3_rate_cl_county |
| S3 | to_rental | bld10pct | -8.660e-03 | 4.54e-04 |  44968943 | 80537 | S3_rate |
| S3 | to_rental | bld10pct | -8.490e-03 | 4.57e-04 |  44885506 | 80537 | S3_rate_aug |
| S3 | to_rental | bld10pct | -8.660e-03 | 9.15e-04 |  44968943 |  2923 | S3_rate_cl_county |
| S3 | appeal_proxy | bld10pct | -3.323e-05 | 3.89e-04 |  50098446 | 80412 | S3_rate |
| S3 | appeal_proxy | bld10pct |  1.607e-04 | 3.87e-04 |  50012616 | 80412 | S3_rate_aug |
| S3 | appeal_proxy | bld10pct | -3.323e-05 | 9.46e-04 |  50098446 |  2917 | S3_rate_cl_county |
| S3 | sale | bld10pct | -2.811e-03 | 8.47e-04 |  57211392 | 80554 | S3_above |
| S3 | sale | bld10pct |  5.065e-03 | 1.07e-03 |  57091048 | 80554 | S3_above_aug |
| S3 | sale | bld10pct | -2.811e-03 | 1.15e-03 |  57211392 |  2923 | S3_above_cl_county |
| S3 | exit | bld10pct |  6.497e-04 | 8.23e-04 |  49060809 | 78995 | S3_above |
| S3 | exit | bld10pct |  2.767e-03 | 8.38e-04 |  48991177 | 78995 | S3_above_aug |
| S3 | exit | bld10pct |  6.497e-04 | 8.70e-04 |  49060809 |  2917 | S3_above_cl_county |
| S3 | exit_prim | bld10pct | -1.151e-03 | 1.06e-03 |  49060809 | 78995 | S3_above |
| S3 | exit_prim | bld10pct |  1.356e-03 | 1.08e-03 |  48991177 | 78995 | S3_above_aug |
| S3 | exit_prim | bld10pct | -1.151e-03 | 1.10e-03 |  49060809 |  2917 | S3_above_cl_county |
| S3 | log_price | bld10pct |  8.959e-02 | 7.92e-03 |   2537498 | 58668 | S3_above |
| S3 | log_price | bld10pct |  8.147e-02 | 7.68e-03 |   2531220 | 58668 | S3_above_aug |
| S3 | log_price | bld10pct |  8.959e-02 | 1.34e-02 |   2537498 |  2700 | S3_above_cl_county |
| S3 | refi | bld10pct | -3.108e-04 | 1.04e-03 |  57211392 | 80554 | S3_above |
| S3 | refi | bld10pct | -3.630e-03 | 1.22e-03 |  57091048 | 80554 | S3_above_aug |
| S3 | refi | bld10pct | -3.108e-04 | 1.12e-03 |  57211392 |  2923 | S3_above_cl_county |
| S3 | cashout | bld10pct | -1.272e-04 | 5.77e-04 |  57211392 | 80554 | S3_above |
| S3 | cashout | bld10pct |  2.814e-04 | 6.25e-04 |  57091048 | 80554 | S3_above_aug |
| S3 | cashout | bld10pct | -1.272e-04 | 5.89e-04 |  57211392 |  2923 | S3_above_cl_county |
| S3 | to_rental | bld10pct | -2.120e-03 | 4.95e-04 |  44968943 | 80537 | S3_above |
| S3 | to_rental | bld10pct | -1.829e-03 | 4.94e-04 |  44885506 | 80537 | S3_above_aug |
| S3 | to_rental | bld10pct | -2.120e-03 | 6.34e-04 |  44968943 |  2923 | S3_above_cl_county |
| S3 | appeal_proxy | bld10pct |  9.486e-04 | 7.31e-04 |  50098446 | 80412 | S3_above |
| S3 | appeal_proxy | bld10pct |  1.121e-03 | 7.31e-04 |  50012616 | 80412 | S3_above_aug |
| S3 | appeal_proxy | bld10pct |  9.486e-04 | 7.42e-04 |  50098446 |  2917 | S3_above_cl_county |
| S3 | log_price | bld10pct | -3.122e-03 | 7.39e-03 |   2537498 | 58668 | S3_rate_rs |
| S3 | log_price | bld10pct | -1.731e-02 | 1.65e-02 |   2537498 | 58668 | S3_above_rs |
| S3 | sale | bld10pct |  1.062e-02 | 3.22e-03 |   7941184 | 48020 | S3_rate_pricesub |
| S3 | sale | bld10pct | -2.047e-02 | 3.13e-03 |   7941184 | 48020 | S3_rate_price |
| S3 | exit | bld10pct |  1.563e-02 | 1.20e-03 |   6159959 | 46940 | S3_rate_pricesub |
| S3 | exit | bld10pct |  1.289e-02 | 1.18e-03 |   6159959 | 46940 | S3_rate_price |
| S3 | refi | bld10pct | -7.258e-04 | 6.83e-04 |   7941184 | 48020 | S3_rate_pricesub |
| S3 | refi | bld10pct |  2.953e-03 | 6.96e-04 |   7941184 | 48020 | S3_rate_price |
| S3 | sale | bld10pct |  7.177e-03 | 5.30e-03 |   7941184 | 48020 | S3_above_pricesub |
| S3 | sale | bld10pct |  2.028e-01 | 9.63e-03 |   7941184 | 48020 | S3_above_price |
| S3 | exit | bld10pct |  5.170e-03 | 3.40e-03 |   6159959 | 46940 | S3_above_pricesub |
| S3 | exit | bld10pct |  4.920e-02 | 3.60e-03 |   6159959 | 46940 | S3_above_price |
| S3 | refi | bld10pct |  4.069e-03 | 2.84e-03 |   7941184 | 48020 | S3_above_pricesub |
| S3 | refi | bld10pct | -2.127e-02 | 2.64e-03 |   7941184 | 48020 | S3_above_price |
| S3 | sale | bld10pct | -1.853e-02 | 9.99e-04 |  92968512 | 80554 | S3_rate_ext |
| S3 | log_price | bld10pct |  4.457e-02 | 2.78e-03 |   3961515 | 63810 | S3_rate_ext |
| S3 | refi | bld10pct |  1.908e-03 | 1.82e-04 |  92968512 | 80554 | S3_rate_ext |
| S3 | sale | bld10pct | -3.510e-03 | 8.02e-04 |  92968512 | 80554 | S3_above_ext |
| S3 | log_price | bld10pct |  1.115e-01 | 8.01e-03 |   3961515 | 63810 | S3_above_ext |
| S3 | refi | bld10pct |  7.071e-04 | 8.99e-04 |  92968512 | 80554 | S3_above_ext |
| S10 | sale | bld10pct | -2.146e-02 | 1.08e-03 | 100119936 | 80554 | S10_rate16_pp |
| S10 | log_price | bld10pct |  4.394e-02 | 3.98e-03 |   4190245 | 64350 | S10_rate16_pp |
| S10 | refi | bld10pct | -2.627e-03 | 2.69e-04 | 100119936 | 80554 | S10_rate16_pp |
| S10 | cashout | bld10pct |  2.502e-04 | 1.33e-04 | 100119936 | 80554 | S10_rate16_pp |
| S10 | sale | bld10pct | -5.652e-03 | 9.22e-04 | 100119936 | 80554 | S10_above16 |
| S10 | log_price | bld10pct |  1.108e-01 | 1.03e-02 |   4190245 | 64350 | S10_above16 |
| S10 | refi | bld10pct | -2.084e-03 | 1.10e-03 | 100119936 | 80554 | S10_above16 |
| S10 | cashout | bld10pct | -1.018e-04 | 5.86e-04 | 100119936 | 80554 | S10_above16 |
| S10 | sale | bld10pct | -2.565e-02 | 1.45e-03 |  93209690 | 76915 | S10_mid24 |
| S10 | log_price | bld10pct |  1.785e-01 | 7.31e-03 |   3953555 | 61506 | S10_mid24 |
| S10 | refi | bld10pct |  6.718e-04 | 8.09e-04 |  93209690 | 76915 | S10_mid24 |
| S10 | cashout | bld10pct | -1.529e-03 | 4.42e-04 |  93209690 | 76915 | S10_mid24 |
| S10 | sale | bld10pct | -2.943e-02 | 1.63e-02 |     19197 | NA | S10_kink40k |
| S10 | log_price | bld10pct |  4.378e-01 | 6.08e-01 |       573 | NA | S10_kink40k |
| S13 | sale | bld10pct |  2.850e-03 | 2.86e-04 |  35534330 | 66046 | S13_incumbent |
| S13 | sale | bld10pct | -2.647e-02 | 2.01e-03 |  35534330 | 66046 | S13_buyer_diff |
| S13 | refi | bld10pct |  3.031e-03 | 3.33e-04 |  35534330 | 66046 | S13_incumbent |
| S13 | refi | bld10pct | -2.936e-03 | 8.00e-04 |  35534330 | 66046 | S13_buyer_diff |
| S4 | sale | bld10pct | -2.170e-02 | 1.16e-03 |  69903592 | 84233 | S4_rate |
| S4 | sale | bld10pct | -3.647e-03 | 7.32e-04 |  30661632 | 72310 | S4_rate_agree |
| S4 | log_price | bld10pct |  4.230e-02 | 6.76e-03 |   3062961 | 62817 | S4_rate |
| S4 | log_price | bld10pct |  4.398e-02 | 9.05e-03 |   1058485 | 48962 | S4_rate_agree |
| S4 | refi | bld10pct | -2.087e-03 | 3.08e-04 |  69903592 | 84233 | S4_rate |
| S4 | refi | bld10pct | -4.480e-03 | 5.25e-04 |  30661632 | 72310 | S4_rate_agree |
| S4 | to_rental | bld10pct | -6.222e-03 | 5.45e-04 |  54416129 | 84222 | S4_rate |
| S4 | to_rental | bld10pct |  4.387e-04 | 6.32e-04 |  24469542 | 72296 | S4_rate_agree |
| S4 | sale | bld10pct | -3.336e-03 | 3.37e-03 |  69903592 | 84233 | S4_above |
| S4 | sale | bld10pct |  1.727e-03 | 3.48e-03 |  30661632 | 72310 | S4_above_agree |
| S4 | log_price | bld10pct | -2.827e-02 | 1.99e-02 |   3062961 | 62817 | S4_above |
| S4 | log_price | bld10pct | -4.263e-02 | 2.47e-02 |   1058485 | 48962 | S4_above_agree |
| S4 | refi | bld10pct | -4.813e-03 | 2.28e-03 |  69903592 | 84233 | S4_above |
| S4 | refi | bld10pct | -5.428e-03 | 2.99e-03 |  30661632 | 72310 | S4_above_agree |
| S4 | to_rental | bld10pct |  1.977e-03 | 2.37e-03 |  54416129 | 84222 | S4_above |
| S4 | to_rental | bld10pct |  4.713e-03 | 2.42e-03 |  24469542 | 72296 | S4_above_agree |
| S7 | sale | bld10pct |  1.370e-02 | 1.32e-02 |    504884 | NA | S7_notax |
| S7 | sale | bld10pct | -2.638e-02 | 5.84e-02 |    210110 | NA | S7_notax_h05 |
| S7 | sale | bld10pct |  4.067e-03 | 9.51e-03 |   1379140 | NA | S7_notax_h2 |
| S7 | log_price | bld10pct | -7.500e-02 | 1.53e-01 |     30593 | NA | S7_notax |
| S7 | log_price | bld10pct | -9.507e-03 | 7.31e-01 |     12983 | NA | S7_notax_h05 |
| S7 | log_price | bld10pct | -6.437e-02 | 1.05e-01 |     75790 | NA | S7_notax_h2 |
| S7 | exit_prim | bld10pct |  2.971e-02 | 1.67e-02 |    423262 | NA | S7_notax |
| S7 | exit_prim | bld10pct |  5.889e-02 | 7.26e-02 |    221852 | NA | S7_notax_h05 |
| S7 | exit_prim | bld10pct |  3.862e-03 | 8.89e-03 |   1817365 | NA | S7_notax_h2 |
| S7 | refi | bld10pct | -1.613e-02 | 1.09e-02 |    512534 | NA | S7_notax |
| S7 | refi | bld10pct | -2.397e-02 | 6.95e-02 |    185985 | NA | S7_notax_h05 |
| S7 | refi | bld10pct | -5.080e-03 | 8.42e-03 |   1099095 | NA | S7_notax_h2 |
| S7 | sale | bld10pct |  3.572e-02 | 2.10e-02 |    262966 | NA | S7_notax_itm |
| S7 | sale | bld10pct | -1.692e-03 | 9.47e-02 |    117815 | NA | S7_notax_itm_h05 |
| S7 | sale | bld10pct |  5.312e-03 | 1.57e-02 |    580755 | NA | S7_notax_itm_h2 |
| S7 | log_price | bld10pct | -5.393e-02 | 1.40e-01 |     23242 | NA | S7_notax_itm |
| S7 | log_price | bld10pct | -2.543e-01 | 6.97e-01 |     10754 | NA | S7_notax_itm_h05 |
| S7 | log_price | bld10pct | -8.166e-02 | 8.81e-02 |     58867 | NA | S7_notax_itm_h2 |
| S7 | exit_prim | bld10pct |  5.279e-02 | 2.61e-02 |    236678 | NA | S7_notax_itm |
| S7 | exit_prim | bld10pct |  1.205e-01 | 1.39e-01 |    121268 | NA | S7_notax_itm_h05 |
| S7 | exit_prim | bld10pct |  9.554e-03 | 1.73e-02 |    646106 | NA | S7_notax_itm_h2 |
| S7 | refi | bld10pct | -2.431e-02 | 1.37e-02 |    373576 | NA | S7_notax_itm |
| S7 | refi | bld10pct | -3.035e-02 | 8.00e-02 |    131695 | NA | S7_notax_itm_h05 |
| S7 | refi | bld10pct | -9.664e-03 | 1.05e-02 |    683805 | NA | S7_notax_itm_h2 |
| S7 | sale | bld10pct |  1.592e-02 | 1.44e-02 |    473455 | NA | S7_notax_tnnh |
| S7 | sale | bld10pct | -2.419e-02 | 6.68e-02 |    203460 | NA | S7_notax_tnnh_h05 |
| S7 | sale | bld10pct |  4.309e-03 | 1.04e-02 |   1237570 | NA | S7_notax_tnnh_h2 |
| S7 | log_price | bld10pct | -2.930e-02 | 1.80e-01 |     30997 | NA | S7_notax_tnnh |
| S7 | log_price | bld10pct |  3.452e-02 | 8.27e-01 |     11814 | NA | S7_notax_tnnh_h05 |
| S7 | log_price | bld10pct | -2.603e-02 | 1.26e-01 |     63232 | NA | S7_notax_tnnh_h2 |
| S7 | exit_prim | bld10pct |  3.149e-02 | 1.61e-02 |    469411 | NA | S7_notax_tnnh |
| S7 | exit_prim | bld10pct |  5.255e-02 | 7.94e-02 |    218187 | NA | S7_notax_tnnh_h05 |
| S7 | exit_prim | bld10pct |  6.118e-03 | 9.73e-03 |   1630234 | NA | S7_notax_tnnh_h2 |
| S7 | refi | bld10pct | -1.365e-02 | 8.84e-03 |    639951 | NA | S7_notax_tnnh |
| S7 | refi | bld10pct | -1.861e-02 | 5.36e-02 |    233660 | NA | S7_notax_tnnh_h05 |
| S7 | refi | bld10pct | -2.754e-03 | 6.66e-03 |   1605380 | NA | S7_notax_tnnh_h2 |
| S7 | sale | bld10pct |  5.114e-03 | 7.17e-03 |   2138801 | NA | S7_all |
| S7 | sale | bld10pct | -1.698e-02 | 3.81e-02 |    831365 | NA | S7_all_h05 |
| S7 | sale | bld10pct |  1.349e-03 | 5.25e-03 |   4332555 | NA | S7_all_h2 |
| S7 | log_price | bld10pct |  4.984e-03 | 6.47e-02 |    128136 | NA | S7_all |
| S7 | log_price | bld10pct | -2.281e-02 | 2.65e-01 |     65622 | NA | S7_all_h05 |
| S7 | log_price | bld10pct |  3.588e-03 | 3.99e-02 |    487254 | NA | S7_all_h2 |
| S7 | exit_prim | bld10pct |  1.689e-02 | 9.97e-03 |   1902714 | NA | S7_all |
| S7 | exit_prim | bld10pct |  3.351e-02 | 5.68e-02 |    772781 | NA | S7_all_h05 |
| S7 | exit_prim | bld10pct |  5.680e-03 | 7.26e-03 |   4107479 | NA | S7_all_h2 |
| S7 | refi | bld10pct |  4.352e-03 | 5.48e-03 |   2809981 | NA | S7_all |
| S7 | refi | bld10pct | -3.615e-03 | 3.22e-02 |    984955 | NA | S7_all_h05 |
| S7 | refi | bld10pct |  1.018e-03 | 4.21e-03 |   5720165 | NA | S7_all_h2 |
| S7 | bunching_below_10k | bld10pct | -2.767e-03 | NA |    261566 | NA | S7_bunch_2019_vs_2016 |
| S7 | sale | bld10pct | -5.844e-03 | 9.65e-03 |    808949 | NA | S7_notax_salesadj |
| S7 | log_price | bld10pct |  1.680e-01 | 1.23e-01 |     40158 | NA | S7_notax_salesadj |
| S8 | sale | bld10pct |  3.785e-03 | 5.69e-03 |  25514320 | 70613 | S8_pooled |
| S8 | log_price | bld10pct |  3.414e-03 | 2.60e-02 |    981904 | 46461 | S8_pooled |
| S8 | exit_prim | bld10pct | -3.732e-03 | 7.92e-03 |  22657870 | 69227 | S8_pooled |
| S8 | refi | bld10pct |  6.546e-04 | 6.21e-03 |  25514320 | 70613 | S8_pooled |
| S8 | sale | bld10pct |  6.548e-03 | 6.07e-03 |   9825944 | 46408 | S8_strict |
| S8 | log_price | bld10pct | -3.110e-03 | 2.78e-02 |    420655 | 31059 | S8_strict |
| S8 | exit_prim | bld10pct | -6.223e-03 | 8.01e-03 |   8772966 | 45418 | S8_strict |
| S8 | refi | bld10pct |  2.158e-03 | 6.38e-03 |   9825944 | 46408 | S8_strict |
| S9a | sale | bld10pct |  1.060e-03 | 1.73e-03 |   1534720 |   480 | S9a_reval |
| S9a | sale | bld10pct |  7.096e-03 | 5.09e-03 |   1534720 |   480 | S9a_cross |
| S9a | log_price | bld10pct |  1.057e-02 | 1.12e-02 |     61489 |   480 | S9a_reval |
| S9a | log_price | bld10pct |  1.996e-01 | 6.86e-02 |     61489 |   480 | S9a_cross |
| S9a | exit_prim | bld10pct | -9.208e-05 | 1.84e-03 |   1261677 |   480 | S9a_reval |
| S9a | exit_prim | bld10pct | -9.013e-04 | 7.15e-03 |   1261677 |   480 | S9a_cross |
| S9a | refi | bld10pct |  1.492e-03 | 2.69e-03 |   1534720 |   480 | S9a_reval |
| S9a | refi | bld10pct | -1.436e-02 | 7.22e-03 |   1534720 |   480 | S9a_cross |
| S9b | sale | bld10pct |  1.910e-02 | 1.70e-03 |   5654168 | 24366 | S9b_base |
| S9b | log_price | bld10pct |  3.107e-02 | 4.34e+03 |    441638 | 19801 | S9b_base |
| S9b | refi | bld10pct | -2.678e-02 | 2.05e-03 |   5654168 | 24366 | S9b_base |
| S10 | sale | bld1pct | -2.021e-02 | 1.21e-03 |   9876524 | 48932 | S10_rate16_pp |
| S10 | log_price | bld1pct |  6.537e-02 | 1.12e-02 |    317928 | 33124 | S10_rate16_pp |
| S10 | refi | bld1pct | -2.800e-03 | 5.55e-04 |   9876524 | 48932 | S10_rate16_pp |
| S10 | cashout | bld1pct |  7.017e-05 | 3.63e-04 |   9876524 | 48932 | S10_rate16_pp |
| S10 | sale | bld1pct | -8.374e-03 | 2.89e-03 |   9876524 | 48932 | S10_above16 |
| S10 | log_price | bld1pct |  1.187e-01 | 2.61e-02 |    317928 | 33124 | S10_above16 |
| S10 | refi | bld1pct | -4.057e-04 | 2.83e-03 |   9876524 | 48932 | S10_above16 |
| S10 | cashout | bld1pct |  2.859e-05 | 1.81e-03 |   9876524 | 48932 | S10_above16 |
| S10 | sale | bld1pct | -2.517e-02 | 2.54e-03 |   9157932 | 46346 | S10_mid24 |
| S10 | log_price | bld1pct |  2.128e-01 | 2.34e-02 |    298491 | 31624 | S10_mid24 |
| S10 | refi | bld1pct |  9.026e-04 | 2.12e-03 |   9157932 | 46346 | S10_mid24 |
| S10 | cashout | bld1pct | -1.943e-03 | 1.39e-03 |   9157932 | 46346 | S10_mid24 |
| S10 | sale | bld1pct | -5.094e-02 | 8.53e-02 |      1459 | NA | S10_kink40k |
| S10 | sale | bld1pct | -1.574e-02 | 8.82e-03 |   9157932 | 46346 | S10_above24_40k |
| S10 | log_price | bld1pct |  5.412e-01 | 1.31e-01 |    298491 | 31624 | S10_above24_40k |
| S10 | refi | bld1pct |  1.009e-03 | 8.90e-03 |   9157932 | 46346 | S10_above24_40k |
| S13 | sale | bld1pct |  2.490e-03 | 6.30e-04 |   3209650 | 33961 | S13_incumbent |
| S13 | sale | bld1pct | -2.885e-02 | 3.77e-03 |   3209650 | 33961 | S13_buyer_diff |
| S13 | refi | bld1pct |  1.730e-03 | 1.11e-03 |   3209650 | 33961 | S13_incumbent |
| S13 | refi | bld1pct |  3.343e-03 | 2.67e-03 |   3209650 | 33961 | S13_buyer_diff |
| S3 | sale | bld1pct |  3.805e-03 | 4.55e-04 |   4652000 | 46615 | S3_rate_inc |
| S3 | exit_prim | bld1pct | -3.059e-03 | 1.02e-03 |   4031096 | 45354 | S3_rate_inc |
| S3 | refi | bld1pct | -2.035e-03 | 5.24e-04 |   4652000 | 46615 | S3_rate_inc |
| S3 | to_rental | bld1pct | -6.444e-03 | 6.71e-04 |   3776178 | 46608 | S3_rate_inc |
| S3 | sale | bld1pct |  1.144e-03 | 1.54e-03 |   4652000 | 46615 | S3_above_inc |
| S3 | exit_prim | bld1pct | -9.932e-04 | 3.11e-03 |   4031096 | 45354 | S3_above_inc |
| S3 | refi | bld1pct |  1.859e-03 | 3.05e-03 |   4652000 | 46615 | S3_above_inc |
| S3 | to_rental | bld1pct | -8.701e-04 | 1.21e-03 |   3776178 | 46608 | S3_above_inc |
| S4 | sale | bld1pct | -1.947e-02 | 2.03e-03 |   6821464 | 53198 | S4_rate |
| S4 | sale | bld1pct | -2.446e-03 | 1.83e-03 |   2911000 | 39946 | S4_rate_agree |
| S4 | log_price | bld1pct | -1.398e-02 | 1.98e-02 |    218977 | 31159 | S4_rate |
| S4 | log_price | bld1pct | -4.509e-02 | 2.57e-02 |     57895 | 18878 | S4_rate_agree |
| S4 | refi | bld1pct | -3.091e-03 | 9.03e-04 |   6821464 | 53198 | S4_rate |
| S4 | refi | bld1pct | -4.925e-03 | 1.24e-03 |   2911000 | 39946 | S4_rate_agree |
| S4 | to_rental | bld1pct | -5.535e-03 | 1.36e-03 |   5267254 | 53183 | S4_rate |
| S4 | to_rental | bld1pct | -1.492e-04 | 1.49e-03 |   2298505 | 39929 | S4_rate_agree |
| S4 | sale | bld1pct | -6.013e-03 | 9.46e-03 |   6821464 | 53198 | S4_above |
| S4 | sale | bld1pct |  4.260e-03 | 9.75e-03 |   2911000 | 39946 | S4_above_agree |
| S4 | log_price | bld1pct |  2.823e-02 | 5.78e-02 |    218977 | 31159 | S4_above |
| S4 | log_price | bld1pct | -9.511e-02 | 6.56e-02 |     57895 | 18878 | S4_above_agree |
| S4 | refi | bld1pct | -9.467e-03 | 7.86e-03 |   6821464 | 53198 | S4_above |
| S4 | refi | bld1pct | -1.596e-02 | 9.20e-03 |   2911000 | 39946 | S4_above_agree |
| S4 | to_rental | bld1pct | -4.475e-03 | 9.20e-03 |   5267254 | 53183 | S4_above |
| S4 | to_rental | bld1pct | -4.802e-03 | 9.57e-03 |   2298505 | 39929 | S4_above_agree |
| S5 | sale | bld1pct | -2.476e-02 | 2.62e-03 |    959072 |  8662 | S5_rate_050 |
| S5 | log_price | bld1pct |  5.642e-02 | 2.38e-02 |     17968 |  6010 | S5_rate_050 |
| S5 | exit_prim | bld1pct | -1.603e-02 | 3.29e-03 |    817716 |  8560 | S5_rate_050 |
| S5 | refi | bld1pct | -5.279e-03 | 1.67e-03 |    959072 |  8662 | S5_rate_050 |
| S5 | sale | bld1pct | -2.470e-02 | 3.69e-03 |    498792 |  7499 | S5_rate_025 |
| S5 | log_price | bld1pct |  4.336e-02 | 3.35e-02 |      7844 |  4889 | S5_rate_025 |
| S5 | exit_prim | bld1pct | -2.229e-02 | 4.30e-03 |    422086 |  7396 | S5_rate_025 |
| S5 | refi | bld1pct | -5.883e-03 | 2.38e-03 |    498792 |  7499 | S5_rate_025 |
| S6 | sale | bld1pct | -4.805e-02 | 4.30e-03 |   5631544 | 48728 | S6_base |
| S6 | sale | bld1pct | -6.865e-02 | 4.41e-03 |   5631544 | 48728 | S6_coarse |
| S6 | sale | bld1pct | -3.891e-02 | 4.54e-03 |   5631544 | 48728 | S6_fine |
| S6 | log_price | bld1pct |  9.655e-03 | 1.99e-02 |    195614 | 28086 | S6_base |
| S6 | log_price | bld1pct |  5.179e-02 | 1.93e-02 |    195614 | 28086 | S6_coarse |
| S6 | log_price | bld1pct |  1.112e-02 | 2.23e-02 |    195478 | 28086 | S6_fine |
| S6 | exit_prim | bld1pct | -2.018e-02 | 5.24e-03 |   4804167 | 47512 | S6_base |
| S6 | exit_prim | bld1pct | -1.369e-02 | 5.15e-03 |   4804167 | 47512 | S6_coarse |
| S6 | exit_prim | bld1pct | -1.783e-02 | 5.46e-03 |   4804167 | 47512 | S6_fine |
| S6 | refi | bld1pct | -2.297e-03 | 4.00e-03 |   5631544 | 48728 | S6_base |
| S6 | refi | bld1pct |  4.893e-03 | 4.48e-03 |   5631544 | 48728 | S6_coarse |
| S6 | refi | bld1pct |  1.129e-02 | 4.16e-03 |   5631544 | 48728 | S6_fine |
| S6 | sale | bld1pct | -6.857e-02 | 5.44e-03 |   5631544 | 48728 | S6_d_cap |
| S6 | sale | bld1pct | -4.585e-02 | 4.31e-03 |   5631544 | 48728 | S6_d_std |
| S6 | log_price | bld1pct |  1.941e-02 | 2.23e-02 |    195614 | 28086 | S6_d_cap |
| S6 | log_price | bld1pct |  8.193e-03 | 2.00e-02 |    195614 | 28086 | S6_d_std |
| S7 | sale | bld1pct |  1.422e-02 | 3.48e-02 |     44354 | NA | S7_notax |
| S7 | sale | bld1pct | -9.935e-03 | 1.86e-01 |     17405 | NA | S7_notax_h05 |
| S7 | sale | bld1pct |  6.638e-03 | 2.46e-02 |     98220 | NA | S7_notax_h2 |
| S7 | log_price | bld1pct | -3.036e-01 | 5.55e-01 |      1960 | NA | S7_notax |
| S7 | log_price | bld1pct | -1.197e+00 | 2.83e+00 |      1086 | NA | S7_notax_h05 |
| S7 | log_price | bld1pct | -9.512e-02 | 3.62e-01 |      5636 | NA | S7_notax_h2 |
| S7 | exit_prim | bld1pct | -1.028e-03 | 5.38e-02 |     42172 | NA | S7_notax |
| S7 | exit_prim | bld1pct | -9.219e-02 | 2.94e-01 |     15124 | NA | S7_notax_h05 |
| S7 | exit_prim | bld1pct | -4.835e-03 | 3.82e-02 |     85592 | NA | S7_notax_h2 |
| S7 | refi | bld1pct | -2.698e-03 | 2.92e-02 |     49601 | NA | S7_notax |
| S7 | refi | bld1pct |  2.401e-02 | 1.42e-01 |     19820 | NA | S7_notax_h05 |
| S7 | refi | bld1pct |  2.663e-03 | 1.86e-02 |    125710 | NA | S7_notax_h2 |
| S7 | sale | bld1pct |  1.313e-02 | 4.45e-02 |     28031 | NA | S7_notax_itm |
| S7 | sale | bld1pct |  4.334e-03 | 2.28e-01 |     11690 | NA | S7_notax_itm_h05 |
| S7 | sale | bld1pct |  1.206e-02 | 3.49e-02 |     56125 | NA | S7_notax_itm_h2 |
| S7 | log_price | bld1pct |  9.559e-02 | 5.31e-01 |      2041 | NA | S7_notax_itm |
| S7 | log_price | bld1pct | -1.485e+00 | 3.14e+00 |       953 | NA | S7_notax_itm_h05 |
| S7 | log_price | bld1pct | -6.742e-02 | 3.24e-01 |      4819 | NA | S7_notax_itm_h2 |
| S7 | exit_prim | bld1pct |  2.261e-02 | 6.63e-02 |     25237 | NA | S7_notax_itm |
| S7 | exit_prim | bld1pct | -9.370e-03 | 3.33e-01 |     11562 | NA | S7_notax_itm_h05 |
| S7 | exit_prim | bld1pct |  8.558e-03 | 4.03e-02 |     59151 | NA | S7_notax_itm_h2 |
| S7 | refi | bld1pct |  1.078e-02 | 2.63e-02 |     36927 | NA | S7_notax_itm |
| S7 | refi | bld1pct | -1.777e-02 | 1.31e-01 |     17270 | NA | S7_notax_itm_h05 |
| S7 | refi | bld1pct | -2.394e-03 | 1.60e-02 |    102500 | NA | S7_notax_itm_h2 |
| S7 | sale | bld1pct |  1.753e-02 | 3.28e-02 |     48605 | NA | S7_notax_tnnh |
| S7 | sale | bld1pct |  4.548e-03 | 1.77e-01 |     19040 | NA | S7_notax_tnnh_h05 |
| S7 | sale | bld1pct |  8.643e-03 | 2.30e-02 |    111005 | NA | S7_notax_tnnh_h2 |
| S7 | log_price | bld1pct | -5.424e-02 | 6.17e-01 |      2065 | NA | S7_notax_tnnh |
| S7 | log_price | bld1pct | -1.516e+00 | 3.14e+00 |      1000 | NA | S7_notax_tnnh_h05 |
| S7 | log_price | bld1pct | -9.220e-02 | 4.16e-01 |      4890 | NA | S7_notax_tnnh_h2 |
| S7 | exit_prim | bld1pct |  1.179e-02 | 5.25e-02 |     43568 | NA | S7_notax_tnnh |
| S7 | exit_prim | bld1pct | -7.387e-02 | 2.97e-01 |     15989 | NA | S7_notax_tnnh_h05 |
| S7 | exit_prim | bld1pct |  4.360e-03 | 3.62e-02 |     91300 | NA | S7_notax_tnnh_h2 |
| S7 | refi | bld1pct |  6.503e-03 | 2.66e-02 |     58758 | NA | S7_notax_tnnh |
| S7 | refi | bld1pct |  2.273e-02 | 1.27e-01 |     21880 | NA | S7_notax_tnnh_h05 |
| S7 | refi | bld1pct |  6.194e-03 | 1.74e-02 |    145100 | NA | S7_notax_tnnh_h2 |
| S7 | sale | bld1pct |  2.282e-02 | 1.60e-02 |    250713 | NA | S7_all |
| S7 | sale | bld1pct | -5.059e-02 | 9.75e-02 |     81385 | NA | S7_all_h05 |
| S7 | sale | bld1pct |  5.564e-03 | 1.30e-02 |    412700 | NA | S7_all_h2 |
| S7 | log_price | bld1pct | -3.905e-05 | 2.05e-01 |     11400 | NA | S7_all |
| S7 | log_price | bld1pct | -1.302e-01 | 8.92e-01 |      5886 | NA | S7_all_h05 |
| S7 | log_price | bld1pct |  5.055e-02 | 1.26e-01 |     38260 | NA | S7_all_h2 |
| S7 | exit_prim | bld1pct | -3.678e-04 | 2.24e-02 |    211133 | NA | S7_all |
| S7 | exit_prim | bld1pct | -5.125e-03 | 7.37e-02 |    119822 | NA | S7_all_h05 |
| S7 | exit_prim | bld1pct | -1.067e-03 | 1.09e-02 |    914469 | NA | S7_all_h2 |
| S7 | refi | bld1pct |  1.064e-02 | 1.68e-02 |    220845 | NA | S7_all |
| S7 | refi | bld1pct |  8.378e-03 | 9.40e-02 |     88735 | NA | S7_all_h05 |
| S7 | refi | bld1pct |  1.261e-03 | 1.21e-02 |    475070 | NA | S7_all_h2 |
| S7 | bunching_below_10k | bld1pct | -9.990e-03 | NA |     26039 | NA | S7_bunch_2019_vs_2016 |
| S7 | sale | bld1pct | -2.274e-02 | 2.16e-02 |     76555 | NA | S7_notax_salesadj |
| S7 | log_price | bld1pct | -1.383e-01 | 3.65e-01 |      5116 | NA | S7_notax_salesadj |
| S8 | sale | bld1pct |  2.701e-02 | 2.11e-02 |   2417392 | 37679 | S8_pooled |
| S8 | log_price | bld1pct |  2.579e-01 | 1.13e-01 |     57901 | 18434 | S8_pooled |
| S8 | exit_prim | bld1pct |  2.619e-02 | 2.98e-02 |   2134142 | 36785 | S8_pooled |
| S8 | refi | bld1pct | -1.126e-02 | 2.15e-02 |   2417392 | 37679 | S8_pooled |
| S8 | sale | bld1pct |  2.859e-02 | 2.15e-02 |    893736 | 20225 | S8_strict |
| S8 | log_price | bld1pct |  2.316e-01 | 1.24e-01 |     21118 | 10538 | S8_strict |
| S8 | exit_prim | bld1pct |  1.777e-02 | 3.03e-02 |    794014 | 19806 | S8_strict |
| S8 | refi | bld1pct | -4.116e-03 | 2.25e-02 |    893736 | 20225 | S8_strict |
| S9a | sale | bld1pct | -2.062e-03 | 4.81e-03 |    121891 |   213 | S9a_reval |
| S9a | sale | bld1pct |  6.714e-04 | 1.77e-02 |    121891 |   213 | S9a_cross |
| S9a | log_price | bld1pct |  4.235e-02 | 3.68e-02 |      3914 |   212 | S9a_reval |
| S9a | log_price | bld1pct |  2.271e-01 | 1.32e-01 |      3914 |   212 | S9a_cross |
| S9a | exit_prim | bld1pct | -7.382e-03 | 5.68e-03 |    100390 |   213 | S9a_reval |
| S9a | exit_prim | bld1pct | -6.193e-03 | 2.08e-02 |    100390 |   213 | S9a_cross |
| S9a | refi | bld1pct |  8.817e-03 | 6.80e-03 |    121891 |   213 | S9a_reval |
| S9a | refi | bld1pct |  1.652e-02 | 1.74e-02 |    121891 |   213 | S9a_cross |
| S9b | sale | bld1pct |  2.716e-02 | 6.60e-03 |    464528 | 11207 | S9b_base |
| S9b | refi | bld1pct | -2.411e-02 | 8.44e-03 |    464528 | 11207 | S9b_base |

## Appendix B. Figures

Event-study figures (PNG + CSV of coefficients) are in `figures/`, named `es_<strategy>_<outcome>_<spec_id>`; descriptive figures `desc_bill16_hist.png`, `desc_rate16_hist.png`, `s7_density_10k.png`.
