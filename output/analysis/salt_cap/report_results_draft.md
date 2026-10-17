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
