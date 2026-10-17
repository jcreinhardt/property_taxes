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
