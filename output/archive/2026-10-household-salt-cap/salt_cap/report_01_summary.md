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
