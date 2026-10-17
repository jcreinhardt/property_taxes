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
