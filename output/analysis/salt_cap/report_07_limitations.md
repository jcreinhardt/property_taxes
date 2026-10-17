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
