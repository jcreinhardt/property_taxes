
## 2026-10-16 — Cluster 10% run of the remaining strategies (Bouchet, `run_r_10pct.sbatch`, 16 CPUs / 320 GB, `SALT_PART` part files)

Jobs submitted 16:49 EDT: S4 29771811, S8 29771812, S7 29771813, S9 29771814, S10 29771815, S13 29771816 (afterany:29766396 = r10_s3).

### Fix 1 — `s9_assessment.R`, S9b `log_price` (job 29771814 failed, `Error in solve.default(V): 'a' is 0-diml`)
The hedonic `log_price` specification used fixed effects `bgk^year + cohort^ca^year`. Since `recent` is a function of `cohort`,
`cohort^ca^year` absorbs `recent × CA × year` exactly; fixest removed all `recent_ca` coefficients and the pre-period test had
an empty matrix. Changed the `log_price` fixed effects to `bgk^year + cohort^ca` (time-invariant cohort × state-group effect in
place of the parcel FE). The DDD term and the `sale`/`refi` specifications (`clip + bgk^year`) are unchanged. S9a results were
already written before the crash and are recomputed identically by the resubmission (S9 part file rows are replaced by key).

### Note — S9b `log_price` (job 29773585, exit 0) is not identified; row kept but uninformative
`S9b_base` for `log_price` returns 0.031 with SE 4,344 and `es_S9b_log_price_S9b_base.csv` has no 2012/2014/2015 coefficients
(removed as collinear by fixest). Reason: `buy_year` is the last purchase up to 2016, so every parcel that sells at arm's length
in 2012–2016 is in a "recent" cohort by construction (recent = 1 for all pre-2017 sales), which makes `recent × CA × year`
collinear with the block-group × year fixed effects in the pre-period. This is a feature of the pre-specified cohort definition,
not a coding error, so the specification was not changed; the row should be read as "not identified". `sale` and `refi`
(parcel FE, all years) are unaffected.
