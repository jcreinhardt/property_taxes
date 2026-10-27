# The 2015→16 and 2017→18 turnover spikes in Data Axle are file cut-off (snapshot timing) artefacts

Residential addresses, 1% building sample (`da_bld1pct` + `tax_bld1pct` + `deeds_bld1pct` + `da_bld1pct_fampos`), Bouchet, 2026-10-16.
SQL in this folder: `spike_decomp.sql` (panel, decomposition, geography, field changes, fid fates), `spike_newfids.sql` (brand-new IDs by year/state,
1% family sample), `spike_timing.sql` (recency dates, sales capture by month; its LOR block overflowed on UINT16 and was re-run as
`spike_lor_increments.sql`). Logs `spike_*_<jobid>.out`. All CSVs here are the job outputs.

## Bottom line

The annual Data Axle files are **not cut 12 months apart**. The file's effective cut-off date (last date of updates it contains) is:

| File | 2012 | 2013 | 2014 | 2015 | 2016 | 2017 | 2018 | 2019 | 2020 | 2021 |
|---|---|---|---|---|---|---|---|---|---|---|
| `max(recency_date)` | 2012-03 | 2013-03 | 2014-03 | 2015-04 | **2016-06** | 2017-05 | **2018-07** | 2019-06 | 2020-06 | 2021-06 |
| Cut-off from sales capture (month where the share of arm's-length sales already showing a new primary falls from ~80% to ~20%) | | mid-Mar | mid-Mar | mid-Apr | **mid-Jun** | mid-May | **early Jul** | mid-Jun | mid-Jun | |
| Interval since previous file (months, from `max(recency_date)`) | | 12 | 12 | 13 | **14** | **11** | **14** | **11** | 12 | 12 |

The 2015→16 pair therefore covers ~14 months of updates (Apr 2015–Jun 2016) and 2017→18 ~14 months (May 2017–Jul 2018), while the
following pairs 2016→17 and 2018→19 cover only ~11 months. Everything move-like scales with the interval: primary-ID changes, tracked
move-ins and move-outs, new IDs with LOR reset, disappearances, brand-new family IDs. Nothing specific to re-keying, address
re-standardisation, geocoding or particular states is behind the spikes. Annualising by the interval removes both spikes:

| Pair t→t+1 | ID change, all residential, backfilled (%) | ID change, 1 primary, both observed (%) | Interval (months) | Annualised, 1 primary (% per 12 months) | Family-level strict mover rate from dataaxle.md (%) | annualised |
|---|---|---|---|---|---|---|
| 2012→13 | 11.43 | 10.66 | 12 | 10.7 | | |
| 2013→14 | 12.19 | 10.74 | 12 | 10.7 | | |
| 2014→15 | 13.15 | 11.41 | 13 | 10.5 | | |
| **2015→16** | **15.46** | **13.41** | **14** | **11.5** | **4.80** | **4.1** |
| 2016→17 | 12.70 | 10.65 | 11 | 11.6 | 3.58 | 3.9 |
| **2017→18** | **15.17** | **13.38** | **14** | **11.5** | **4.69** | **4.0** |
| 2018→19 | 12.65 | 10.91 | 11 | 11.9 | 3.28 | 3.6 |
| 2019→20 | 12.84 | 11.12 | 12 | 11.1 | | |
| 2020→21 | 14.29 | 12.98 | 12 | 13.0 | | |

Annualised address turnover is a flat 10.5–11.9% from 2012 to 2019 (12 months of updates ≈ 11–12% of residential addresses get a new
primary record), and 2020→21 is the only genuine rise (Covid). The two-year sums are smooth without any correction
(2015→16 + 2016→17 = 24.1, 2017→18 + 2018→19 = 24.3, 2019→20 + 2020→21 = 24.1 pp).

## Evidence

### 1. Length of residence of *continuing* primaries jumps by two in the spike pairs and stalls in the next (`spike_lor_increments.csv`)
Same `family_id`, same address, primary in both years, LOR 2–40 in t (so no resets): LOR(t+1) − LOR(t) is

| Pair | +1 | +2 | 0 | LOR-implied interval (months) |
|---|---|---|---|---|
| 2012→13 | 98.6 | 0.7 | 0.0 | 12.1 |
| 2013→14 | 99.7 | 0.1 | 0.0 | 12.0 |
| 2014→15 | 89.3 | 8.1 | 0.0 | 13.0 |
| **2015→16** | 70.7 | **28.8** | 0.0 | **15.5** |
| 2016→17 | 86.5 | 0.1 | **13.2** | **10.4** |
| **2017→18** | 81.8 | **18.0** | 0.0 | **14.2** |
| 2018→19 | 94.2 | 0.1 | **5.5** | **11.3** |
| 2019→20 | 99.5 | 0.2 | 0.0 | 12.0 |
| 2020→21 | 99.5 | 0.1 | 0.0 | 12.0 |

LOR is months since arrival rounded to years, so a file cut k months later than 12 after the previous one moves k/12 of the clocks by two
years, and a short interval freezes k/12 of them. The implied dates chain to the `recency_date` maxima within about a month
(Mar 2014 → Apr 2015 → Jul 2016 → May 2017 → Jul 2018 → Jun 2019). LOR is *not* randomly re-derived: resets ≤1 are 0.02% and other
jumps ≤0.5% in every pair. (In the earlier `spike_lor_ctrl.csv`, which includes LOR = 1 in t, the same pattern: +2 for 32%/19%, 0 for 16%/7%.)

### 2. Deed sales show the cut-off directly (`spike_sale_capture.csv`)
Arm's-length sales at residential addresses, by sale month: share whose primary `family_id` in the file of the sale year already differs from the
previous file (the remainder shows up in the next file; "captured by file s+1" is a flat 82–87% in every year).

| Sale month | 2013 | 2014 | 2015 | 2016 | 2017 | 2018 | 2019 | 2020 |
|---|---|---|---|---|---|---|---|---|
| Jan | 78 | 82 | 81 | 86 | 82 | 81 | 78 | 77 |
| Feb | 73 | 76 | 80 | 86 | 81 | 83 | 80 | 78 |
| Mar | **41** | **45** | 76 | 86 | 80 | 84 | 82 | 78 |
| Apr | 24 | 25 | **42** | 85 | 76 | 83 | 82 | 77 |
| May | 21 | 23 | 21 | 81 | **40** | 85 | 76 | 68 |
| Jun | 18 | 20 | 19 | **52** | 21 | 78 | **39** | **42** |
| Jul | 20 | 19 | 21 | 24 | 20 | **42** | 21 | 22 |
| Aug–Dec | 19–20 | 19–21 | 19–22 | 22–23 | 17–20 | 23–26 | 18–20 | 17–19 |

The 2016 file includes sales through mid-June 2016, the 2018 file through early July 2018; the 2017 and 2019 files stop in mid-May / mid-June.
Sales captured in the sale-year file: 34% (2015), **52% (2016)**, 40% (2017), **57% (2018)**, 46% (2019) — the same bumps.

### 3. Decomposition of the ID changes (`spike_decomp.csv`, `spike_fate0.csv`; one primary in both years, both years observed)
Shares in % of all pairs; "excess" = spike pair minus the mean of the two adjacent pairs.

| New primary vs old primary | 2014→15 | 2015→16 | 2016→17 | 2017→18 | 2018→19 | excess 2015→16 | excess 2017→18 |
|---|---|---|---|---|---|---|---|
| (b) same person id under a new family id | 0.27 | 0.32 | 0.21 | 0.25 | 0.20 | +0.08 | +0.04 |
| (a) same surname, different persons | 0.35 | 0.39 | 0.29 | 0.37 | 0.29 | +0.07 | +0.08 |
| different surname, subfamily already at the address promoted to primary | 3.27 | 3.70 | 3.79 | 4.31 | 3.66 | +0.17 | +0.58 |
| different surname, family id tracked from another address (75–80% LOR ≤ 1) | 2.09 | 2.43 | 1.58 | 2.43 | 1.48 | +0.59 | +0.90 |
| (c) different surname, brand-new id, LOR ≤ 1 | 3.02 | 3.57 | 2.87 | 3.63 | 2.53 | +0.63 | +0.93 |
| (d) different surname, brand-new id, LOR ≥ 2 (carried tenure) | 0.81 | 0.91 | 0.55 | 0.67 | 0.55 | +0.23 | +0.12 |
| different surname, old id returning after a gap | 1.60 | 2.09 | 1.35 | 1.72 | 2.19 | +0.61 | −0.06 |
| **total ID change** | 11.41 | 13.41 | 10.65 | 13.38 | 10.91 | **+2.38** | **+2.60** |

Re-keys of the same people (same person id or same surname) are 0.5–0.7% of pairs in every year and contribute ≤0.15 pp of the excess. The
excess is in the categories that look like genuine turnover — a different household arriving, 75–100% of them with LOR reset — plus
proportionate increases in subfamily promotions and gap returns. (e) is not a category of ID changes: an address with no record in t+1 is
backfilled and shows no change; the share of observed addresses with no record next year also follows the interval (5.7% 2015→16, 4.2%
2016→17, 5.1% 2017→18, 3.9% 2018→19).

Fate of the old primary (`spike_fate0.csv`): demoted to subfamily at the same address 4.4/4.7% (spike pairs) vs 3.3–4.5; tracked to another
address 3.2/3.4 vs 2.2–2.7; vanished and back at the same address later 1.8/1.7 vs 1.2–1.8; vanished for good 3.8/3.3 vs 2.6–3.7. All move in
proportion; the one-year flicker (old id back as primary in t+2) is 8–30% within categories with no spike-year excess, so flicker is not the
mechanism (Q4).

### 4. No other field changes (`spike_fields.csv`)
For continuing families and for ID-change families alike, dpbc, ZIP+4, unit, street suffix/direction, city, county and tract differ between t and
t+1 in ≤0.1–2% of pairs in every year (county/tract 1.6–2.7% only in 2014→15). Geocodes (lat/lon to 4 decimals) change for 8–9% in 2017→18 **and**
2018→19 and 29% in 2020→21, identically for controls and changers, so the geocode re-runs are not what produces the turnover spikes. The only
record fields that move with the spikes are `recency_date` (updated for 86%/75% of continuing families vs 33–45%) and `estmtd_home_val`
(re-estimated), which simply reflect the longer update window.

### 5. Geography (`spike_by_state.csv`, `spike_by_county.csv`)
The excess is national: every state is above its adjacent-pair mean in both spike pairs (2015→16: +0.45 to +4.3 pp; 2017→18: +0.96 to +5.1 pp).
States above +3 pp are small or high-mobility ones (2015→16: SD, MT, OK, GA, OR, ME, ID, TN, AK, TX, VT, RI, WA; 2017→18: ND, SD, WY, AZ, MT, IA,
NM, CA, MD, WV, OK), and the top counties by excess are small-sample noise. Dropping states would not help.

### 6. Brand-new family IDs and record counts (`spike_newfids_national.csv`, `spike_newfids_state.csv`; 1% family sample)
Share of records whose `family_id` is seen for the first time: 13.7% (2015), **14.6% (2016)**, 12.8% (2017), **13.5% (2018)**, 10.6% (2019);
share of families absent from the next file: 21.1% (2015→16), 15.1% (2016→17), 16.9% (2017→18), 13.6% (2018→19). Mild bumps, in proportion to
the interval. Records grow +2.4% in 2016 and +6.6% in 2018, but also +6.3% in 2019 (no spike), so growth per se is not the cause. The only
regional oddity is the Pacific/Southwest block (CA −8%, HI −6%, AK −6%, AZ −3% records in 2016; +11–21% in 2018), which does not stand out in
the turnover excess.

## What a researcher should do

1. **Do not treat consecutive file years as 12-month intervals.** Date each file at its cut-off (`max(recency_date)` by `record_year`, or the
   table above) and annualise move/turnover rates by the elapsed months, or model moves per month of exposure. This removes both spikes and
   also the depressed 2016→17 and 2018→19 values. For the SALT event study the untreated-looking 2017→18 pair spans May 2017–Jul 2018 (14
   months) and 2018→19 spans only 11 months, so an uncorrected "treatment-year" jump of ~25% is mechanical.
2. For dating individual moves, use the destination record's LOR / `recency_date` (or the CoreLogic sale date) rather than the file year.
3. No need to drop states, re-link via person ids or treat same-surname new IDs as non-moves for *this* problem: in-place re-keys are a flat
   0.5–0.7% of address-years. (Linking by person id / name still matters for the separate under-tracking problem described in dataaxle.md.)
4. The same interval correction applies to any other annual-difference statistic from Data Axle (new-ID shares, disappearance rates, LOR
   transitions, brand-new records), and the 2014→15 pair (13 months) is mildly affected too.
