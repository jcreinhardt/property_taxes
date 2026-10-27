---
date: 2026-10-16
title: CoreLogic's owner-occupancy code is the mailing-address rule; tested against tax-bill discounts
question: Is CoreLogic's owner-occupied flag accurate? Check it in homestead-exemption states by comparing tax bills.
status: current
code: property_taxes 0fcca23 (local main; analysis outputs uncommitted at time of writing)
tags: CoreLogic, owner-occupancy, homestead, tax bills
---

## Answer
`owner_occupancy_code` is a deterministic rule, not an observation: O (80% of single-family parcel-years) ⇔ the bill's mailing address
equals the situs address (74.5 pp) or `homestead_exempt_indicator` is set (the remaining 2.6–3.1 pp are 94–100% homestead-flagged);
A ⇔ mail elsewhere and no flag; blank ⇔ PO box. The homestead flag is populated only where counties deliver it (MN 77%, FL 63%, TX 57%,
SC 16%, UT 1%, zero in non-homestead states).

Judged by the bill (effective rate = tax / calculated value, relative to the 90th percentile in the same county × tax area × year; a
parcel is "discounted" below 80% of it), in states with a structural owner-occupant discount and a clear second mode (SC 0.25–0.30 of
the full rate, ID 0.50, IN 0.45, AL 0.45–0.50, WV 0.50, MS 0.50, MI ≈ 0.5):

| State | Discounted | Coded O | P(O \| discounted) | P(O \| not discounted) | P(discounted \| coded A) |
|---|---|---|---|---|---|
| SC | 74.7% | 78.7% | 91.3% | 38.3% | 34.6% |
| IN | 65.2% | 82.5% | 96.5% | 55.3% | 13.6% |
| AL | 66.6% | 77.3% | 90.7% | 48.3% | 28.4% |
| MS | 63.2% | 78.0% | 97.8% | 40.9% | 9.2% |
| WV | 61.5% | 78.6% | 90.7% | 57.3% | 31.7% |
| MI (2019) | 72.3% | 84.7% | 92.6% | 60.7% | 42.9% |

Recall is good (91–98% of bill-verified owner-occupants are coded O); specificity is poor (38–61% of non-discounted homes are coded O,
barely lower when recently or ever-sold homes are excluded), and 9–43% of homes coded absentee receive the owner-occupant discount. The
code overstates owner-occupancy by 5–15 pp in these states. Fixed-dollar exemption states (FL, TX, NY, GA, LA, IL, MN) show a continuous
discount, not a second mode; apparent bimodality in RI, MA, CT, TN, KS, CO, NH, DE is town rate variation (no tax-area code).

## How
- **Outline:** 1% building sample of `tax` with bill/exemption/tax-area columns 2012–2021 → SFR with structure (Michigan: no structure
  filter, improvement value never filled) → cell = county × `coalesce(tax_area_code, tax_rate_area_code)` × year (≥ 20 parcels) → relative
  rate → histogram by state; cross-tab with code, homestead flag, mailing rule, recent sales (deeds sample).
- **Choices:** rate on `calculated_total_value` (assessed values are already net of exemptions in UT and others); threshold 0.8; effective
  rates restricted to 0.05%–10%; recent sale = arm's-length deed in t−1 or t.
- **Inputs (cluster):** `~/ptax_explore/samples/tax_bill_bld1pct.parquet` (built by `oo_flag/tax_bill_bld1pct.sql`), `tax_bld1pct.parquet`,
  `deeds_bld1pct.parquet`.
- **Scripts:** `output/validation/dataaxle/move_tracking/oo_flag/{oo_flag_xtab,oo_flag_xtab2,oo_bill,oo_bill_refine,oo_bill_mi}.sql`; CSVs
  there; doc `text/data_exploration/dataaxle.md` §4.5c ("Is CoreLogic's owner_occupancy_code trustworthy?").
- **Check:** code O 80.3% of SFR parcel-years, mail-at-property 75.0%, O-and-mail 74.5%; SC pooled 2013–20: P(O | disc) 0.913,
  P(O | no disc) 0.383.

## Open
Treat O as "bill mailed to the property"; where bill fields exist use the discount; otherwise combine O with a Data Axle primary family at
the address and the homestead flag.
