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
