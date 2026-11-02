---
date: 2026-10-28
title: Can we see household age and first-time buyers in Data Axle and CoreLogic?
question: Does Data Axle have data on age, and is there any way to identify first-time home buyers?
status: current
code: property_taxes branch claude/housing-stock-salt-cap-0a687c (after 3873b77; unpushed)
tags: Data Axle, age, first-time buyers, data note
---

## Answer
**Age: yes.** Each Data Axle family record lists up to five adults with an age (`age_i`; adults only, minimum 18;
missing for 47% of persons in 2006 and 18% in 2017; the 2024 file has only decade codes for person 1 and persons 2–5),
plus `head_hh_age_code`, 100% filled in bands: A–I under 65 in 5-year bands, J = 65+ inferred (3%), K/L/M = 65–69 /
70–74 / 75+ reported (21%). The under-65 bands are partly modelled like income and tenure. The 1% parcel panel already
carries the primary family's age per parcel-year (`age_prim`) and in 2016 (`age16`). Name + age is not a national
person key (53% unique; 78% with state).

**First-time buyers: no flag** in CoreLogic deeds, mortgages, the roll or Data Axle (HMDA has none either and is not in
the database). Three approximations, best combined:
1. *Buyer-name history:* an arm's-length buyer whose full-name hash (`o1_full` / deed buyer hash) never appears as an
   earlier owner or grantor in the same state. Needs a national lookup on the cluster; common names are misclassified as
   repeat buyers (name alone is unique for 19% nationally).
2. *Data Axle trajectory:* the new primary family at the sold parcel, followed back to its previous address; first-time
   if that address was renter-coded (`owner_renter_status` 1–3) or multi-unit and the family was never at an owner-coded
   address. Limits from the 2026-10-27 tracking entry: 53% of sellers and 24% of leavers are tracked, and the lost
   records are the thin single-person renter profiles first-time buyers tend to have; tenure is modelled except code 9.
3. *Proxies on the 1% panel, available now:* `fha_ind` in `mortgages.parquet` (first-timers ≈ 80% of FHA purchase
   loans), loan amount / price > 0.95, `age_prim` < 35.

Suggested definition: (1) plus (3), checked against the national first-time share (about a third of purchases 2017–19,
NAR). Age would then split the SALT-cap results by first-time vs repeat buyers, the one margin where the cap binds on
entry rather than relocation.

## How
- **Outline:** data note from the field documentation, no runs.
- **Inputs:** `.claude/skills/dataaxle/SKILL.md`, `text/data_exploration/dataaxle.md` §1 (fields), §4.3x (tracking);
  `code/build/corelogic/bouchet/p_parcel_panel_1pct.sql` (`age_prim`, `ownrent_prim`, `fha_ind`, `buyer_rel`, `vest`).
- **Check:** `head_hh_age_code` fill 100%; `age_i` NULL 47% (2006) / 18% (2017); sellers tracked 53%.

## Open
Prototype the first-time flag on the 1% panel (laptop): FHA or age < 35 among arm's-length buyers 2013–24, then the
name-history lookup as a SOM job.
