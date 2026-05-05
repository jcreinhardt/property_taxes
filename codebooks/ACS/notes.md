# ACS IPUMS Extract Notes (usa_00003)

Source: IPUMS USA, single-year ACS samples, 2011–2024.

---

## Observation structure

Each row is a **person**. Persons are nested within households.

| Identifier | Description |
|---|---|
| `year` | Survey year (2011–2024) |
| `sample` | 6-digit IPUMS sample code (first 4 digits = year, last 2 = sample within year) |
| `serial` | IPUMS household serial number — unique within sample (8 digits) |
| `pernum` | Person number within household (consecutive from 1) |
| `cbserial` | Original Census Bureau household serial number (13 digits) |

**Use `serial` (not `cbserial`) for standard analysis:**
- **Unique person key:** `sample + serial + pernum`
- **Unique household key:** `sample + serial`

`cbserial` is the Census Bureau's original identifier, preserved for linking back to raw CB source files. Its analogous person identifier is `cbpernum` (not in this extract by default). Both `serial` and `cbserial` uniquely identify households within a sample, but IPUMS documentation uses `serial` as the canonical key.

All housing/household variables repeat identically for every person in the household. For household-level analysis, restrict to `pernum == 1`.

---

## Weights

| Variable | Use for |
|---|---|
| `perwt` | Person-level analysis |
| `hhwt` | Household-level analysis |
| `cluster` + `strata` | Correct standard errors for complex survey design (Taylor series) |

Dollar amounts are **nominal (contemporary dollars)**. Inflate using CPI or the IPUMS `adjust` factor.
2020 ACS uses experimental weights to correct for COVID-19 data collection disruptions.

---

## Geography

| Variable | Level | Notes |
|---|---|---|
| `statefip` | State | FIPS code |
| `countyfip` | County | FIPS, state-dependent; identifiable counties only |
| `puma` | PUMA | Public Use Microdata Area |
| `cpuma1020` | PUMA | Consistent 2010–2020 definition — use for panel comparisons |
| `met2013` / `met2023` | Metro area | 2013 / 2023 OMB delineations; identifiable areas only |
| `density` | PUMA | Population-weighted density |
| `metro` / `pctmetro` | PUMA | Metro status and share of PUMA population in metro area |
| `region` | Region | Census region and division |

Counties and metros are only populated for sufficiently large areas (100k+ population threshold).

---

## Income

All income variables are **person-level**, covering the **prior 12 months**, in **nominal dollars**.

| Variable | What it measures |
|---|---|
| `inctot` | Total personal income from all sources (pre-tax) |
| `hhincome` | Sum of income of all household members age 15+ (household-level) |
| `ftotinc` | Family income — excludes non-relatives (narrower than `hhincome`) |
| `incwage` | Wages and salaries only (excludes self-employment) |

- Missing/N/A coded as `9999999`
- Inflation-adjust before pooling years

---

## Housing, home value, and property taxes

All housing variables are **household-level**, universe **owner-occupied units** unless noted.

| Variable | What it measures | Notes |
|---|---|---|
| `ownershp` | Tenure: 1=owned, 2=rented | All households |
| `valueh` | Self-reported home value (dollars) | Continuous; `9999999` = N/A |
| `proptx99` | Annual property taxes | **Binned** (codes 1–111); not exact dollars — use bin midpoints or treat as ordinal |
| `owncost` | Total monthly owner costs | Derived sum: mortgage + taxes + insurance + utilities + condo fees |
| `taxincl` | Mortgage payment includes property taxes? | 1=no, 2=yes |
| `insincl` | Mortgage payment includes insurance? | 1=no, 2=yes |
| `propinsr` | Annual property insurance cost | |
| `mortgage` | Mortgage status | 1=free & clear, 3=mortgage, 4=contract to purchase |
| `mortamt1` | Monthly first mortgage payment | May include taxes/insurance — check `taxincl` |
| `mortamt2` | Monthly second mortgage payment | |
| `rooms` / `bedrooms` | Unit size | |
| `builtyr2` | Decade structure was built | |
| `unitsstr` | Units in structure | |
| `rent` / `rentgrs` | Contract rent / gross rent | Renters only |

Key caveat: `proptx99` bins get coarser at high values (top bin is $10,000+ in early years, rising to $100,000+ from 2018). `owncost` includes taxes but is not a clean tax measure on its own.

---

## Constructed variable: SALT (`acs_salt` table)

Built by `code/build_database/ACS_microdata/02_impute_salt_acs.R`. One row per household-head (`pernum = 1`).

`salt = proptx_mid + stateinctax`

| Column | Description |
|---|---|
| `proptx_mid` | Bin midpoint of `proptx99`; 0 for renters and "None" |
| `proptx_topcoded` | TRUE if obs is in the top-coded bin for that year |
| `stateinctax` | State income tax imputed via NBER TAXSIM grid + interpolation |
| `salt` | Sum of above two components |

**Limitations:**
- Property taxes are binned; midpoints introduce measurement error, especially at high values
- Top code changes in 2018 (code 69 = $10k+ pre-2018; code 159 = $100k+ from 2018) — comparability break at the high end around TCJA
- Renters assigned 0 property tax
- State income taxes are interpolated from a grid (~170k rows), not computed exactly per household
- Spouse wages identified via `relate = 2`; excludes households where spouse is absent or not the second listed person
- TAXSIM grid uses a fixed representative age (45) and ignores non-wage non-head income

---

## Migration

All migration variables are **person-level**, universe **age 1+**, referring to **residence 1 year ago**.

| Variable | What it measures | Notes |
|---|---|---|
| `migrate1` | Migration status: 1=same house, 2=moved within state, 3=moved between states, 4=abroad | |
| `migrate1d` | Detailed: adds within-county vs. between-county, contiguous vs. non-contiguous states | |
| `migplac1` | State or country 1 year ago | 3-digit FIPS/country code |
| `migcounty1` | County 1 year ago (FIPS) | State-dependent — combine with `migplac1`; `000` = non-mover OR county not identifiable |
| `migpuma1` | Migration PUMA 1 year ago | State-dependent — combine with `migplac1` |
| `migmet131` | Metro area 1 year ago (2013 delineations) | Inferred from migration PUMAs; subject to match error |
| `pwstate2` / `pwcounty` | Place of work — state / county | |

`migcounty1 = 000` is ambiguous: use `migrate1` to distinguish non-movers from movers to unidentifiable counties.

PUMA definition changes: 2000 definitions (2005–2011), 2010 definitions (2012–2021), 2020 definitions (2022+). Cross-period migration comparisons require care.

---

## Demographics

All demographic variables are **person-level**.

| Category | Variables |
|---|---|
| Basic | `sex`, `age`, `birthyr`, `birthqtr` |
| Marital status | `marst`, `marrno`, `marrinyr`, `yrmarr`, `divinyr`, `widinyr` |
| Fertility | `fertyr` |
| Race / ethnicity | `race` / `raced`, `hispan` / `hispand` |
| Nativity / immigration | `bpl` / `bpld`, `citizen`, `yrnatur`, `yrimmig`, `yrsusa1`, `yrsusa2` |
| Language | `language` / `languaged`, `speakeng` |
| Education | `educ` / `educd`, `gradeatt`, `schltype`, `degfield` / `degfield2` |
| Employment | `empstat`, `labforce`, `occ` / `occ2010`, `ind` / `indnaics` |
| Work hours / weeks | `uhrswork`, `wkswork1` |
| Household role | `relate` / `related`, `gq` |

---

## Household composition

| Variable | What it measures |
|---|---|
| `hhtype` | Household type |
| `nfams` / `nsubfam` | Number of families / subfamilies |
| `ncouples` / `nmothers` / `nfathers` | Household composition counts |
| `multgen` / `multgend` | Multigenerational household |
| `coupletype` / `ssmc` | Couple type / same-sex married couple |
| `gchouse` | Grandchildren present |
