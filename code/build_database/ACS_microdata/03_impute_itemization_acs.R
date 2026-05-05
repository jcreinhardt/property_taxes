gc()
rm(list = ls())

library(duckdb)
library(DBI)
library(dplyr)

con <- dbConnect(duckdb::duckdb(), dbdir = "../../../data/database.db")

# ============================================================
# Freddie Mac 30-year fixed annual average rates
# Source: Freddie Mac Primary Mortgage Market Survey
# ============================================================

freddie_rates <- tibble::tibble(
  year        = 2013:2023,
  annual_rate = c(0.0398, 0.0417, 0.0385, 0.0365, 0.0399,
                  0.0454, 0.0394, 0.0311, 0.0296, 0.0534, 0.0681)
)

# ============================================================
# Standard deduction by year × filing status
# Source: IRS Rev. Proc. for each year
# ============================================================

std_ded <- tibble::tribble(
  ~year, ~single,  ~mfj,   ~hoh,
  2013L,    6100L, 12200L,  8950L,
  2014L,    6200L, 12400L,  9100L,
  2015L,    6300L, 12600L,  9250L,
  2016L,    6300L, 12600L,  9300L,
  2017L,    6350L, 12700L,  9350L,
  2018L,   12000L, 24000L, 18000L,
  2019L,   12200L, 24400L, 18350L,
  2020L,   12400L, 24800L, 18650L,
  2021L,   12550L, 25100L, 18800L,
  2022L,   12950L, 25900L, 19400L,
  2023L,   13850L, 27700L, 20800L
)

# ============================================================
# Pull household heads
# ============================================================

message("Pulling household heads from ACS...")

heads <- dbGetQuery(con, "
  SELECT
    year, serial, pernum,
    marst,
    nchild,
    mortgage,
    mortamt1,
    mortamt2,
    taxincl,
    insincl,
    propinsr,
    imp_proptx,
    imp_salt
  FROM acs_microdata
  WHERE pernum = 1
    AND gq IN (1, 2, 5)
    AND year BETWEEN 2013 AND 2023
") |> as_tibble()

message(sprintf("Pulled %d household heads", nrow(heads)))

# ============================================================
# Filing status
# nchild: number of own children in household (IPUMS variable)
# HoH: unmarried head with dependent children present
# ============================================================

heads <- heads |>
  mutate(
    imp_fstatus = case_when(
      marst == 1L                ~ "MFJ",
      marst != 1L & nchild > 0L ~ "HoH",
      TRUE                       ~ "Single"
    )
  )

cat("\nFiling status distribution:\n")
print(count(heads, imp_fstatus) |> mutate(pct = round(100 * n / sum(n), 1)))

# ============================================================
# Mortgage interest
# Strategy: back out implied loan balance from monthly P&I payment
#   and current market rate, then compute first-year interest.
# Assumes 30-year term originating at current-year market rate.
# Overstates interest for seasoned loans; uses actual payments
#   rather than an LTV assumption.
# mortgage %in% c(3,4): ACS only populates mortgage for owner-occupiers,
#   so no separate ownershp filter is needed.
# ============================================================

message("Imputing mortgage interest...")

heads <- heads |>
  left_join(freddie_rates, by = "year") |>
  mutate(
    # Adjust mortamt1 for bundled property taxes and insurance
    mortamt1_adj = case_when(
      mortgage %in% c(3L, 4L) ~ {
        adj <- as.numeric(coalesce(mortamt1, 0L))
        # taxincl == 2: property taxes bundled into mortamt1
        adj <- adj - if_else(taxincl == 2L & !is.na(imp_proptx), imp_proptx / 12, 0)
        # insincl == 2: homeowner's insurance bundled into mortamt1
        # propinsr == 0 means N/A (renters/group quarters)
        adj <- adj - if_else(insincl == 2L & coalesce(propinsr, 0L) > 0L,
                             propinsr / 12, 0)
        pmax(adj, 0)
      },
      TRUE ~ 0
    ),
    # Total monthly P&I (first + second mortgage)
    monthly_pi = mortamt1_adj + if_else(
      mortgage %in% c(3L, 4L),
      as.numeric(coalesce(mortamt2, 0L)),
      0
    ),
    # Implied loan balance: B = P_m * (1 - (1 + r_m)^-360) / r_m
    imp_loan_balance = if_else(
      mortgage %in% c(3L, 4L) & monthly_pi > 0 & !is.na(annual_rate),
      {
        r_m <- annual_rate / 12
        monthly_pi * (1 - (1 + r_m)^(-360)) / r_m
      },
      0
    ),
    # Annual interest (first-year): B * r, uncapped
    imp_mort_interest = imp_loan_balance * coalesce(annual_rate, 0),
    # Capped versions for itemizable total (not stored separately)
    mort_int_2017 = pmin(imp_loan_balance, 1e6)   * coalesce(annual_rate, 0),
    mort_int_2018 = pmin(imp_loan_balance, 7.5e5) * coalesce(annual_rate, 0)
  )

cat("\nMortgage interest summary (mortgaged owner-occupiers):\n")
print(
  heads |>
    filter(mortgage %in% c(3L, 4L)) |>
    summarise(
      n                = n(),
      pct_balance_gt1m = round(100 * mean(imp_loan_balance > 1e6, na.rm = TRUE), 1),
      mean_balance     = round(mean(imp_loan_balance, na.rm = TRUE), 0),
      mean_mort_int    = round(mean(imp_mort_interest, na.rm = TRUE), 0)
    ) |> as.data.frame()
)

# ============================================================
# Standard deductions
# ============================================================

heads <- heads |>
  left_join(std_ded, by = "year") |>
  mutate(
    # Current-law standard deduction (post-TCJA 2018+ values; pre-2018 is current law for those years)
    imp_std_deduction_2018 = case_when(
      imp_fstatus == "MFJ" ~ mfj,
      imp_fstatus == "HoH" ~ hoh,
      TRUE                 ~ single
    ),
    # 2017-rule standard deduction: fixed at 2017 levels for all years (counterfactual)
    imp_std_deduction_2017 = case_when(
      imp_fstatus == "MFJ" ~ 12700L,
      imp_fstatus == "HoH" ~ 9350L,
      TRUE                 ~ 6350L
    )
  ) |>
  select(-single, -mfj, -hoh)

# ============================================================
# Itemizable totals and itemization dummies
# Medical expenses and charitable donations: not observed in ACS, set to 0
# ============================================================

message("Computing itemizable totals and itemization dummies...")

heads <- heads |>
  mutate(
    imp_itemizable_2017 = coalesce(imp_salt, 0) + mort_int_2017,
    imp_itemizable_2018 = pmin(coalesce(imp_salt, 0), 10000) + mort_int_2018,
    imp_itemizes_2017   = as.integer(imp_itemizable_2017 > imp_std_deduction_2017),
    imp_itemizes_2018   = as.integer(imp_itemizable_2018 > imp_std_deduction_2018)
  ) |>
  select(-mortamt1_adj, -monthly_pi, -annual_rate, -mort_int_2017, -mort_int_2018)

# ============================================================
# Sanity checks
# ============================================================

cat("\nItemization rate by year (heads with non-missing imp_salt):\n")
print(
  heads |>
    filter(!is.na(imp_salt)) |>
    group_by(year) |>
    summarise(
      n                = n(),
      pct_itemize_2017 = round(100 * mean(imp_itemizes_2017), 1),
      pct_itemize_2018 = round(100 * mean(imp_itemizes_2018), 1),
      .groups = "drop"
    ) |> as.data.frame()
)

cat("\nItemization rate by quintile of itemizable_2017 (all years):\n")
print(
  heads |>
    filter(!is.na(imp_salt)) |>
    mutate(q = ntile(imp_itemizable_2017, 5)) |>
    group_by(q) |>
    summarise(
      mean_itemizable_2017 = round(mean(imp_itemizable_2017), 0),
      mean_itemizable_2018 = round(mean(imp_itemizable_2018), 0),
      pct_itemize_2017     = round(100 * mean(imp_itemizes_2017), 1),
      pct_itemize_2018     = round(100 * mean(imp_itemizes_2018), 1),
      .groups = "drop"
    ) |> as.data.frame()
)

# ============================================================
# Write to database
# ============================================================

message("Adding itemization columns to acs_microdata...")

item_keys <- heads |>
  select(
    year, serial,
    imp_loan_balance,
    imp_mort_interest,
    imp_itemizable_2017,
    imp_itemizable_2018,
    imp_std_deduction_2017,
    imp_std_deduction_2018,
    imp_itemizes_2017,
    imp_itemizes_2018
  )

dbWriteTable(con, "_item_keys", item_keys, overwrite = TRUE)
rm(item_keys); gc()

new_cols <- c(
  "imp_loan_balance",
  "imp_mort_interest",
  "imp_itemizable_2017",
  "imp_itemizable_2018",
  "imp_std_deduction_2017",
  "imp_std_deduction_2018",
  "imp_itemizes_2017",
  "imp_itemizes_2018"
)

for (col in new_cols) {
  dbExecute(con, sprintf("ALTER TABLE acs_microdata DROP COLUMN IF EXISTS %s", col))
}

dbExecute(con, "ALTER TABLE acs_microdata ADD COLUMN imp_loan_balance       DOUBLE")
dbExecute(con, "ALTER TABLE acs_microdata ADD COLUMN imp_mort_interest      DOUBLE")
dbExecute(con, "ALTER TABLE acs_microdata ADD COLUMN imp_itemizable_2017    DOUBLE")
dbExecute(con, "ALTER TABLE acs_microdata ADD COLUMN imp_itemizable_2018    DOUBLE")
dbExecute(con, "ALTER TABLE acs_microdata ADD COLUMN imp_std_deduction_2017 INTEGER")
dbExecute(con, "ALTER TABLE acs_microdata ADD COLUMN imp_std_deduction_2018 INTEGER")
dbExecute(con, "ALTER TABLE acs_microdata ADD COLUMN imp_itemizes_2017      INTEGER")
dbExecute(con, "ALTER TABLE acs_microdata ADD COLUMN imp_itemizes_2018      INTEGER")

dbExecute(con, "
  UPDATE acs_microdata
  SET imp_loan_balance       = k.imp_loan_balance,
      imp_mort_interest      = k.imp_mort_interest,
      imp_itemizable_2017    = k.imp_itemizable_2017,
      imp_itemizable_2018    = k.imp_itemizable_2018,
      imp_std_deduction_2017 = k.imp_std_deduction_2017,
      imp_std_deduction_2018 = k.imp_std_deduction_2018,
      imp_itemizes_2017      = k.imp_itemizes_2017,
      imp_itemizes_2018      = k.imp_itemizes_2018
  FROM _item_keys k
  WHERE acs_microdata.year   = k.year
    AND acs_microdata.serial = k.serial
    AND acs_microdata.pernum = 1
")

dbExecute(con, "DROP TABLE _item_keys")

cat("\nItemization summary by year (heads only):\n")
print(dbGetQuery(con, "
  SELECT year,
    COUNT(*)                                AS n_heads,
    ROUND(AVG(imp_loan_balance), 0)         AS mean_loan_balance,
    ROUND(AVG(imp_mort_interest), 0)        AS mean_mort_interest,
    ROUND(AVG(imp_itemizable_2017), 0)      AS mean_itemizable_2017,
    ROUND(AVG(imp_itemizable_2018), 0)      AS mean_itemizable_2018,
    ROUND(100 * AVG(imp_itemizes_2017), 1)  AS pct_itemize_2017,
    ROUND(100 * AVG(imp_itemizes_2018), 1)  AS pct_itemize_2018
  FROM acs_microdata
  WHERE pernum = 1
  GROUP BY year ORDER BY year
"))

message("Done. Itemization columns added to acs_microdata.")
dbDisconnect(con, shutdown = TRUE)
