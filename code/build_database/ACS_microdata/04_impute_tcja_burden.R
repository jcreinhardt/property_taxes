gc()
rm(list = ls())

library(duckdb)
library(DBI)
library(dplyr)
library(usincometaxes)

con <- dbConnect(duckdb::duckdb(), dbdir = "../../../data/database.db")

# ============================================================
# FIPS → state abbreviation map
# ============================================================

fips_to_state <- c(
  "1"="AL","2"="AK","4"="AZ","5"="AR","6"="CA","8"="CO","9"="CT",
  "10"="DE","11"="DC","12"="FL","13"="GA","15"="HI","16"="ID",
  "17"="IL","18"="IN","19"="IA","20"="KS","21"="KY","22"="LA",
  "23"="ME","24"="MD","25"="MA","26"="MI","27"="MN","28"="MS",
  "29"="MO","30"="MT","31"="NE","32"="NV","33"="NH","34"="NJ",
  "35"="NM","36"="NY","37"="NC","38"="ND","39"="OH","40"="OK",
  "41"="OR","42"="PA","44"="RI","45"="SC","46"="SD","47"="TN",
  "48"="TX","49"="UT","50"="VT","51"="VA","53"="WA","54"="WV",
  "55"="WI","56"="WY"
)

# ============================================================
# Pull household heads (income + previously-imputed SALT/itemization)
# ============================================================

message("Pulling household heads from ACS...")

heads <- dbGetQuery(con, "
  SELECT
    year, serial, pernum,
    statefip,
    marst,
    age,
    incwage,
    inctot,
    hhwt,
    imp_proptx,
    imp_stateinctax,
    imp_itemizable_2017,
    imp_itemizable_2018,
    std_deduction_2017,
    std_deduction_2018,
    imp_itemizes_2017,
    imp_itemizes_2018
  FROM acs_microdata
  WHERE pernum = 1
    AND gq IN (1, 2, 5)
    AND year BETWEEN 2011 AND 2023
") |> as_tibble()

message(sprintf("Pulled %d household heads", nrow(heads)))

# ============================================================
# Pull spouse wages (same as 02_impute_salt_acs.R)
# ============================================================

message("Pulling spouse wages...")

spouse_wages <- dbGetQuery(con, "
  SELECT year, serial, incwage AS swages
  FROM acs_microdata
  WHERE relate = 2
    AND gq IN (1, 2, 5)
    AND year BETWEEN 2011 AND 2023
") |> as_tibble()

spouse_wages <- spouse_wages |>
  group_by(year, serial) |>
  summarise(swages = sum(swages, na.rm = TRUE), .groups = "drop")

heads <- heads |>
  left_join(spouse_wages, by = c("year", "serial")) |>
  mutate(
    swages      = coalesce(swages, 0L),
    state       = fips_to_state[as.character(statefip)],
    mstat       = if_else(marst == 1L, 2L, 1L),
    pwages      = pmax(0L, as.integer(if_else(incwage == 9999999L, 0L, incwage))),
    swages      = as.integer(swages),
    othinc      = as.integer(pmax(0L,
                    if_else(inctot  == 9999999L, 0L, inctot) -
                    if_else(incwage == 9999999L, 0L, incwage)
                  )),
    total_wages = pwages + swages
  )

# ============================================================
# Part (a): Change in deductible amount — pure arithmetic
#
# Total deduction = max(itemizable, standard deduction).
# SALT components: each gets full amount up to headroom left by the other.
# When proptx + stateinctax <= 10k, both are fully deductible post-TCJA;
# otherwise each is capped at 10k minus what the other component uses.
# NAs propagate: top-coded proptx (NA) leaves proptx-component deltas as NA.
# ============================================================

message("Computing deduction changes (Part a)...")

heads <- heads |>
  mutate(
    imp_deduction_2017  = pmax(coalesce(imp_itemizable_2017, 0), std_deduction_2017),
    imp_deduction_2018  = pmax(coalesce(imp_itemizable_2018, 0), std_deduction_2018),
    imp_delta_deduction = imp_deduction_2018 - imp_deduction_2017,

    # Property tax deduction: pre-TCJA full if itemizing; post-TCJA up to headroom
    proptx_ded_2017        = if_else(coalesce(imp_itemizes_2017, 0L) == 1L, imp_proptx, 0),
    proptx_ded_2018        = if_else(coalesce(imp_itemizes_2018, 0L) == 1L,
                               pmin(imp_proptx, pmax(0, 10000 - coalesce(imp_stateinctax, 0))),
                               0),
    imp_delta_proptx_ded   = proptx_ded_2018 - proptx_ded_2017,

    # State income tax deduction: sinctax has priority claim on the $10k SALT cap;
    # proptx gets whatever headroom remains (already computed correctly above).
    sinctax_ded_2017         = if_else(coalesce(imp_itemizes_2017, 0L) == 1L, coalesce(imp_stateinctax, 0), 0),
    sinctax_ded_2018         = if_else(coalesce(imp_itemizes_2018, 0L) == 1L,
                                 pmin(coalesce(imp_stateinctax, 0), 10000),
                                 0),
    imp_delta_stateinctax_ded = sinctax_ded_2018 - sinctax_ded_2017
  ) |>
  select(-proptx_ded_2017, -proptx_ded_2018, -sinctax_ded_2017, -sinctax_ded_2018)

cat("\nDeduction change summary (non-missing SALT, 2018-2023):\n")
print(
  heads |>
    filter(!is.na(imp_itemizable_2017), year >= 2018) |>
    summarise(
      n                      = n(),
      mean_delta_deduction   = round(mean(imp_delta_deduction,        na.rm = TRUE), 0),
      mean_delta_proptx      = round(mean(imp_delta_proptx_ded,       na.rm = TRUE), 0),
      mean_delta_stateinctax = round(mean(imp_delta_stateinctax_ded,  na.rm = TRUE), 0),
      pct_lost_deduction     = round(100 * mean(imp_delta_deduction < 0, na.rm = TRUE), 1)
    ) |> as.data.frame()
)

# ============================================================
# Part (b): Marginal federal income tax rate via TAXSIM
# ============================================================

message("Sampling observations for TAXSIM...")

taxsim_max_year <- 2023L
heads_sample <- heads |> filter(year <= taxsim_max_year)

p05 <- quantile(heads_sample$total_wages, 0.05)
p95 <- quantile(heads_sample$total_wages, 0.95)

set.seed(42)
top100 <- heads_sample |> slice_max(total_wages, n = 100, with_ties = FALSE)
pool   <- heads_sample |> anti_join(top100, by = c("year", "serial"))

tail_pool   <- pool |> filter(total_wages <= p05 | total_wages >= p95)
middle_pool <- pool |> filter(total_wages >  p05 & total_wages <  p95)

tail_sample   <- slice_sample(tail_pool,   n = 25000L, replace = TRUE)
middle_sample <- slice_sample(middle_pool, n = 75000L, replace = TRUE)

taxsim_sample <- bind_rows(tail_sample, middle_sample, top100)
message(sprintf("Sample: %d obs (tail p05<=%.0f, p95>=%.0f, 3x upsampled)",
                nrow(taxsim_sample), p05, p95))

taxsim_input <- taxsim_sample |>
  mutate(taxsimid = row_number()) |>
  transmute(
    taxsimid = taxsimid,
    year     = as.integer(year),
    state    = state,
    mstat    = as.integer(mstat),
    pwages   = as.integer(pwages),
    swages   = as.integer(swages),
    page     = as.integer(age),
    othinc   = as.integer(othinc)
  )

# Identify marginal federal rate column from full TAXSIM output.
# TAXSIM variable v18 = marginal federal income tax rate on primary wages.
message("Running small TAXSIM test to identify marginal federal rate column...")
test_result <- taxsim_calculate_taxes(
  .data                  = taxsim_input |> slice_sample(n = 200),
  return_all_information = TRUE
)
cat("TAXSIM full output columns:\n")
print(names(test_result))

# Try to match v18 first, then common synonyms
mfr_col <- names(test_result)
if (length(mfr_col) == 0)
  mfr_col <- names(test_result)[grep("mtr.*fed|fed.*mtr|federal.*marg|marg.*federal",
                                      names(test_result), ignore.case = TRUE)]
if (length(mfr_col) == 0) {
  cat("\nERROR: Could not auto-detect marginal federal rate column.\n")
  cat("All available columns:\n"); print(names(test_result))
  stop("Inspect TAXSIM output above and set mfr_col manually.")
}
mfr_col <- mfr_col[5]
message(sprintf("Using column '%s' as marginal federal income tax rate", mfr_col))

# Sanity-check scale: TAXSIM returns rates in percent (0–100); values near 10-40 expected.
cat(sprintf("  Test sample '%s': mean=%.1f, min=%.1f, max=%.1f\n",
    mfr_col,
    mean(test_result[[mfr_col]], na.rm = TRUE),
    min(test_result[[mfr_col]], na.rm = TRUE),
    max(test_result[[mfr_col]], na.rm = TRUE)))

message("Running TAXSIM on full sample (return_all_information = TRUE)...")
taxsim_result <- taxsim_calculate_taxes(
  .data                  = taxsim_input,
  return_all_information = TRUE
)

taxsim_sample <- taxsim_sample |>
  mutate(taxsimid = row_number()) |>
  left_join(
    taxsim_result |> select(taxsimid, mfr_pct = !!sym(mfr_col)),
    by = "taxsimid"
  ) |>
  mutate(marginal_fed_rate = mfr_pct / 100)  # TAXSIM returns percent

cat("\nMarginal federal rate by year (sample):\n")
print(
  taxsim_sample |>
    group_by(year) |>
    summarise(
      mean_mfr = round(mean(marginal_fed_rate, na.rm = TRUE), 3),
      p25_mfr  = round(quantile(marginal_fed_rate, 0.25, na.rm = TRUE), 3),
      p75_mfr  = round(quantile(marginal_fed_rate, 0.75, na.rm = TRUE), 3),
      .groups  = "drop"
    ) |> as.data.frame()
)

# ============================================================
# Interpolate marginal federal rate to all ACS households
# Same approx() grid strategy as 02_impute_salt_acs.R
# ============================================================

message("Interpolating marginal federal rate to all ACS households...")

taxsim_nested <- taxsim_sample |>
  group_by(year, state, mstat, total_wages) |>
  summarise(marginal_fed_rate = mean(marginal_fed_rate), .groups = "drop") |>
  arrange(year, state, mstat, total_wages) |>
  group_by(year, state, mstat) |>
  summarise(
    tw  = list(total_wages),
    mfr = list(marginal_fed_rate),
    .groups = "drop"
  )

heads <- heads |>
  left_join(taxsim_nested, by = c("year", "state", "mstat")) |>
  group_by(year, state, mstat) |>
  mutate(imp_marginal_fed_rate = {
    tw  <- tw[[1]]
    mfr <- mfr[[1]]
    if (is.null(tw) || length(tw) < 2)
      rep(NA_real_, n())
    else
      approx(tw, mfr, xout = total_wages, rule = 2)$y
  }) |>
  ungroup() |>
  select(-tw, -mfr)

n_na_mfr <- sum(is.na(heads$imp_marginal_fed_rate))
message(sprintf("NA marginal_fed_rate: %d heads (%.2f%%)",
                n_na_mfr, 100 * n_na_mfr / nrow(heads)))

# ============================================================
# Additional federal tax burden from SALT cap
# Positive value = more taxes paid under TCJA rules
# ============================================================

heads <- heads |>
  mutate(
    imp_additional_fed_tax_proptx      = -imp_delta_proptx_ded      * coalesce(imp_marginal_fed_rate, 0),
    imp_additional_fed_tax_stateinctax = -imp_delta_stateinctax_ded * coalesce(imp_marginal_fed_rate, 0),
    imp_additional_fed_tax_salt        = imp_additional_fed_tax_proptx + imp_additional_fed_tax_stateinctax
  )

# ============================================================
# Summary tables
# ============================================================

cat("\n--- Deduction and tax burden by income decile (2018-2023) ---\n")
print(
  heads |>
    filter(!is.na(imp_itemizable_2017), year >= 2018) |>
    mutate(income_decile = ntile(total_wages, 10)) |>
    group_by(income_decile) |>
    summarise(
      n                       = n(),
      mean_total_wages        = round(mean(total_wages), 0),
      mean_delta_deduction    = round(mean(imp_delta_deduction,             na.rm = TRUE), 0),
      mean_delta_proptx_ded   = round(mean(imp_delta_proptx_ded,            na.rm = TRUE), 0),
      mean_delta_sinctax_ded  = round(mean(imp_delta_stateinctax_ded,       na.rm = TRUE), 0),
      mean_mfr                = round(mean(imp_marginal_fed_rate,           na.rm = TRUE), 3),
      mean_additional_tax     = round(mean(imp_additional_fed_tax_salt,     na.rm = TRUE), 0),
      .groups = "drop"
    ) |> as.data.frame()
)

cat("\n--- By itemization group (2018-2023) ---\n")
print(
  heads |>
    filter(!is.na(imp_itemizable_2017), year >= 2018) |>
    mutate(item_group = case_when(
      coalesce(imp_itemizes_2017, 0L) == 1L & coalesce(imp_itemizes_2018, 0L) == 1L ~ "Always itemized",
      coalesce(imp_itemizes_2017, 0L) == 1L & coalesce(imp_itemizes_2018, 0L) == 0L ~ "Stopped itemizing (TCJA)",
      coalesce(imp_itemizes_2017, 0L) == 0L                                         ~ "Never itemized"
    )) |>
    group_by(item_group) |>
    summarise(
      n                    = n(),
      mean_delta_deduction = round(mean(imp_delta_deduction,         na.rm = TRUE), 0),
      mean_additional_tax  = round(mean(imp_additional_fed_tax_salt, na.rm = TRUE), 0),
      .groups = "drop"
    ) |> as.data.frame()
)

cat("\n--- By state (weighted, 2018-2023) ---\n")
state_lookup <- tibble(
  statefip = as.integer(names(fips_to_state)),
  state_abbr = unname(fips_to_state)
)
print(
  heads |>
    filter(!is.na(imp_itemizable_2017), year >= 2018, !is.na(imp_additional_fed_tax_salt)) |>
    left_join(state_lookup, by = "statefip") |>
    group_by(state_abbr) |>
    summarise(
      n                       = n(),
      wtd_delta_deduction     = round(weighted.mean(imp_delta_deduction,         hhwt, na.rm = TRUE), 0),
      wtd_additional_tax      = round(weighted.mean(imp_additional_fed_tax_salt, hhwt, na.rm = TRUE), 0),
      wtd_tax_from_proptx     = round(weighted.mean(imp_additional_fed_tax_proptx,      hhwt, na.rm = TRUE), 0),
      wtd_tax_from_stateinctax = round(weighted.mean(imp_additional_fed_tax_stateinctax, hhwt, na.rm = TRUE), 0),
      .groups = "drop"
    ) |>
    arrange(desc(wtd_additional_tax)) |>
    as.data.frame()
)

# ============================================================
# Write to database
# ============================================================

message("Adding TCJA burden columns to acs_microdata...")

burden_keys <- heads |>
  select(
    year, serial,
    imp_deduction_2017,
    imp_deduction_2018,
    imp_delta_deduction,
    imp_delta_proptx_ded,
    imp_delta_stateinctax_ded,
    imp_marginal_fed_rate,
    imp_additional_fed_tax_proptx,
    imp_additional_fed_tax_stateinctax,
    imp_additional_fed_tax_salt
  )

dbWriteTable(con, "_burden_keys", burden_keys, overwrite = TRUE)
rm(burden_keys); gc()

new_cols <- c(
  "imp_deduction_2017",
  "imp_deduction_2018",
  "imp_delta_deduction",
  "imp_delta_proptx_ded",
  "imp_delta_stateinctax_ded",
  "imp_marginal_fed_rate",
  "imp_additional_fed_tax_proptx",
  "imp_additional_fed_tax_stateinctax",
  "imp_additional_fed_tax_salt"
)

for (col in new_cols) {
  dbExecute(con, sprintf("ALTER TABLE acs_microdata DROP COLUMN IF EXISTS %s", col))
}

dbExecute(con, "ALTER TABLE acs_microdata ADD COLUMN imp_deduction_2017                 DOUBLE")
dbExecute(con, "ALTER TABLE acs_microdata ADD COLUMN imp_deduction_2018                 DOUBLE")
dbExecute(con, "ALTER TABLE acs_microdata ADD COLUMN imp_delta_deduction                DOUBLE")
dbExecute(con, "ALTER TABLE acs_microdata ADD COLUMN imp_delta_proptx_ded               DOUBLE")
dbExecute(con, "ALTER TABLE acs_microdata ADD COLUMN imp_delta_stateinctax_ded          DOUBLE")
dbExecute(con, "ALTER TABLE acs_microdata ADD COLUMN imp_marginal_fed_rate              DOUBLE")
dbExecute(con, "ALTER TABLE acs_microdata ADD COLUMN imp_additional_fed_tax_proptx      DOUBLE")
dbExecute(con, "ALTER TABLE acs_microdata ADD COLUMN imp_additional_fed_tax_stateinctax DOUBLE")
dbExecute(con, "ALTER TABLE acs_microdata ADD COLUMN imp_additional_fed_tax_salt        DOUBLE")

dbExecute(con, "
  UPDATE acs_microdata
  SET imp_deduction_2017                 = k.imp_deduction_2017,
      imp_deduction_2018                 = k.imp_deduction_2018,
      imp_delta_deduction                = k.imp_delta_deduction,
      imp_delta_proptx_ded               = k.imp_delta_proptx_ded,
      imp_delta_stateinctax_ded          = k.imp_delta_stateinctax_ded,
      imp_marginal_fed_rate              = k.imp_marginal_fed_rate,
      imp_additional_fed_tax_proptx      = k.imp_additional_fed_tax_proptx,
      imp_additional_fed_tax_stateinctax = k.imp_additional_fed_tax_stateinctax,
      imp_additional_fed_tax_salt        = k.imp_additional_fed_tax_salt
  FROM _burden_keys k
  WHERE acs_microdata.year   = k.year
    AND acs_microdata.serial = k.serial
    AND acs_microdata.pernum = 1
")

dbExecute(con, "DROP TABLE _burden_keys")

cat("\nTCJA burden summary by year (heads only):\n")
print(dbGetQuery(con, "
  SELECT year,
    COUNT(*)                                           AS n_heads,
    ROUND(AVG(imp_delta_deduction), 0)                 AS mean_delta_deduction,
    ROUND(AVG(imp_marginal_fed_rate), 3)               AS mean_marginal_fed_rate,
    ROUND(AVG(imp_additional_fed_tax_salt), 0)         AS mean_additional_fed_tax,
    ROUND(AVG(imp_additional_fed_tax_proptx), 0)       AS mean_tax_from_proptx,
    ROUND(AVG(imp_additional_fed_tax_stateinctax), 0)  AS mean_tax_from_stateinctax
  FROM acs_microdata
  WHERE pernum = 1
  GROUP BY year ORDER BY year
"))

message("Done. TCJA burden columns added to acs_microdata.")
dbDisconnect(con, shutdown = TRUE)
