gc()
rm(list = ls())

library(duckdb)
library(DBI)
library(dplyr)
library(usincometaxes)

con <- dbConnect(duckdb::duckdb(), dbdir = "../../../data/database.db")

# ============================================================
# Component 1: proptx99 midpoint lookup
# ============================================================

proptx99_lookup <- tibble::tibble(
  proptx99 = c(
    0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L,
    11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L, 21L,
    22L, 23L, 24L, 25L, 26L, 27L, 28L, 29L, 30L, 31L,
    32L, 33L, 34L, 35L, 36L, 37L, 38L, 39L, 40L, 41L,
    42L, 43L, 44L, 45L, 46L, 47L, 48L, 49L, 50L, 51L,
    52L, 53L, 54L, 55L, 56L, 57L, 58L, 59L, 60L, 61L, 62L,
    63L, 64L, 65L, 66L, 67L, 68L, 69L, 70L, 71L, 72L, 73L,
    74L, 75L, 76L, 77L, 78L, 79L, 80L, 81L, 82L, 83L, 84L,
    85L, 86L, 87L, 88L, 89L, 90L, 91L, 92L, 93L, 94L, 95L,
    96L, 97L, 98L, 99L, 100L, 101L, 102L, 103L, 104L, 105L,
    106L, 107L, 108L, 109L, 110L, 111L, 112L, 113L, 114L,
    115L, 116L, 117L, 118L, 119L, 120L, 121L, 122L, 123L,
    124L, 125L, 126L, 127L, 128L, 129L, 130L, 131L, 132L,
    133L, 134L, 135L, 136L, 137L, 138L, 139L, 140L, 141L,
    142L, 143L, 144L, 145L, 146L, 147L, 148L, 149L, 150L,
    151L, 152L, 153L, 154L, 155L, 156L, 157L, 158L, 159L
  ),
  proptx_mid = c(
    NA_real_,   # 000 N/A
    0,          # 001 None
    25,         # 002 $1-49
    75,         # 003 $50-99
    125, 175, 225, 275, 325, 375, 425, 475,  # 004-011
    525, 575, 625, 675, 725, 775, 825, 875,  # 012-019
    925, 975,   # 020-021
    1050, 1150, 1250, 1350, 1450, 1550, 1650, 1750, 1850, 1950,  # 022-031
    2050, 2150, 2250, 2350, 2450, 2550, 2650, 2750, 2850, 2950,  # 032-041
    3050, 3150, 3250, 3350, 3450, 3550, 3650, 3750, 3850, 3950,  # 042-051
    4050, 4150, 4250, 4350, 4450,  # 052-056
    4500,       # 057 $4500 (1990 US Samples)
    4550,       # 058 $4500-4599
    4650, 4750, 4850, 4950,  # 059-062
    5250,       # 063 $5000-5499
    5750,       # 064 $5500-5999
    6500,       # 065 $6000-6999
    7500,       # 066 $7000-7999
    8500,       # 067 $8000-8999
    9500,       # 068 $9000-9999
    10500, 11500, 12500, 13500, 14500, 15500, 16500, 17500, 18500, 19500,  # 069-078
    20500, 21500, 22500, 23500, 24500, 25500, 26500, 27500, 28500, 29500,  # 079-088
    # 089: label says $30000-39999 but other codes fill that range individually
    35000,      # 089 (label artifact — codes 090-098 cover 31k-39k)
    31500, 32500, 33500, 34500, 35500, 36500, 37500, 38500, 39500,  # 090-098
    40500, 41500, 42500, 43500, 44500, 45500, 46500, 47500, 48500, 49500,  # 099-108
    50500, 51500, 52500, 53500, 54500, 55500, 56500, 57500, 58500, 59500,  # 109-118
    60500, 61500, 62500, 63500, 64500, 65500, 66500, 67500, 68500, 69500,  # 119-128
    70500, 71500, 72500, 73500, 74500, 75500, 76500, 77500, 78500, 79500,  # 129-138
    80500, 81500, 82500, 83500, 84500, 85500, 86500, 87500, 88500, 89500,  # 139-148
    90500, 91500, 92500, 93500, 94500, 95500, 96500, 97500, 98500, 99500,  # 149-158
    100000      # 159 $100,000+ (2018+)
  ),
  # top-coded: code 69 pre-2018, code 159 from 2018 onward
  is_topcoded_pre2018  = c(rep(FALSE, 69), TRUE, rep(FALSE, 90)),
  is_topcoded_2018plus = c(rep(FALSE, 159), TRUE)
)

# ============================================================
# Component 2: Pull household heads + spouse wages from ACS
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
    proptx99,
    ownershp,
    hhwt
  FROM acs_microdata
  WHERE pernum = 1
    AND gq IN (1, 2, 5)
") |> as_tibble()

message("Pulling spouse wages...")

spouse_wages <- dbGetQuery(con, "
  SELECT year, serial, incwage AS swages
  FROM acs_microdata
  WHERE relate = 2
    AND gq IN (1, 2, 5)
") |> as_tibble()

spouse_wages <- spouse_wages |>
  group_by(year, serial) |>
  summarise(
    swages           = sum(swages, na.rm = TRUE),
    spouse_duplicate = n() > 1,
    .groups = "drop"
  )

n_dup <- sum(spouse_wages$spouse_duplicate)
message(sprintf("Households with >1 relate=2 person: %d (%.2f%%)",
                n_dup, 100 * n_dup / nrow(spouse_wages)))
if (n_dup > 0) {
  cat("Duplicate spouse counts by year:\n")
  print(
    spouse_wages |>
      filter(spouse_duplicate) |>
      count(year) |>
      as.data.frame()
  )
}

heads <- heads |>
  left_join(spouse_wages, by = c("year", "serial")) |>
  mutate(swages = coalesce(swages, 0L))

# ============================================================
# Component 3: FIPS → state abbreviation map
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
# Component 4: Prepare TAXSIM inputs for all heads
# ============================================================

heads_taxsim <- heads |>
  mutate(
    state  = fips_to_state[as.character(statefip)],
    mstat  = if_else(marst == 1L, 2L, 1L),
    pwages = pmax(0L, as.integer(if_else(incwage == 9999999L, 0L, incwage))),
    swages = as.integer(swages),
    othinc = as.integer(pmax(0L,
      if_else(inctot  == 9999999L, 0L, inctot) -
      if_else(incwage == 9999999L, 0L, incwage)
    )),
    total_wages = pwages + swages
  )

na_states <- heads_taxsim |> filter(is.na(state))
if (nrow(na_states) > 0) {
  message(sprintf("WARNING: %d heads have unmatched statefip (state = NA):", nrow(na_states)))
  print(count(na_states, statefip, year))
}

# ============================================================
# Component 5: Sample 100k obs for TAXSIM, upsampling tails
# ============================================================

message("Sampling observations for TAXSIM...")

taxsim_max_year <- 2023L
n_dropped <- sum(heads_taxsim$year > taxsim_max_year)
if (n_dropped > 0)
  message(sprintf("Dropping %d obs with year > %d (not yet supported by TAXSIM)", n_dropped, taxsim_max_year))
heads_taxsim_sample <- heads_taxsim |> filter(year <= taxsim_max_year)

p05 <- quantile(heads_taxsim_sample$total_wages, 0.05)
p95 <- quantile(heads_taxsim$total_wages, 0.95)

set.seed(42)
top100 <- heads_taxsim_sample |> slice_max(total_wages, n = 100, with_ties = FALSE)

pool <- heads_taxsim_sample |> anti_join(top100, by = c("year", "serial"))

# Stratified sample: split pool once, then sample each stratum separately.
# 3x upsampling of ~10% tails ≈ 25k tail draws, 75k middle draws.
tail_pool   <- pool |> filter(total_wages <= p05 | total_wages >= p95)
middle_pool <- pool |> filter(total_wages >  p05 & total_wages <  p95)

tail_sample   <- slice_sample(tail_pool,   n = 25000L, replace = TRUE)
middle_sample <- slice_sample(middle_pool, n = 75000L, replace = TRUE)

taxsim_sample <- bind_rows(tail_sample, middle_sample, top100)

message(sprintf(
  "Sample: %d obs (tail p05<=%.0f, p95>=%.0f, 3x upsampled)",
  nrow(taxsim_sample), p05, p95
))

coerce_check <- taxsim_sample |>
  summarise(
    na_year   = sum(is.na(year)),
    na_state  = sum(is.na(state)),
    na_mstat  = sum(is.na(mstat)),
    na_pwages = sum(is.na(pwages)),
    na_swages = sum(is.na(swages)),
    na_age    = sum(is.na(age)),
    na_othinc = sum(is.na(othinc))
  )
cat("NAs before coercion:\n")
print(as.data.frame(coerce_check))

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

message("Running TAXSIM speed tests...")
for (n_test in c(100L, 1000L)) {
  t0 <- proc.time()["elapsed"]
  taxsim_calculate_taxes(
    .data                  = taxsim_input |> slice_sample(n = n_test),
    return_all_information = FALSE
  )
  dt <- proc.time()["elapsed"] - t0
  message(sprintf("  n=%5d: %.1fs  (extrapolated 100k: %.0fs)", n_test, dt, dt * 100000 / n_test))
}

message("Running TAXSIM on sample...")

taxsim_result <- taxsim_calculate_taxes(
  .data                  = taxsim_input,
  return_all_information = FALSE
)

taxsim_sample <- taxsim_sample |>
  mutate(taxsimid = row_number()) |>
  left_join(taxsim_result |> select(taxsimid, stateinctax = siitax), by = "taxsimid")

# ============================================================
# Component 6: Interpolate state income tax to all ACS households
# ============================================================

message("Interpolating state income tax to ACS households...")

# Pre-nest sample by year × state × mstat so we join once (O(n)) instead of
# filtering 100k rows per group (O(n_groups × n_sample)).
n_na_siitax <- sum(is.na(taxsim_sample$stateinctax))
message(sprintf("TAXSIM sample: %d NA stateinctax (%.2f%%)", n_na_siitax, 100 * n_na_siitax / nrow(taxsim_sample)))
if (n_na_siitax > 0) {
  cat("NA stateinctax by year × state:\n")
  print(taxsim_sample |> filter(is.na(stateinctax)) |> count(year, state) |> as.data.frame())
}

taxsim_nested <- taxsim_sample |>
  group_by(year, state, mstat, total_wages) |>
  summarise(stateinctax = mean(stateinctax), .groups = "drop") |>
  arrange(year, state, mstat, total_wages) |>
  group_by(year, state, mstat) |>
  summarise(
    tw  = list(total_wages),
    tax = list(stateinctax),
    .groups = "drop"
  )

heads_with_tax <- heads_taxsim |>
  left_join(taxsim_nested, by = c("year", "state", "mstat")) |>
  group_by(year, state, mstat) |>
  mutate(stateinctax = {
    tw  <- tw[[1]]
    tax <- tax[[1]]
    if (is.null(tw) || length(tw) < 2)
      rep(NA_real_, n())
    else
      approx(tw, tax, xout = total_wages, rule = 2)$y
  }) |>
  ungroup() |>
  select(-tw, -tax)

# ============================================================
# Component 6: Property tax midpoint + SALT
# ============================================================

message("Constructing SALT...")

heads_with_tax <- heads_with_tax |>
  left_join(
    proptx99_lookup |> select(proptx99, proptx_mid, is_topcoded_pre2018, is_topcoded_2018plus),
    by = "proptx99"
  ) |>
  mutate(
    proptx_topcoded = case_when(
      year < 2018 & is_topcoded_pre2018  ~ TRUE,
      year >= 2018 & is_topcoded_2018plus ~ TRUE,
      TRUE ~ FALSE
    ),
    # Renters and "None" get 0; top-coded and N/A get NA
    proptx_mid = case_when(
      proptx_topcoded          ~ NA_real_,
      ownershp != 1            ~ 0,
      proptx99 == 1            ~ 0,
      TRUE                     ~ proptx_mid
    ),
    stateinctax = pmax(0, stateinctax),
    salt = proptx_mid + stateinctax
  ) |>
  select(
    year, serial, pernum,
    statefip, state,
    marst, mstat, age,
    incwage, pwages, swages, total_wages, spouse_duplicate, inctot, othinc,
    ownershp, proptx99, proptx_mid, proptx_topcoded,
    stateinctax,
    salt,
    hhwt
  )

# ============================================================
# Component 7: Sanity checks
# ============================================================

message("Running sanity checks...")

earners <- heads_with_tax |> filter(pwages > 0, !is.na(stateinctax))

cat("\n1. Mean state income tax by state (earners only, all years pooled):\n")
print(
  earners |>
    group_by(state) |>
    summarise(
      mean_stateinctax = round(mean(stateinctax), 0),
      n                = n(),
      .groups = "drop"
    ) |>
    arrange(mean_stateinctax) |>
    as.data.frame()
)

cat("\n2. Zero-tax states — should be near $0 (AK, FL, NV, SD, TX, WA, WY, NH, TN):\n")
zero_tax_states <- c("AK", "FL", "NV", "SD", "TX", "WA", "WY", "NH", "TN")
print(
  earners |>
    filter(state %in% zero_tax_states) |>
    group_by(state) |>
    summarise(mean_stateinctax = round(mean(stateinctax), 0), .groups = "drop") |>
    as.data.frame()
)

cat("\n3. Income monotonicity — correlation of total_wages and stateinctax within state-year (should be strongly positive):\n")
print(
  earners |>
    filter(!is.na(stateinctax)) |>
    group_by(year, state) |>
    filter(n() >= 10) |>
    summarise(cor = cor(total_wages, stateinctax, use = "complete.obs"), .groups = "drop") |>
    filter(!is.na(cor)) |>
    summarise(min_cor = min(cor), mean_cor = mean(cor), pct_negative = mean(cor < 0)) |>
    as.data.frame()
)

cat("\n4. National mean SALT components (earner owner-occupiers, excl. NAs):\n")
print(
  heads_with_tax |>
    filter(pwages > 0, ownershp == 1, !is.na(salt)) |>
    summarise(
      mean_proptax    = round(mean(proptx_mid,   na.rm = TRUE), 0),
      mean_stateinctax = round(mean(stateinctax, na.rm = TRUE), 0),
      mean_salt       = round(mean(salt,         na.rm = TRUE), 0),
      .groups = "drop"
    ) |>
    as.data.frame()
)

cat("\n5. MFJ vs single mean state income tax (earners only):\n")
print(
  earners |>
    group_by(mstat) |>
    summarise(mean_stateinctax = round(mean(stateinctax), 0), n = n(), .groups = "drop") |>
    mutate(filing = if_else(mstat == 2L, "MFJ", "Single")) |>
    select(filing, mean_stateinctax, n) |>
    as.data.frame()
)

cat("\n6. SALT components by income quintile (earner owner-occupiers):\n")
print(
  heads_with_tax |>
    filter(pwages > 0, ownershp == 1, !is.na(salt)) |>
    mutate(income_quintile = ntile(total_wages, 5)) |>
    group_by(income_quintile) |>
    summarise(
      mean_wages       = round(mean(total_wages), 0),
      mean_proptax     = round(mean(proptx_mid,    na.rm = TRUE), 0),
      mean_stateinctax = round(mean(stateinctax,   na.rm = TRUE), 0),
      mean_salt        = round(mean(salt,          na.rm = TRUE), 0),
      .groups = "drop"
    ) |>
    as.data.frame()
)

# ============================================================
# Component 8: Write to database
# ============================================================

message("Adding SALT columns to acs_microdata...")

salt_keys <- heads_with_tax |>
  select(year, serial,
         imp_mstat       = mstat,
         imp_totalwages  = total_wages,
         imp_stateinctax = stateinctax,
         imp_proptx      = proptx_mid,
         imp_salt        = salt)

dbWriteTable(con, "_salt_keys", salt_keys, overwrite = TRUE)
rm(salt_keys); gc()

# Drop columns if already present (safe re-run)
for (col in c("imp_mstat", "imp_totalwages", "imp_stateinctax", "imp_proptx", "imp_salt")) {
  dbExecute(con, sprintf("ALTER TABLE acs_microdata DROP COLUMN IF EXISTS %s", col))
}

dbExecute(con, "ALTER TABLE acs_microdata ADD COLUMN imp_mstat       INTEGER")
dbExecute(con, "ALTER TABLE acs_microdata ADD COLUMN imp_totalwages  INTEGER")
dbExecute(con, "ALTER TABLE acs_microdata ADD COLUMN imp_stateinctax DOUBLE")
dbExecute(con, "ALTER TABLE acs_microdata ADD COLUMN imp_proptx      DOUBLE")
dbExecute(con, "ALTER TABLE acs_microdata ADD COLUMN imp_salt        DOUBLE")

# Update only head rows — non-heads remain NULL
dbExecute(con, "
  UPDATE acs_microdata
  SET imp_mstat       = s.imp_mstat,
      imp_totalwages  = s.imp_totalwages,
      imp_stateinctax = s.imp_stateinctax,
      imp_proptx      = s.imp_proptx,
      imp_salt        = s.imp_salt
  FROM _salt_keys s
  WHERE acs_microdata.year   = s.year
    AND acs_microdata.serial = s.serial
    AND acs_microdata.pernum = 1
")

dbExecute(con, "DROP TABLE _salt_keys")

cat("\nSALT summary by year (heads only):\n")
print(dbGetQuery(con, "
  SELECT year,
    COUNT(*)                        AS n_households,
    ROUND(AVG(imp_salt), 0)         AS mean_salt,
    ROUND(AVG(imp_proptx), 0)       AS mean_proptax,
    ROUND(AVG(imp_stateinctax), 0)  AS mean_stateinctax
  FROM acs_microdata
  WHERE pernum = 1
  GROUP BY year ORDER BY year
"))

message("Done. SALT columns added to acs_microdata.")
dbDisconnect(con, shutdown = TRUE)
