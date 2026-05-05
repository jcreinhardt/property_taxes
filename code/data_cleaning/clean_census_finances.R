# Clean and load
gc()
rm(list = ls())

library(duckdb)
library(dplyr)
library(tidyr)

# Use test database (change to database.db when merging)
con <- dbConnect(
  duckdb::duckdb(),
  dbdir = "../../data/census_finances_test.db"
)

df <- dbGetQuery(con, "SELECT * FROM govt_finances_county")
cat("Rows loaded:", nrow(df), "\n")
cat("Years:", paste(sort(unique(df$year)), collapse = ", "), "\n")
cat("Counties:", n_distinct(df$county_fips), "\n\n")

# ---- Step 1: Balance panel check ----
county_years <- df |>
  group_by(county_fips) |>
  summarise(n_years = n_distinct(year), .groups = "drop")

all_years <- length(2012:2022)
cat("Counties observed in all", all_years, "years:",
    sum(county_years$n_years == all_years), "\n")
cat("Counties with gaps:",
    sum(county_years$n_years < all_years), "\n\n")

balanced_counties <- county_years |>
  filter(n_years == all_years) |>
  pull(county_fips)

df <- df |>
  mutate(in_balanced_panel = county_fips %in% balanced_counties)

# ---- Step 2: Estimation flags ----
# Census flag: 'A' = actual, 'E' = estimated/imputed, 'W' = suppressed (-> NA)
df <- df |>
  mutate(
    property_tax_estimated = !is.na(flag_property_tax) &
      flag_property_tax == "E"
  )

cat("Estimated property_tax_rev observations:",
    sum(df$property_tax_estimated, na.rm = TRUE), "\n\n")

# ---- Step 3: Suppression rates by year ----
revenue_vars <- c(
  "property_tax_rev", "general_sales_tax_rev", "ind_income_tax_rev",
  "total_tax_rev", "federal_ig_rev", "state_ig_rev", "local_ig_rev",
  "other_own_source"
)

# Only keep revenue_vars that actually exist in the data
revenue_vars <- intersect(revenue_vars, names(df))

cat("Suppression (% NA) by year for key variables:\n")
df |>
  group_by(year) |>
  summarise(
    n = n(),
    pct_miss_property_tax = round(100 * mean(is.na(property_tax_rev)), 1),
    pct_miss_total_tax    = round(100 * mean(is.na(total_tax_rev)), 1),
    .groups = "drop"
  ) |>
  arrange(year) |>
  print(n = Inf)
cat("\n")

# ---- Step 4: Plausibility check ----
# Property tax cannot exceed total taxes; flag but don't drop
df <- df |>
  mutate(
    rev_check_flag = case_when(
      !is.na(property_tax_rev) & !is.na(total_tax_rev) &
        property_tax_rev > total_tax_rev * 1.01 ~
        "property_tax_exceeds_total_tax",
      TRUE ~ NA_character_
    )
  )

cat("Rows with plausibility flags:",
    sum(!is.na(df$rev_check_flag)), "\n\n")

# ---- Step 5: Winsorize at within-county 1st/99th percentile ----
# Same approach as cleaning.sh: county-level quantiles, flag not drop.
# Negative revenue values are set to NA (cannot be negative).
winsorize_county <- function(x) {
  q <- quantile(x, probs = c(0.01, 0.99), na.rm = TRUE)
  x_win <- pmax(pmin(x, q[2]), q[1])
  if_else(x < 0, NA_real_, x_win)
}

df <- df |>
  group_by(county_fips) |>
  mutate(across(
    all_of(revenue_vars),
    winsorize_county,
    .names = "{.col}_wins"
  )) |>
  ungroup()

# ---- Step 6: CPI-U deflation to 2017 dollars ----
# BLS CPI-U annual averages (series CUUR0000SA0), 2012-2022.
# 2017 = base year (TCJA passage). Verify against BLS before submission.
cpi_u <- tibble(
  year = 2012L:2022L,
  cpi  = c(229.594, 232.957, 236.736, 237.017, 240.007,
           245.120, 251.107, 255.657, 258.811, 270.970, 292.655)
)

cpi_2017 <- cpi_u |> filter(year == 2017L) |> pull(cpi)

cpi_u <- cpi_u |>
  mutate(deflator = cpi_2017 / cpi)

df <- df |>
  left_join(cpi_u, by = "year") |>
  mutate(across(
    ends_with("_wins"),
    ~ .x * deflator,
    .names = "{gsub('_wins$', '_real', .col)}"
  )) |>
  select(-cpi, -deflator)

# ---- Step 7: Final coverage summary ----
cat("Final coverage summary:\n")
df |>
  group_by(year) |>
  summarise(
    n_counties = n(),
    n_balanced = sum(in_balanced_panel),
    pct_property_tax_nonmissing = round(
      100 * mean(!is.na(property_tax_rev)), 1
    ),
    pct_property_tax_estimated = round(
      100 * mean(property_tax_estimated, na.rm = TRUE), 1
    ),
    mean_property_tax_real_mil = round(
      mean(property_tax_rev_real, na.rm = TRUE) / 1000, 2
    ),
    .groups = "drop"
  ) |>
  arrange(year) |>
  print(n = Inf)
cat("(mean_property_tax_real_mil is in millions of 2017 USD)\n\n")

# ---- Write cleaned table ----
dbWriteTable(
  con, "govt_finances_county_clean", as.data.frame(df), overwrite = TRUE
)
cat("Wrote 'govt_finances_county_clean' to census_finances_test.db\n")

dbDisconnect(con, shutdown = TRUE)
