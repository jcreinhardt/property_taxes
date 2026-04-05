# Clean and load
gc()
rm(list = ls())
getwd()

# Load libraries
library(dplyr)
library(stringr)
library(ggplot2)
library(tidyr)
library(duckdb)

# Connect to the DuckDB database
con <- dbConnect(duckdb::duckdb(), dbdir = "../../../data/database.db")

###############################################################################
# Aggregate-level comparison 
###############################################################################


# Load data
zipcode_data <- dbGetQuery(con, "select * from returns_zipcode")
county_data <- dbGetQuery(con, "select * from returns_county")
hud_crosswalk <- dbGetQuery(con, "select * from hud_zip_to_county")

# Standardize keys
str(zipcode_data)
str(hud_crosswalk)
str(county_data)

# Use 2023Q1 crosswalk for 2022 filings, hence subtract 1 from year
hud_crosswalk <- hud_crosswalk |>
    mutate(year = year - 1)

# Aggregate zipcode data to county-year using res_ratio
numeric_cols <- zipcode_data |>
    select(where(is.numeric)) |>
    names()

value_vars <- setdiff(numeric_cols, c("year", "zipcode", "statefips"))

zipcode_aggregated_to_county <- zipcode_data |> 
    left_join(hud_crosswalk, by = c("zipcode" = "zip", "year")) |> 
    group_by(county, year) |>
    summarise(
        across(all_of(value_vars), ~ sum(.x * res_ratio, na.rm = TRUE)),
        .groups = "drop"
    )

# Compute 5 digit fips code
county_data <- county_data |> 
    mutate(county = statefips * 1000 + countyfips)

# Compare to county-level data
county_compare <- zipcode_aggregated_to_county |> 
    left_join(county_data, by = c("county", "year"), suffix = c("_zip", "_county"))

# Check for number of NAs (not merged) across columns
na_by_column <- county_compare |>
    summarise(across(everything(), ~ sum(is.na(.)))) |>
    pivot_longer(cols = everything(), names_to = "column", values_to = "na_count") |>
    arrange(desc(na_count))

print(na_by_column, n = Inf)

# Plot returns filed across all years
ggplot(county_compare, aes(x = returns_county, y = returns_zip)) +
    geom_point() +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "Returns filed across all years",
         x = "Returns filed at county level",
         y = "Returns filed at zipcode level")

lm(returns_county ~ returns_zip, data = county_compare) |> summary()

# Individuals filed across all years
ggplot(county_compare, aes(x = individuals_county, y = individuals_zip)) +
    geom_point() +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "Individuals filed across all years",
         x = "Individuals filed at county level",
         y = "Individuals filed at zipcode level")

lm(individuals_county ~ individuals_zip, data = county_compare) |> summary()

# Itemization share across all years
county_compare |>
    mutate(
        itemization_share_county = itemized_amount_county / returns_county,
        itemization_share_zip = itemized_amount_zip / returns_zip
        ) |>
    ggplot(aes(x = itemization_share_county, y = itemization_share_zip)) +
    geom_point() +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "Itemization share across all years",
         x = "Itemization share at county level",
         y = "Itemization share at zipcode level")

county_compare |>
    mutate(
        itemization_share_county = itemized_amount_county / returns_county,
        itemization_share_zip = itemized_amount_zip / returns_zip
        ) |>
    lm(itemization_share_county ~ itemization_share_zip, data = _) |> 
    summary()

# Additional plots and regressions (explicit, line-by-line)

# State and local income taxes (returns)
ggplot(county_compare, aes(x = state_and_local_income_taxes_returns_county, y = state_and_local_income_taxes_returns_zip)) +
    geom_point(alpha = 0.4) +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "State & local income taxes (returns)",
         x = "County", y = "ZIP-aggregated")
lm(state_and_local_income_taxes_returns_county ~ state_and_local_income_taxes_returns_zip, data = county_compare) |> summary()

# State and local income taxes (amount)
ggplot(county_compare, aes(x = state_and_local_income_taxes_amount_county, y = state_and_local_income_taxes_amount_zip)) +
    geom_point(alpha = 0.4) +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "State & local income taxes (amount)",
         x = "County", y = "ZIP-aggregated")
lm(state_and_local_income_taxes_amount_county ~ state_and_local_income_taxes_amount_zip, data = county_compare) |> summary()

# State and local sales taxes (returns)
ggplot(county_compare, aes(x = state_and_local_sales_tax_returns_county, y = state_and_local_sales_tax_returns_zip)) +
    geom_point(alpha = 0.4) +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "State & local sales taxes (returns)",
         x = "County", y = "ZIP-aggregated")
lm(state_and_local_sales_tax_returns_county ~ state_and_local_sales_tax_returns_zip, data = county_compare) |> summary()

# State and local sales taxes (amount)
ggplot(county_compare, aes(x = state_and_local_sales_tax_amount_county, y = state_and_local_sales_tax_amount_zip)) +
    geom_point(alpha = 0.4) +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "State & local sales taxes (amount)",
         x = "County", y = "ZIP-aggregated")
lm(state_and_local_sales_tax_amount_county ~ state_and_local_sales_tax_amount_zip, data = county_compare) |> summary()

# Property taxes (returns)
ggplot(county_compare, aes(x = property_taxes_returns_county, y = property_taxes_returns_zip)) +
    geom_point(alpha = 0.4) +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "Property taxes (returns)",
         x = "County", y = "ZIP-aggregated")
lm(property_taxes_returns_county ~ property_taxes_returns_zip, data = county_compare) |> summary()

# Property taxes (amount)
ggplot(county_compare, aes(x = property_taxes_amount_county, y = property_taxes_amount_zip)) +
    geom_point(alpha = 0.4) +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "Property taxes (amount)",
         x = "County", y = "ZIP-aggregated")
lm(property_taxes_amount_county ~ property_taxes_amount_zip, data = county_compare) |> summary()

# Taxes paid (returns)
ggplot(county_compare, aes(x = taxes_paid_returns_county, y = taxes_paid_returns_zip)) +
    geom_point(alpha = 0.4) +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "Taxes paid (returns)",
         x = "County", y = "ZIP-aggregated")
lm(taxes_paid_returns_county ~ taxes_paid_returns_zip, data = county_compare) |> summary()

# Taxes paid (amount)
ggplot(county_compare, aes(x = taxes_paid_amount_county, y = taxes_paid_amount_zip)) +
    geom_point(alpha = 0.4) +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "Taxes paid (amount)",
         x = "County", y = "ZIP-aggregated")
lm(taxes_paid_amount_county ~ taxes_paid_amount_zip, data = county_compare) |> summary()

# Mortgage interest (returns)
ggplot(county_compare, aes(x = mortgage_interest_returns_county, y = mortgage_interest_returns_zip)) +
    geom_point(alpha = 0.4) +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "Mortgage interest (returns)",
         x = "County", y = "ZIP-aggregated")
lm(mortgage_interest_returns_county ~ mortgage_interest_returns_zip, data = county_compare) |> summary()

# Mortgage interest (amount)
ggplot(county_compare, aes(x = mortgage_interest_amount_county, y = mortgage_interest_amount_zip)) +
    geom_point(alpha = 0.4) +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "Mortgage interest (amount)",
         x = "County", y = "ZIP-aggregated")
lm(mortgage_interest_amount_county ~ mortgage_interest_amount_zip, data = county_compare) |> summary()

# Charitable contributions (returns)
ggplot(county_compare, aes(x = charitable_contributions_returns_county, y = charitable_contributions_returns_zip)) +
    geom_point(alpha = 0.4) +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "Charitable contributions (returns)",
         x = "County", y = "ZIP-aggregated")
lm(charitable_contributions_returns_county ~ charitable_contributions_returns_zip, data = county_compare) |> summary()

# Charitable contributions (amount)
ggplot(county_compare, aes(x = charitable_contributions_amount_county, y = charitable_contributions_amount_zip)) +
    geom_point(alpha = 0.4) +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "Charitable contributions (amount)",
         x = "County", y = "ZIP-aggregated")
lm(charitable_contributions_amount_county ~ charitable_contributions_amount_zip, data = county_compare) |> summary()

###############################################################################
# AGI-level comparison 
###############################################################################

# Load AGI-level data
zipcode_data_agi <- dbGetQuery(con, "select * from returns_zipcode_agi")
county_data_agi <- dbGetQuery(con, "select * from returns_county_agi")
hud_crosswalk_agi <- dbGetQuery(con, "select * from hud_zip_to_county")

# Use 2023Q1 crosswalk for 2022 filings, hence subtract 1 from year
hud_crosswalk_agi <- hud_crosswalk_agi |>
    mutate(year = year - 1)

# Aggregate zipcode data to county-year-agi using res_ratio
numeric_cols_agi <- zipcode_data_agi |>
    select(where(is.numeric)) |>
    names()

value_vars_agi <- setdiff(numeric_cols_agi, c("year", "zipcode", "statefips", "agi_stub"))

# Check how many observations there are per zip-year-cell
zipcode_data_agi |>
    reframe(n = n(), .by = c(zipcode, year)) |>
    reframe(count = n(), .by = n) 

# Which zipcodes are missing some agi stubs?
zipcode_data_agi |>
    reframe(n = n(), zipcode_year = paste(zipcode, year, sep = "_"), .by = c(zipcode, year)) |>
    reframe(count = n(), .by = zipcode_year) |>
    filter(count != 6) 

zipcode_aggregated_to_county_agi <- zipcode_data_agi |>
    left_join(hud_crosswalk_agi, by = c("zipcode" = "zip", "year")) |>
    group_by(county, year, agi_stub) |>
    summarise(
        across(all_of(value_vars_agi), ~ sum(.x * res_ratio, na.rm = TRUE)),
        .groups = "drop"
    )

# Compute 5 digit fips code
county_data_agi <- county_data_agi |>
    mutate(county = statefips * 1000 + countyfips)

# Compare to county-level data
county_compare_agi <- zipcode_aggregated_to_county_agi |>
    left_join(county_data_agi, by = c("county", "year", "agi_stub"), suffix = c("_zip", "_county"))

# Check for number of NAs (not merged) across columns
na_by_column_agi <- county_compare_agi |>
    summarise(across(everything(), ~ sum(is.na(.)))) |>
    pivot_longer(cols = everything(), names_to = "column", values_to = "na_count") |>
    arrange(desc(na_count))

print(na_by_column_agi, n = Inf)

# Plot returns filed across all years (AGI stubs)
ggplot(county_compare_agi, aes(x = returns_county, y = returns_zip)) +
    geom_point() +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "Returns filed across all years (AGI stubs)",
         x = "Returns filed at county level",
         y = "Returns filed at zipcode level")

lm(returns_county ~ returns_zip, data = county_compare_agi) |> summary()

# Individuals filed across all years (AGI stubs)
ggplot(county_compare_agi, aes(x = individuals_county, y = individuals_zip)) +
    geom_point() +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "Individuals filed across all years (AGI stubs)",
         x = "Individuals filed at county level",
         y = "Individuals filed at zipcode level")

lm(individuals_county ~ individuals_zip, data = county_compare_agi) |> summary()

# Itemization share across all years (AGI stubs)
county_compare_agi |>
    mutate(
        itemization_share_county = itemized_amount_county / returns_county,
        itemization_share_zip = itemized_amount_zip / returns_zip
        ) |>
    ggplot(aes(x = itemization_share_county, y = itemization_share_zip)) +
    geom_point() +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "Itemization share across all years (AGI stubs)",
         x = "Itemization share at county level",
         y = "Itemization share at zipcode level")

county_compare_agi |>
    mutate(
        itemization_share_county = itemized_amount_county / returns_county,
        itemization_share_zip = itemized_amount_zip / returns_zip
        ) |>
    lm(itemization_share_county ~ itemization_share_zip, data = _) |>
    summary()

# State and local income taxes (returns)
ggplot(county_compare_agi, aes(x = state_and_local_income_taxes_returns_county, y = state_and_local_income_taxes_returns_zip)) +
    geom_point(alpha = 0.4) +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "State & local income taxes (returns, AGI stubs)",
         x = "County", y = "ZIP-aggregated")
lm(state_and_local_income_taxes_returns_county ~ state_and_local_income_taxes_returns_zip, data = county_compare_agi) |> summary()

# State and local income taxes (amount)
ggplot(county_compare_agi, aes(x = state_and_local_income_taxes_amount_county, y = state_and_local_income_taxes_amount_zip)) +
    geom_point(alpha = 0.4) +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "State & local income taxes (amount, AGI stubs)",
         x = "County", y = "ZIP-aggregated")
lm(state_and_local_income_taxes_amount_county ~ state_and_local_income_taxes_amount_zip, data = county_compare_agi) |> summary()

# State and local sales taxes (returns)
ggplot(county_compare_agi, aes(x = state_and_local_sales_tax_returns_county, y = state_and_local_sales_tax_returns_zip)) +
    geom_point(alpha = 0.4) +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "State & local sales taxes (returns, AGI stubs)",
         x = "County", y = "ZIP-aggregated")
lm(state_and_local_sales_tax_returns_county ~ state_and_local_sales_tax_returns_zip, data = county_compare_agi) |> summary()

# State and local sales taxes (amount)
ggplot(county_compare_agi, aes(x = state_and_local_sales_tax_amount_county, y = state_and_local_sales_tax_amount_zip)) +
    geom_point(alpha = 0.4) +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "State & local sales taxes (amount, AGI stubs)",
         x = "County", y = "ZIP-aggregated")
lm(state_and_local_sales_tax_amount_county ~ state_and_local_sales_tax_amount_zip, data = county_compare_agi) |> summary()

# Property taxes (returns)
ggplot(county_compare_agi, aes(x = property_taxes_returns_county, y = property_taxes_returns_zip)) +
    geom_point(alpha = 0.4) +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "Property taxes (returns, AGI stubs)",
         x = "County", y = "ZIP-aggregated")
lm(property_taxes_returns_county ~ property_taxes_returns_zip, data = county_compare_agi) |> summary()

# Property taxes (amount)
ggplot(county_compare_agi, aes(x = property_taxes_amount_county, y = property_taxes_amount_zip)) +
    geom_point(alpha = 0.4) +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "Property taxes (amount, AGI stubs)",
         x = "County", y = "ZIP-aggregated")
lm(property_taxes_amount_county ~ property_taxes_amount_zip, data = county_compare_agi) |> summary()

# Taxes paid (returns)
ggplot(county_compare_agi, aes(x = taxes_paid_returns_county, y = taxes_paid_returns_zip)) +
    geom_point(alpha = 0.4) +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "Taxes paid (returns, AGI stubs)",
         x = "County", y = "ZIP-aggregated")
lm(taxes_paid_returns_county ~ taxes_paid_returns_zip, data = county_compare_agi) |> summary()

# Taxes paid (amount)
ggplot(county_compare_agi, aes(x = taxes_paid_amount_county, y = taxes_paid_amount_zip)) +
    geom_point(alpha = 0.4) +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "Taxes paid (amount, AGI stubs)",
         x = "County", y = "ZIP-aggregated")
lm(taxes_paid_amount_county ~ taxes_paid_amount_zip, data = county_compare_agi) |> summary()

# Mortgage interest (returns)
ggplot(county_compare_agi, aes(x = mortgage_interest_returns_county, y = mortgage_interest_returns_zip)) +
    geom_point(alpha = 0.4) +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "Mortgage interest (returns, AGI stubs)",
         x = "County", y = "ZIP-aggregated")
lm(mortgage_interest_returns_county ~ mortgage_interest_returns_zip, data = county_compare_agi) |> summary()

# Mortgage interest (amount)
ggplot(county_compare_agi, aes(x = mortgage_interest_amount_county, y = mortgage_interest_amount_zip)) +
    geom_point(alpha = 0.4) +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "Mortgage interest (amount, AGI stubs)",
         x = "County", y = "ZIP-aggregated")
lm(mortgage_interest_amount_county ~ mortgage_interest_amount_zip, data = county_compare_agi) |> summary()

# Charitable contributions (returns)
ggplot(county_compare_agi, aes(x = charitable_contributions_returns_county, y = charitable_contributions_returns_zip)) +
    geom_point(alpha = 0.4) +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "Charitable contributions (returns, AGI stubs)",
         x = "County", y = "ZIP-aggregated")
lm(charitable_contributions_returns_county ~ charitable_contributions_returns_zip, data = county_compare_agi) |> summary()

# Charitable contributions (amount)
ggplot(county_compare_agi, aes(x = charitable_contributions_amount_county, y = charitable_contributions_amount_zip)) +
    geom_point(alpha = 0.4) +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "Charitable contributions (amount, AGI stubs)",
         x = "County", y = "ZIP-aggregated")
lm(charitable_contributions_amount_county ~ charitable_contributions_amount_zip, data = county_compare_agi) |> summary()
