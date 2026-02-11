# Clean and load
gc()
rm(list = ls())
getwd()

# Load libraries
library(data.table)
library(dplyr)
library(tidyr)
library(stringr)
library(purrr)
library(duckdb)

# Paths
county_returns_dir <- "../../data/SOI/county_returns_w_agi"
county_files <- list.files(county_returns_dir, pattern = "\\.csv$", full.names = TRUE)

if (length(county_files) == 0) {
    stop("No county return files found.")
}

# Read files
county_returns_list <- county_files |>
    set_names(basename) |>
    map(fread)

# Add years from first two digits of filename (e.g., "11..." -> 2011)
file_years <- names(county_returns_list) |>
    str_extract("^\\d{2}") |>
    as.numeric()
file_years <- 2000 + file_years

county_returns_list <- map2(
    county_returns_list,
    file_years,
    ~ mutate(.x, year = .y)
)

# Check for differences in variable names
var_names <- county_returns_list |>
    map(names)

baseline_vars <- var_names[[1]]

diff_table <- tibble(
    file = names(var_names),
    n_vars = map_int(var_names, length),
    extra_vars = map_chr(var_names, ~ paste(setdiff(.x, baseline_vars), collapse = ", ")),
    missing_vars = map_chr(var_names, ~ paste(setdiff(baseline_vars, .x), collapse = ", "))
)

print(diff_table)

# Check for required variables across all files
required_vars <- c(
    "STATEFIPS", "STATE", "COUNTYFIPS", "COUNTYNAME", "AGI_STUB",
    "N1", "N2", "N04470", "A04470", "A00101", "N17000", "A17000", "N18425", "A18425",
    "N18450", "A18450", "N18500", "A18500", "N18800", "A18800",
    "N18460", "A18460", "N18300", "A18300", "N19300", "A19300",
    "N19500", "A19500", "N19530", "A19530", "N19570", "A19570",
    "N19700", "A19700",
    "N20950", "A20950", "N21020", "A21020"
)

required_check <- tibble(
    file = names(var_names),
    missing_required = map_chr(
        var_names,
        ~ paste(setdiff(required_vars, .x), collapse = ", ")
    )
)

print(required_check)

if (any(required_check$missing_required != "")) {
    warning("Some files are missing required variables.")
}

# Merge selected variables into one table
county_returns_selected <- county_returns_list |>
    map(~ select(.x, any_of(c(required_vars, "year")))) |>
    bind_rows()

cat("Merged rows:", nrow(county_returns_selected), "\n")

# Tabulate NAs by year (wide table)
na_by_year_wide <- county_returns_selected |>
    group_by(year) |>
    summarise(
        total_obs = n(),
        across(all_of(required_vars), ~ sum(is.na(.))),
        .groups = "drop"
    ) |>
    arrange(year)

View(na_by_year_wide)

# Aggregate to county-year-AGI stub by summing across filers
county_returns_selected <- county_returns_selected |>
    group_by(STATEFIPS, STATE, COUNTYFIPS, COUNTYNAME, AGI_STUB, year) |>
    summarise(across(where(is.numeric), ~ sum(.x, na.rm = TRUE)), .groups = "drop")

county_data_agi <- county_returns_selected |>
    rename(
        returns = N1,
        individuals = N2,
        returns_w_items = N04470,
        itemized_amount = A04470,
        agi_for_itemizers = A00101,
        medical_expenses_returns = N17000,
        medical_expenses_amount = A17000,
        state_and_local_income_taxes_returns = N18425,
        state_and_local_income_taxes_amount = A18425,
        state_and_local_sales_tax_returns = N18450,
        state_and_local_sales_tax_amount = A18450,
        property_taxes_returns = N18500,
        property_taxes_amount = A18500,
        personal_property_taxes_returns = N18800,
        personal_property_taxes_amount = A18800,
        limited_state_and_local_taxes_returns = N18460,
        limited_state_and_local_taxes_amount = A18460,
        taxes_paid_returns = N18300,
        taxes_paid_amount = A18300,
        mortgage_interest_returns = N19300,
        mortgage_interest_amount = A19300,
        home_mortgage_personal_seller_returns = N19500,
        home_mortgage_personal_seller_amount = A19500,
        deductible_points_returns = N19530,
        deductible_points_amount = A19530,
        investment_interest_returns = N19570,
        investment_interest_amount = A19570,
        charitable_contributions_returns = N19700,
        charitable_contributions_amount = A19700
    ) |>
    mutate(
        other_deductions_returns = if_else(is.na(N20950), N21020, N20950),
        other_deductions_amount = if_else(is.na(A20950), A21020, A20950)
    ) |>
    select(-N21020, -A21020, -N20950, -A20950) |>
    rename_with(tolower)

View(county_data_agi)

# Export to database
con <- dbConnect(duckdb::duckdb(), dbdir = '../../../data/database.db')
dbWriteTable(
    con,
    "returns_county_agi",
    county_data_agi |> select(-state, -countyname),
    overwrite = TRUE
)
dbDisconnect(con, shutdown = TRUE)
