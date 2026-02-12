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
library(ggplot2)
library(duckdb)

# Connect to the DuckDB database
con <- dbConnect(duckdb::duckdb(), dbdir = '../../../data/database.db')

# Paths
zipcode_returns_dir <- "../../../data/SOI/zipcode_returns"
zipcode_files <- list.files(zipcode_returns_dir, pattern = "\\.csv$", full.names = TRUE)

if (length(zipcode_files) == 0) {
    stop("No zipcode return files found.")
}

# Read files
zipcode_returns_list <- zipcode_files |>
    set_names(basename) |>
    map(fread)

# Add years from first two digits of filename (e.g., "11..." -> 2011)
file_years <- names(zipcode_returns_list) |>
    str_extract("^\\d{2}") |>
    as.numeric()
file_years <- 2000 + file_years

zipcode_returns_list <- map2(
    zipcode_returns_list,
    file_years,
    ~ mutate(.x, year = .y)
)

# Check for differences in variable names
var_names <- zipcode_returns_list |>
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
    "STATEFIPS", "STATE", "ZIPCODE", "AGI_STUB",
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
zipcode_returns_selected <- zipcode_returns_list |>
    map(~ select(.x, any_of(c(required_vars, "year")))) |>
    bind_rows()

cat("Merged rows:", nrow(zipcode_returns_selected), "\n")

# Drop state totals (zipcode = 00000) and AGI totals (agi_stub = 0)
zipcode_returns_selected <- zipcode_returns_selected |>
    filter(ZIPCODE != 0)

# Tabulate NAs by year (wide table)
na_by_year_wide <- zipcode_returns_selected |>
    group_by(year) |>
    summarise(
        total_obs = n(),
        across(all_of(required_vars), ~ sum(is.na(.))),
        .groups = "drop"
    ) |>
    arrange(year)

View(na_by_year_wide)

# Rename variables to match county_returns and conventions
zipcode_data <- zipcode_returns_selected |>
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
    select(-AGI_STUB, -N21020, -A21020, -N20950, -A20950) |> 
    rename_with(tolower)

View(zipcode_data)

# How many returns are there?
zipcode_data |>
    reframe(total_returns = sum(returns), .by = year)

# That's how much compared to county-level totals?
dbGetQuery(con, "select year, sum(returns) as total_returns from returns_county group by year order by year")

# How many individuals?
zipcode_data |>
    reframe(total_individuals = sum(individuals), .by = year)

# Itemization share over time
zipcode_data |>
    reframe(itemization_share = sum(returns_w_items) / sum(returns), .by = year)

# Total amount itemized over time
zipcode_data |>
    reframe(total_itemized_amount = sum(itemized_amount) / 1000000000, .by = year)

# If itemizing, average amount itemized
zipcode_data |>
    reframe(avg_itemized_amount = sum(itemized_amount) / sum(returns_w_items), .by = year)

# Income tax deductions per filer over time
zipcode_data |>
    reframe(avg_salt_deduction = sum(state_and_local_income_taxes_amount) / sum(returns), .by = year)

# Income tax deductions conditional on itemizing
zipcode_data |>
    reframe(avg_salt_deduction = sum(state_and_local_income_taxes_amount) / sum(returns_w_items), .by = year)

# Sales tax deductions per filer over time
zipcode_data |>
    reframe(avg_sales_tax_deduction = sum(state_and_local_sales_tax_amount) / sum(returns), .by = year)

# Sales tax deductions conditional on itemizing
zipcode_data |>
    reframe(avg_sales_tax_deduction = sum(state_and_local_sales_tax_amount) / sum(returns_w_items), .by = year)

# Property tax deductions per filer over time 
zipcode_data |>
    reframe(avg_property_tax_deduction = sum(property_taxes_amount) / sum(returns), .by = year)

# Property tax deductions conditional on itemizing
zipcode_data |>
    reframe(avg_property_tax_deduction = sum(property_taxes_amount) / sum(returns_w_items), .by = year)

# SALT deductions per filer over time
zipcode_data |>
    reframe(avg_salt_deduction = sum(taxes_paid_amount) / sum(returns), .by = year)

# SALT deductions conditional on itemizing 
zipcode_data |>
    reframe(avg_salt_deduction = sum(taxes_paid_amount) / sum(returns_w_items), .by = year)

# Mortgage interest deductions per filer over time
zipcode_data |>
    reframe(avg_mortgage_interest_deduction = sum(mortgage_interest_amount) / sum(returns), .by = year)

# Mortgage interest deductions conditional on itemizing
zipcode_data |>
    reframe(avg_mortgage_interest_deduction = sum(mortgage_interest_amount) / sum(returns_w_items), .by = year)

# Charitable contributions per filer over time
zipcode_data |>
    reframe(avg_charitable_contributions_deduction = sum(charitable_contributions_amount) / sum(returns), .by = year)

# Charitable contributions conditional on itemizing 
zipcode_data |>
    reframe(avg_charitable_contributions_deduction = sum(charitable_contributions_amount) / sum(returns_w_items), .by = year)

# Split up deductions over time (millions)
zipcode_data |>
    reframe(
        salt_taxes = sum(taxes_paid_amount) / 1000000,
        mortgage_interest = sum(mortgage_interest_amount) / 1000000,
        charitable_contributions = sum(charitable_contributions_amount) / 1000000,
        other_deductions = sum(itemized_amount) / 1000000 -
            salt_taxes - mortgage_interest - charitable_contributions,
        .by = year
    ) |>
    pivot_longer(
        cols = c(salt_taxes, mortgage_interest, charitable_contributions, other_deductions),
        names_to = "deduction_type",
        values_to = "amount_millions"
    ) |>
    ggplot(aes(x = year, y = amount_millions, group = deduction_type, fill = deduction_type)) +
        geom_bar(stat = "identity") +
        labs(title = "Deductions by Year", x = "Year", y = "Deductions (Billions)") +
        theme_classic() +
        scale_x_continuous(breaks = unique(zipcode_data$year)) +
        scale_fill_viridis_d() +
        theme(legend.position = "bottom")

# Repeat exercise per filer
zipcode_data |>
    reframe(
        salt_taxes = sum(taxes_paid_amount) / sum(returns),
        mortgage_interest = sum(mortgage_interest_amount) / sum(returns),
        charitable_contributions = sum(charitable_contributions_amount) / sum(returns),
        other_deductions = sum(itemized_amount) / sum(returns) -
            salt_taxes - mortgage_interest - charitable_contributions,
        .by = year
    ) |>
    pivot_longer(
        cols = c(salt_taxes, mortgage_interest, charitable_contributions, other_deductions),
        names_to = "deduction_type",
        values_to = "deduction_per_filer"
    ) |>
    ggplot(aes(x = year, y = deduction_per_filer, group = deduction_type, fill = deduction_type)) +
        geom_bar(stat = "identity") +
        labs(title = "Deductions by Year", x = "Year", y = "Deductions (Share)") +
        theme_classic() +
        scale_x_continuous(breaks = unique(zipcode_data$year)) +
        scale_fill_viridis_d() +
        theme(legend.position = "bottom")

# Repeat exercise conditional on itemizing
zipcode_data |>
    reframe(
        salt_taxes = sum(taxes_paid_amount) / sum(returns_w_items),
        mortgage_interest = sum(mortgage_interest_amount) / sum(returns_w_items),
        charitable_contributions = sum(charitable_contributions_amount) / sum(returns_w_items),
        other_deductions = sum(itemized_amount) / sum(returns_w_items) -
            salt_taxes - mortgage_interest - charitable_contributions,
        .by = year
    ) |>
    pivot_longer(
        cols = c(salt_taxes, mortgage_interest, charitable_contributions, other_deductions),
        names_to = "deduction_type",
        values_to = "deduction_conditional_on_itemizing"
    ) |>
    ggplot(aes(x = year, y = deduction_conditional_on_itemizing, group = deduction_type, fill = deduction_type)) +
        geom_bar(stat = "identity") +
        labs(title = "Deductions by Year (Conditional on Itemizing)", x = "Year", y = "Deductions (000s)") +
        theme_classic() +
        scale_x_continuous(breaks = unique(zipcode_data$year)) +
        scale_fill_viridis_d() +
        theme(legend.position = "bottom")

# Export to database
dbWriteTable(
    con,
    "returns_zipcode",
    zipcode_data |> select(-state),
    overwrite = TRUE
)
dbDisconnect(con, shutdown = TRUE)
