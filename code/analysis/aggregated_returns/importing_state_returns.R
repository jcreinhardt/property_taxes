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
library(readxl)
library(tigris)

# Connect to the DuckDB database
con <- dbConnect(duckdb::duckdb(), dbdir = '../../../data/database.db')

# Paths
state_returns_dir <- "../../../data/SOI/state_returns"
state_files <- list.files(state_returns_dir, pattern = "\\.(csv|xlsx)$", full.names = TRUE) |>
    keep(~ str_detect(basename(.x), "^\\d{2}")) |>
    keep(~ {
        yr <- as.numeric(str_sub(basename(.x), 1, 2))
        !is.na(yr) && yr >= 12 && yr <= 22
    })

if (length(state_files) == 0) {
    stop("No state return files found.")
}

read_state_file <- function(path) {
    file_name <- basename(path)
    # 2013 file has a few header lines before the variable names
    if (str_detect(file_name, "^13") && str_detect(file_name, "\\.xlsx$")) {
        return(read_excel(path, skip = 2) |> as.data.table())
    }
    if (str_detect(file_name, "^13")) {
        return(fread(path, skip = 2))
    }
    if (str_detect(file_name, "\\.xlsx$")) {
        return(read_excel(path) |> as.data.table())
    }
    fread(path)
}

# Convert numeric-like strings with commas to numeric
convert_commas_to_numeric <- function(df) {
    df |>
        mutate(
            across(
                where(is.character) & !matches("^STATE$"),
                ~ as.numeric(gsub(",", "", .x))
            )
        )
}

# Read files and standardize column names
state_returns_list <- state_files |>
    set_names(basename) |>
    map(read_state_file) |>
    map(~ { names(.x) <- toupper(names(.x)); .x }) |>
    map(convert_commas_to_numeric)

# Add years from first two digits of filename (e.g., "12..." -> 2012)
file_years <- names(state_returns_list) |>
    str_extract("^\\d{2}") |>
    as.numeric()
file_years <- 2000 + file_years

if (!any(file_years == 2013, na.rm = TRUE)) {
    warning("No 2013 CSV found in state_returns_dir.")
}

state_returns_list <- map2(
    state_returns_list,
    file_years,
    ~ mutate(.x, year = .y)
)

# Check for differences in variable names
var_names <- state_returns_list |>
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
    "STATE", "AGI_STUB",
    "N1", "N2", "N04470", "A04470",
    "N17000", "A17000",
    "N18425", "A18425", "N18450", "A18450",
    "N18500", "A18500", "N18460", "A18460",
    "N18300", "A18300", "N18800", "A18800",
    "N19300", "A19300", "N19500", "A19500",
    "N19530", "A19530", "N19570", "A19570",
    "N19700", "A19700", "N20950", "A20950",
    "N21020", "A21020"
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
state_returns_selected <- state_returns_list |>
    map(~ select(.x, any_of(c(required_vars, "year")))) |>
    bind_rows()

cat("Merged rows:", nrow(state_returns_selected), "\n")

# Drop US totals and AGI totals
state_returns_selected <- state_returns_selected |>
    filter(!STATE %in% c("US", "OA", "PR")) |>
    filter(AGI_STUB != 0)

# Tabulate NAs by year (wide table)
na_by_year_wide <- state_returns_selected |>
    group_by(year) |>
    summarise(
        total_obs = n(),
        across(all_of(required_vars), ~ sum(is.na(.))),
        .groups = "drop"
    ) |>
    arrange(year)

View(na_by_year_wide)

# Rename variables to match county_returns and conventions
state_returns <- state_returns_selected |>
    rename( 
        returns = N1,
        individuals = N2,
        returns_w_items = N04470,
        itemized_amount = A04470,
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

View(state_returns)

# Filers over time
state_returns |>
    reframe(total_returns = sum(returns), .by = year)

# Individuals over time
state_returns |>
    reframe(total_individuals = sum(individuals), .by = year)

# Itemization share over time by agi
state_returns |>
    reframe(itemization_share = sum(returns_w_items) / sum(returns), .by = c(year, agi_stub)) |> 
    pivot_wider(names_from = agi_stub, values_from = itemization_share)

# Total amount itemized over time
state_returns |>
    reframe(total_itemized_amount = sum(itemized_amount) / 1000000000, .by = c(year, agi_stub)) |> 
    pivot_wider(names_from = agi_stub, values_from = total_itemized_amount)

# If itemizing, average amount itemized
state_returns |>
    reframe(avg_itemized_amount = sum(itemized_amount) / sum(returns_w_items), .by = c(year, agi_stub)) |> 
    pivot_wider(names_from = agi_stub, values_from = avg_itemized_amount)

# Income tax deductions per filer over time
state_returns |>
    reframe(avg_salt_deduction = sum(state_and_local_income_taxes_amount) / sum(returns), .by = c(year, agi_stub)) |> 
    pivot_wider(names_from = agi_stub, values_from = avg_salt_deduction)

# Income tax deductions conditional on itemizing
state_returns |>
    reframe(avg_salt_deduction = sum(state_and_local_income_taxes_amount) / sum(returns_w_items), .by = c(year, agi_stub)) |> 
    pivot_wider(names_from = agi_stub, values_from = avg_salt_deduction)

# Sales tax deductions per filer over time
state_returns |>
    reframe(avg_sales_tax_deduction = sum(state_and_local_sales_tax_amount) / sum(returns), .by = c(year, agi_stub)) |> 
    pivot_wider(names_from = agi_stub, values_from = avg_sales_tax_deduction)

# Sales tax deductions conditional on itemizing
state_returns |>
    reframe(avg_sales_tax_deduction = sum(state_and_local_sales_tax_amount) / sum(returns_w_items), .by = c(year, agi_stub)) |> 
    pivot_wider(names_from = agi_stub, values_from = avg_sales_tax_deduction)

# Property tax deductions per filer over time 
state_returns |>
    reframe(avg_property_tax_deduction = sum(property_taxes_amount) / sum(returns), .by = c(year, agi_stub)) |> 
    pivot_wider(names_from = agi_stub, values_from = avg_property_tax_deduction)

# Property tax deductions conditional on itemizing
state_returns |>
    reframe(avg_property_tax_deduction = sum(property_taxes_amount) / sum(returns_w_items), .by = c(year, agi_stub)) |> 
    pivot_wider(names_from = agi_stub, values_from = avg_property_tax_deduction)

# SALT deductions per filer over time
state_returns |>
    reframe(avg_salt_deduction = sum(taxes_paid_amount) / sum(returns), .by = c(year, agi_stub)) |> 
    pivot_wider(names_from = agi_stub, values_from = avg_salt_deduction)

# SALT deductions conditional on itemizing 
state_returns |>
    reframe(avg_salt_deduction = sum(taxes_paid_amount) / sum(returns_w_items), .by = c(year, agi_stub)) |> 
    pivot_wider(names_from = agi_stub, values_from = avg_salt_deduction)

# Mortgage interest deductions per filer over time
state_returns |>
    reframe(avg_mortgage_interest_deduction = sum(mortgage_interest_amount) / sum(returns), .by = c(year, agi_stub)) |> 
    pivot_wider(names_from = agi_stub, values_from = avg_mortgage_interest_deduction)

# Mortgage interest deductions conditional on itemizing
state_returns |>
    reframe(avg_mortgage_interest_deduction = sum(mortgage_interest_amount) / sum(returns_w_items), .by = c(year, agi_stub)) |> 
    pivot_wider(names_from = agi_stub, values_from = avg_mortgage_interest_deduction)

# Charitable contributions per filer over time
state_returns |>
    reframe(avg_charitable_contributions_deduction = sum(charitable_contributions_amount) / sum(returns), .by = c(year, agi_stub)) |> 
    pivot_wider(names_from = agi_stub, values_from = avg_charitable_contributions_deduction)

# Charitable contributions conditional on itemizing 
    state_returns |>
    reframe(avg_charitable_contributions_deduction = sum(charitable_contributions_amount) / sum(returns_w_items), .by = c(year, agi_stub)) |> 
    pivot_wider(names_from = agi_stub, values_from = avg_charitable_contributions_deduction)

# Split up deductions over time (millions)
state_returns |>
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
        labs(title = "Deductions by Year", x = "Year", y = "Deductions (Millions)") +
        theme_classic() +
        scale_x_continuous(breaks = unique(state_returns$year)) +
        scale_fill_viridis_d() +
        theme(legend.position = "bottom")

# Repeat exercise per filer
state_returns |>
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
        scale_x_continuous(breaks = unique(state_returns$year)) +
        scale_fill_viridis_d() +
        theme(legend.position = "bottom")

# Repeat exercise conditional on itemizing
state_returns |>
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
        scale_x_continuous(breaks = unique(state_returns$year)) +
        scale_fill_viridis_d() +
        theme(legend.position = "bottom")

# Merge state names to fips codes
fips_codes_df <- tigris::fips_codes

state_fips_map <- fips_codes_df |>
    select(state_code, state) |>
    distinct() |>
    mutate(statefips = as.numeric(state_code))

state_returns <- state_returns |>
    left_join(state_fips_map, by = "state") |>
    rename(state_name = state, state = statefips)

# Export to database
dbWriteTable(
    con,
    "returns_state",
    state_returns |> select(-state_code),
    overwrite = TRUE
)
dbDisconnect(con, shutdown = TRUE)
