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
    filter(ZIPCODE != 0) |> 
    filter(ZIPCODE != 99999)

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

# Aggregate to one observation per zipcode-year
zipcode_data <- zipcode_data |>
    select(-any_of(c("state", "statefips"))) |>
    group_by(year, zipcode) |>
    summarise(
        across(where(is.numeric) & !matches("^(year|zipcode)$"), ~ sum(.x, na.rm = TRUE)),
        .groups = "drop"
    )

# How many returns are there?
zipcode_data |>
    reframe(total_returns = sum(returns), .by = year)

# That's how much compared to county-level totals?
dbGetQuery(con, "select year, sum(returns) as total_returns from returns_county group by year order by year")

# Number of zipcodes per year
zipcode_data |>
    reframe(n_zipcodes = n_distinct(zipcode), .by = year)

# Compare to number of zipcodes in HUD crosswalk
dbGetQuery(con, "select count(distinct zipcode) as n_zipcodes from hud_zip_to_county group by year order by year")

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

# Compare zipcode aggregates to state aggregates via HUD crosswalk
hud_crosswalk <- dbGetQuery(con, "select * from hud_zip_to_county") |>
    mutate(year = year - 1) # Since you file the year after the tax year

numeric_cols <- zipcode_data |>
    select(where(is.numeric)) |>
    names()

value_vars <- setdiff(numeric_cols, c("year", "zipcode", "statefips"))

setdiff(zipcode_data$zipcode, hud_crosswalk$zipcode)

zipcode_data |> 
    filter(zipcode %in% setdiff(zipcode_data$zipcode, hud_crosswalk$zipcode)) |>
    View() # The other-totals are quite large, about 3m returns

zip_to_county <- zipcode_data |> 
    left_join(hud_crosswalk, by = c("zipcode", "year")) |> 
    filter(!is.na(county) & county != 99999) |> # Not sure what the 99999 counties are
    group_by(county, year) |>
    summarise(
        across(all_of(value_vars), ~ sum(.x * res_ratio, na.rm = TRUE)),
        .groups = "drop"
    )

zip_to_state <- zip_to_county |>
    mutate(statefips = county %/% 1000) |>
    group_by(statefips, year) |>
    summarise(
        across(all_of(value_vars), sum, na.rm = TRUE),
        .groups = "drop"
    )

state_returns <- dbGetQuery(con, "select * from returns_state") |>
    group_by(state, year) |>
    summarise(
        across(where(is.numeric), sum, na.rm = TRUE),
        .groups = "drop"
    ) |>
    select(-agi_stub)

zip_v_state <- zip_to_state |>
    rename(state = statefips) |>
    left_join(state_returns, by = c("state", "year"), suffix = c("_zip", "_state"))

## Total returns
zip_v_state |>
    reframe(
        state_total = sum(returns_state),
        zip_total = sum(returns_zip),
        coverage = zip_total / state_total,
        .by = year
    )
lm(returns_zip ~ returns_state, data = zip_v_state) |> summary()
ggplot(zip_v_state, aes(x = returns_zip, y = returns_state)) +
    geom_point() +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "Returns filed across all years",
         x = "Returns filed at zipcode level",
         y = "Returns filed at state level")

## Individuals
zip_v_state |>
    reframe(
        state_total = sum(individuals_state),
        zip_total = sum(individuals_zip),
        coverage = zip_total / state_total,
        .by = year
    )
lm(individuals_zip ~ individuals_state, data = zip_v_state) |> summary()
ggplot(zip_v_state, aes(x = individuals_zip, y = individuals_state)) +
    geom_point() +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "Individuals filed across all years",
         x = "Individuals filed at zipcode level",
         y = "Individuals filed at state level")

## Itemizers (higher since mostly located in larger zipcodes?)
zip_v_state |>
    reframe(
        state_total = sum(returns_w_items_state),
        zip_total = sum(returns_w_items_zip),
        coverage = zip_total / state_total,
        .by = year
    )
lm(returns_w_items_zip ~ returns_w_items_state, data = zip_v_state) |> summary()
ggplot(zip_v_state, aes(x = returns_w_items_zip, y = returns_w_items_state)) +
    geom_point() +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "Number of itemizers across all years",
         x = "Number of itemizers at zipcode level",
         y = "Number of itemizers at state level")

## Itemized amount
zip_v_state |>
    reframe(
        state_total = sum(itemized_amount_state),
        zip_total = sum(itemized_amount_zip),
        coverage = zip_total / state_total,
        .by = year
    )
lm(itemized_amount_zip ~ itemized_amount_state, data = zip_v_state) |> summary()
ggplot(zip_v_state, aes(x = itemized_amount_zip, y = itemized_amount_state)) +
    geom_point() +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "Itemized amount across all years",
         x = "Itemized amount at zipcode level",
         y = "Itemized amount at state level")

## SALT deductions (<4% loss)
zip_v_state |>
    reframe(
        state_total = sum(taxes_paid_amount_state),
        zip_total = sum(taxes_paid_amount_zip),
        coverage = zip_total / state_total,
        .by = year
    )
lm(taxes_paid_amount_zip ~ taxes_paid_amount_state, data = zip_v_state) |> summary()
ggplot(zip_v_state, aes(x = taxes_paid_amount_zip, y = taxes_paid_amount_state)) +
    geom_point() +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "SALT deductions across all years",
         x = "SALT deductions at zipcode level",
         y = "SALT deductions at state level")

## State income tax (<7% loss)
zip_v_state |>
    reframe(
        state_total = sum(state_and_local_income_taxes_amount_state),
        zip_total = sum(state_and_local_income_taxes_amount_zip),
        coverage = zip_total / state_total,
        .by = year
    )
lm(state_and_local_income_taxes_amount_zip ~ state_and_local_income_taxes_amount_state, data = zip_v_state) |> summary()
ggplot(zip_v_state, aes(x = state_and_local_income_taxes_amount_zip, y = state_and_local_income_taxes_amount_state)) +
    geom_point() +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "State income tax deductions across all years",
         x = "State income tax deductions at zipcode level",
         y = "State income tax deductions at state level")

## State sales tax
zip_v_state |>
    reframe(
        state_total = sum(state_and_local_sales_tax_amount_state),
        zip_total = sum(state_and_local_sales_tax_amount_zip),
        coverage = zip_total / state_total,
        .by = year
    )
lm(state_and_local_sales_tax_amount_zip ~ state_and_local_sales_tax_amount_state, data = zip_v_state) |> summary()
ggplot(zip_v_state, aes(x = state_and_local_sales_tax_amount_zip, y = state_and_local_sales_tax_amount_state)) +
    geom_point() +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "State sales tax deductions across all years",
         x = "State sales tax deductions at zipcode level",
         y = "State sales tax deductions at state level")

## Property tax deductions (<2.5% loss)
zip_v_state |>
    reframe(
        state_total = sum(property_taxes_amount_state),
        zip_total = sum(property_taxes_amount_zip),
        coverage = zip_total / state_total,
        .by = year
    )
lm(property_taxes_amount_zip ~ property_taxes_amount_state, data = zip_v_state) |> summary()
ggplot(zip_v_state, aes(x = property_taxes_amount_zip, y = property_taxes_amount_state)) +
    geom_point() +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "Property tax deductions across all years",
         x = "Property tax deductions at zipcode level",
         y = "Property tax deductions at state level")

## Mortgage interest deductions
zip_v_state |>
    reframe(
        state_total = sum(mortgage_interest_amount_state),
        zip_total = sum(mortgage_interest_amount_zip),
        coverage = zip_total / state_total,
        .by = year
    )
lm(mortgage_interest_amount_zip ~ mortgage_interest_amount_state, data = zip_v_state) |> summary()
ggplot(zip_v_state, aes(x = mortgage_interest_amount_zip, y = mortgage_interest_amount_state)) +
    geom_point() +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "Mortgage interest deductions across all years",
         x = "Mortgage interest deductions at zipcode level",
         y = "Mortgage interest deductions at state level")

## Charitable contributions deductions
zip_v_state |>
    reframe(
        state_total = sum(charitable_contributions_amount_state),
        zip_total = sum(charitable_contributions_amount_zip),
        coverage = zip_total / state_total,
        .by = year
    )
lm(charitable_contributions_amount_zip ~ charitable_contributions_amount_state, data = zip_v_state) |> summary()
ggplot(zip_v_state, aes(x = charitable_contributions_amount_zip, y = charitable_contributions_amount_state)) +
    geom_point() +
    geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
    geom_smooth(method = "lm", se = FALSE, color = "blue") +
    labs(title = "Charitable contributions across all years",
         x = "Charitable contributions at zipcode level",
         y = "Charitable contributions at state level")

# Export to database
dbWriteTable(
    con,
    "returns_zipcode",
    zipcode_data,
    overwrite = TRUE
)
dbDisconnect(con, shutdown = TRUE)
