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
zipcode_returns_dir <- "../../../data/SOI/zipcode_returns_w_agi"
zipcode_files <- list.files(zipcode_returns_dir, pattern = "\\.csv$", full.names = TRUE)

if (length(zipcode_files) == 0) {
    stop("No zipcode return files found.")
}

# Read files and standardize column names
zipcode_returns_list <- zipcode_files |>
    set_names(basename) |>
    map(fread) |>
    map(~ { names(.x) <- toupper(names(.x)); .x })

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

# Turns out 2011 does not split into categories agi 1 and 2
# So we need to turn 2 into 3, 3 into 4, etc.
zipcode_returns_list[[1]] <- zipcode_returns_list[[1]] |>
    mutate(AGI_STUB = if_else(AGI_STUB == 7, 8, AGI_STUB)) |>
    mutate(AGI_STUB = if_else(AGI_STUB == 6, 7, AGI_STUB)) |>
    mutate(AGI_STUB = if_else(AGI_STUB == 5, 6, AGI_STUB)) |>
    mutate(AGI_STUB = if_else(AGI_STUB == 4, 5, AGI_STUB)) |>
    mutate(AGI_STUB = if_else(AGI_STUB == 3, 4, AGI_STUB)) |>
    mutate(AGI_STUB = if_else(AGI_STUB == 2, 3, AGI_STUB))

# And for all others, we need to combine agi 2 and 3 into 3.
zipcode_returns_list[2:length(zipcode_returns_list)] <- map(
    zipcode_returns_list[2:length(zipcode_returns_list)],
    ~ .x |>
    mutate(new_agi_stub = if_else(AGI_STUB == 2, 3, AGI_STUB)) |>
    reframe(
        across(where(is.numeric) & !matches("^AGI_STUB$|^NEW_AGI_STUB$"), ~ sum(.x, na.rm = TRUE)),
        .by = c(STATEFIPS, STATE, ZIPCODE, year, new_agi_stub)
    ) |>
    rename(AGI_STUB = new_agi_stub)
)

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
zipcode_data_agi <- zipcode_returns_selected |>
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

View(zipcode_data_agi)

# Share of returns in each AGI stub by year
zipcode_data_agi |>
    reframe(total_returns = sum(returns) / 1000000, .by = c(year, agi_stub)) |>
    pivot_wider(names_from = agi_stub, values_from = total_returns)

# Itemization share over time (by AGI stub)
zipcode_data_agi |>
    reframe(itemization_share = sum(returns_w_items) / sum(returns), .by = c(year, agi_stub)) |>
    pivot_wider(names_from = agi_stub, values_from = itemization_share)

# Total amount itemized over time (by AGI stub)
zipcode_data_agi |>
    reframe(total_itemized_amount = sum(itemized_amount) / 1000000000, .by = c(year, agi_stub)) |> 
    pivot_wider(names_from = agi_stub, values_from = total_itemized_amount)

# If itemizing, average amount itemized (by AGI stub)
zipcode_data_agi |>
    reframe(avg_itemized_amount = sum(itemized_amount) / sum(returns_w_items), .by = c(year, agi_stub)) |>
    pivot_wider(names_from = agi_stub, values_from = avg_itemized_amount)

# Graph: itemized components by year, faceted by AGI stub
zipcode_data_agi |>
    reframe(
        salt_taxes = sum(taxes_paid_amount) / 1000000,
        mortgage_interest = sum(mortgage_interest_amount) / 1000000,
        charitable_contributions = sum(charitable_contributions_amount) / 1000000,
        other_deductions = sum(itemized_amount) / 1000000 -
            salt_taxes - mortgage_interest - charitable_contributions,
        .by = c(year, agi_stub)
    ) |>
    pivot_longer(
        cols = c(salt_taxes, mortgage_interest, charitable_contributions, other_deductions),
        names_to = "deduction_type",
        values_to = "amount_millions"
    ) |>
    ggplot(aes(x = year, y = amount_millions, group = deduction_type, fill = deduction_type)) +
        geom_bar(stat = "identity") +
        labs(title = "Deductions by Year and AGI Stub", x = "Year", y = "Deductions (Millions)") +
        theme_classic() +
        scale_x_continuous(breaks = c(2011, 2016, 2021)) +
        scale_fill_viridis_d() +
        facet_wrap(~ agi_stub) +
        theme(legend.position = "bottom")

# Graph: itemized components per filer by year, faceted by AGI stub
zipcode_data_agi |>
    reframe(
        salt_taxes = sum(taxes_paid_amount) / sum(returns),
        mortgage_interest = sum(mortgage_interest_amount) / sum(returns),
        charitable_contributions = sum(charitable_contributions_amount) / sum(returns),
        other_deductions = sum(itemized_amount) / sum(returns) -
            salt_taxes - mortgage_interest - charitable_contributions,
        .by = c(year, agi_stub)
    ) |>
    pivot_longer(
        cols = c(salt_taxes, mortgage_interest, charitable_contributions, other_deductions),
        names_to = "deduction_type",
        values_to = "deduction_per_filer"
    ) |>
    ggplot(aes(x = year, y = deduction_per_filer, group = deduction_type, fill = deduction_type)) +
        geom_bar(stat = "identity") +
        labs(title = "Deductions per Filer by Year and AGI Stub", x = "Year", y = "Deductions (Per Filer)") +
        theme_classic() +
        scale_x_continuous(breaks = c(2011, 2016, 2021)) +
        scale_fill_viridis_d() +
        facet_wrap(~ agi_stub) +
        theme(legend.position = "bottom")

# Export to database
dbWriteTable(
    con,
    "returns_zipcode_agi",
    zipcode_data_agi |> select(-state),
    overwrite = TRUE
)
dbDisconnect(con, shutdown = TRUE)
