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

# Paths
county_returns_dir <- "../../../data/SOI/county_returns_w_agi"
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
    "STATEFIPS", "STATE", "COUNTYFIPS", "COUNTYNAME", "agi_stub",
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
county_returns_list[[1]] <- county_returns_list[[1]] |>
    mutate(agi_stub = if_else(agi_stub == 7, 8, agi_stub)) |>
    mutate(agi_stub = if_else(agi_stub == 6, 7, agi_stub)) |>
    mutate(agi_stub = if_else(agi_stub == 5, 6, agi_stub)) |>
    mutate(agi_stub = if_else(agi_stub == 4, 5, agi_stub)) |>
    mutate(agi_stub = if_else(agi_stub == 3, 4, agi_stub)) |>
    mutate(agi_stub = if_else(agi_stub == 2, 3, agi_stub))
# And for all others, we need to combine agi 2 and 3 into 3.
county_returns_list[2:length(county_returns_list)] <- map(
    county_returns_list[2:length(county_returns_list)],
    ~ .x |>
    mutate(new_agi_stub = if_else(agi_stub == 2, 3, agi_stub)) |>
    select(-agi_stub) |>
    reframe(
        across(where(is.numeric), ~ sum(.x, na.rm = TRUE)),
        .by = c(STATEFIPS, STATE, COUNTYFIPS, COUNTYNAME, year, new_agi_stub)
    ) |>
    rename(agi_stub = new_agi_stub)
)


# Merge selected variables into one table
county_returns_selected <- county_returns_list |>
    map(~ select(.x, any_of(c(required_vars, "year")))) |>
    bind_rows()

cat("Merged rows:", nrow(county_returns_selected), "\n")


# Look at supression of agi cells, that is compare totals here to totals * 8 from county_returns
con <- dbConnect(duckdb::duckdb(), dbdir = '../../../data/database.db')
dbGetQuery(con, "select year, 8 * count(1) as total_returns from returns_county group by year order by year")
dbDisconnect(con, shutdown = TRUE)
# Around 12.5% are suppressed.

# Drop state totals (countyfips = 0)
county_returns_selected <- county_returns_selected |>
    filter(COUNTYFIPS != 0)

# Drop county totals (agi_stub = 0)
county_returns_selected <- county_returns_selected |>
    filter(agi_stub != 0)
    
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

# 2020, Loving County, Texas has two observations in agi_stub = 1.
# county_returns_selected |>
#     filter(COUNTYFIPS == 301 & STATEFIPS == 48 & year == 2020 & agi_stub == 1)
# county_returns_selected <- county_returns_selected |>
#     group_by(COUNTYFIPS, STATEFIPS, year, agi_stub) |>
#     slice_head(n = 1) |>
#     ungroup()

# Rename variables to match county_returns and conventions
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

# Checks analogous to county-level data
na_by_year_wide_agi <- county_data_agi |>
    group_by(year) |>
    summarise(
        total_obs = n(),
        across(everything(), ~ sum(is.na(.)), .names = "na_{.col}"),
        .groups = "drop"
    ) |>
    arrange(year)

View(na_by_year_wide_agi)

# Total number of returns by year and AGI stub
county_data_agi |>
    reframe(total_returns = sum(returns) / 1000000, .by = c(year, agi_stub)) |> 
    pivot_wider(names_from = agi_stub, values_from = total_returns)

# Total number of 'individuals' by year and AGI stub
county_data_agi |>
    reframe(total_individuals = sum(individuals) / 1000000, .by = c(year, agi_stub)) |> 
    pivot_wider(names_from = agi_stub, values_from = total_individuals)

# Itemization share over time (by AGI stub) -> why did some regions with 0 income itemize in 2011?
county_data_agi |>
    reframe(itemization_share = sum(returns_w_items) / sum(returns), .by = c(year, agi_stub)) |> 
    pivot_wider(names_from = agi_stub, values_from = itemization_share)

# Total amount itemized over time (by AGI stub)
county_data_agi |>
    reframe(total_itemized_amount = sum(itemized_amount) / 1000000000, .by = c(year, agi_stub)) |>
    pivot_wider(names_from = agi_stub, values_from = total_itemized_amount)

# If itemizing, average amount itemized (by AGI stub)
county_data_agi |>
    reframe(avg_itemized_amount = sum(itemized_amount) / sum(returns_w_items), .by = c(year, agi_stub)) |>
    pivot_wider(names_from = agi_stub, values_from = avg_itemized_amount)

# Graph: itemized components by year, faceted by AGI stub
county_data_agi |>
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
county_data_agi |>
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
con <- dbConnect(duckdb::duckdb(), dbdir = '../../../data/database.db')
dbWriteTable(
    con,
    "returns_county_agi",
    county_data_agi |> select(-state, -countyname),
    overwrite = TRUE
)
dbDisconnect(con, shutdown = TRUE)

