# Clean and load
gc()
rm(list = ls())
getwd()

# Load libraries
library(data.table)
library(tidyr)
library(dplyr)
library(stringr)
library(purrr)
library(duckdb)

# Connect to the DuckDB database
con <- dbConnect(duckdb::duckdb(), dbdir = '../../../data/database.db')

########################################################
# Inflows
########################################################

# Paths for inflows
inflow_dir <- "../../../data/SOI/migration_county"
inflow_files <- list.files(inflow_dir, pattern = "^countyinflow\\d{4}\\.csv$", full.names = TRUE)

if (length(inflow_files) == 0) {
    stop("No county migration files found.")
}

# Read files
inflow_list <- inflow_files |>
    set_names(basename) |>
    map(fread)

# Check for differences in variable names
var_names <- inflow_list |>
    map(names)

baseline_inflow_vars <- var_names[[1]]

inflow_diff_table <- tibble(
    file = names(var_names),
    n_vars = map_int(var_names, length),
    extra_vars = map_chr(var_names, ~ paste(setdiff(.x, baseline_inflow_vars), collapse = ", ")),
    missing_vars = map_chr(var_names, ~ paste(setdiff(baseline_inflow_vars, .x), collapse = ", "))
)

print(inflow_diff_table)

# Add years from filename (e.g., "countyinflow1112.csv" -> 2011)
file_years <- names(inflow_list) |>
    str_extract("\\d{4}") |>
    str_sub(1, 2) |>
    as.numeric()
file_years <- 2000 + file_years # If I move in 2018, this will show up in 2019.

# Add years to dataframes
inflow_list <- map2(
    inflow_list,
    file_years,
    ~ mutate(.x, year = .y)
)

# Bind dataframes
inflow_data <- bind_rows(inflow_list)

# Rename variables
inflow_data <- inflow_data |>
    rename(
        destination_state = y2_statefips,
        origin_state = y1_statefips,
        origin_state_name = y1_state,
        origin_county_name = y1_countyname,
        returns = n1,
        individuals = n2
    ) |> 
    mutate(
        destination_fips = 1000 * destination_state + y2_countyfips,
        origin_fips = 1000 * origin_state + y1_countyfips
    ) |> 
    select(-y1_countyfips, -y2_countyfips)

# Drop state totals
inflow_data <- inflow_data |>
    filter(destination_fips %% 1000 != 0)

# Drop header records
inflow_data <- inflow_data |>
    filter(
        !str_detect(origin_county_name, "Total Migration") &
        !str_detect(origin_county_name, "Non-migrants")
    )

# Convert censored items to NAs
inflow_data <- inflow_data |>
    mutate(
        across(c(returns, individuals, agi), ~ if_else(.x == - 1, NA_real_, .x))
    )

# Check for duplicates
inflow_data <- inflow_data |> distinct()

########################################################
# Outflows
########################################################

# Paths for outflows
outflow_dir <- "../../../data/SOI/migration_county"
outflow_files <- list.files(outflow_dir, pattern = "^countyoutflow\\d{4}\\.csv$", full.names = TRUE)

if (length(outflow_files) == 0) {
    stop("No county outflow files found.")
}

# Read files
outflow_list <- outflow_files |>
    set_names(basename) |>
    map(fread)

# Check for differences in variable names
outflow_var_names <- outflow_list |>
    map(names)

baseline_outflow_vars <- outflow_var_names[[1]]

outflow_diff_table <- tibble(
    file = names(outflow_var_names),
    n_vars = map_int(outflow_var_names, length),
    extra_vars = map_chr(outflow_var_names, ~ paste(setdiff(.x, baseline_outflow_vars), collapse = ", ")),
    missing_vars = map_chr(outflow_var_names, ~ paste(setdiff(baseline_outflow_vars, .x), collapse = ", "))
)

print(outflow_diff_table)

# Add years from filename (e.g., "countyoutflow1112.csv" -> 2011)
file_years <- names(outflow_list) |>
    str_extract("\\d{4}") |>
    str_sub(1, 2) |>
    as.numeric()
file_years <- 2000 + file_years

# Add years to dataframes
outflow_list <- map2(
    outflow_list,
    file_years,
    ~ mutate(.x, year = .y)
)

# Bind dataframes
outflow_data <- bind_rows(outflow_list)

# Rename variables
outflow_data <- outflow_data |>
    rename(
        origin_state = y1_statefips,
        destination_state = y2_statefips,
        destination_state_name = y2_state,
        destination_county_name = y2_countyname,
        returns = n1,
        individuals = n2
    ) |> 
    mutate(
        origin_fips = 1000 * origin_state + y1_countyfips,
        destination_fips = 1000 * destination_state + y2_countyfips
    ) |> 
    select(-y1_countyfips, -y2_countyfips)

# Drop state totals
outflow_data <- outflow_data |>
    filter(origin_fips %% 1000 != 0)

# Drop header records
outflow_data <- outflow_data |>
    filter(
        !str_detect(destination_county_name, "Total Migration") &
        !str_detect(destination_county_name, "Non-migrants")
    )

# Recode censored items as NA
outflow_data <- outflow_data |>
    mutate(
        across(c(returns, individuals, agi), ~ if_else(.x == - 1, NA_real_, .x))
    )

# Remove duplicates
outflow_data <- outflow_data |> distinct()





########################################################
# Construct a balanced panel of inflows and outflows
########################################################

# Which inflow-pairs are observed throughout?
inflow_data |> 
    reframe(n = sum(returns > 0), .by = c(destination_fips, origin_fips)) |> 
    reframe(n(), .by = n) |> 
    arrange(desc(n))

inflow_pairs <- inflow_data |> 
    mutate(n = sum(returns > 0), .by = c(destination_fips, origin_fips)) |> 
    filter(n == 11) |> 
    distinct(destination_fips, origin_fips)

# Which outflow-pairs are observed throughout?
outflow_data |> 
    reframe(n = sum(returns > 0), .by = c(origin_fips, destination_fips)) |> 
    reframe(n(), .by = n) |> 
    arrange(desc(n))

outflow_pairs <- outflow_data |> 
    mutate(n = sum(returns > 0), .by = c(origin_fips, destination_fips)) |> 
    filter(n == 11) |> 
    distinct(origin_fips, destination_fips)

# Inflow from origin A into destination B should mirror outflow 
# from origin A into destination B. 
inflow_data |> 
    inner_join(inflow_pairs, by = c("destination_fips", "origin_fips")) |> 
    inner_join(outflow_pairs, by = c("origin_fips", "destination_fips")) |> 
    left_join(outflow_data, by = c("origin_fips", "destination_fips", "year"), suffix = c("_inflow", "_outflow")) |> 
    mutate(
        return_diff = abs(returns_inflow - returns_outflow),
        individual_diff = abs(individuals_inflow - individuals_outflow),
        agi_diff = abs(agi_inflow - agi_outflow)
    ) |> 
    filter(return_diff > 0) |> 
    View()

# Create a balanced migration panel for non-censored counties
balanced_panel <- expand_grid(
    inflow_pairs,
    year = 2011:2021
) |> 
    inner_join(
        inflow_data |> select(destination_fips, origin_fips, year, origin_state_name, origin_county_name, returns, individuals, agi),
        by = c("destination_fips", "origin_fips", "year")
    ) 

# Convert character columns to UTF-8
balanced_panel <- balanced_panel |>
    mutate(
        across(where(is.character), ~ iconv(.x, "latin1", "UTF-8"))
    )

# Export to database
dbWriteTable(
    con,
    "migration_county_balanced",
    balanced_panel,
    overwrite = TRUE
)
dbDisconnect(con, shutdown = TRUE)
