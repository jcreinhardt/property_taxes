# Clean and load
gc()
rm(list = ls())
getwd()

# Load libraries
library(data.table)
library(dplyr)
library(stringr)
library(purrr)

# Connect to the DuckDB database
con <- dbConnect(duckdb::duckdb(), dbdir = '../../../data/database.db')

########################################################
# Inflows
########################################################

inflow_dir <- "../../../data/SOI/migration_state"
inflow_files <- list.files(inflow_dir, pattern = "^stateinflow\\d{4}\\.csv$", full.names = TRUE)

if (length(inflow_files) == 0) {
    stop("No state inflow files found.")
}

# Read files
inflow_list <- inflow_files |>
    set_names(basename) |>
    map(fread)

# Check for differences in variable names
inflow_var_names <- inflow_list |>
    map(names)

baseline_inflow_vars <- inflow_var_names[[1]]

inflow_diff_table <- tibble(
    file = names(inflow_var_names),
    n_vars = map_int(inflow_var_names, length),
    extra_vars = map_chr(inflow_var_names, ~ paste(setdiff(.x, baseline_inflow_vars), collapse = ", ")),
    missing_vars = map_chr(inflow_var_names, ~ paste(setdiff(baseline_inflow_vars, .x), collapse = ", "))
)

print(inflow_diff_table)

# Add years from filename (e.g., "stateinflow1112.csv" -> 2011)
file_years <- names(inflow_list) |>
    str_extract("\\d{4}") |>
    str_sub(1, 2) |>
    as.numeric()
file_years <- 2000 + file_years

# Keep years up to 2011
keep_idx <- which(file_years >= 2011)
inflow_list <- inflow_list[keep_idx]
file_years <- file_years[keep_idx]

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
        origin_state_description = y1_state_name,
        returns = n1,
        individuals = n2,
        agi = AGI
    )

# Drop header records
inflow_data <- inflow_data |>
    filter(
        !str_detect(origin_state_description, "Total Migration") &
        !str_detect(origin_state_description, "Non-migrants")
    )

# Convert censored items to NAs
inflow_data <- inflow_data |>
    mutate(
        across(c(returns, individuals, agi), ~ if_else(.x == - 1, NA_real_, .x))
    )

# Remove duplicates
inflow_data <- inflow_data |> distinct()



########################################################
# Outflows
########################################################

outflow_dir <- "../../../data/SOI/migration_state"
outflow_files <- list.files(outflow_dir, pattern = "^stateoutflow\\d{4}\\.csv$", full.names = TRUE)

if (length(outflow_files) == 0) {
    stop("No state outflow files found.")
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

# Add years from filename (e.g., "stateoutflow1112.csv" -> 2011)
file_years <- names(outflow_list) |>
    str_extract("\\d{4}") |>
    str_sub(1, 2) |>
    as.numeric()
file_years <- 2000 + file_years

# Keep years up to 2011
keep_idx <- which(file_years >= 2011)
outflow_list <- outflow_list[keep_idx]
file_years <- file_years[keep_idx]

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
        destination_state_description = y2_state_name,
        returns = n1,
        individuals = n2,
        agi = AGI
    )

# Drop header records
outflow_data <- outflow_data |>
    filter(
        !str_detect(destination_state_description, "Total Migration") &
        !str_detect(destination_state_description, "Non-migrants")
    )

# Convert censored items to NAs
outflow_data <- outflow_data |>
    mutate(
        across(c(returns, individuals, agi), ~ if_else(.x == - 1, NA_real_, .x))
    )

# Remove duplicates
outflow_data <- outflow_data |> distinct() 


########################################################
# Compare and write to database
########################################################

# Which inflow-pairs are observed throughout?
inflow_data |> 
    reframe(n = sum(returns > 0), .by = c(destination_state, origin_state)) |> 
    reframe(n(), .by = n) |> 
    arrange(desc(n))

inflow_pairs <- inflow_data |> 
    mutate(n = sum(returns > 0), .by = c(destination_state, origin_state)) |> 
    filter(n == 11) |> 
    distinct(destination_state, origin_state)

# Which outflow-pairs are observed throughout?
outflow_data |> 
    reframe(n = sum(returns > 0), .by = c(origin_state, destination_state)) |> 
    reframe(n(), .by = n) |> 
    arrange(desc(n))

outflow_pairs <- outflow_data |> 
    mutate(n = sum(returns > 0), .by = c(origin_state, destination_state)) |> 
    filter(n == 11) |> 
    distinct(origin_state, destination_state)

# Inflow from origin A into destination B should mirror outflow 
# from origin A into destination B. 
inflow_data |> 
    inner_join(inflow_pairs, by = c("destination_state", "origin_state")) |> 
    inner_join(outflow_pairs, by = c("origin_state", "destination_state")) |> 
    left_join(outflow_data, by = c("origin_state", "destination_state", "year"), suffix = c("_inflow", "_outflow")) |> 
    mutate(
        return_diff = abs(returns_inflow - returns_outflow),
        individual_diff = abs(individuals_inflow - individuals_outflow),
        agi_diff = abs(agi_inflow - agi_outflow)
    ) |> 
    filter(return_diff > 0) |> 
    View()

outflow_data |> 
    inner_join(outflow_pairs, by = c("origin_state", "destination_state")) |> 
    inner_join(inflow_pairs, by = c("destination_state", "origin_state")) |> 
    left_join(inflow_data, by = c("destination_state", "origin_state", "year"), suffix = c("_outflow", "_inflow")) |> 
    mutate(
        return_diff = abs(returns_outflow - returns_inflow),
        individual_diff = abs(individuals_outflow - individuals_inflow),
        agi_diff = abs(agi_outflow - agi_inflow)
    ) |> 
    filter(return_diff > 0) |> 
    View()

# Export to database
dbWriteTable(
    con,
    "migration_state",
    inflow_data,
    overwrite = TRUE
)
dbDisconnect(con, shutdown = TRUE)
