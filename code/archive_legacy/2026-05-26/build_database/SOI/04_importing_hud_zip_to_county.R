# Clean and load
gc()
rm(list = ls())
getwd()

# Load libraries
library(readxl)
library(dplyr)
library(stringr)
library(purrr)
library(duckdb)

# Paths
hud_dir <- "../../../data/hud/zip_to_county"
hud_files <- list.files(hud_dir, pattern = "\\.xlsx$", full.names = TRUE)

if (length(hud_files) == 0) {
    stop("No HUD zip-to-county files found.")
}

# Read files
hud_list <- hud_files |>
    set_names(basename) |>
    map(read_excel) |>
    map(~ { names(.x) <- tolower(names(.x)); .x })

# Check for differences in variable names
var_names <- hud_list |>
    map(names)

baseline_vars <- var_names[[1]]

diff_table <- tibble(
    file = names(var_names),
    n_vars = map_int(var_names, length),
    extra_vars = map_chr(var_names, ~ paste(setdiff(.x, baseline_vars), collapse = ", ")),
    missing_vars = map_chr(var_names, ~ paste(setdiff(baseline_vars, .x), collapse = ", "))
)

print(diff_table)

# Add year from filename (e.g., ZIP_COUNTY_032014.xlsx -> 2014)
file_years <- names(hud_list) |>
    str_extract("\\d{6}") |>
    str_sub(3, 6) |>
    as.numeric()

hud_list <- map2(
    hud_list,
    file_years,
    ~ mutate(.x, year = .y)
)

# Check total counts per file (observations, unique ZIPs, unique counties)
file_counts <- map2_dfr(
    hud_list,
    names(hud_list),
    ~ tibble(
        file = .y,
        n_rows = nrow(.x),
        n_zip = n_distinct(.x$zip),
        n_county = n_distinct(.x$county)
    )
)

print(file_counts)

# Combine and clean key fields
hud_crosswalk <- bind_rows(hud_list) |>
    mutate(
        zip = str_pad(as.character(zip), 5, pad = "0"),
        county = str_pad(as.character(county), 5, pad = "0")
    ) |> 
    select(-usps_zip_pref_city, -usps_zip_pref_state)

# Convert to numeric
hud_crosswalk <- hud_crosswalk |>
    mutate(zip = as.numeric(zip), county = as.numeric(county))
# Check NA counts for all columns
na_by_column <- hud_crosswalk |>
    summarise(across(everything(), ~ sum(is.na(.)))) |>
    pivot_longer(cols = everything(), names_to = "column", values_to = "na_count") |>
    arrange(desc(na_count))

print(na_by_column)

# Check sum of ratios within each county-year
ratio_sums <- hud_crosswalk |>
    group_by(year, zip) |>
    summarise(
        res_ratio_sum = sum(res_ratio, na.rm = TRUE),
        bus_ratio_sum = sum(bus_ratio, na.rm = TRUE),
        oth_ratio_sum = sum(oth_ratio, na.rm = TRUE),
        .groups = "drop"
    )

print(ratio_sums)

# Rename to match convention
hud_crosswalk <- hud_crosswalk |>
    rename(zipcode = zip)

# Export to database
con <- dbConnect(duckdb::duckdb(), dbdir = '../../../data/database.db')
dbWriteTable(
    con,
    "hud_zip_to_county",
    hud_crosswalk,
    overwrite = TRUE
)
dbDisconnect(con, shutdown = TRUE)
