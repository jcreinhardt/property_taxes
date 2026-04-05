# Clean up
gc()
rm(list = ls())
getwd()

# Load libraries
library(duckdb)
library(tidycensus)
library(data.table)
library(dplyr)
library(tidyr)
library(tidyverse)
library(purrr)
library(ggplot2)
library(fixest)
library(stringr)
library(rdrobust)
library(binsreg)
library(sf)
library(tigris)

# Load SOI migration data
df_list <- list.files("../../data/migration_SOI/", pattern = "^countyout.*\\.csv$", full.names = TRUE) |> 
    map(fread) 

# Remove some aggregate rows
add_up_outflows <- function(df) {
    df |> 
        filter(y1_countyfips != 0 & y2_countyfips != 0) |>
        filter(y1_statefips %in% 1:59 & y2_statefips %in% 1:59) |>
        mutate(fips = y1_statefips * 1000 + y1_countyfips) |>
        reframe(
            n1 = sum(n1),
            n2 = sum(n2),
            agi = sum(agi),
            .by = fips
        ) 
}

outflows <- df_list |>
    map(add_up_outflows) 

# Add years back in
file_ends <- list.files("../../data/migration_SOI/", pattern = "^countyout.*\\.csv$", full.names = FALSE) |>
    str_extract("\\d{4}") |>
    as.numeric() 
years <- file_ends %% 100

add_years <- function(df, year) {
    df |>
        mutate(year = year)
}

outflows <- outflows |>
    map2(years, add_years) |> 
    bind_rows()


# Repeat for inflows
add_up_inflows <- function(df) {
    df |>
        filter(y1_countyfips != 0 & y2_countyfips != 0) |>
        filter(y1_statefips %in% 1:59 & y2_statefips %in% 1:59) |>
        mutate(fips = y2_statefips * 1000 + y2_countyfips) |>
        reframe(
            n1 = sum(n1),
            n2 = sum(n2),
            agi = sum(agi),
            .by = fips) 
}

inflows <- list.files("../../data/migration_SOI/", pattern = "^countyin.*\\.csv$", full.names = TRUE) |>
    map(fread) |>
    map(add_up_inflows) 

file_ends <- list.files("../../data/migration_SOI/", pattern = "^countyin.*\\.csv$", full.names = FALSE) |>
    str_extract("\\d{4}") |>
    as.numeric() 
years <- file_ends %% 100

inflows <- inflows |>
    map2(years, add_years) |>
    bind_rows()

# Combine and calculate net migration (inflows has non-US territories)
net_migration <- inflows |>
    rename(
        inmigration_filers = n1,
        inmigration_pop = n2,
        inmigration_agi = agi
    ) |>
    inner_join(outflows, by = c("fips", "year")) |>
    rename(
        outmigration_filers = n1,
        outmigration_pop = n2,
        outmigration_agi = agi
    ) |>
    mutate(
        net_migration_filers = inmigration_filers - outmigration_filers,
        net_migration_pop = inmigration_pop - outmigration_pop,
        net_migration_agi = inmigration_agi - outmigration_agi
    )

# Write to database
fwrite(net_migration, "../../data/migration_SOI/net_migration.csv")

# Load SOI zip code level filings
df_list <- list.files(
        "../../data/zipcode_SOI/", 
        pattern = "^(1[1-9]|2[0-2])zpallagi\\.csv$", 
        full.names = TRUE
    ) |> 
    map(fread) 

# Fix some variable naming issues
# Function to convert all variable names to lower-case for each dataframe in a list
to_lower_names <- function(df) {
    names(df) <- tolower(names(df))
    df
}
df_list <- map(df_list, to_lower_names)


# Add filing years
file_ends <- list.files(
    "../../data/zipcode_SOI/", 
    pattern = "^(1[1-9]|2[0-2])zpallagi\\.csv$", 
    full.names = FALSE
) |>
    substr(0,2) |>
    as.numeric() 
years <- file_ends + 2000  

zipcode_filings <- df_list |>
    map2(years, add_years) |>
    bind_rows()

# Drop some aggregate categories
zipcode_filings <- zipcode_filings |>
    filter(zipcode != 0 & zipcode != 99999)

# Tabulate some key variables
unique(zipcode_filings$state)
sum(zipcode_filings$n1) 
zipcode_filings |> reframe(n = sum(n1), .by = year)
zipcode_filings |> filter(year == 2022) |> reframe(n = sum(n18425))

# Share of itemizers over time
zipcode_filings |> 
    reframe(
        total_itemizers = sum(n04470),
        total_filers = sum(n1),
        .by = c(statefips, zipcode, year)
    ) |>
    reframe(share = sum(total_itemizers) / sum(total_filers), .by = year) |>
    arrange(year)

# How balanced is the panel?
zipcode_filings |> reframe(n = length(unique(zipcode)), .by = year) |> arrange(year)
zipcode_filings |>
    reframe(obs = 1, .by = c(zipcode, year)) |>
    reframe(n = n(), .by = zipcode) |>
    reframe(occurences = n(), .by = n) |>
    arrange(n)

# Isolate the years for which the zip codes only show up once
zipcode_filings |>
    group_by(zipcode) |>
    filter(n_distinct(year) == 1) |>
    ungroup() |>
    reframe(n = n(), .by = year)

# Distribution in change in itemizer share across zip codes
zipcode_filings |>
    reframe(
        total_itemizers = sum(n04470),
        total_filers = sum(n1),
        .by = c(statefips, zipcode, year)
    ) |>
    mutate(
        itemizer_share = total_itemizers / total_filers,
        period = ifelse(year < 2018, "pre-2018", "post-2018")
    ) |>
    reframe(avg = mean(itemizer_share, na.rm = TRUE), .by = c(statefips, zipcode, period)) |>
    reframe(change = avg[period == "post-2018"] - avg[period == "pre-2018"], .by = c(statefips, zipcode)) |> 
    ggplot(aes(x = change)) +
    geom_histogram() +
    labs(
        title = "Distribution of Change in Itemizer Share Across Zip Codes",
        x = "Change in Itemizer Share",
        y = "Frequency"
    ) +
    xlim(-1, .1) +
    theme_classic()

# Distribution in change in itemizer share across zip codes and agi classes
zipcode_filings |>
    reframe(
        total_itemizers = sum(n04470),
        total_filers = sum(n1),
        .by = c(statefips, zipcode, agi_stub, year)
    ) |>
    mutate(
        itemizer_share = total_itemizers / total_filers,
        period = ifelse(year < 2018, "pre-2018", "post-2018")
    ) |>
    reframe(avg = mean(itemizer_share, na.rm = TRUE), .by = c(statefips, zipcode, agi_stub, period)) |>
    reframe(change = avg[period == "post-2018"] - avg[period == "pre-2018"], .by = c(statefips, zipcode, agi_stub)) |> 
    ggplot(aes(x = change, fill = agi_stub)) +
    geom_histogram() +
    labs(
        title = "Distribution of Change in Itemizer Share Across Zip Codes and AGI Classes",
        x = "Change in Itemizer Share",
        y = "Frequency"
    ) +
    xlim(-1, .1) +
    theme_classic() +
    facet_wrap(~ agi_stub)

# Merge in some transactional data
transactions <- read.csv("../../data/zipcode_transactions.csv")

nrow(transactions)
nrow(zipcode_filings) / 6

# The transaction data has states or zipcodes that are typos.
# Drop all observations where the state is not consistent with the zipcode.
# Load the HUD zipcode to county crosswalk to compare
crosswalk <- read_excel("../../data/hud_zip_to_county_crosswalk.xlsx") |> 
    mutate(
        zip = str_pad(ZIP, 5, pad = "0"),
        state = as.numeric(substr(COUNTY, 1, 2))
    ) |> 
    reframe(exists = 1, .by = c(zip, state))

transactions <- transactions |>
    left_join(crosswalk, by = c("zip", "state")) 

ggplot(transactions, aes(x = share_transacted)) +
    geom_density() +
    labs(
        title = "Distribution of Share of Transactions",
        x = "Share of Transactions",
        y = "Frequency"
    ) +
    theme_classic() +
    facet_wrap(~ exists)

transactions <- transactions |>
    filter(exists == 1) |>
    select(-exists)

itemization_averages <- zipcode_filings |>
    reframe(
        total_itemizers = sum(n04470),
        total_filers = sum(n1),
        .by = c(statefips, zipcode, year)
    ) |>
    mutate(
        itemizer_share = total_itemizers / total_filers,
        period = ifelse(year < 2018, "pre-2018", "post-2018")
    ) |>
    reframe(avg = mean(itemizer_share, na.rm = TRUE), .by = c(statefips, zipcode, period)) |>
    reframe(
        baseline = avg[period == "pre-2018"],
        change = avg[period == "post-2018"] - avg[period == "pre-2018"], 
        .by = c(statefips, zipcode)
    )

transactions <- transactions |>
    mutate(zipcode = as.numeric(zip)) |>
    left_join(itemization_averages, by = c("zipcode", "state" = "statefips"))
fwrite(transactions, "../../data/zipcode_SOI/transactions_w_itemizations.csv")

# Look at averages by itemization change groups
transactions |> 
    filter(tax_year %in% c(2014:2022)) |>
    mutate(group = ntile(change, 5)) |>
    reframe(avg = mean(share_transacted, na.rm = TRUE), .by = c(group, tax_year)) |>
    filter(!is.na(group) & tax_year < 2021) |>
    ggplot(aes(x = tax_year, y = avg, color = as.factor(group), group = as.factor(group))) +
    geom_line() +
    geom_point() +
    geom_vline(xintercept = 2017, linetype = "dashed", color = "red") +
    labs(
        title = "Share of Transactions by Itemization Change Group",
        x = "Tax Year",
        y = "Share of Transactions",
        color = "Itemization Change Group"
    ) +
    theme_classic()

# Averages by baseline exposure
transactions |> 
    filter(tax_year %in% c(2014:2022)) |>
    mutate(group = ntile(baseline, 5)) |>
    reframe(avg = mean(share_transacted, na.rm = TRUE), .by = c(group, tax_year)) |>
    filter(!is.na(group) & tax_year < 2021) |>
    ggplot(aes(x = tax_year, y = avg, color = as.factor(group), group = as.factor(group))) +
    geom_line() +
    geom_point() +
    geom_vline(xintercept = 2017, linetype = "dashed", color = "red") +
    labs(
        title = "Share of Transactions by Baseline Itemization Exposure",
        x = "Tax Year",
        y = "Share of Transactions",
        color = "Baseline Itemization Exposure"
    ) +
    theme_classic()

# Groups based on realized change: Residualize within state-year (FEs)
transactions |> 
    filter(tax_year %in% c(2014:2022)) |>
    mutate(group = ntile(change, 5)) |>
    mutate(resid = share_transacted - mean(share_transacted, na.rm = TRUE), .by = c(state, tax_year)) |>
    reframe(avg = mean(resid, na.rm = TRUE), .by = c(group, tax_year)) |>
    filter(!is.na(group) & tax_year < 2021) |>
    ggplot(aes(x = tax_year, y = avg, color = as.factor(group), group = as.factor(group))) +
    geom_line() +
    geom_point() +
    geom_vline(xintercept = 2017, linetype = "dashed", color = "red") +
    labs(
        title = "Share of Transactions by Realized Itemization Change Group",
        x = "Tax Year",
        y = "Share of Transactions",
        color = "Realized Itemization Change Group"
    ) +
    theme_classic()

# Group based on exposure: Residualize within state-year (FEs)
transactions |> 
    filter(tax_year %in% c(2014:2022)) |>
    mutate(group = ntile(baseline, 5)) |>
    mutate(resid = share_transacted - mean(share_transacted, na.rm = TRUE), .by = c(state, tax_year)) |>
    reframe(avg = mean(resid, na.rm = TRUE), .by = c(group, tax_year)) |>
    filter(!is.na(group) & tax_year < 2021) |>
    ggplot(aes(x = tax_year, y = avg, color = as.factor(group), group = as.factor(group))) +
    geom_line() +
    geom_point() +
    geom_vline(xintercept = 2017, linetype = "dashed", color = "red") +
    labs(
        title = "Share of Transactions by Baseline Itemization Exposure Group",
        x = "Tax Year",
        y = "Share of Transactions",
        color = "Baseline Itemization Exposure"
    ) +
    theme_classic()

# Regression versions of this
transactions |> 
    filter(tax_year %in% c(2014:2022)) |>
    feols(
        share_transacted ~ i(tax_year, change, ref = 2017) | tax_year + state^zipcode,
        cluster = ~ state^zipcode
    ) |>
    iplot()

transactions |> 
    filter(tax_year %in% c(2014:2022)) |>
    feols(
        share_transacted ~ i(tax_year, baseline, ref = 2017) | tax_year + state^zipcode,
        cluster = ~ state^zipcode
    ) |>
    iplot()
transactions |> 
    filter(tax_year %in% c(2014:2022)) |>
    feols(
        share_transacted ~ i(tax_year, change, ref = 2017) | state^tax_year + state^zipcode,
        cluster = ~ state^zipcode
    ) |>
    iplot()

transactions |> 
    filter(tax_year %in% c(2014:2022)) |>
    feols(
        share_transacted ~ i(tax_year, baseline, ref = 2017) | state^tax_year + state^zipcode,
        cluster = ~ state^zipcode
    ) |>
    iplot()


