# Clean and load
gc()
rm(list = ls())
getwd()

# Load libraries
library(dplyr)
library(stringr)
library(ggplot2)
library(tigris)
library(duckdb)
library(fixest)

# Load from database
con <- dbConnect(duckdb::duckdb(), dbdir = "../../../data/database.db")
state_returns <- dbGetQuery(con, "select * from returns_state")

# Compute exposure measures
exposure_all <- state_returns |>
    reframe(
        avg_salt_per_filer = sum(state_and_local_income_taxes_amount, na.rm = TRUE) / sum(returns, na.rm = TRUE),
        .by = c(state, year)
    ) |>
    mutate(pre_period = if_else(year < 2018, 1, 0)) |>
    reframe(
        exposure_all = mean(avg_salt_per_filer[pre_period == 1]) -
            mean(avg_salt_per_filer[pre_period == 0]),
        .by = state
    )

exposure_top <- state_returns |>
    filter(agi_stub == max(agi_stub)) |>
    reframe(
        avg_salt_per_filer = sum(state_and_local_income_taxes_amount, na.rm = TRUE) / sum(returns, na.rm = TRUE),
        .by = c(state, year)
    ) |>
    mutate(pre_period = if_else(year < 2018, 1, 0)) |>
    reframe(
        exposure_top = mean(avg_salt_per_filer[pre_period == 1]) -
            mean(avg_salt_per_filer[pre_period == 0]),
        .by = state
    )

# Merge exposures
state_returns <- state_returns |>
    left_join(exposure_all, by = "state") |>
    left_join(exposure_top, by = "state")

# Add weights
state_returns <- state_returns |>
    mutate(
        w1 = sum(returns),
        w2 = sum(returns[agi_stub == max(agi_stub)]),
        .by = c(state, year)
    )

# TWFE of exposure on number of returns in different AGI classes

## AGI > 1,000,000
state_returns |>
    filter(agi_stub == 10) |>
    feols(
        returns ~ i(year, exposure_all, ref = 2017) - 1 | state,
        weights = ~ w1,
        cluster = ~ state
    ) |>
    iplot()
state_returns |>
    filter(agi_stub == 10) |>
    feols(
        returns ~ i(year, exposure_all, ref = 2017) - 1 | state,
        weights = ~ w2,
        cluster = ~ state
    ) |>
    iplot()
state_returns |>
    filter(agi_stub == 10) |>
    feols(
        returns ~ i(year, exposure_top, ref = 2017) - 1 | state,
        weights = ~ w1,
        cluster = ~ state
    ) |>
    iplot()
state_returns |>
    filter(agi_stub == 10) |>
    feols(
        returns ~ i(year, exposure_top, ref = 2017) - 1 | state,
        weights = ~ w2,
        cluster = ~ state
    ) |>
    iplot()

## AGI > 500,000
state_returns |> 
    filter(agi_stub > 8) |> 
    reframe(
        returns = sum(returns), 
        exposure_all = mean(exposure_all), 
        w1 = sum(w1),
        .by = c(state, year)) |>
    feols(
        returns ~ i(year, exposure_all, ref = 2017) - 1 | state,
        weights = ~ w1,
        cluster = ~ state
    ) |>
    iplot()
state_returns |> 
    filter(agi_stub > 8) |> 
    reframe(
        returns = sum(returns), 
        exposure_all = mean(exposure_all), 
        w2 = sum(w2),
        .by = c(state, year)) |>
    feols(
        returns ~ i(year, exposure_all, ref = 2017) - 1 | state,
        weights = ~ w2,
        cluster = ~ state
    ) |>
    iplot()
state_returns |> 
    filter(agi_stub > 8) |> 
    reframe(
        returns = sum(returns), 
        exposure_top = mean(exposure_top), 
        w1 = sum(w1),
        .by = c(state, year)) |>
    feols(
        returns ~ i(year, exposure_top, ref = 2017) - 1 | state,
        weights = ~ w1,
        cluster = ~ state
    ) |>
    iplot()
state_returns |> 
    filter(agi_stub > 8) |> 
    reframe(
        returns = sum(returns), 
        exposure_top = mean(exposure_top), 
        w2 = sum(w2),
        .by = c(state, year)) |>
    feols(
        returns ~ i(year, exposure_top, ref = 2017) - 1 | state,
        weights = ~ w2,
        cluster = ~ state
    ) |>
    iplot()

## AGI > 250,000
state_returns |> 
    filter(agi_stub > 7) |> 
    reframe(
        returns = sum(returns), 
        exposure_all = mean(exposure_all), 
        w1 = sum(w1),
        .by = c(state, year)) |>
    feols(
        returns ~ i(year, exposure_all, ref = 2017) - 1 | state,
        weights = ~ w1,
        cluster = ~ state
    ) |>
    iplot()
state_returns |> 
    filter(agi_stub > 7) |> 
    reframe(
        returns = sum(returns), 
        exposure_all = mean(exposure_all), 
        w2 = sum(w2),
        .by = c(state, year)) |>
    feols(
        returns ~ i(year, exposure_all, ref = 2017) - 1 | state,
        weights = ~ w2,
        cluster = ~ state
    ) |>
    iplot()
state_returns |> 
    filter(agi_stub > 7) |> 
    reframe(
        returns = sum(returns), 
        exposure_top = mean(exposure_top), 
        w1 = sum(w1),
        .by = c(state, year)) |>
    feols(
        returns ~ i(year, exposure_top, ref = 2017) - 1 | state,
        weights = ~ w1,
        cluster = ~ state
    ) |>
    iplot()
state_returns |> 
    filter(agi_stub > 7) |> 
    reframe(
        returns = sum(returns), 
        exposure_top = mean(exposure_top), 
        w2 = sum(w2),
        .by = c(state, year)) |>
    feols(
        returns ~ i(year, exposure_top, ref = 2017) - 1 | state,
        weights = ~ w2,
        cluster = ~ state
    ) |>
    iplot()


# TWFE of exposure on share of returns in different AGI classes
state_returns |> 
    reframe(
        share = sum(returns[agi_stub == 10]) / sum(returns),
        exposure_all = mean(exposure_all),
        w1 = w1[agi_stub == 10],
        .by = c(state, year)
    ) |>
    feols(
        share ~ i(year, exposure_all, ref = 2017) - 1 | state,
        weights = ~ w1,
        cluster = ~ state
    ) |>
    iplot()
state_returns |> 
    reframe(
        share = sum(returns[agi_stub > 8]) / sum(returns),
        exposure_all = mean(exposure_all),
        w2 = w1[agi_stub == 10],
        .by = c(state, year)
    ) |>
    feols(
        share ~ i(year, exposure_all, ref = 2017) - 1 | state,
        weights = ~ w2,
        cluster = ~ state
    ) |>
    iplot()
state_returns |> 
    reframe(
        share = sum(returns[agi_stub > 7]) / sum(returns),
        exposure_all = mean(exposure_all),
        w1 = w1[agi_stub == 10],
        .by = c(state, year)
    ) |>
    feols(
        share ~ i(year, exposure_all, ref = 2017) - 1 | state,
        weights = ~ w1,
        cluster = ~ state
    ) |>
    iplot()
