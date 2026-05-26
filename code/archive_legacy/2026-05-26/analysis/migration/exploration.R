# Clean and load
gc()
rm(list = ls())
getwd()

# Load libraries
library(tidyverse)
library(stringr)
library(ggplot2)
library(fixest)
library(duckdb)
library(binsreg)

# Load from database
con <- dbConnect(duckdb::duckdb(), dbdir = "../../../data/database.db")
migration <- dbGetQuery(con, "select * from migration_county_balanced")
migration_state <- dbGetQuery(con, "select * from migration_state")
county_returns <- dbGetQuery(con, "select * from returns_county")
county_returns_agi <- dbGetQuery(con, "select * from returns_county_agi")
state_returns <- dbGetQuery(con, "select * from returns_state")

########################################################
# Compute exposure at the state level 
########################################################

# Step 1: Impute marginal tax rates
## Compute filing status weights directly from returns data (ignores MFS and QSS)
## Impute marginal tax rates within AGI bins
## For single filers, logic is:
## - Under $10k: below $12k std. ded.
## - $10–25k: taxable $0–13k, mostly 10%
## - $25–50k: taxable $13–38k, all 12%
## - $50–75k: taxable $38–63k, all 22%
## - $75–100k: taxable $63–88k, mostly 22%, top clips 24%
## - $100–200k: taxable $88–188k, mostly 24%, top clips 32%
## - $200–500k: taxable $188–488k, mostly 35%
## - $500k–1M: all 37%
## - $1M+: 37%
## For married filers, logic is:
## - Under $10k: below $18k std. ded.
## - $10–25k: taxable $0–7k, straddles 0% and 10%
## - $25–50k: taxable $7–32k, straddles 10%/12%
## - $50–75k: taxable $32–57k, mostly 12%, top clips 22%
## - $75–100k: taxable $57–82k, solidly 22%
## - $100–200k: taxable $82–182k, mostly 24%, top clips 32%
## - $200–500k: taxable $182–482k, blends 32%/35%
## - $500k–1M: solidly 37%
## - $1M+: 37%
## For head of household filers, logic is:
## - Under $10k: below $18k std. ded.
## - $10–25k: taxable $0–7k, straddles 0% and 10%
## - $25–50k: taxable $7–32k, straddles 10%/12%
## - $50–75k: taxable $32–57k, mostly 12%, top clips 22%
## - $75–100k: taxable $57–82k, solidly 22%
## - $100–200k: taxable $82–182k, mostly 24%, top clips 32%
## - $200–500k: taxable $182–482k, blends 32%/35%
## - $500k–1M: solidly 37%
## - $1M+: 37%
## For all filers, logic is:
## - Under $10k: below $12k std. ded.
## - $10–25k: taxable $0–13k, mostly 10%
## - $25–50k: taxable $13–38k, all 12%
## - $50–75k: taxable $38–63k, all 22%
## - $75–100k: taxable $63–88k, mostly 22%, top clips 24%
## - $100–200k: taxable $88–188k, mostly 24%, top clips 32%
## - $200–500k: taxable $188–488k, mostly 35%
## - $500k–1M: all 37%
## - $1M+: 37%
state_returns <- state_returns |>
  mutate(
    w_mfj    = joint_returns / returns,
    w_single = single_returns  / returns,
    w_hoh    = head_of_household_returns / returns,
    mtr_single = case_when(
      agi_stub == 0  ~ NA_real_,
      agi_stub == 1  ~ 0.00,
      agi_stub == 2  ~ 0.00,
      agi_stub == 3  ~ 0.105,
      agi_stub == 4  ~ 0.12,
      agi_stub == 5  ~ 0.22,
      agi_stub == 6  ~ 0.225,
      agi_stub == 7  ~ 0.25,
      agi_stub == 8  ~ 0.345,
      agi_stub == 9  ~ 0.37,
      agi_stub == 10 ~ 0.37,
      .default = NA_real_
    ),

    mtr_mfj = case_when(
      agi_stub == 0  ~ NA_real_,
      agi_stub == 1  ~ 0.00,
      agi_stub == 2  ~ 0.00,
      agi_stub == 3  ~ 0.00,
      agi_stub == 4  ~ 0.105,
      agi_stub == 5  ~ 0.12,
      agi_stub == 6  ~ 0.12,
      agi_stub == 7  ~ 0.22,
      agi_stub == 8  ~ 0.3,
      agi_stub == 9  ~ 0.3675,
      agi_stub == 10 ~ 0.37,
      .default = NA_real_
    ),

    mtr_hoh = case_when(
      agi_stub == 0  ~ NA_real_,
      agi_stub == 1  ~ 0.00,
      agi_stub == 2  ~ 0.00,
      agi_stub == 3  ~ 0.04,
      agi_stub == 4  ~ 0.115,
      agi_stub == 5  ~ 0.14,
      agi_stub == 6  ~ 0.22,
      agi_stub == 7  ~ 0.25,
      agi_stub == 8  ~ 0.3475,
      agi_stub == 9  ~ 0.37,
      agi_stub == 10 ~ 0.37,
      .default = NA_real_
    ),
    mtr_weighted = w_mfj * mtr_mfj + w_single * mtr_single + w_hoh * mtr_hoh
    )

# Step 2: Compute estimated tax savings through SALT deduction
state_returns <- state_returns |>
    mutate(
        salt_savings = taxes_paid_amount * mtr_weighted
    )

# Step 3: Benchmark by computing total savings for each year
state_returns |> 
    reframe(total_salt_savings = sum(salt_savings, na.rm = TRUE), .by = year)

# Step 4: Drop in tax savings due to SALT pre vs. post TCJA
exposure_all <- state_returns |> 
    reframe(
        avg_salt_savings_state_year = sum(salt_savings, na.rm = TRUE) / sum(returns, na.rm = TRUE),
        .by = c(state, year)
    ) |>
    mutate(pre_period = if_else(year < 2018, 1, 0)) |>
    reframe(
        exposure_all = mean(avg_salt_savings_state_year[pre_period == 1]) -
            mean(avg_salt_savings_state_year[pre_period == 0]),
        .by = state
    )

exposure_top <- state_returns |>  
    filter(agi_stub == max(agi_stub)) |>
    reframe(
        avg_salt_savings_state_year = sum(salt_savings, na.rm = TRUE) / sum(returns, na.rm = TRUE),
        .by = c(state, year)
    ) |>
    mutate(pre_period = if_else(year < 2018, 1, 0)) |>
    reframe(
        exposure_top = mean(avg_salt_savings_state_year[pre_period == 1]) -
            mean(avg_salt_savings_state_year[pre_period == 0]),
        .by = state
    )




########################################################
# Compute net migration and normalize variables
########################################################

# Compute filers, individuals, and AGI at the state level
state_totals <- state_returns |>
    reframe(
        returns = sum(returns),
        individuals = sum(individuals),
        .by = c(state, year)
    )

# Compute net migration for each state-pair
net_migr_state <- migration_state |> 
  filter(origin_state != destination_state) |>                    # drop within-state flows
  inner_join(migration_state |> select(-origin_state_name, -origin_state_description),
             by = c("origin_state" = "destination_state",
                    "destination_state" = "origin_state",
                    "year"),
             suffix = c("_out", "_in")) |> 
  mutate(
    net_returns = returns_in - returns_out,
    net_individuals = individuals_in - individuals_out,
    ) |>
  filter(origin_state < destination_state)

# Normalize migration as share migrating and agi as agi per mover
net_migr_state <- net_migr_state |>
    left_join(
        state_totals |> rename(total_returns_origin = returns, total_individuals_origin = individuals), 
        by = c("origin_state" = "state", "year")) |>
    mutate(
        net_returns_share = net_returns / total_returns_origin,
        net_individuals_share = net_individuals / total_individuals_origin,
        avg_agi_diff = agi_in / returns_in - agi_out / returns_out
    ) |> 
    filter(year > 2011)

# Compute differences in exposure across states
net_migr_state <- net_migr_state |>
    left_join(exposure_all, by = c("origin_state" = "state"), suffix = c("_origin", "")) |>
    left_join(exposure_top, by = c("origin_state" = "state"), suffix = c("_origin", "")) |> 
    left_join(exposure_all, by = c("destination_state" = "state"), suffix = c("_destination", "")) |>
    left_join(exposure_top, by = c("destination_state" = "state"), suffix = c("_destination", "")) |> 
    rename(exposure_all_origin = exposure_all, exposure_top_origin = exposure_top) |>
    mutate(
        exposure_all_diff = exposure_all_destination - exposure_all_origin,
        exposure_top_diff = exposure_top_destination - exposure_top_origin
    )

########################################################
# Look at trends in migration across states
########################################################

# Pre-treatment correlation
net_migr_state |>
    filter(year == 2017) |>
    ggplot(aes(x = exposure_all_diff, y = net_returns_share)) +
    geom_point() 

net_migr_state |>
    filter(year == 2017) |>
    ggplot(aes(x = exposure_all_diff, y = net_returns_share)) +
    geom_point()

net_migr_state |>
    filter(year == 2017) |>
    ggplot(aes(x = exposure_all_diff, y = avg_agi_diff)) +
    geom_point()

# Correlation over time
net_migr_state |>
    feols(
        net_returns_share ~ i(year, exposure_all_diff) - 1,
        cluster = ~ origin_state^destination_state
    ) |> iplot()

net_migr_state |>
    feols(net_individuals_share ~ i(year, exposure_all_diff) - 1,
        cluster = ~ origin_state^destination_state
    ) |> iplot()

net_migr_state |>
    feols(avg_agi_diff ~ i(year, exposure_all_diff) - 1,
        cluster = ~ origin_state^destination_state
    ) |> iplot()

# TWFE model
net_migr_state |>
    feols(net_returns_share ~ i(year, exposure_all_diff, ref = 2017) - 1 | origin_state^destination_state + year,
        cluster = ~ origin_state^destination_state
    ) |> iplot()

net_migr_state |>
    feols(net_individuals_share ~ i(year, exposure_all_diff, ref = 2017) - 1 | origin_state^destination_state + year,
        cluster = ~ origin_state^destination_state
    ) |> iplot()

net_migr_state |>
    feols(avg_agi_diff ~ i(year, exposure_all_diff, ref = 2017) - 1 | origin_state^destination_state + year,
        cluster = ~ origin_state^destination_state
    ) |> iplot()

# TWFE controlling for initial migration incentives (salt diffs)
initial_salt <- state_returns |>
    reframe(
        initial_salt_pr = sum(salt_savings, na.rm = TRUE) / sum(returns, na.rm = TRUE),
        initial_salt_pc = sum(salt_savings, na.rm = TRUE) / sum(individuals, na.rm = TRUE),
        .by = c(state, year)
    ) |> 
    filter(year == 2017)

net_migr_state <- net_migr_state |>
    left_join(
        initial_salt |> 
            rename(
                salt_origin_pr = initial_salt_pr, 
                salt_origin_pc = initial_salt_pc
            ), 
        by = c("origin_state" = "state")) |>
    left_join(
        initial_salt |> 
            rename(
                salt_destination_pr = initial_salt_pr, 
                salt_destination_pc = initial_salt_pc
            ), 
        by = c("destination_state" = "state")
    )

net_migr_state |> 
    feols(net_returns_share ~ i(year, exposure_all_diff, ref = 2017) + i(year, salt_origin_pr, ref = 2017) - 1 | origin_state^destination_state + year,
        cluster = ~ origin_state^destination_state
    ) |> iplot()






########################################################
# Compute exposure at the county level 
########################################################


# Step 1: Impute marginal tax rates for county AGI stubs
# Stubs present: 1, 3, 4, 5, 6, 8
# (stub 2 merged into stub 3; stub 7 merged into stub 8)
# - 1: $0
# - 3: $1-$50K  (merged $1-25K and $25-50K)
# - 4: $50-$75K
# - 5: $75-$100K
# - 6: $100-$200K
# - 8: $200K+   (merged $200K-$1M and $1M+)
county_returns_agi <- county_returns_agi |>
    mutate(
        w_mfj    = joint_returns / returns,
        w_single = single_returns / returns,
        w_hoh    = head_of_household_returns / returns,
        mtr_single = case_when(
            agi_stub == 0  ~ NA_real_,
            agi_stub == 1  ~ 0.00,
            agi_stub == 2  ~ 0.00,
            agi_stub == 3  ~ 0.1149,
            agi_stub == 4  ~ 0.1566,
            agi_stub == 5  ~ 0.25,
            agi_stub == 6  ~ 0.25,
            agi_stub == 7  ~ 0.2793,
            agi_stub == 8  ~ 0.396,
            .default = NA_real_
            ),
        mtr_mfj = case_when(
            agi_stub == 0  ~ NA_real_,
            agi_stub == 1  ~ 0.00,
            agi_stub == 2  ~ 0.00,
            agi_stub == 3  ~ 0.028,
            agi_stub == 4  ~ 0.1211,
            agi_stub == 5  ~ 0.15,
            agi_stub == 6  ~ 0.1632,
            agi_stub == 7  ~ 0.2578,
            agi_stub == 8  ~ 0.396,
            .default = NA_real_
            ),
        mtr_hoh = case_when(
            agi_stub == 0  ~ NA_real_,
            agi_stub == 1  ~ 0.00,
            agi_stub == 2  ~ 0.00,
            agi_stub == 3  ~ 0.0503,
            agi_stub == 4  ~ 0.1384,
            agi_stub == 5  ~ 0.177,
            agi_stub == 6  ~ 0.25,
            agi_stub == 7  ~ 0.2654,
            agi_stub == 8  ~ 0.396,
            .default = NA_real_
            ),
        mtr_weighted = w_mfj * mtr_mfj + w_single * mtr_single + w_hoh * mtr_hoh,
        salt_savings = taxes_paid_amount * mtr_weighted
    ) |> 
    filter(year > 2011)

# Step 2: Compute gross SALT and federal refund per filer by county-year
# salt_pp: gross SALT paid per filer
# salt_refund_pp: federal tax offset from SALT deduction (MTR-weighted)
# effective_salt_pp: net out-of-pocket SALT cost = salt_pp - salt_refund_pp
salt_per_county_year <- county_returns_agi |>
    reframe(
        salt_pp           = sum(taxes_paid_amount, na.rm = TRUE) / sum(returns, na.rm = TRUE),
        salt_refund_pp    = sum(salt_savings,       na.rm = TRUE) / sum(returns, na.rm = TRUE),
        effective_salt_pp = salt_pp - salt_refund_pp,
        .by = c(county, year)
    )

# Step 3: Validate total salt refund size
county_returns_agi |> 
    reframe(total_salt_savings = sum(salt_savings, na.rm = TRUE), .by = year)

# Step 4: Compute county-level exposure
exposure_c <- county_returns_agi |> 
    reframe(salt_savings = sum(salt_savings, na.rm = TRUE), .by = c(year, county)) |> 
    reframe(
        exposure = salt_savings[year == 2018] - salt_savings[year == 2017], 
        .by = county
    )

# Step 4: Compute SALT differential for each observed pair-year
salt_differentials <- expand_grid(
    origin_county = unique(county_returns_agi$county),
    destination_county = unique(county_returns_agi$county),
    year = 2017:2018
) |> 
    left_join(salt_per_county_year |> rename_with(~ paste0(.x, "_origin"), -c(county, year)),
              by = c("origin_county" = "county", "year")) |>
    left_join(salt_per_county_year |> rename_with(~ paste0(.x, "_dest"), -c(county, year)),
              by = c("destination_county" = "county", "year")) |>
    mutate(salt_differential = effective_salt_pp_origin - effective_salt_pp_dest)   

# Step 5: Compute exposure
salt_exposure <- salt_differentials |>
    reframe(
        exposure = salt_differential[year == 2018] - salt_differential[year == 2017],
        salt_origin = salt_pp_origin[year == 2017],
        salt_destination = salt_pp_dest[year == 2017],
        .by = c(origin_county, destination_county)
    )




###################################################################
# Compare outmigration rates across differentially exposed counties
###################################################################

# Merge exposure numbers
migration <- migration |> 
    left_join(exposure_c, by = c("destination_fips" = "county")) |> 
    rename(dest_exposure = exposure) |> 
    left_join(exposure_c, by = c("origin_fips" = "county")) |> 
    rename(orig_exposure = exposure)

# Merge taxfiler totals from county-level returns data
migration <- migration |> 
    left_join(
        county_returns |> 
            select(county, year, returns, individuals) |> 
            rename(tot_returns_dest = returns, tot_individuals_dest = individuals),
        by = c("destination_fips" = "county", "year")
    ) |> 
    left_join(
        county_returns |> 
            select(county, year, returns, individuals) |> 
            rename(tot_returns_orig = returns, tot_individuals_orig = individuals),
        by = c("origin_fips" = "county", "year")
    )

# Share migrating over exposure
migration |> 
    mutate(
        outmigration_share = sum(individuals, na.rm = TRUE) / tot_individuals_orig,
        period = case_when(
            year %in% 2017 ~ "pre-period",
            year %in% 2018 ~ "treatment",
            year %in% 2016 ~ "placebo"
        ),
        state = origin_fips %/% 1000,
        .by = c(year, origin_fips)
    ) |> 
    # mutate(
    #     outmigration_share = outmigration_share - mean(outmigration_share), .by = c(state, year)
    #     ) |> 
    filter(orig_exposure > - 10000) |>
    binsreg(
        x = orig_exposure,
        y = outmigration_share, 
        data = _, 
        by = period,
        nbins = 7,
        plotyrange = c(0, Inf) #c(-Inf, 0)
    )

# Correlation between outmigration and exposure over time
migration |> 
    mutate(
        outmigration_share = sum(individuals, na.rm = TRUE) / tot_individuals_orig,
        period = case_when(
            year %in% 2017 ~ "pre-period",
            year %in% 2018 ~ "treatment",
            year %in% 2016 ~ "placebo"
        ),
        state = origin_fips %/% 1000,
        .by = c(year, origin_fips)
    ) |> 
    filter(orig_exposure > - 15000) |>
    feols(
        outmigration_share ~ i(year, orig_exposure),
        vcov = ~origin_fips) |> 
    summary()

# TWFE version
migration |> 
    mutate(
        outmigration_share = sum(individuals, na.rm = TRUE) / tot_individuals_orig,
        period = case_when(
            year %in% 2017 ~ "pre-period",
            year %in% 2018 ~ "treatment",
            year %in% 2016 ~ "placebo"
        ),
        state = origin_fips %/% 1000,
        .by = c(year, origin_fips)
    ) |> 
    filter(orig_exposure > - 15000) |>
    feols(
        outmigration_share ~ i(year, orig_exposure, ref = 2017) | year + origin_fips,
        vcov = ~origin_fips) |> 
    iplot()

# Average destination exposure conditional on migrating
migration |> 
    mutate(
        share = returns / tot_returns_orig,
        dest_exposure = weighted.mean(dest_exposure, w = share),
        period = case_when(
            year %in% 2017 ~ "pre-period",
            year %in% 2018 ~ "treatment",
            year %in% 2016 ~ "placebo"
        ),
        state = origin_fips %/% 1000,
        .by = c(year, origin_fips)
    ) |> View()
    # mutate(
    #     outmigration_share = outmigration_share - mean(outmigration_share), .by = c(state, year)
    #     ) |> 
    filter(orig_exposure > - 10000) |>
    binsreg(
        x = orig_exposure,
        y = dest_exposure, 
        data = _, 
        by = period,
        at = "median",
        nbins = 7,
        plotyrange = c(-Inf, 0)
    )
