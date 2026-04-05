# Clean and load
gc()
rm(list = ls())
getwd()

# Load libraries
library(dplyr)
library(stringr)
library(ggplot2)
library(fixest)
library(binsreg)
library(duckdb)

# Load from database
con <- dbConnect(duckdb::duckdb(), dbdir = "../../../data/database.db")
migration <- dbGetQuery(con, "select * from migration_state")
returns <- dbGetQuery(con, "select * from returns_state")

########################################################
# Construct exposure measures
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
returns <- returns |>
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
returns <- returns |>
    mutate(
        salt_savings = taxes_paid_amount * mtr_weighted
    )

# Step 3: Benchmark by computing total savings for each year
returns |> 
    reframe(total_salt_savings = sum(salt_savings, na.rm = TRUE), .by = year)

# Step 4: Drop in tax savings due to SALT pre vs. post TCJA
exposure_all <- returns |> 
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

exposure_top <- returns |>  
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
# Trends in out-migration for differently exposed states
########################################################

# Merge exposures to migration data
migration <- migration |>
    left_join(exposure_all, by = c("origin_state" = "state")) |>
    left_join(exposure_top, by = c("origin_state" = "state"))

# State return data is only available from 2012 onward and for US states
migration <- migration |>
    filter(year >= 2012) |> 
    filter(origin_state %in% 1:56 & destination_state %in% 1:56)

# Merge filer totals to migration data
filers <- returns |>
    reframe(
        total_filers = sum(returns),
        .by = c(state, year)
    )
migration <- migration |>
  left_join(filers |> rename(total_filers_orig = total_filers),
            by = c("origin_state" = "state", "year")) |> 
  left_join(filers |> rename(total_filers_dest = total_filers),
            by = c("destination_state" = "state", "year"))

# Trends in outmigration for differently exposed states

## Levels
migration |> 
    arrange(year, exposure_all) |>
    mutate(
        cdf = cumsum(total_filers_orig) / sum(total_filers_orig),
        .by = year
    ) |> 
    mutate(group = case_when(cdf[year == 2017] < .5 ~ "Bottom 50%", TRUE ~ "Top 50%"), .by = c(destination_state, origin_state)) |>
    reframe(total_migration = sum(returns, na.rm = TRUE), .by = c(group, year)) |> 
    ggplot(aes(x = year, y = total_migration, color = group, group = group)) +
    geom_line() +
    geom_point() +
    scale_color_viridis_d() +
    theme_classic()

## Shares
migration |> 
    arrange(year, exposure_all) |>
    mutate(
        cdf = cumsum(total_filers_orig) / sum(total_filers_orig),
        .by = year
    ) |> 
    mutate(group = case_when(cdf[year == 2017] < .5 ~ "Bottom 50%", TRUE ~ "Top 50%"), .by = c(destination_state, origin_state)) |>
    reframe(total_migration_share = sum(returns, na.rm = TRUE) / sum(total_filers_orig), .by = c(group, year)) |> 
    ggplot(aes(x = year, y = total_migration_share, color = group, group = group)) +
    geom_line() +
    geom_point() +
    scale_color_viridis_d() +
    theme_classic()

## AGI conditional on moving
migration |> 
    arrange(year, exposure_all) |>
    mutate(
        cdf = cumsum(total_filers_orig) / sum(total_filers_orig),
        .by = year
    ) |> 
    mutate(group = case_when(cdf[year == 2017] < .5 ~ "Bottom 50%", TRUE ~ "Top 50%"), .by = c(destination_state, origin_state)) |>
    reframe(avg_agi = sum(agi, na.rm = TRUE) / sum(returns, na.rm = TRUE), .by = c(group, year)) |> 
    ggplot(aes(x = year, y = avg_agi, color = group, group = group)) +
    geom_line() +
    geom_point() +
    scale_color_viridis_d() +
    theme_classic()

# Regression based versions of this
migration |> 
    feols(returns ~ i(year, exposure_all, ref = 2017)  - 1 | origin_state^destination_state + year,
    data = _,
    cluster = ~ origin_state^destination_state,
    weights = ~ total_filers_orig
    ) |> iplot()

migration |> 
    arrange(year, exposure_all) |>
    mutate(
        cdf = cumsum(total_filers_orig) / sum(total_filers_orig),
        .by = year
    ) |> 
    mutate(treated = case_when(cdf[year == 2017] < .5 ~ FALSE, TRUE ~ TRUE), .by = c(destination_state, origin_state)) |>
    feols(returns ~ i(year, treated, ref = 2017) - 1 | origin_state^destination_state + year,
    data = _,
    cluster = ~ origin_state^destination_state,
    weights = ~ total_filers_orig
    ) |> iplot()

migration |> 
    feols(log(returns) ~ i(year, exposure_all, ref = 2017) - 1 | origin_state^destination_state + year,
    data = _,
    cluster = ~ origin_state^destination_state,
    weights = ~ total_filers_orig
    ) |> iplot()

migration |> 
    arrange(year, exposure_all) |>
    mutate(
        cdf = cumsum(total_filers_orig) / sum(total_filers_orig),
        .by = year
    ) |> 
    mutate(treated = case_when(cdf[year == 2017] < .5 ~ FALSE, TRUE ~ TRUE), .by = c(destination_state, origin_state)) |>
    feols(log(returns) ~ i(year, treated, ref = 2017) - 1 | origin_state^destination_state + year,
    data = _,
    cluster = ~ origin_state^destination_state,
    weights = ~ total_filers_orig
    ) |> iplot()

migration |> 
    mutate(share = returns / total_filers_orig) |>
    feols(share ~ i(year, exposure_all, ref = 2017) - 1 | origin_state^destination_state + year,
    data = _,
    cluster = ~ origin_state^destination_state,
    weights = ~ total_filers_orig
    ) |> iplot()

migration |> 
    arrange(year, exposure_all) |>
    mutate(
        cdf = cumsum(total_filers_orig) / sum(total_filers_orig),
        .by = year
    ) |> 
    mutate(treated = case_when(cdf[year == 2017] < .5 ~ FALSE, TRUE ~ TRUE), .by = c(destination_state, origin_state)) |>
    mutate(share = returns / total_filers_orig) |>
    feols(share ~ i(year, treated, ref = 2017) - 1 | origin_state^destination_state + year,
    data = _,
    cluster = ~ origin_state^destination_state,
    weights = ~ total_filers_orig
    ) |> iplot()

migration |> 
    mutate(avg_agi = agi / returns) |>
    feols(avg_agi ~ i(year, exposure_all, ref = 2016) - 1 | origin_state^destination_state + year,
    data = _,
    cluster = ~ origin_state^destination_state,
    weights = ~ total_filers_orig
    ) |> iplot()

migration |> 
    arrange(year, exposure_all) |>
    mutate(
        cdf = cumsum(total_filers_orig) / sum(total_filers_orig),
        .by = year
    ) |> 
    mutate(treated = case_when(cdf[year == 2017] < .5 ~ FALSE, TRUE ~ TRUE), .by = c(destination_state, origin_state)) |>
    mutate(avg_agi = agi / returns) |>
    feols(avg_agi ~ i(year, treated, ref = 2016) - 1 | origin_state^destination_state + year,
    data = _,
    cluster = ~ origin_state^destination_state,
    weights = ~ total_filers_orig
    ) |> iplot()




########################################################
# Trends in in-migration for differently exposed states
########################################################

# Merge exposures to migration data
migration <- migration |>
    select(-exposure_all, -exposure_top) |>
    left_join(exposure_all, by = c("destination_state" = "state")) |>
    left_join(exposure_top, by = c("destination_state" = "state"))

## Levels
migration |> 
    arrange(year, exposure_all) |>
    mutate(
        cdf = cumsum(total_filers_dest) / sum(total_filers_dest),
        .by = year
    ) |> 
    mutate(group = case_when(cdf[year == 2017] < .5 ~ "Bottom 50%", TRUE ~ "Top 50%"), .by = c(origin_state, destination_state)) |>
    reframe(total_migration = sum(returns, na.rm = TRUE), .by = c(group, year)) |> 
    ggplot(aes(x = year, y = total_migration, color = group, group = group)) +
    geom_line() +
    geom_point() +
    scale_color_viridis_d() +
    theme_classic()

## Shares
migration |> 
    arrange(year, exposure_all) |>
    mutate(
        cdf = cumsum(total_filers_dest) / sum(total_filers_dest),
        .by = year
    ) |> 
    mutate(group = case_when(cdf[year == 2017] < .5 ~ "Bottom 50%", TRUE ~ "Top 50%"), .by = c(origin_state, destination_state)) |>
    reframe(total_migration_share = sum(returns, na.rm = TRUE) / sum(total_filers_dest), .by = c(group, year)) |> 
    ggplot(aes(x = year, y = total_migration_share, color = group, group = group)) +
    geom_line() +
    geom_point() +
    scale_color_viridis_d() +
    theme_classic()

## AGI conditional on moving
migration |> 
    arrange(year, exposure_all) |>
    mutate(
        cdf = cumsum(total_filers_dest) / sum(total_filers_dest),
        .by = year
    ) |> 
    mutate(group = case_when(cdf[year == 2017] < .5 ~ "Bottom 50%", TRUE ~ "Top 50%"), .by = c(origin_state, destination_state)) |>
    reframe(avg_agi = sum(agi, na.rm = TRUE) / sum(returns, na.rm = TRUE), .by = c(group, year)) |> 
    ggplot(aes(x = year, y = avg_agi, color = group, group = group)) +
    geom_line() +
    geom_point() +
    scale_color_viridis_d() +
    theme_classic()

## Regression based versions of this
migration |> 
    feols(returns ~ i(year, exposure_all, ref = 2016) - 1  | origin_state^destination_state + year,
    data = _,
    cluster = ~ origin_state^destination_state,
    weights = ~ total_filers_dest
    ) |> iplot()

migration |> 
    arrange(year, exposure_all) |>
    mutate(
        cdf = cumsum(total_filers_dest) / sum(total_filers_dest),
        .by = year
    ) |> 
    mutate(group = case_when(cdf[year == 2017] < .5 ~ "Bottom 50%", TRUE ~ "Top 50%"), .by = c(origin_state, destination_state)) |>
    mutate(treated = case_when(cdf[year == 2017] < .5 ~ FALSE, TRUE ~ TRUE), .by = c(origin_state, destination_state)) |>
    feols(returns ~ i(year, treated, ref = 2016) - 1 | origin_state^destination_state + year,
    data = _,
    cluster = ~ origin_state^destination_state,
    weights = ~ total_filers_dest
    ) |> iplot()

migration |> 
    feols(log(returns) ~ i(year, exposure_all, ref = 2016) - 1 | origin_state^destination_state + year,
    data = _,
    cluster = ~ origin_state^destination_state,
    weights = ~ total_filers_dest
    ) |> iplot()

migration |> 
    arrange(year, exposure_all) |>
    mutate(
        cdf = cumsum(total_filers_dest) / sum(total_filers_dest),
        .by = year
    ) |> 
    mutate(group = case_when(cdf[year == 2017] < .5 ~ "Bottom 50%", TRUE ~ "Top 50%"), .by = c(origin_state, destination_state)) |>
    mutate(treated = case_when(cdf[year == 2017] < .5 ~ FALSE, TRUE ~ TRUE), .by = c(origin_state, destination_state)) |>
    feols(log(returns) ~ i(year, treated, ref = 2016) - 1 | origin_state^destination_state + year,
    data = _,
    cluster = ~ origin_state^destination_state,
    weights = ~ total_filers_dest
    ) |> iplot()

migration |> 
    mutate(share = returns / total_filers_orig) |>
    feols(share ~ i(year, exposure_all, ref = 2016) - 1 | origin_state^destination_state + year,
    data = _,
    cluster = ~ origin_state^destination_state,
    weights = ~ total_filers_dest
    ) |> iplot()

migration |> 
    arrange(year, exposure_all) |>
    mutate(
        cdf = cumsum(total_filers_dest) / sum(total_filers_dest),
        .by = year
    ) |> 
    mutate(share = returns / total_filers_orig) |>
    mutate(treated = case_when(cdf[year == 2017] < .5 ~ FALSE, TRUE ~ TRUE), .by = c(origin_state, destination_state)) |>
    feols(share ~ i(year, treated, ref = 2016) - 1 | origin_state^destination_state + year,
    data = _,
    cluster = ~ origin_state^destination_state,
    weights = ~ total_filers_dest
    ) |> iplot()

migration |> 
    mutate(avg_agi = agi / returns) |>
    feols(avg_agi ~ i(year, exposure_all, ref = 2016) - 1 | origin_state^destination_state + year,
    data = _,
    cluster = ~ origin_state^destination_state,
    weights = ~ total_filers_dest
    ) |> iplot()

migration |> 
    arrange(year, exposure_all) |>
    mutate(
        cdf = cumsum(total_filers_dest) / sum(total_filers_dest),
        .by = year
    ) |> 
    mutate(treated = case_when(cdf[year == 2017] < .5 ~ FALSE, TRUE ~ TRUE), .by = c(origin_state, destination_state)) |>
    mutate(avg_agi = agi / returns) |>
    feols(avg_agi ~ i(year, treated, ref = 2016) - 1 | origin_state^destination_state + year,
    data = _,
    cluster = ~ origin_state^destination_state,
    weights = ~ total_filers_dest
    ) |> iplot()



########################################################
# Trends in net migration for differently exposed states
########################################################

# Compute net migration
total_outflows <- migration |>
    reframe(
        total_outflows = sum(returns, na.rm = TRUE), 
        total_agi_out = sum(agi, na.rm = TRUE),
        .by = c(origin_state, year))
total_inflows <- migration |>
    reframe(
        total_inflows = sum(returns, na.rm = TRUE), 
        total_agi_in = sum(agi, na.rm = TRUE),
        .by = c(destination_state, year)
    )
net_migration <- total_outflows |>
    left_join(total_inflows, by = c("origin_state" = "destination_state", "year")) |>
    rename(state = origin_state) |>
    mutate(net_migration = total_outflows - total_inflows)

# Merge to exposure
net_migration <- net_migration |>
    left_join(exposure_all, by = c("state" = "state")) |>
    left_join(exposure_top, by = c("state" = "state"))

# Merge to number of filers
filers <- returns |>
    reframe(total_filers = sum(returns), .by = c(state, year))
net_migration <- net_migration |>
    left_join(filers, by = c("state" = "state", "year"))

## Levels
net_migration |> 
    arrange(year, exposure_all) |>
    mutate(
        cdf = cumsum(total_filers) / sum(total_filers),
        .by = year
    ) |> 
    mutate(group = case_when(cdf[year == 2017] < .5 ~ "Bottom 50%", TRUE ~ "Top 50%"), .by = c(state, exposure_all)) |>
    reframe(total_migration = sum(net_migration, na.rm = TRUE), .by = c(group, year)) |> 
    ggplot(aes(x = year, y = total_migration, color = group, group = group)) +
    geom_line() +
    geom_point() +
    scale_color_viridis_d() +
    theme_classic()

## Shares
net_migration |> 
    arrange(year, exposure_all) |>
    mutate(
        cdf = cumsum(total_filers) / sum(total_filers),
        .by = year
    ) |> 
    mutate(group = case_when(cdf[year == 2017] < .5 ~ "Bottom 50%", TRUE ~ "Top 50%"), .by = c(state, exposure_all)) |>
    reframe(total_migration_share = sum(net_migration, na.rm = TRUE) / sum(total_filers), .by = c(group, year)) |> 
    ggplot(aes(x = year, y = total_migration_share, color = group, group = group)) +
    geom_line() +
    geom_point() +
    scale_color_viridis_d() +
    theme_classic()

# Regression based versions of this
net_migration |> 
    feols(net_migration ~ i(year, exposure_all, ref = 2016) - 1 | state + year,
    data = _,
    cluster = ~ state,
    weights = ~ total_filers
    ) |> iplot()

net_migration |> 
    arrange(year, exposure_all) |>
    mutate(
        cdf = cumsum(total_filers) / sum(total_filers),
        .by = year
    ) |> 
    mutate(treated = case_when(cdf[year == 2017] < .5 ~ FALSE, TRUE ~ TRUE), .by = c(state, exposure_all)) |>
    feols(net_migration ~ i(year, treated, ref = 2016) - 1 | state + year,
    data = _,
    cluster = ~ state,
    weights = ~ total_filers
    ) |> iplot()

net_migration |> 
    mutate(share = net_migration / total_filers) |>
    feols(share ~ i(year, exposure_all, ref = 2016) - 1 | state + year,
    data = _,
    cluster = ~ state,
    weights = ~ total_filers
    ) |> iplot()

net_migration |> 
    arrange(year, exposure_all) |>
    mutate(
        cdf = cumsum(total_filers) / sum(total_filers),
        .by = year) |> 
    mutate(treated = case_when(cdf[year == 2017] < .5 ~ FALSE, TRUE ~ TRUE), .by = c(state, exposure_all)) |>
    mutate(share = net_migration / total_filers) |>
    feols(share ~ i(year, treated, ref = 2016) - 1 | state + year,
    data = _,
    cluster = ~ state,
    weights = ~ total_filers
    ) |> iplot()    



########################################################
# Analyze full migration matrix
########################################################

# New exposure measure: 
# Change in SALT saved by moving from A to B
# (Assumes consumption bundles don't change)

# Compute gross SALT and federal refund per filer by state and year
# salt_pp: gross SALT paid per filer (taxes_paid_amount / returns)
# salt_refund_pp: federal tax offset from SALT deduction (MTR-weighted savings / returns)
# effective_salt_pp: net out-of-pocket SALT cost = salt_pp - salt_refund_pp
salt_per_state_year <- returns |>
    reframe(
        salt_pp         = sum(taxes_paid_amount, na.rm = TRUE) / sum(returns, na.rm = TRUE),
        salt_refund_pp  = sum(salt_savings,       na.rm = TRUE) / sum(returns, na.rm = TRUE),
        effective_salt_pp = salt_pp - salt_refund_pp,
        .by = c(state, year)
    )

# Compute state-year level differentials in effective SALT
salt_differentials <- expand_grid(
    origin_state = unique(returns$state),
    destination_state = unique(returns$state),
    year = unique(returns$year)
) |>
    left_join(salt_per_state_year |> rename_with(~ paste0(.x, "_origin"), -c(state, year)),
              by = c("origin_state" = "state", "year")) |>
    left_join(salt_per_state_year |> rename_with(~ paste0(.x, "_dest"), -c(state, year)),
              by = c("destination_state" = "state", "year")) |>
    mutate(salt_differential = effective_salt_pp_origin - effective_salt_pp_dest)

# For each origin-destination pair, compute drop in SALT wedge
exposure_salt <- salt_differentials |>
    mutate(pre_period = if_else(year < 2018, 1, 0)) |>
    reframe(
        exposure_salt = mean(salt_differential[pre_period == 1]) - mean(salt_differential[pre_period == 0]),
        .by = c(origin_state, destination_state)
    )

# Merge to migration data
migration <- migration |>
    inner_join(exposure_salt, by = c("origin_state", "destination_state"))

# Raw trends in means

## Levels
migration |> 
    arrange(year, exposure_salt) |>
    mutate(
        cdf = cumsum(total_filers_orig) / sum(total_filers_orig),
        .by = year
    ) |> 
    mutate(group = case_when(cdf[year == 2017] < .5 ~ "Bottom 50%", TRUE ~ "Top 50%"), .by = c(destination_state, origin_state)) |>
    reframe(total_migration = sum(returns, na.rm = TRUE), .by = c(group, year)) |> 
    ggplot(aes(x = year, y = total_migration, color = group, group = group)) +
    geom_line() +
    geom_point() +
    scale_color_viridis_d() +
    theme_classic()

## Shares
migration |> 
    arrange(year, exposure_salt) |>
    mutate(
        cdf = cumsum(total_filers_orig) / sum(total_filers_orig),
        .by = year
    ) |> 
    mutate(group = case_when(cdf[year == 2017] < .5 ~ "Bottom 50%", TRUE ~ "Top 50%"), .by = c(destination_state, origin_state)) |>
    reframe(total_migration_share = sum(returns, na.rm = TRUE) / sum(total_filers_orig), .by = c(group, year)) |> 
    ggplot(aes(x = year, y = total_migration_share, color = group, group = group)) +
    geom_line() +
    geom_point() +
    scale_color_viridis_d() +
    theme_classic()

## AGI conditional on moving    
migration |> 
    arrange(year, exposure_salt) |>
    mutate(
        cdf = cumsum(total_filers_orig) / sum(total_filers_orig),
        .by = year
    ) |> 
    mutate(group = case_when(cdf[year == 2017] < .5 ~ "Bottom 50%", TRUE ~ "Top 50%"), .by = c(destination_state, origin_state)) |>
    reframe(avg_agi = sum(agi, na.rm = TRUE) / sum(returns, na.rm = TRUE), .by = c(group, year)) |> 
    ggplot(aes(x = year, y = avg_agi, color = group, group = group)) +
    geom_line() +
    geom_point() +
    scale_color_viridis_d() +
    theme_classic()

# Regression based versions of this
migration |> 
    feols(returns ~ i(year, exposure_salt, ref = 2016) - 1 | origin_state^destination_state + year,
    data = _,
    cluster = ~ origin_state^destination_state,
    weights = ~ total_filers_orig
    ) |> iplot()

migration |> 
    arrange(year, exposure_salt) |>
    mutate(
        cdf = cumsum(total_filers_orig) / sum(total_filers_orig),
        .by = year
    ) |> 
    mutate(treated = case_when(cdf[year == 2017] < .5 ~ FALSE, TRUE ~ TRUE), .by = c(destination_state, origin_state)) |>
    feols(returns ~ i(year, treated, ref = 2016) - 1 | origin_state^destination_state + year,
    data = _,
    cluster = ~ origin_state^destination_state,
    weights = ~ total_filers_orig
    ) |> iplot()

migration |> 
    feols(log(returns) ~ i(year, exposure_salt, ref = 2016) - 1 | origin_state^destination_state + year,
    data = _,
    cluster = ~ origin_state^destination_state,
    weights = ~ total_filers_orig
    ) |> iplot()

migration |> 
    arrange(year, exposure_salt) |>
    mutate(
        cdf = cumsum(total_filers_orig) / sum(total_filers_orig),
        .by = year) |> 
    mutate(treated = case_when(cdf[year == 2017] < .5 ~ FALSE, TRUE ~ TRUE), .by = c(destination_state, origin_state)) |>
    feols(log(returns) ~ i(year, treated, ref = 2016) - 1 | origin_state^destination_state + year,
    data = _,
    cluster = ~ origin_state^destination_state,
    weights = ~ total_filers_orig
    ) |> iplot()

migration |> 
    mutate(share = returns / total_filers_orig) |>
    feols(share ~ i(year, exposure_salt, ref = 2016) - 1 | origin_state^destination_state + year,
    data = _,   
    cluster = ~ origin_state^destination_state,
    weights = ~ total_filers_orig
    ) |> iplot()

migration |> 
    arrange(year, exposure_salt) |>
    mutate(
        cdf = cumsum(total_filers_orig) / sum(total_filers_orig),
        .by = year) |> 
    mutate(treated = case_when(cdf[year == 2017] < .5 ~ FALSE, TRUE ~ TRUE), .by = c(destination_state, origin_state)) |>
    mutate(share = returns / total_filers_orig) |>
    feols(share ~ i(year, treated, ref = 2016) - 1 | origin_state^destination_state + year,
    data = _,
    cluster = ~ origin_state^destination_state,
    weights = ~ total_filers_orig
    ) |> iplot()
