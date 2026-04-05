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

# Load from database
con <- dbConnect(duckdb::duckdb(), dbdir = "../../../data/database.db")
migration <- dbGetQuery(con, "select * from migration_state")

# Add up migration flows across destinations
agg_inflows <- migration |>
    group_by(destination_state, year) |>
    dplyr::summarize(
        returns = sum(returns_inflow),
        individuals = sum(individuals_inflow),
        agi = sum(agi_inflow),
        .groups = "drop"
    )

agg_outflows <- migration |>
    group_by(origin_state, year) |>
    dplyr::summarize(
        returns = sum(returns_outflow),
        individuals = sum(individuals_outflow),
        agi = sum(agi_outflow),
        .groups = "drop"
    )

net_migration <- agg_inflows |>
    inner_join(agg_outflows, by = c("destination_state" = "origin_state", "year"), suffix = c("_inflow", "_outflow")) |>
    mutate(
        net_returns = returns_inflow - returns_outflow,
        net_individuals = individuals_inflow - individuals_outflow,
        net_agi = agi_inflow - agi_outflow
    ) |> 
    rename(state = destination_state)
########################################################
# Plot net migration for FL, TX, CA, NY
########################################################

states_to_plot <- c(06, 12, 36, 48)
net_migration |>
    filter(state %in% states_to_plot) |>
    ggplot(aes(x = year, y = net_returns, color = factor(state), group = factor(state))) +
    geom_line() +
    geom_point() +
    scale_color_viridis_d() +
    geom_vline(xintercept = 2017) +
    labs(
        title = "Net migration (returns) by state",
        x = "Year",
        y = "Net returns",
        color = "State"
    ) +
    theme_classic()

net_migration |>
    filter(state %in% states_to_plot) |>
    ggplot(aes(x = year, y = returns_inflow, color = factor(state), group = factor(state))) +
    geom_line() +
    geom_point() +
    scale_color_viridis_d() +
    geom_vline(xintercept = 2017) +
    labs(
        title = "Inflow of returns by state",
        x = "Year",
        y = "Inflow of returns",
        color = "State"
    ) +
    theme_classic()

net_migration |>
    filter(state %in% states_to_plot) |>
    ggplot(aes(x = year, y = returns_outflow, color = factor(state), group = factor(state))) +
    geom_line() +
    geom_point() +
    scale_color_viridis_d() +
    geom_vline(xintercept = 2017) +
    labs(
        title = "Outflow of returns by state",
        x = "Year",
        y = "Outflow of returns",
        color = "State"
    ) +
    theme_classic()


########################################################
# Changes in SALT deductions vs. changes in net migration
########################################################

# Load returns
state_returns <- dbGetQuery(con, "select * from returns_state")

# Compute average SALT per filer (all and top AGI)
exposure_all_agi <- state_returns |>
    reframe(
        avg_salt_per_filer = sum(state_and_local_income_taxes_amount, na.rm = TRUE) / sum(returns, na.rm = TRUE),
        .by = c(state, year)
    ) |> 
    mutate(pre_period = if_else(year < 2018, 1, 0)) |> 
    reframe(
        exposure = mean(avg_salt_per_filer[pre_period == 1]) - mean(avg_salt_per_filer[pre_period == 0]),
        .by = state
    )

max_agi_stub <- max(state_returns$agi_stub, na.rm = TRUE)

exposure_top_agi <- state_returns |>
    filter(agi_stub == max_agi_stub) |>
    reframe(
        avg_salt_per_filer = sum(state_and_local_income_taxes_amount, na.rm = TRUE) / sum(returns, na.rm = TRUE),
        .by = c(state, year)
    ) |> 
    mutate(pre_period = if_else(year < 2018, 1, 0)) |> 
    reframe(
        exposure = mean(avg_salt_per_filer[pre_period == 1]) - mean(avg_salt_per_filer[pre_period == 0]),
        .by = state
    )

# Compute total filers by state
total_filers <- state_returns |>
    reframe(total_filers = sum(returns), .by = c(state, year)) |>
    rename(w = total_filers)

# Merge migration to exposures and weights
net_migration <- net_migration |>
    left_join(exposure_all_agi |> rename(exposure_all = exposure), by = "state") |>
    left_join(exposure_top_agi |> rename(exposure_top = exposure), by = "state") |>
    left_join(total_filers, by = c("state", "year"))

# Plot exposure against changes in net migration
net_migration |> 
    mutate(pre_period = if_else(year < 2018, 1, 0)) |> 
    filter(year > 2011) |> 
    reframe(
        migration_change = mean(net_returns[pre_period == 1]) - mean(net_returns[pre_period == 0]),
        migration_change_pct = mean(net_returns[pre_period == 1] / w[pre_period == 1]) - mean(net_returns[pre_period == 0] / w[pre_period == 0]),
        exposure_all = mean(exposure_all),
        exposure_top = mean(exposure_top),
        .by = state
    ) |> 
    ggplot(aes(x = exposure_all, y = migration_change_pct)) +
    geom_point() +
    geom_smooth(method = "lm") +
    labs(
        title = "Changes in net migration vs. changes in SALT deductions",
        x = "Changes in SALT deductions",
        y = "Changes in net migration",
        size = "Total filers"
    ) +
    theme_classic()

########################################################
# Regression coefficients over time: net returns on exposure measures
########################################################

estimates_all_agi <- feols(
    net_returns ~ i(year, exposure_all) - 1,
    data = net_migration,
    weights = ~ w
) |> 
    coeftable() |> 
    as.data.frame() |>
    rename(estimate = Estimate, std_error = `Std. Error`) |> 
    mutate(
        lower = estimate - 1.96 * std_error, upper = estimate + 1.96 * std_error,
        year = 2012:2021,
        type = "All AGI classes"
    )

estimates_top_agi <- feols(
    net_returns ~ i(year, exposure_top) - 1,
    data = net_migration,
    weights = ~ w
) |> 
    coeftable() |> 
    as.data.frame() |>
    rename(estimate = Estimate, std_error = `Std. Error`) |> 
    mutate(
        lower = estimate - 1.96 * std_error, upper = estimate + 1.96 * std_error,
        year = 2012:2021,
        type = "Top AGI class"
    )

estimates <- bind_rows(estimates_all_agi, estimates_top_agi) |> 
    mutate(
        estimate_standardized = estimate / estimate[year == 2017],
        lower_standardized = lower / estimate[year == 2017],
        upper_standardized = upper / estimate[year == 2017],
        .by = type
    ) |> 
    mutate(year = ifelse(type == "All AGI classes", year + 0.05, year - 0.05))
ggplot(estimates, aes(x = year, y = estimate_standardized, color = type, group = type)) +
    geom_point() +
    geom_errorbar(aes(ymin = lower_standardized, ymax = upper_standardized), width = 0.2) +
    geom_hline(yintercept = 0, linetype = "dashed") +
    geom_vline(xintercept = 2017.5, linetype = "dashed", color = "red") +
    labs(
        title = "Migration estimates by exposure measure",
        x = "Year",
        y = "Estimate (95% CI)",
        color = "Exposure measure"
    ) +
    theme_classic() +
    scale_color_viridis_d()

estimates_inflow_all_agi <- feols(
    returns_inflow ~ i(year, exposure_all) - 1,
    data = net_migration,
    weights = ~ w
) |> 
    coeftable() |> 
    as.data.frame() |>
    rename(estimate = Estimate, std_error = `Std. Error`) |> 
    mutate(
        lower = estimate - 1.96 * std_error, upper = estimate + 1.96 * std_error,
        year = 2012:2021,
        type = "All AGI classes (Inflow)"
    )

estimates_outflow_all_agi <- feols(
    returns_outflow ~ i(year, exposure_all) - 1,
    data = net_migration,
    weights = ~ w
) |> 
    coeftable() |> 
    as.data.frame() |>
    rename(estimate = Estimate, std_error = `Std. Error`) |> 
    mutate(
        lower = estimate - 1.96 * std_error, upper = estimate + 1.96 * std_error,
        year = 2012:2021,
        type = "All AGI classes (Outflow)"
    )

estimates_inflow_top_agi <- feols(
    returns_inflow ~ i(year, exposure_top) - 1,
    data = net_migration,
    weights = ~ w
) |> 
    coeftable() |> 
    as.data.frame() |>
    rename(estimate = Estimate, std_error = `Std. Error`) |> 
    mutate(
        lower = estimate - 1.96 * std_error, upper = estimate + 1.96 * std_error,
        year = 2012:2021,
        type = "Top AGI class (Inflow)"
    )

estimates_outflow_top_agi <- feols(
    returns_outflow ~ i(year, exposure_top) - 1,
    data = net_migration,
    weights = ~ w
) |> 
    coeftable() |> 
    as.data.frame() |>
    rename(estimate = Estimate, std_error = `Std. Error`) |> 
    mutate(
        lower = estimate - 1.96 * std_error, upper = estimate + 1.96 * std_error,
        year = 2012:2021,
        type = "Top AGI class (Outflow)"
    )

estimates <- bind_rows(
    estimates_inflow_all_agi, 
    estimates_inflow_top_agi,
    estimates_outflow_all_agi,
    estimates_outflow_top_agi
) |> 
    mutate(
        estimate_standardized = estimate / estimate[year == 2017],
        lower_standardized = lower / estimate[year == 2017],
        upper_standardized = upper / estimate[year == 2017],
        .by = type
    ) |> 
    mutate(year = case_when(
        type == "All AGI classes (Inflow)" ~ year + 0.05,
        type == "All AGI classes (Outflow)" ~ year - 0.05,
        type == "Top AGI class (Inflow)" ~ year + 0.1,
        type == "Top AGI class (Outflow)" ~ year - 0.1,
        TRUE ~ year
    )) 
    
estimates|>  
    ggplot(aes(x = year, y = estimate_standardized, color = type, group = type)) +
    geom_point() +
    geom_errorbar(aes(ymin = lower_standardized, ymax = upper_standardized), width = 0.2) +
    geom_hline(yintercept = 0, linetype = "dashed") +
    geom_vline(xintercept = 2017.5, linetype = "dashed", color = "red") +
    labs(
        title = "Migration estimates by exposure measure",
        x = "Year",
        y = "Estimate (95% CI)",
        color = "Exposure measure"
    ) +
    theme_classic() +
    scale_color_viridis_d()



# TWFE version of this
feols(
    net_returns ~ i(year, exposure_all, ref = 2017) - 1 | state,
    data = net_migration |> filter(year > 2011),
    weights = ~ w,
    cluster = ~ state
) |>
    iplot()
feols(
    net_returns ~ i(year, exposure_top, ref = 2017) - 1 | state,
    data = net_migration |> filter(year > 2011),
    weights = ~ w,
    cluster = ~ state
) |>
    iplot()

feols(
    returns_inflow ~ i(year, exposure_all, ref = 2017) - 1 | state,
    data = net_migration |> filter(year > 2011),
    weights = ~ w,
    cluster = ~ state
) |>
    iplot()
feols(
    returns_inflow ~ i(year, exposure_top, ref = 2017) - 1 | state,
    data = net_migration |> filter(year > 2011),
    weights = ~ w,
    cluster = ~ state
) |>
    iplot()
feols(
    returns_outflow ~ i(year, exposure_all, ref = 2017) - 1 | state,
    data = net_migration |> filter(year > 2011),
    weights = ~ w,
    cluster = ~ state
) |>
    iplot()
feols(
    returns_outflow ~ i(year, exposure_top, ref = 2017) - 1 | state,
    data = net_migration |> filter(year > 2011),
    weights = ~ w,
    cluster = ~ state
) |>
    iplot()

# Average AGI of movers by origin-destination pair
pair_avg_agi <- migration |>
    mutate(
        avg_agi_inflow = agi_inflow / returns_inflow,
        avg_agi_outflow = agi_outflow / returns_outflow
    ) |>
    select(origin_state, destination_state, year, returns_inflow, returns_outflow, avg_agi_inflow, avg_agi_outflow)

# Aggregate to state-year (weighted by returns)
avg_agi_inflows <- pair_avg_agi |>
    group_by(destination_state, year) |>
    dplyr::summarize(
        avg_agi_inflow = weighted.mean(avg_agi_inflow, returns_inflow, na.rm = TRUE),
        .groups = "drop"
    )

avg_agi_outflows <- pair_avg_agi |>
    group_by(origin_state, year) |>
    dplyr::summarize(
        avg_agi_outflow = weighted.mean(avg_agi_outflow, returns_outflow, na.rm = TRUE),
        .groups = "drop"
    )

net_migration_agi <- avg_agi_inflows |>
    inner_join(avg_agi_outflows, by = c("destination_state" = "origin_state", "year")) |>
    mutate(
        net_avg_agi = avg_agi_inflow - avg_agi_outflow
    ) |>
    rename(state = destination_state)

# Plot inflow average AGI for FL, TX, CA, NY
net_migration_agi |>
    filter(state %in% states_to_plot) |>
    ggplot(aes(x = year, y = avg_agi_inflow, color = factor(state), group = factor(state))) +
    geom_line() +
    geom_point() +
    scale_color_viridis_d() +
    geom_vline(xintercept = 2017) +
    labs(
        title = "Inflow average AGI of movers by state",
        x = "Year",
        y = "Inflow average AGI",
        color = "State"
    ) +
    theme_classic()

# Plot outflow average AGI for FL, TX, CA, NY
net_migration_agi |>
    filter(state %in% states_to_plot) |>
    ggplot(aes(x = year, y = avg_agi_outflow, color = factor(state), group = factor(state))) +
    geom_line() +
    geom_point() +
    scale_color_viridis_d() +
    geom_vline(xintercept = 2017) +
    labs(
        title = "Outflow average AGI of movers by state",
        x = "Year",
        y = "Outflow average AGI",
        color = "State"
    ) +
    theme_classic()

# Plot net average AGI for FL, TX, CA, NY
states_to_plot <- c(06, 12, 36, 48)
net_migration_agi |>
    filter(state %in% states_to_plot) |>
    ggplot(aes(x = year, y = net_avg_agi, color = factor(state), group = factor(state))) +
    geom_line() +
    geom_point() +
    scale_color_viridis_d() +
    geom_vline(xintercept = 2017) +
    labs(
        title = "Net average AGI of movers by state",
        x = "Year",
        y = "Net average AGI",
        color = "State"
    ) +
    theme_classic()

# Regression coefficients over time: net avg AGI on exposure measures
net_migration_agi <- net_migration_agi |>
    left_join(exposure_all_agi |> rename(exposure_all = exposure), by = "state") |>
    left_join(exposure_top_agi |> rename(exposure_top = exposure), by = "state")

# Add number of returns to weigh
net_migration_agi <- net_migration_agi |>
    left_join(returns_by_state_year, by = c("state", "year"))

estimates_agi_all <- feols(
    net_avg_agi ~ i(year, exposure_all) - 1,
    data = net_migration_agi,
    weights = ~ w
) |>
    coeftable() |>
    as.data.frame() |>
    rename(estimate = Estimate, std_error = `Std. Error`) |>
    mutate(
        lower = estimate - 1.96 * std_error, upper = estimate + 1.96 * std_error,
        year = 2012:2021,
        type = "All AGI classes"
    )

estimates_agi_top <- feols(
    net_avg_agi ~ i(year, exposure_top) - 1,
    data = net_migration_agi,
    weights = ~ w
) |>
    coeftable() |>
    as.data.frame() |>
    rename(estimate = Estimate, std_error = `Std. Error`) |>
    mutate(
        lower = estimate - 1.96 * std_error, upper = estimate + 1.96 * std_error,
        year = 2012:2021,
        type = "Top AGI class"
    )

estimates_agi <- bind_rows(
    estimates_agi_all |> mutate(type = "All AGI classes"),
    estimates_agi_top |> mutate(type = "Top AGI class")
) |>
    mutate(
        estimate_standardized = estimate / estimate[year == 2017],
        lower_standardized = lower / estimate[year == 2017],
        upper_standardized = upper / estimate[year == 2017],
        .by = type
    ) |>
    mutate(year = case_when(
        type == "All AGI classes" ~ year + 0.05,
        type == "Top AGI class" ~ year - 0.05,
        TRUE ~ year
    ))

ggplot(estimates_agi, aes(x = year, y = estimate_standardized, color = type, group = type)) +
    geom_point() +
    geom_errorbar(aes(ymin = lower_standardized, ymax = upper_standardized), width = 0.2) +
    geom_hline(yintercept = 0, linetype = "dashed") +
    geom_vline(xintercept = 2017.5, linetype = "dashed", color = "red") +
    labs(
        title = "Net avg AGI estimates by exposure measure",
        x = "Year",
        y = "Estimate (95% CI)",
        color = "Exposure measure"
    ) +
    theme_classic() +
    scale_color_viridis_d()

# Let's try our hands on a two-way-fixed-effects model
feols(
    net_avg_agi ~ i(year, exposure_all, ref = 2017) - 1 | state,
    data = net_migration_agi |> filter(year > 2011),
    weights = ~ w,
    cluster = ~ state
) |>
    iplot()
feols(
    net_avg_agi ~ i(year, exposure_top, ref = 2017) - 1 | state,
    data = net_migration_agi |> filter(year > 2011),
    weights = ~ w,
    cluster = ~ state
) |>
    iplot()
feols(
    avg_agi_inflow ~ i(year, exposure_all, ref = 2017) - 1 | state,
    data = net_migration_agi |> filter(year > 2011),
    weights = ~ w,
    cluster = ~ state
) |>
    iplot()
feols(
    avg_agi_inflow ~ i(year, exposure_top, ref = 2017) - 1 | state,
    data = net_migration_agi |> filter(year > 2011),
    weights = ~ w,
    cluster = ~ state
) |>
    iplot()
feols(
    avg_agi_outflow ~ i(year, exposure_all, ref = 2017) - 1 | state,
    data = net_migration_agi |> filter(year > 2011),
    weights = ~ w,
    cluster = ~ state
) |>
    iplot()
feols(
    avg_agi_outflow ~ i(year, exposure_top, ref = 2017) - 1 | state,
    data = net_migration_agi |> filter(year > 2011),
    weights = ~ w,
    cluster = ~ state
) |>
    iplot()

# Exposure of origin/destination for flows
exposure_all_map <- exposure_all_agi |> rename(exposure_all = exposure)
exposure_top_map <- exposure_top_agi |> rename(exposure_top = exposure)

migration_with_exposure <- migration |>
    left_join(exposure_all_map, by = c("origin_state" = "state")) |>
    rename(exposure_all_origin = exposure_all) |>
    left_join(exposure_all_map, by = c("destination_state" = "state")) |>
    rename(exposure_all_dest = exposure_all) |>
    left_join(exposure_top_map, by = c("origin_state" = "state")) |>
    rename(exposure_top_origin = exposure_top) |>
    left_join(exposure_top_map, by = c("destination_state" = "state")) |>
    rename(exposure_top_dest = exposure_top)

inflow_exposure <- migration_with_exposure |>
    group_by(destination_state, year) |>
    dplyr::summarize(
        inflow_exposure_all = weighted.mean(exposure_all_origin, returns_inflow, na.rm = TRUE),
        inflow_exposure_top = weighted.mean(exposure_top_origin, returns_inflow, na.rm = TRUE),
        .groups = "drop"
    )

outflow_exposure <- migration_with_exposure |>
    group_by(origin_state, year) |>
    dplyr::summarize(
        outflow_exposure_all = weighted.mean(exposure_all_dest, returns_outflow, na.rm = TRUE),
        outflow_exposure_top = weighted.mean(exposure_top_dest, returns_outflow, na.rm = TRUE),
        .groups = "drop"
    )

net_flow_exposure <- inflow_exposure |>
    inner_join(outflow_exposure, by = c("destination_state" = "origin_state", "year")) |>
    mutate(
        net_exposure_all = inflow_exposure_all - outflow_exposure_all,
        net_exposure_top = inflow_exposure_top - outflow_exposure_top
    ) |>
    rename(state = destination_state)

# Plot net exposure for FL, TX, CA, NY
states_to_plot <- c(06, 12, 36, 48)
net_flow_exposure |>
    filter(state %in% states_to_plot) |>
    ggplot(aes(x = year, y = net_exposure_all, color = factor(state), group = factor(state))) +
    geom_line() +
    geom_point() +
    scale_color_viridis_d() +
    geom_vline(xintercept = 2017) +
    labs(
        title = "Net exposure (all AGI) by state",
        x = "Year",
        y = "Net exposure",
        color = "State"
    ) +
    theme_classic()

net_flow_exposure |>
    filter(state %in% states_to_plot) |>
    ggplot(aes(x = year, y = net_exposure_top, color = factor(state), group = factor(state))) +
    geom_line() +
    geom_point() +
    scale_color_viridis_d() +
    geom_vline(xintercept = 2017) +
    labs(
        title = "Net exposure (top AGI) by state",
        x = "Year",
        y = "Net exposure",
        color = "State"
    ) +
    theme_classic()

# Regression coefficients over time: net exposure on exposure measures
net_flow_exposure <- net_flow_exposure |>
    left_join(exposure_all_map, by = "state") |>
    left_join(exposure_top_map, by = "state")

# Add number of returns to weigh
net_flow_exposure <- net_flow_exposure |>
    left_join(returns_by_state_year, by = c("state", "year"))

estimates_netexp_all <- feols(
    net_exposure_all ~ i(year, exposure_all) - 1,
    data = net_flow_exposure,
    weights = ~ w
) |>
    coeftable() |>
    as.data.frame() |>
    rename(estimate = Estimate, std_error = `Std. Error`) |>
    mutate(
        lower = estimate - 1.96 * std_error, upper = estimate + 1.96 * std_error,
        year = 2012:2021,
        type = "All AGI classes"
    )

estimates_netexp_top <- feols(
    net_exposure_top ~ i(year, exposure_top) - 1,
    data = net_flow_exposure,
    weights = ~ w
) |>
    coeftable() |>
    as.data.frame() |>
    rename(estimate = Estimate, std_error = `Std. Error`) |>
    mutate(
        lower = estimate - 1.96 * std_error, upper = estimate + 1.96 * std_error,
        year = 2012:2021,
        type = "Top AGI class"
    )

estimates_netexp <- bind_rows(
    estimates_netexp_all |> mutate(type = "All AGI classes"),
    estimates_netexp_top |> mutate(type = "Top AGI class")
) |>
    mutate(
        estimate_standardized = estimate / estimate[year == 2017],
        lower_standardized = lower / estimate[year == 2017],
        upper_standardized = upper / estimate[year == 2017],
        .by = type
    ) |>
    mutate(year = case_when(
        type == "All AGI classes" ~ year + 0.05,
        type == "Top AGI class" ~ year - 0.05,
        TRUE ~ year
    ))

ggplot(estimates_netexp, aes(x = year, y = estimate_standardized, color = type, group = type)) +
    geom_point() +
    geom_errorbar(aes(ymin = lower_standardized, ymax = upper_standardized), width = 0.2) +
    geom_hline(yintercept = 0, linetype = "dashed") +
    geom_vline(xintercept = 2017.5, linetype = "dashed", color = "red") +
    labs(
        title = "Net exposure estimates by exposure measure",
        x = "Year",
        y = "Estimate (95% CI)",
        color = "Exposure measure"
    ) +
    theme_classic() +
    scale_color_viridis_d()

# Try the TWFE version of this
feols(
    net_exposure_all ~ i(year, exposure_all, ref = 2017) - 1 | state,
    data = net_flow_exposure |> filter(year > 2011),
    weights = ~ w,
    cluster = ~ state
) |>
    iplot()
feols(
    net_exposure_top ~ i(year, exposure_top, ref = 2017) - 1 | state,
    data = net_flow_exposure |> filter(year > 2011),
    weights = ~ w,
    cluster = ~ state
) |>
    iplot()





