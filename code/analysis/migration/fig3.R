# Clean up workspace
gc()
rm(list = ls())
getwd()

# Load libraries
library(tidyverse)
library(duckdb)
library(fixest)

# Load from database
con <- dbConnect(duckdb::duckdb(), dbdir = "../../../data/database.db")
migration <- dbGetQuery(con, "select * from migration_state")
returns   <- dbGetQuery(con, "select * from returns_state")
dbDisconnect(con, shutdown = TRUE)

# Impute marginal tax rates
returns <- returns |>
    mutate(
        w_mfj    = joint_returns / returns,
        w_single = single_returns / returns,
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
        mtr_weighted = w_mfj * mtr_mfj + w_single * mtr_single + w_hoh * mtr_hoh,
        salt_savings = taxes_paid_amount * mtr_weighted
    )

# Compute effective SALT per filer by state-year
salt_per_state_year <- returns |>
    reframe(
        salt_pp           = sum(taxes_paid_amount, na.rm = TRUE) / sum(returns, na.rm = TRUE),
        salt_refund_pp    = sum(salt_savings,       na.rm = TRUE) / sum(returns, na.rm = TRUE),
        effective_salt_pp = salt_pp - salt_refund_pp,
        .by = c(state, year)
    )

# Compute state-pair SALT differentials and exposure
salt_differentials <- expand_grid(
    origin_state      = unique(returns$state),
    destination_state = unique(returns$state),
    year              = unique(returns$year)
) |>
    left_join(salt_per_state_year |> rename_with(~ paste0(.x, "_origin"), -c(state, year)),
              by = c("origin_state" = "state", "year")) |>
    left_join(salt_per_state_year |> rename_with(~ paste0(.x, "_dest"), -c(state, year)),
              by = c("destination_state" = "state", "year")) |>
    mutate(salt_differential = effective_salt_pp_origin - effective_salt_pp_dest)

exposure_salt <- salt_differentials |>
    mutate(pre_period = if_else(year < 2018, 1, 0)) |>
    reframe(
        exposure_salt = mean(salt_differential[pre_period == 1]) -
                        mean(salt_differential[pre_period == 0]),
        .by = c(origin_state, destination_state)
    )

# Merge exposure and filer weights onto migration
migration <- migration |>
    filter(year >= 2012) |>
    filter(origin_state %in% 1:56 & destination_state %in% 1:56) |>
    inner_join(exposure_salt, by = c("origin_state", "destination_state"))

filers <- returns |>
    reframe(total_filers = sum(returns), .by = c(state, year))

migration <- migration |>
    left_join(filers |> rename(total_filers_orig = total_filers),
              by = c("origin_state" = "state", "year"))

# -----------------------------------------------------------------------------
# Figure 3: Event-study — log in-migration × SALT exposure (state level)
# -----------------------------------------------------------------------------

migration |>
    feols(
        log(returns) ~ i(year, exposure_salt, ref = 2017) - 1 |
            origin_state^destination_state + year,
        data = _,
        cluster = ~ origin_state^destination_state,
        weights = ~ total_filers_orig
    ) |>
    coeftable() |>
    as.data.frame() |>
    rownames_to_column("coef_name") |>
    rename(estimate = Estimate, std_error = `Std. Error`) |>
    mutate(
        lower = estimate - 1.96 * std_error,
        upper = estimate + 1.96 * std_error,
        year  = as.integer(str_extract(coef_name, "\\d{4}"))
    ) |>
    bind_rows(data.frame(year = 2017L, estimate = 0, std_error = 0, lower = 0, upper = 0)) |>
    arrange(year) |>
    ggplot(aes(x = year, y = estimate)) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "grey50") +
    geom_vline(xintercept = 2017.5, linetype = "dashed", color = "grey30") +
    geom_errorbar(aes(ymin = lower, ymax = upper), width = 0.2, color = "#23373b") +
    geom_point(color = "#23373b") +
    labs(x = "", y = "Percent change in migration") +
    theme(
        panel.background = element_rect(fill = "#fafafa", color = NA),
        plot.background  = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        axis.line.x = element_line(color = "grey30"),
        axis.line.y = element_line(color = "grey30"),
        legend.position = "none",
        axis.text.x = element_text(color = "grey20"),
        axis.text.y = element_text(color = "grey20"),
        text = element_text(color = "grey20"),
    ) +
    scale_x_continuous(breaks = seq(2012, 2022, by = 2))
ggsave("../../../output/fig3.png", width = 16, height = 8, units = "cm")

# -----------------------------------------------------------------------------
# Figure 4: Event-study — average AGI per mover × SALT exposure (state level)
# -----------------------------------------------------------------------------

migration |>
    mutate(avg_agi = agi / returns) |>
    feols(
        avg_agi ~ i(year, exposure_salt, ref = 2017) - 1 |
            origin_state^destination_state + year,
        data = _,
        cluster = ~ origin_state^destination_state,
        weights = ~ total_filers_orig
    ) |>
    coeftable() |>
    as.data.frame() |>
    rownames_to_column("coef_name") |>
    rename(estimate = Estimate, std_error = `Std. Error`) |>
    mutate(
        lower = estimate - 1.96 * std_error,
        upper = estimate + 1.96 * std_error,
        year  = as.integer(str_extract(coef_name, "\\d{4}"))
    ) |>
    bind_rows(data.frame(year = 2017L, estimate = 0, std_error = 0, lower = 0, upper = 0)) |>
    arrange(year) |>
    ggplot(aes(x = year, y = estimate)) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "grey50") +
    geom_vline(xintercept = 2017.5, linetype = "dashed", color = "grey30") +
    geom_errorbar(aes(ymin = lower, ymax = upper), width = 0.2, color = "#23373b") +
    geom_point(color = "#23373b") +
    labs(x = "", y = "Effect on average AGI per mover ($)") +
    theme(
        panel.background = element_rect(fill = "#fafafa", color = NA),
        plot.background  = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        axis.line.x = element_line(color = "grey30"),
        axis.line.y = element_line(color = "grey30"),
        legend.position = "none",
        axis.text.x = element_text(color = "grey20"),
        axis.text.y = element_text(color = "grey20"),
        text = element_text(color = "grey20"),
    ) +
    scale_x_continuous(breaks = seq(2012, 2022, by = 2))
ggsave("../../../output/fig4.png", width = 16, height = 8, units = "cm")
