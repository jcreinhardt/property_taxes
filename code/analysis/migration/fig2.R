# Clean up workspace
gc()
rm(list = ls())
getwd()

# Load libraries
library(tidyverse)
library(duckdb)
library(viridis)

# Load from database
con <- dbConnect(duckdb::duckdb(), dbdir = "../../../data/database.db")
migration <- dbGetQuery(con, "select * from migration_county_balanced")
county_returns <- dbGetQuery(con, "select * from returns_county")
county_returns_agi <- dbGetQuery(con, "select * from returns_county_agi")
dbDisconnect(con, shutdown = TRUE)

# Impute marginal tax rates for county AGI stubs
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
            agi_stub == 1 ~ 0.00,
            agi_stub == 3 ~ 0.05,
            agi_stub == 4 ~ 0.22,
            agi_stub == 5 ~ 0.225,
            agi_stub == 6 ~ 0.25,
            agi_stub == 8 ~ 0.365,
            .default = NA_real_
        ),
        mtr_mfj = case_when(
            agi_stub == 1 ~ 0.00,
            agi_stub == 3 ~ 0.05,
            agi_stub == 4 ~ 0.12,
            agi_stub == 5 ~ 0.22,
            agi_stub == 6 ~ 0.24,
            agi_stub == 8 ~ 0.36,
            .default = NA_real_
        ),
        mtr_hoh = case_when(
            agi_stub == 1 ~ 0.00,
            agi_stub == 3 ~ 0.06,
            agi_stub == 4 ~ 0.14,
            agi_stub == 5 ~ 0.22,
            agi_stub == 6 ~ 0.25,
            agi_stub == 8 ~ 0.365,
            .default = NA_real_
        ),
        mtr_weighted = w_mfj * mtr_mfj + w_single * mtr_single + w_hoh * mtr_hoh,
        salt_savings = taxes_paid_amount * mtr_weighted
    )

salt_per_county_year <- county_returns_agi |>
    reframe(
        salt_pp           = sum(taxes_paid_amount, na.rm = TRUE) / sum(returns, na.rm = TRUE),
        salt_refund_pp    = sum(salt_savings,       na.rm = TRUE) / sum(returns, na.rm = TRUE),
        effective_salt_pp = salt_pp - salt_refund_pp,
        .by = c(county, year)
    )

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

salt_exposure <- salt_differentials |>
    reframe(
        exposure = salt_differential[year == 2018] - salt_differential[year == 2017],
        .by = c(origin_county, destination_county)
    )

migration <- migration |>
    inner_join(salt_exposure,
               by = c("origin_fips" = "origin_county", "destination_fips" = "destination_county"))

filers <- county_returns |>
    reframe(total_filers = sum(returns), .by = c(county, year))

migration <- migration |>
    inner_join(filers |> rename(total_filers_dest = total_filers),
               by = c("destination_fips" = "county", "year"))

# Figure 2: In-migration levels by SALT exposure group
plot_data <- migration |>
    arrange(year, exposure) |>
    mutate(
        cdf = cumsum(total_filers_dest) / sum(total_filers_dest),
        .by = year
    ) |>
    mutate(
        group = case_when(cdf[year == 2017] < .5 ~ "Low SALT Exposure", TRUE ~ "High SALT Exposure"),
        .by = c(origin_fips, destination_fips)
    ) |>
    reframe(total_migration = sum(returns, na.rm = TRUE), .by = c(group, year))

# Counterfactual: intercept = pre-2017 average of high series, trend follows low series
low_series  <- plot_data |> filter(group == "Low SALT Exposure")
high_pre_avg <- plot_data |> filter(group == "High SALT Exposure", year < 2017) |> pull(total_migration) |> mean()
low_pre_avg  <- low_series |> filter(year < 2017) |> pull(total_migration) |> mean()

counterfactual <- low_series |>
    filter(year >= 2017) |>
    mutate(
        total_migration = high_pre_avg + (total_migration - low_pre_avg),
        group = "Parallel Trend counterfactual"
    )

plot_all <- bind_rows(plot_data, counterfactual)

ggplot(plot_all, aes(x = year, y = total_migration, color = group, group = group, linetype = group)) +
    geom_line() +
    geom_point(data = filter(plot_all, group != "Parallel Trend counterfactual")) +
    geom_vline(xintercept = 2017.5, linetype = "dashed", color = "grey30") +
    scale_color_manual(
        name = NULL,
        values = c(
            "Low SALT Exposure" = "#23373b",
            "High SALT Exposure" = "#eb811b",
            "Parallel Trend counterfactual" = "#eb811b"
        )
    ) +
    scale_linetype_manual(
        name = NULL,
        values = c(
            "Low SALT Exposure" = "solid",
            "High SALT Exposure" = "solid",
            "Parallel Trend counterfactual" = "dashed"
        )
    ) +
    labs(x = "", y = "In-migration (returns)") +
    theme(
        panel.background = element_rect(fill = "#fafafa", color = NA),
        plot.background = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        axis.line.x = element_line(color = "grey30"),
        axis.line.y = element_line(color = "grey30"),
        legend.position = "right",
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.box.background = element_rect(fill = "#fafafa", color = "grey30"),
        legend.key = element_rect(fill = "#fafafa", color = NA),
        legend.text = element_text(size = 7),
        axis.text.x = element_text(color = "grey20"),
        axis.text.y = element_text(color = "grey20"),
        text = element_text(color = "grey20"),
    ) +
    scale_x_continuous(breaks = seq(2012, 2022, by = 2))
ggsave("../../../output/fig2.png", width = 16, height = 10, units = "cm")

# Figure 2b: In-migration share by SALT exposure group
plot_data_share <- migration |>
    arrange(year, exposure) |>
    mutate(
        cdf = cumsum(total_filers_dest) / sum(total_filers_dest),
        .by = year
    ) |>
    mutate(
        group = case_when(cdf[year == 2017] < .5 ~ "Low SALT Exposure", TRUE ~ "High SALT Exposure"),
        .by = c(origin_fips, destination_fips)
    ) |>
    reframe(
        migration_share = sum(returns, na.rm = TRUE) / sum(total_filers_dest, na.rm = TRUE),
        .by = c(group, year)
    )

low_series_share  <- plot_data_share |> filter(group == "Low SALT Exposure")
high_share_pre_avg <- plot_data_share |> filter(group == "High SALT Exposure", year < 2017) |> pull(migration_share) |> mean()
low_share_pre_avg  <- low_series_share |> filter(year < 2017) |> pull(migration_share) |> mean()

counterfactual_share <- low_series_share |>
    filter(year >= 2017) |>
    mutate(
        migration_share = high_share_pre_avg + (migration_share - low_share_pre_avg),
        group = "Parallel Trend counterfactual"
    )

plot_all_share <- bind_rows(plot_data_share, counterfactual_share)

ggplot(plot_all_share, aes(x = year, y = migration_share, color = group, group = group, linetype = group)) +
    geom_line() +
    geom_point(data = filter(plot_all_share, group != "Parallel Trend counterfactual")) +
    geom_vline(xintercept = 2017.5, linetype = "dashed", color = "grey30") +
    scale_color_manual(
        name = NULL,
        values = c(
            "Low SALT Exposure" = "#23373b",
            "High SALT Exposure" = "#eb811b",
            "Parallel Trend counterfactual" = "#eb811b"
        )
    ) +
    scale_linetype_manual(
        name = NULL,
        values = c(
            "Low SALT Exposure" = "solid",
            "High SALT Exposure" = "solid",
            "Parallel Trend counterfactual" = "dashed"
        )
    ) +
    labs(x = "", y = "In-migration (share of destination filers)") +
    theme(
        panel.background = element_rect(fill = "#fafafa", color = NA),
        plot.background = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        axis.line.x = element_line(color = "grey30"),
        axis.line.y = element_line(color = "grey30"),
        legend.position = "right",
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.box.background = element_rect(fill = "#fafafa", color = "grey30"),
        legend.key = element_rect(fill = "#fafafa", color = NA),
        legend.text = element_text(size = 7),
        axis.text.x = element_text(color = "grey20"),
        axis.text.y = element_text(color = "grey20"),
        text = element_text(color = "grey20"),
    ) +
    scale_x_continuous(breaks = seq(2012, 2022, by = 2))
ggsave("../../../output/fig2_share.png", width = 16, height = 10, units = "cm")
