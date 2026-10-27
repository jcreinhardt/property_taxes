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

# Per-script output folder
OUTPUT_DIR <- "../../../output/analysis/migration/fig2"
if (!dir.exists(OUTPUT_DIR)) dir.create(OUTPUT_DIR, recursive = TRUE)

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
ggsave(file.path(OUTPUT_DIR, "fig2.png"), width = 16, height = 10,
       units = "cm")

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
ggsave(file.path(OUTPUT_DIR, "fig2_share.png"), width = 16, height = 10,
       units = "cm")

# =============================================================================
# ACS: pooled treatment vs control mover rates (TWFE sample definitions)
# =============================================================================

library(DBI)
library(scales)
library(glue)

DB_PATH <- "../../../data/database.db"

CONTROL_VARS <- c("age", "bedrooms", "nchild", "rooms", "yrmarr", "nfams")
MISSING_NUMERIC <- c(9999999L, 9999998L, 9999997L, 999999L, 99999L, 9999L)

clean_control <- function(x, var_name) {
  x <- if (is.factor(x)) as.vector(x) else x
  num <- suppressWarnings(as.numeric(x))
  if (!all(is.na(num))) {
    x <- num
    x[x %in% MISSING_NUMERIC] <- NA_real_
    if (var_name %in% c("hhincome", "valueh", "inctot", "ftotinc", "incwage")) {
      x[x >= 9999990] <- NA_real_
    }
  }
  x
}

prepare_controls <- function(dat, control_vars) {
  out <- dat
  for (v in control_vars) {
    x <- as.numeric(clean_control(out[[v]], v))
    med <- stats::median(x, na.rm = TRUE)
    if (!is.finite(med)) med <- 0
    x[is.na(x)] <- med
    out[[v]] <- x
  }
  out
}

summarize_tc_by_year <- function(dat, control_bands, treat_bands) {
  dat |>
    dplyr::filter(group %in% c(control_bands, treat_bands)) |>
    dplyr::mutate(
      tc_group = dplyr::if_else(
        group %in% treat_bands,
        "Treated",
        "Control"
      )
    ) |>
    dplyr::group_by(year, tc_group) |>
    dplyr::summarise(
      mover_rate = mean(mover, na.rm = TRUE),
      n = dplyr::n(),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      se = sqrt(mover_rate * (1 - mover_rate) / n),
      ci_lo = pmax(0, mover_rate - 1.96 * se),
      ci_hi = pmin(1, mover_rate + 1.96 * se)
    )
}

plot_tc_summary <- function(
    summary_df,
    title,
    subtitle,
    png_path,
    csv_path
) {
  p <- ggplot2::ggplot(
    summary_df,
    ggplot2::aes(
      x = year,
      y = mover_rate,
      color = tc_group,
      fill = tc_group,
      group = tc_group
    )
  ) +
    ggplot2::geom_ribbon(
      ggplot2::aes(ymin = ci_lo, ymax = ci_hi),
      alpha = 0.15,
      color = NA
    ) +
    ggplot2::geom_line(linewidth = 0.9) +
    ggplot2::geom_point(size = 1.7) +
    ggplot2::geom_vline(
      xintercept = 2017.5,
      linetype = "dashed",
      color = "grey30"
    ) +
    ggplot2::scale_color_manual(
      name = NULL,
      values = c("Control" = "#23373b", "Treated" = "#eb811b")
    ) +
    ggplot2::scale_fill_manual(
      guide = "none",
      values = c("Control" = "#23373b", "Treated" = "#eb811b")
    ) +
    ggplot2::scale_x_continuous(breaks = scales::pretty_breaks()) +
    ggplot2::scale_y_continuous(
      labels = scales::percent_format(accuracy = 0.1)
    ) +
    ggplot2::labs(
      x = NULL,
      y = "Mover rate",
      title = title,
      subtitle = subtitle
    ) +
    ggplot2::theme_minimal(base_size = 11) +
    ggplot2::theme(
      panel.grid.minor = ggplot2::element_blank(),
      legend.position = "right"
    )

  ggplot2::ggsave(png_path, p, width = 8.5, height = 5.5, dpi = 150)
  utils::write.csv(summary_df, csv_path, row.names = FALSE)
}

# --- MFJ (marst = 1): control q-2,q-1 vs treated q+1,q+2 --------------------

P_ANCHOR_MFJ <- 0.707482
BAND_W_MFJ   <- 0.03

con_acs <- DBI::dbConnect(
  duckdb::duckdb(),
  dbdir = DB_PATH,
  read_only = TRUE
)

control_cols <- paste(CONTROL_VARS, collapse = ", ")
mfj_sql <- glue::glue("
  SELECT
    year,
    statefip,
    migrate1,
    CAST(imp_itemizable_2017 AS DOUBLE) AS imp_itemizable_2017,
    {control_cols}
  FROM acs_microdata
  WHERE pernum = 1
    AND gq IN (1, 2, 5)
    AND marst = 1
    AND migrate1 != 0
    AND imp_itemizable_2017 IS NOT NULL
")

acs_mfj <- DBI::dbGetQuery(con_acs, mfj_sql) |>
  tibble::as_tibble() |>
  prepare_controls(CONTROL_VARS) |>
  dplyr::mutate(
    year = as.integer(year),
    statefip = as.integer(statefip),
    mover = as.integer(migrate1 %in% c(2L, 3L, 4L)),
    pctl = dplyr::percent_rank(imp_itemizable_2017),
    group = dplyr::case_when(
      pctl >= (P_ANCHOR_MFJ - 5 * BAND_W_MFJ) &
        pctl < (P_ANCHOR_MFJ - 4 * BAND_W_MFJ) ~ "q-5",
      pctl >= (P_ANCHOR_MFJ - 4 * BAND_W_MFJ) &
        pctl < (P_ANCHOR_MFJ - 3 * BAND_W_MFJ) ~ "q-4",
      pctl >= (P_ANCHOR_MFJ - 3 * BAND_W_MFJ) &
        pctl < (P_ANCHOR_MFJ - 2 * BAND_W_MFJ) ~ "q-3",
      pctl >= (P_ANCHOR_MFJ - 2 * BAND_W_MFJ) &
        pctl < (P_ANCHOR_MFJ - 1 * BAND_W_MFJ) ~ "q-2",
      pctl >= (P_ANCHOR_MFJ - 1 * BAND_W_MFJ) &
        pctl < P_ANCHOR_MFJ ~ "q-1",
      pctl >= P_ANCHOR_MFJ &
        pctl < (P_ANCHOR_MFJ + 1 * BAND_W_MFJ) ~ "q+1",
      pctl >= (P_ANCHOR_MFJ + 1 * BAND_W_MFJ) &
        pctl < (P_ANCHOR_MFJ + 2 * BAND_W_MFJ) ~ "q+2",
      pctl >= (P_ANCHOR_MFJ + 2 * BAND_W_MFJ) &
        pctl < (P_ANCHOR_MFJ + 3 * BAND_W_MFJ) ~ "q+3",
      pctl >= (P_ANCHOR_MFJ + 3 * BAND_W_MFJ) &
        pctl < (P_ANCHOR_MFJ + 4 * BAND_W_MFJ) ~ "q+4",
      pctl >= (P_ANCHOR_MFJ + 4 * BAND_W_MFJ) &
        pctl < (P_ANCHOR_MFJ + 5 * BAND_W_MFJ) ~ "q+5",
      TRUE ~ NA_character_
    )
  ) |>
  dplyr::filter(!is.na(group))

mfj_bin_summary <- acs_mfj |>
  dplyr::group_by(year, group) |>
  dplyr::summarise(
    mover_rate = mean(mover, na.rm = TRUE),
    n = dplyr::n(),
    .groups = "drop"
  ) |>
  dplyr::mutate(
    se = sqrt(mover_rate * (1 - mover_rate) / n),
    ci_lo = pmax(0, mover_rate - 1.96 * se),
    ci_hi = pmin(1, mover_rate + 1.96 * se)
  )

utils::write.csv(
  mfj_bin_summary,
  file.path(OUTPUT_DIR, "fig2_mfj_bin_summary.csv"),
  row.names = FALSE
)

mfj_tc_summary <- summarize_tc_by_year(
  acs_mfj,
  control_bands = c("q-2", "q-1"),
  treat_bands = c("q+1", "q+2")
)

plot_tc_summary(
  mfj_tc_summary,
  title = "MFJ mover rate: treatment vs control (TWFE sample)",
  subtitle = paste(
    "Control: q-2, q-1; treated: q+1, q+2;",
    "bands 3pp wide at 70.7th pct anchor; 95% CIs"
  ),
  png_path = file.path(OUTPUT_DIR, "fig2_mfj_treatment_control.png"),
  csv_path = file.path(OUTPUT_DIR, "fig2_mfj_treatment_control_summary.csv")
)

# --- Singles: control q-2,q-1 vs treated q+1..q+4 -------------------------

STD_DED_SINGLE_2017 <- 6350L
BAND_W_SINGLE       <- 0.03

singles_sql <- glue::glue("
  SELECT
    year,
    statefip,
    migrate1,
    CAST(imp_itemizable_2017 AS DOUBLE) AS imp_itemizable_2017,
    {control_cols}
  FROM acs_microdata
  WHERE pernum = 1
    AND gq IN (1, 2, 5)
    AND marst != 1
    AND COALESCE(nchild, 0) = 0
    AND migrate1 != 0
    AND imp_itemizable_2017 IS NOT NULL
")

acs_singles <- DBI::dbGetQuery(con_acs, singles_sql) |>
  tibble::as_tibble() |>
  prepare_controls(CONTROL_VARS)

DBI::dbDisconnect(con_acs, shutdown = TRUE)

s2017 <- acs_singles |> dplyr::filter(year == 2017L)
P_ANCHOR_SINGLE <- mean(
  s2017$imp_itemizable_2017 <= STD_DED_SINGLE_2017,
  na.rm = TRUE
)

band_bounds <- function(k) {
  lo <- P_ANCHOR_SINGLE - k * BAND_W_SINGLE
  hi <- P_ANCHOR_SINGLE - (k - 1L) * BAND_W_SINGLE
  c(lo, hi)
}

acs_singles <- acs_singles |>
  dplyr::group_by(year) |>
  dplyr::mutate(pctl = dplyr::percent_rank(imp_itemizable_2017)) |>
  dplyr::ungroup() |>
  dplyr::mutate(
    year = as.integer(year),
    statefip = as.integer(statefip),
    mover = as.integer(migrate1 %in% c(2L, 3L, 4L)),
    group = dplyr::case_when(
      pctl >= band_bounds(4)[1] & pctl < band_bounds(4)[2] ~ "q-4",
      pctl >= band_bounds(3)[1] & pctl < band_bounds(3)[2] ~ "q-3",
      pctl >= band_bounds(2)[1] & pctl < band_bounds(2)[2] ~ "q-2",
      pctl >= band_bounds(1)[1] & pctl < band_bounds(1)[2] ~ "q-1",
      pctl >= P_ANCHOR_SINGLE &
        pctl < P_ANCHOR_SINGLE + 1 * BAND_W_SINGLE ~ "q+1",
      pctl >= P_ANCHOR_SINGLE + 1 * BAND_W_SINGLE &
        pctl < P_ANCHOR_SINGLE + 2 * BAND_W_SINGLE ~ "q+2",
      pctl >= P_ANCHOR_SINGLE + 2 * BAND_W_SINGLE &
        pctl < P_ANCHOR_SINGLE + 3 * BAND_W_SINGLE ~ "q+3",
      pctl >= P_ANCHOR_SINGLE + 3 * BAND_W_SINGLE &
        pctl < P_ANCHOR_SINGLE + 4 * BAND_W_SINGLE ~ "q+4",
      TRUE ~ NA_character_
    )
  ) |>
  dplyr::filter(!is.na(group))

singles_bin_summary <- acs_singles |>
  dplyr::group_by(year, group) |>
  dplyr::summarise(
    mover_rate = mean(mover, na.rm = TRUE),
    n = dplyr::n(),
    .groups = "drop"
  ) |>
  dplyr::mutate(
    se = sqrt(mover_rate * (1 - mover_rate) / n),
    ci_lo = pmax(0, mover_rate - 1.96 * se),
    ci_hi = pmin(1, mover_rate + 1.96 * se)
  )

utils::write.csv(
  singles_bin_summary,
  file.path(OUTPUT_DIR, "fig2_singles_bin_summary.csv"),
  row.names = FALSE
)

singles_tc_summary <- summarize_tc_by_year(
  acs_singles,
  control_bands = c("q-2", "q-1"),
  treat_bands = c("q+1", "q+2", "q+3", "q+4")
)

plot_tc_summary(
  singles_tc_summary,
  title = "Single filer mover rate: treatment vs control (TWFE sample)",
  subtitle = glue::glue(
    "Control: q-2, q-1; treated: q+1..q+4; ",
    "anchor {scales::percent(P_ANCHOR_SINGLE, 0.1)}; 95% CIs"
  ),
  png_path = file.path(OUTPUT_DIR, "fig2_singles_treatment_control.png"),
  csv_path = file.path(
    OUTPUT_DIR,
    "fig2_singles_treatment_control_summary.csv"
  )
)
