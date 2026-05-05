# Clean and load
gc()
rm(list = ls())

DB_PATH_RETURNS <- "../../../data/database.db"
DB_PATH_FINANCE <- "../../../data/census_finances_test.db"
OUTPUT_DIR <- "../../../output/local_govt"

if (!dir.exists(OUTPUT_DIR)) {
  dir.create(OUTPUT_DIR, recursive = TRUE)
}

con_returns <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB_PATH_RETURNS)
con_finance <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB_PATH_FINANCE)

county_returns_agi <- DBI::dbGetQuery(
  con_returns,
  "SELECT * FROM returns_county_agi"
)

govt_finances <- DBI::dbGetQuery(
  con_finance,
  "SELECT * FROM govt_finances_county"
)

DBI::dbDisconnect(con_returns, shutdown = TRUE)
DBI::dbDisconnect(con_finance, shutdown = TRUE)

# Match the exposure construction used in migration/county_to_county.R.
county_returns_agi <- county_returns_agi |>
  dplyr::mutate(
    year = as.integer(year),
    county = as.integer(county),
    w_mfj = joint_returns / returns,
    w_single = single_returns / returns,
    w_hoh = head_of_household_returns / returns,
    mtr_single = dplyr::case_when(
      agi_stub == 1 ~ 0.00,
      agi_stub == 3 ~ 0.05,
      agi_stub == 4 ~ 0.22,
      agi_stub == 5 ~ 0.225,
      agi_stub == 6 ~ 0.25,
      agi_stub == 8 ~ 0.365,
      .default = NA_real_
    ),
    mtr_mfj = dplyr::case_when(
      agi_stub == 1 ~ 0.00,
      agi_stub == 3 ~ 0.05,
      agi_stub == 4 ~ 0.12,
      agi_stub == 5 ~ 0.22,
      agi_stub == 6 ~ 0.24,
      agi_stub == 8 ~ 0.36,
      .default = NA_real_
    ),
    mtr_hoh = dplyr::case_when(
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
  dplyr::reframe(
    salt_pp = sum(taxes_paid_amount, na.rm = TRUE) / sum(returns, na.rm = TRUE),
    salt_refund_pp = sum(salt_savings, na.rm = TRUE) / sum(returns, na.rm = TRUE),
    effective_salt_pp = salt_pp - salt_refund_pp,
    .by = c(county, year)
  )

salt_exposure <- salt_per_county_year |>
  dplyr::filter(year %in% c(2017L, 2018L)) |>
  dplyr::reframe(
    exposure_1718 = effective_salt_pp[year == 2018L] -
      effective_salt_pp[year == 2017L],
    effective_salt_pp_2017 = effective_salt_pp[year == 2017L],
    .by = county
  ) |>
  dplyr::filter(!is.na(exposure_1718))

analysis_df <- govt_finances |>
  dplyr::mutate(
    year = as.integer(year),
    county_fips = as.integer(county_fips),
    state_fips = as.integer(state_fips)
  ) |>
  dplyr::inner_join(
    salt_exposure,
    by = c("county_fips" = "county")
  ) |>
  dplyr::mutate(
    log_total_tax_rev = log1p(total_tax_rev),
    log_property_tax_rev = log1p(property_tax_rev)
  ) |>
  dplyr::filter(!is.na(log_total_tax_rev))

spec_formulas <- list(
  spec_state_year = paste(
    "log_total_tax_rev ~ i(year, exposure_1718, ref = 2017) - 1",
    "| county_fips + state_fips^year"
  )
)

run_spec <- function(formula_text, data_frame) {
  fit <- fixest::feols(
    stats::as.formula(formula_text),
    data = data_frame,
    cluster = ~county_fips
  )
  fit
}

model_fits <- purrr::imap(spec_formulas, ~ run_spec(.x, analysis_df))

fixest::etable(
  model_fits$spec_state_year,
  tex = FALSE,
  file = file.path(OUTPUT_DIR, "tbl_effective_salt_year_interactions.txt")
)

coef_plot_data <- purrr::imap_dfr(
  model_fits,
  function(model_fit, model_name) {
    broom::tidy(model_fit, conf.int = TRUE) |>
      dplyr::filter(stringr::str_detect(term, "^year::.*:exposure_1718$")) |>
      dplyr::mutate(
        year = as.integer(stringr::str_extract(term, "(?<=year::)\\d+")),
        model = model_name
      )
  }
)

coef_ref <- tibble::tibble(
  year = 2017L,
  estimate = 0,
  conf.low = 0,
  conf.high = 0
)

coef_plot_data <- coef_plot_data |>
  dplyr::bind_rows(
    tidyr::expand_grid(model = names(model_fits), coef_ref)
  ) |>
  dplyr::mutate(
    model_label = dplyr::case_match(
      model,
      "spec_state_year" ~ "County + state-year FE",
      .default = model
    )
  )

ggplot2::ggplot(
  coef_plot_data,
  ggplot2::aes(
    x = year,
    y = estimate,
    ymin = conf.low,
    ymax = conf.high,
    color = model_label
  )
) +
  ggplot2::geom_hline(yintercept = 0, linetype = "dashed", color = "grey50") +
  ggplot2::geom_vline(xintercept = 2017.5, linetype = "dashed", color = "grey30") +
  ggplot2::geom_errorbar(width = 0.15) +
  ggplot2::geom_point() +
  ggplot2::scale_color_manual(
    values = c(
      "County + state-year FE" = "#23373b"
    )
  ) +
  ggplot2::scale_x_continuous(breaks = seq(2012, 2022, by = 2)) +
  ggplot2::coord_cartesian(ylim = c(-0.08, 0.05)) +
  ggplot2::labs(
    x = "",
    y = "Percent change in local revenue",
    color = "Specification"
  ) +
  ggplot2::theme(
    panel.background = ggplot2::element_rect(fill = "#fafafa", color = NA),
    plot.background = ggplot2::element_rect(fill = "#fafafa", color = NA),
    panel.grid.major = ggplot2::element_blank(),
    panel.grid.minor = ggplot2::element_blank(),
    axis.line.x = ggplot2::element_line(color = "grey30"),
    axis.line.y = ggplot2::element_line(color = "grey30"),
    legend.position = "none",
    legend.background = ggplot2::element_rect(fill = "#fafafa", color = NA),
    legend.box.background = ggplot2::element_rect(fill = "#fafafa", color = NA),
    legend.key = ggplot2::element_rect(fill = "#fafafa", color = NA),
    axis.text.x = ggplot2::element_text(color = "grey20"),
    axis.text.y = ggplot2::element_text(color = "grey20"),
    text = ggplot2::element_text(color = "grey20")
  )

ggplot2::ggsave(
  file.path(OUTPUT_DIR, "fig_effective_salt_year_interactions_total_tax.png"),
  width = 18,
  height = 12,
  units = "cm"
)

readr::write_csv(
  coef_plot_data |>
    dplyr::arrange(model_label, year),
  file.path(OUTPUT_DIR, "coef_effective_salt_year_interactions_total_tax.csv")
)

saveRDS(
  model_fits,
  file.path(OUTPUT_DIR, "models_effective_salt_year_interactions_total_tax.rds")
)

# Differential impacts by revenue source with county and state-year fixed effects.
source_outcomes <- c(
  "property_tax_rev",
  "federal_ig_rev",
  "state_ig_rev"
)

source_formulas <- purrr::set_names(
  source_outcomes,
  source_outcomes
) |>
  purrr::map(
    ~ paste0(
      "log1p(", .x, ") ~ i(year, exposure_1718, ref = 2017) - 1",
      " | county_fips + state_fips^year"
    )
  )

source_model_fits <- purrr::imap(
  source_formulas,
  ~ run_spec(.x, analysis_df)
)

fixest::etable(
  source_model_fits$property_tax_rev,
  source_model_fits$federal_ig_rev,
  source_model_fits$state_ig_rev,
  tex = FALSE,
  file = file.path(
    OUTPUT_DIR,
    "tbl_effective_salt_by_revenue_source_county_state_year.txt"
  )
)

source_coef_data <- purrr::imap_dfr(
  source_model_fits,
  function(model_fit, source_name) {
    broom::tidy(model_fit, conf.int = TRUE) |>
      dplyr::filter(stringr::str_detect(term, "^year::.*:exposure_1718$")) |>
      dplyr::mutate(
        year = as.integer(stringr::str_extract(term, "(?<=year::)\\d+")),
        source = dplyr::case_match(
          source_name,
          "federal_ig_rev" ~ "Federal IG revenue",
          "state_ig_rev" ~ "State IG revenue",
          "property_tax_rev" ~ "Property tax revenue",
          .default = source_name
        )
      )
  }
)

source_coef_data <- source_coef_data |>
  dplyr::bind_rows(
    tidyr::expand_grid(
      source = c(
        "Federal IG revenue",
        "State IG revenue",
        "Property tax revenue"
      ),
      coef_ref
    )
  ) |>
  dplyr::mutate(
    source = factor(
      source,
      levels = c(
        "Federal IG revenue",
        "State IG revenue",
        "Property tax revenue"
      )
    )
  )

ggplot2::ggplot(
  source_coef_data,
  ggplot2::aes(
    x = year,
    y = estimate,
    ymin = conf.low,
    ymax = conf.high,
    color = source
  )
) +
  ggplot2::geom_hline(yintercept = 0, linetype = "dashed", color = "grey50") +
  ggplot2::geom_vline(xintercept = 2017.5, linetype = "dashed", color = "grey30") +
  ggplot2::geom_errorbar(
    width = 0.15,
    position = ggplot2::position_dodge(width = 0.4)
  ) +
  ggplot2::geom_point(position = ggplot2::position_dodge(width = 0.4)) +
  ggplot2::scale_color_manual(
    values = c(
      "Federal IG revenue" = "#23373b",
      "State IG revenue" = "#14B03D",
      "Property tax revenue" = "#eb811b"
    )
  ) +
  ggplot2::scale_x_continuous(breaks = seq(2012, 2022, by = 2)) +
  ggplot2::labs(
    x = "",
    y = "Percent change in local revenue",
    color = "Revenue source"
  ) +
  ggplot2::theme(
    panel.background = ggplot2::element_rect(fill = "#fafafa", color = NA),
    plot.background = ggplot2::element_rect(fill = "#fafafa", color = NA),
    panel.grid.major = ggplot2::element_blank(),
    panel.grid.minor = ggplot2::element_blank(),
    axis.line.x = ggplot2::element_line(color = "grey30"),
    axis.line.y = ggplot2::element_line(color = "grey30"),
    legend.position = "bottom",
    legend.background = ggplot2::element_rect(fill = "#fafafa", color = NA),
    legend.box.background = ggplot2::element_rect(fill = "#fafafa", color = NA),
    legend.key = ggplot2::element_rect(fill = "#fafafa", color = NA),
    axis.text.x = ggplot2::element_text(color = "grey20"),
    axis.text.y = ggplot2::element_text(color = "grey20"),
    text = ggplot2::element_text(color = "grey20")
  )

ggplot2::ggsave(
  file.path(OUTPUT_DIR, "fig_effective_salt_year_interactions_revenue_sources.png"),
  width = 18,
  height = 12,
  units = "cm"
)

readr::write_csv(
  source_coef_data |>
    dplyr::arrange(source, year),
  file.path(
    OUTPUT_DIR,
    "coef_effective_salt_by_revenue_source_county_state_year.csv"
  )
)

saveRDS(
  source_model_fits,
  file.path(
    OUTPUT_DIR,
    "models_effective_salt_by_revenue_source_county_state_year.rds"
  )
)

message("Saved outputs to: ", OUTPUT_DIR)
