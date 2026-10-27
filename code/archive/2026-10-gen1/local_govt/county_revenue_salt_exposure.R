# Clean and load
gc()
rm(list = ls())

DB_PATH_RETURNS <- "../../../data/database.db"
DB_PATH_FINANCE <- "../../../data/census_finances_test.db"
OUTPUT_DIR <- "../../../output/analysis/local_govt/county_revenue_salt_exposure"

if (!dir.exists(OUTPUT_DIR)) dir.create(OUTPUT_DIR, recursive = TRUE)

con_returns <- DBI::dbConnect(duckdb::duckdb(),
                              dbdir = DB_PATH_RETURNS,
                              read_only = TRUE)
con_finance <- DBI::dbConnect(duckdb::duckdb(),
                              dbdir = DB_PATH_FINANCE,
                              read_only = TRUE)

county_returns_agi <- DBI::dbGetQuery(con_returns,
                                      "SELECT * FROM returns_county_agi")
govt_finances_co <- DBI::dbGetQuery(con_finance,
                                    "SELECT * FROM govt_finances_county")
govt_finances_ca <- DBI::dbGetQuery(con_finance,
                                    "SELECT * FROM govt_finances_county_area")
govt_finances_sd <- DBI::dbGetQuery(con_finance,
                                    "SELECT * FROM govt_finances_sd_by_county")

DBI::dbDisconnect(con_returns, shutdown = TRUE)
DBI::dbDisconnect(con_finance, shutdown = TRUE)

# ---- Build county-level effective-SALT exposure (2017 -> 2018) ----
county_returns_agi <- county_returns_agi |>
  dplyr::mutate(
    year = as.integer(year),
    county = as.integer(county),
    w_mfj = joint_returns / returns,
    w_single = single_returns / returns,
    w_hoh = head_of_household_returns / returns,
    mtr_single = dplyr::case_when(
      agi_stub == 1 ~ 0.00, agi_stub == 3 ~ 0.05, agi_stub == 4 ~ 0.22,
      agi_stub == 5 ~ 0.225, agi_stub == 6 ~ 0.25, agi_stub == 8 ~ 0.365,
      .default = NA_real_
    ),
    mtr_mfj = dplyr::case_when(
      agi_stub == 1 ~ 0.00, agi_stub == 3 ~ 0.05, agi_stub == 4 ~ 0.12,
      agi_stub == 5 ~ 0.22, agi_stub == 6 ~ 0.24, agi_stub == 8 ~ 0.36,
      .default = NA_real_
    ),
    mtr_hoh = dplyr::case_when(
      agi_stub == 1 ~ 0.00, agi_stub == 3 ~ 0.06, agi_stub == 4 ~ 0.14,
      agi_stub == 5 ~ 0.22, agi_stub == 6 ~ 0.25, agi_stub == 8 ~ 0.365,
      .default = NA_real_
    ),
    mtr_weighted = w_mfj * mtr_mfj + w_single * mtr_single + w_hoh * mtr_hoh,
    salt_savings = taxes_paid_amount * mtr_weighted
  )

salt_per_county_year <- county_returns_agi |>
  dplyr::reframe(
    salt_pp = sum(taxes_paid_amount, na.rm = TRUE) /
      sum(returns, na.rm = TRUE),
    salt_refund_pp = sum(salt_savings, na.rm = TRUE) /
      sum(returns, na.rm = TRUE),
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

# ---- Helpers ----
# Census-of-Governments years: full enumeration of all local governments.
# Annual-survey years (everything else 2012-2022) sample only a fraction of
# municipal/township/special/school-district units, so summing across govt
# types produces a mechanically smaller county-area total in those years.
# County-only outcomes are unaffected (counties are in the certainty stratum
# and surveyed every year). The county-area regressions therefore restrict
# to COG years; county-only regressions use all years.
COG_YEARS <- c(2012L, 2017L, 2022L)

prepare_finance_df <- function(df, restrict_years = NULL) {
  out <- df |>
    dplyr::mutate(
      year = as.integer(year),
      county_fips = as.integer(county_fips),
      state_fips = as.integer(state_fips)
    ) |>
    dplyr::inner_join(salt_exposure,
                      by = c("county_fips" = "county"))
  if (!is.null(restrict_years)) {
    out <- dplyr::filter(out, year %in% restrict_years)
  }
  out
}

run_event_study <- function(data_frame, outcome_col, exclude_years = NULL) {
  data_use <- data_frame |>
    dplyr::mutate(.y = log1p(.data[[outcome_col]])) |>
    dplyr::filter(!is.na(.y), is.finite(.y))
  if (!is.null(exclude_years)) {
    data_use <- dplyr::filter(data_use, !year %in% exclude_years)
  }
  if (nrow(data_use) < 200L) return(NULL)
  fit <- fixest::feols(
    .y ~ i(year, exposure_1718, ref = 2017) - 1 |
      county_fips + state_fips^year,
    data = data_use,
    cluster = ~county_fips
  )
  broom::tidy(fit, conf.int = TRUE) |>
    dplyr::filter(stringr::str_detect(term,
                                      "^year::.*:exposure_1718$")) |>
    dplyr::mutate(
      year = as.integer(stringr::str_extract(term,
                                             "(?<=year::)[0-9]+")),
      outcome = outcome_col
    )
}

# Per-outcome year exclusions for items where the 2022 ALFIN public-use file
# dropped or restructured the underlying item codes:
#   - cash_securities: W codes (W01/W31/W61) are not reported in 2022,
#     so the 2022 value is mechanically zero and the coefficient is spurious.
OUTCOME_EXCLUDE_YEARS <- list(
  cash_securities = 2022L
)

run_outcome_group <- function(data_frame, outcome_cols, panel_label) {
  res <- purrr::map(outcome_cols, function(oc) {
    run_event_study(data_frame, oc,
                    exclude_years = OUTCOME_EXCLUDE_YEARS[[oc]])
  }) |>
    purrr::compact() |>
    dplyr::bind_rows()
  if (nrow(res) == 0) return(tibble::tibble())
  # Add 2017 reference at zero for every outcome.
  ref <- tidyr::expand_grid(
    outcome = unique(res$outcome),
    year = 2017L,
    estimate = 0, std.error = NA_real_,
    conf.low = 0, conf.high = 0
  )
  dplyr::bind_rows(res, ref) |>
    dplyr::mutate(panel = panel_label)
}

# Outcome groups -- each entry is c(column_name = "display label")
spending_outcomes <- c(
  total_expenditure          = "Total expenditure",
  total_capital_outlay       = "Capital outlay (F+G)",
  amount_e12                 = "Elementary & secondary education",
  amount_e62                 = "Police protection",
  amount_e44                 = "Regular highways",
  amount_welfare_total       = "Welfare (E74+E75+E77+E79+J67+J68)"
)

substitution_outcomes <- c(
  amount_u20 = "Hospital current charges",
  amount_u30 = "Sewerage charges",
  amount_t13 = "Motor-fuel sales tax",
  amount_t15 = "Public-utilities tax",
  amount_t19 = "Other selective-sales tax"
)

balance_outcomes <- c(
  lt_debt_outstanding = "Long-term debt outstanding",
  lt_debt_issued      = "Long-term debt issued",
  cash_securities     = "Cash & securities"
)

# Add the welfare-total composite to both finance frames.
add_welfare_total <- function(df) {
  parts <- intersect(c("amount_e74", "amount_e75", "amount_e77",
                       "amount_e79", "amount_j67", "amount_j68"),
                     names(df))
  if (length(parts) == 0) {
    df$amount_welfare_total <- NA_real_
    return(df)
  }
  df$amount_welfare_total <- rowSums(df[parts], na.rm = TRUE)
  df$amount_welfare_total[df$amount_welfare_total == 0] <- NA_real_
  df
}

govt_finances_co <- add_welfare_total(govt_finances_co)
govt_finances_ca <- add_welfare_total(govt_finances_ca)

df_co <- prepare_finance_df(govt_finances_co)
df_ca <- prepare_finance_df(govt_finances_ca, restrict_years = COG_YEARS)

# ---- Headline regressions (preserve existing outputs) ----
headline_outcomes <- c(
  total_tax_rev    = "Total tax revenue",
  property_tax_rev = "Property tax revenue",
  federal_ig_rev   = "Federal IG revenue",
  state_ig_rev     = "State IG revenue"
)

headline_co <- run_outcome_group(df_co, names(headline_outcomes),
                                 "County government only")
headline_ca <- run_outcome_group(df_ca, names(headline_outcomes),
                                 "County area, COG years only (2012, 2017, 2022)")
headline_all <- dplyr::bind_rows(headline_co, headline_ca) |>
  dplyr::mutate(
    label = dplyr::recode(outcome, !!!headline_outcomes),
    label = factor(label, levels = unname(headline_outcomes))
  )

readr::write_csv(
  headline_all |> dplyr::arrange(panel, label, year),
  file.path(OUTPUT_DIR,
            "coef_effective_salt_by_revenue_source_county_state_year.csv")
)

# Keep the legacy single-outcome (total_tax_rev) CSV for downstream readers.
readr::write_csv(
  headline_co |>
    dplyr::filter(outcome == "total_tax_rev") |>
    dplyr::mutate(model = "spec_state_year",
                  model_label = "County + state-year FE") |>
    dplyr::select(term, estimate, std.error, statistic, p.value,
                  conf.low, conf.high, year, model, model_label),
  file.path(OUTPUT_DIR,
            "coef_effective_salt_year_interactions_total_tax.csv")
)

# ---- Plot helper ----
shared_theme <- ggplot2::theme(
  panel.background = ggplot2::element_rect(fill = "#fafafa", color = NA),
  plot.background = ggplot2::element_rect(fill = "#fafafa", color = NA),
  strip.background = ggplot2::element_rect(fill = "#fafafa", color = NA),
  panel.grid.major = ggplot2::element_blank(),
  panel.grid.minor = ggplot2::element_blank(),
  axis.line.x = ggplot2::element_line(color = "grey30"),
  axis.line.y = ggplot2::element_line(color = "grey30"),
  axis.text.x = ggplot2::element_text(color = "grey20"),
  axis.text.y = ggplot2::element_text(color = "grey20"),
  text = ggplot2::element_text(color = "grey20"),
  legend.position = "bottom",
  legend.background = ggplot2::element_rect(fill = "#fafafa", color = NA),
  legend.box.background = ggplot2::element_rect(fill = "#fafafa", color = NA),
  legend.key = ggplot2::element_rect(fill = "#fafafa", color = NA)
)

plot_event_study <- function(coef_df, outcome_labels, fname,
                             y_label = "Coefficient",
                             width_cm = 22, height_cm = 14) {
  coef_df <- coef_df |>
    dplyr::mutate(
      label = dplyr::recode(outcome, !!!outcome_labels),
      label = factor(label, levels = unname(outcome_labels))
    )
  p <- ggplot2::ggplot(coef_df,
    ggplot2::aes(x = year, y = estimate,
                 ymin = conf.low, ymax = conf.high,
                 color = label)) +
    ggplot2::geom_hline(yintercept = 0, linetype = "dashed",
                        color = "grey50") +
    ggplot2::geom_vline(xintercept = 2017.5, linetype = "dashed",
                        color = "grey30") +
    ggplot2::geom_errorbar(width = 0.15,
                           position = ggplot2::position_dodge(width = 0.4)) +
    ggplot2::geom_point(position = ggplot2::position_dodge(width = 0.4)) +
    ggplot2::scale_x_continuous(breaks = seq(2012, 2022, by = 2)) +
    ggplot2::facet_wrap(~ panel, ncol = 2) +
    ggplot2::labs(x = "", y = y_label, color = NULL) +
    shared_theme
  ggplot2::ggsave(file.path(OUTPUT_DIR, fname),
                  p, width = width_cm, height = height_cm, units = "cm")
  invisible(p)
}

# ---- Headline event-study (revenue sources) -- two panels ----
plot_event_study(
  headline_all, headline_outcomes,
  fname = "fig_effective_salt_year_interactions_revenue_sources.png",
  y_label = "Percent change in local revenue"
)

# Backwards-compatible: also output the total-tax single-outcome figure.
plot_event_study(
  headline_co |> dplyr::filter(outcome == "total_tax_rev"),
  headline_outcomes,
  fname = "fig_effective_salt_year_interactions_total_tax.png",
  y_label = "Percent change in local revenue",
  width_cm = 18, height_cm = 12
)

# ---- Property-tax-only figure: log(y), no +1 floor; three panels ----
# Drop county-years with non-positive property tax so log() is well-defined.
df_co_pt <- df_co |> dplyr::filter(property_tax_rev > 0)
df_ca_pt <- df_ca |> dplyr::filter(property_tax_rev > 0)
df_sd_pt <- prepare_finance_df(govt_finances_sd) |>
  dplyr::filter(property_tax_rev > 0)

run_pt_spec <- function(df, fe_rhs, panel_label, spec_label) {
  data_use <- df |>
    dplyr::mutate(.y = log(property_tax_rev)) |>
    dplyr::filter(is.finite(.y))
  fit <- fixest::feols(
    stats::as.formula(paste(".y ~ i(year, exposure_1718, ref = 2017) - 1 |",
                            fe_rhs)),
    data = data_use, cluster = ~county_fips
  )
  res <- broom::tidy(fit, conf.int = TRUE) |>
    dplyr::filter(stringr::str_detect(term, "^year::.*:exposure_1718$")) |>
    dplyr::mutate(year = as.integer(stringr::str_extract(term,
                                                         "(?<=year::)[0-9]+")))
  ref <- tibble::tibble(year = 2017L, estimate = 0,
                        std.error = NA_real_, conf.low = 0, conf.high = 0)
  dplyr::bind_rows(res, ref) |>
    dplyr::mutate(panel = panel_label, spec = spec_label)
}

pt_coefs <- dplyr::bind_rows(
  run_pt_spec(df_co_pt, "county_fips + state_fips^year",
              "County government only", "County + state-year FE"),
  run_pt_spec(df_ca_pt, "county_fips + state_fips^year",
              "County area, COG years only (2012, 2017, 2022)",
              "County + state-year FE"),
  run_pt_spec(df_sd_pt, "county_fips + state_fips^year",
              "Independent school districts, summed to county",
              "County + state-year FE")
) |>
  dplyr::mutate(panel = factor(panel, levels = c(
    "County government only",
    "Independent school districts, summed to county",
    "County area, COG years only (2012, 2017, 2022)"
  )))

readr::write_csv(
  pt_coefs |> dplyr::arrange(panel, year),
  file.path(OUTPUT_DIR, "coef_property_tax_response.csv")
)

p_pt <- ggplot2::ggplot(pt_coefs,
  ggplot2::aes(x = year, y = estimate,
               ymin = conf.low, ymax = conf.high)) +
  ggplot2::geom_hline(yintercept = 0, linetype = "dashed", color = "grey50") +
  ggplot2::geom_vline(xintercept = 2017.5, linetype = "dashed", color = "grey30") +
  ggplot2::geom_errorbar(width = 0.15, color = "#23373b") +
  ggplot2::geom_point(color = "#23373b") +
  ggplot2::scale_x_continuous(breaks = seq(2012, 2022, by = 2)) +
  ggplot2::facet_wrap(~ panel, ncol = 3) +
  ggplot2::labs(x = "", y = "Percent change in property tax revenue") +
  shared_theme
ggplot2::ggsave(file.path(OUTPUT_DIR, "fig_property_tax_response.png"),
                p_pt, width = 28, height = 12, units = "cm")

# ---- Spending response ----
spending_co <- run_outcome_group(df_co, names(spending_outcomes),
                                 "County government only")
spending_ca <- run_outcome_group(df_ca, names(spending_outcomes),
                                 "County area, COG years only (2012, 2017, 2022)")
spending_all <- dplyr::bind_rows(spending_co, spending_ca)

plot_event_study(
  spending_all, spending_outcomes,
  fname = "fig_expenditure_response.png",
  y_label = "Percent change in local spending",
  height_cm = 16
)
readr::write_csv(
  spending_all |> dplyr::arrange(panel, outcome, year),
  file.path(OUTPUT_DIR, "coef_expenditure_response.csv")
)

# ---- Revenue substitution (charges / non-T01-T09-T40 taxes) ----
sub_co <- run_outcome_group(df_co, names(substitution_outcomes),
                            "County government only")
sub_ca <- run_outcome_group(df_ca, names(substitution_outcomes),
                            "County area, COG years only (2012, 2017, 2022)")
sub_all <- dplyr::bind_rows(sub_co, sub_ca)

plot_event_study(
  sub_all, substitution_outcomes,
  fname = "fig_substitute_revenue.png",
  y_label = "Percent change in revenue source",
  height_cm = 16
)
readr::write_csv(
  sub_all |> dplyr::arrange(panel, outcome, year),
  file.path(OUTPUT_DIR, "coef_substitute_revenue.csv")
)

# ---- Balance-sheet response ----
bal_co <- run_outcome_group(df_co, names(balance_outcomes),
                            "County government only")
bal_ca <- run_outcome_group(df_ca, names(balance_outcomes),
                            "County area, COG years only (2012, 2017, 2022)")
bal_all <- dplyr::bind_rows(bal_co, bal_ca)

plot_event_study(
  bal_all, balance_outcomes,
  fname = "fig_balance_sheet.png",
  y_label = "Percent change in balance-sheet stock/flow",
  height_cm = 14
)
readr::write_csv(
  bal_all |> dplyr::arrange(panel, outcome, year),
  file.path(OUTPUT_DIR, "coef_balance_sheet.csv")
)

message("Saved outputs to: ", OUTPUT_DIR)
