# Build a county-year median property-tax-rate panel from CoreLogic, then
# run an event study against the SALT-exposure measure.
gc()
rm(list = ls())

DB_PATH_CORELOGIC <- "../../../data/corelogic/corelogic.db"
DB_PATH_RETURNS   <- "../../../data/database.db"
OUTPUT_DIR        <- "../../../output/analysis/local_govt/county_median_tax_rate"

if (!dir.exists(OUTPUT_DIR)) dir.create(OUTPUT_DIR, recursive = TRUE)

# ---- 1. Aggregate: median(total_tax_amount / calculated_total_value) ----
# By county (fips_code, 5-digit) x tax_year, restricted to records with
# positive tax and positive value, and a sane upper bound on the rate.
con_cl <- DBI::dbConnect(duckdb::duckdb(),
                         dbdir = DB_PATH_CORELOGIC, read_only = TRUE)

agg_sql <- "
SELECT
  fips_code AS county_fips,
  tax_year   AS year,
  COUNT(*)   AS n_parcels,
  median(total_tax_amount / calculated_total_value) AS median_tax_rate
FROM tax
WHERE total_tax_amount > 0
  AND calculated_total_value > 1000
  AND total_tax_amount / calculated_total_value BETWEEN 0 AND 0.10
  AND tax_year BETWEEN 2013 AND 2022
GROUP BY fips_code, tax_year
ORDER BY fips_code, tax_year
"
message("Aggregating CoreLogic tax records...")
tax_panel <- DBI::dbGetQuery(con_cl, agg_sql)
DBI::dbDisconnect(con_cl, shutdown = TRUE)

cat("Tax-rate panel: ", nrow(tax_panel),
    " county-year rows; ", length(unique(tax_panel$county_fips)),
    " distinct counties; years ",
    min(tax_panel$year), "-", max(tax_panel$year), "\n", sep = "")

# Export panel as CSV
readr::write_csv(tax_panel,
                 file.path(OUTPUT_DIR, "median_tax_rate_panel.csv"))
cat("Saved median_tax_rate_panel.csv\n")

# ---- 2. Build SALT exposure (same construction as the headline analysis) ----
con_r <- DBI::dbConnect(duckdb::duckdb(),
                        dbdir = DB_PATH_RETURNS, read_only = TRUE)
returns <- DBI::dbGetQuery(con_r, "SELECT * FROM returns_county_agi")
DBI::dbDisconnect(con_r, shutdown = TRUE)

returns <- returns |>
  dplyr::mutate(
    year = as.integer(year), county = as.integer(county),
    w_mfj = joint_returns / returns,
    w_s   = single_returns / returns,
    w_h   = head_of_household_returns / returns,
    mtr_mfj = dplyr::case_when(
      agi_stub == 1 ~ 0.00, agi_stub == 3 ~ 0.05, agi_stub == 4 ~ 0.12,
      agi_stub == 5 ~ 0.22, agi_stub == 6 ~ 0.24, agi_stub == 8 ~ 0.36,
      .default = NA_real_),
    mtr_single = dplyr::case_when(
      agi_stub == 1 ~ 0.00, agi_stub == 3 ~ 0.05, agi_stub == 4 ~ 0.22,
      agi_stub == 5 ~ 0.225, agi_stub == 6 ~ 0.25, agi_stub == 8 ~ 0.365,
      .default = NA_real_),
    mtr_hoh = dplyr::case_when(
      agi_stub == 1 ~ 0.00, agi_stub == 3 ~ 0.06, agi_stub == 4 ~ 0.14,
      agi_stub == 5 ~ 0.22, agi_stub == 6 ~ 0.25, agi_stub == 8 ~ 0.365,
      .default = NA_real_),
    mtr_w = w_mfj * mtr_mfj + w_s * mtr_single + w_h * mtr_hoh,
    salt_savings = taxes_paid_amount * mtr_w)

salt_yr <- returns |>
  dplyr::reframe(
    eff_salt = (sum(taxes_paid_amount, na.rm = TRUE) -
                  sum(salt_savings, na.rm = TRUE)) /
                sum(returns, na.rm = TRUE),
    .by = c(county, year))

salt_exposure <- salt_yr |>
  dplyr::filter(year %in% c(2017L, 2018L)) |>
  dplyr::reframe(
    exposure_1718 = eff_salt[year == 2018L] - eff_salt[year == 2017L],
    .by = county) |>
  dplyr::filter(!is.na(exposure_1718))

# ---- 3. Merge tax panel with exposure ----
analysis_df <- tax_panel |>
  dplyr::mutate(
    county_fips = as.integer(county_fips),
    year        = as.integer(year),
    state_fips  = county_fips %/% 1000L) |>
  dplyr::inner_join(salt_exposure,
                    by = c("county_fips" = "county")) |>
  dplyr::filter(median_tax_rate > 0,
                is.finite(median_tax_rate))

cat("Analysis sample:", nrow(analysis_df), "county-year rows;",
    length(unique(analysis_df$county_fips)), "counties\n")

# ---- 4. Event study: two specifications ----
run_spec <- function(fe_rhs, spec_label) {
  fit <- fixest::feols(
    stats::as.formula(paste(
      "log(median_tax_rate) ~ i(year, exposure_1718, ref = 2017) - 1 |",
      fe_rhs)),
    data = analysis_df, cluster = ~county_fips)
  res <- broom::tidy(fit, conf.int = TRUE) |>
    dplyr::filter(stringr::str_detect(term, "^year::.*:exposure_1718$")) |>
    dplyr::mutate(year = as.integer(stringr::str_extract(term,
                                                         "(?<=year::)[0-9]+")))
  ref <- tibble::tibble(year = 2017L, estimate = 0,
                        std.error = NA_real_, conf.low = 0, conf.high = 0)
  dplyr::bind_rows(res, ref) |> dplyr::mutate(spec = spec_label)
}

coefs <- dplyr::bind_rows(
  run_spec("county_fips + year", "County + year FE"),
  run_spec("county_fips + state_fips^year", "County + state-year FE")
)

readr::write_csv(coefs |> dplyr::arrange(spec, year),
                 file.path(OUTPUT_DIR, "coef_median_tax_rate.csv"))

# ---- 5. Plot ----
shared_theme <- ggplot2::theme(
  panel.background  = ggplot2::element_rect(fill = "#fafafa", color = NA),
  plot.background   = ggplot2::element_rect(fill = "#fafafa", color = NA),
  strip.background  = ggplot2::element_rect(fill = "#fafafa", color = NA),
  panel.grid.major  = ggplot2::element_blank(),
  panel.grid.minor  = ggplot2::element_blank(),
  axis.line.x       = ggplot2::element_line(color = "grey30"),
  axis.line.y       = ggplot2::element_line(color = "grey30"),
  axis.text         = ggplot2::element_text(color = "grey20"),
  text              = ggplot2::element_text(color = "grey20"),
  legend.position   = "bottom",
  legend.background = ggplot2::element_rect(fill = "#fafafa", color = NA),
  legend.box.background = ggplot2::element_rect(fill = "#fafafa", color = NA),
  legend.key        = ggplot2::element_rect(fill = "#fafafa", color = NA)
)

p <- ggplot2::ggplot(coefs,
  ggplot2::aes(x = year, y = estimate,
               ymin = conf.low, ymax = conf.high, color = spec)) +
  ggplot2::geom_hline(yintercept = 0, linetype = "dashed", color = "grey50") +
  ggplot2::geom_vline(xintercept = 2017.5, linetype = "dashed", color = "grey30") +
  ggplot2::geom_errorbar(width = 0.15,
                         position = ggplot2::position_dodge(width = 0.4)) +
  ggplot2::geom_point(position = ggplot2::position_dodge(width = 0.4)) +
  ggplot2::scale_x_continuous(breaks = seq(2013, 2022, by = 1)) +
  ggplot2::labs(x = "", y = "Percent change in median property-tax rate",
                color = NULL) +
  shared_theme

ggplot2::ggsave(file.path(OUTPUT_DIR, "fig_median_tax_rate_response.png"),
                p, width = 22, height = 12, units = "cm")

message("Saved fig_median_tax_rate_response.png and coef_median_tax_rate.csv")
