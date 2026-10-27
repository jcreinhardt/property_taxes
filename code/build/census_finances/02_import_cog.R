# Formerly code/data_prep/build_database/census_gov_finances/02_import_census_finances.R (moved in the 2026-10-27 restructuring).
# Clean and load
gc()
rm(list = ls())

library(data.table)
library(dplyr)
library(tidyr)
library(stringr)
library(purrr)
library(duckdb)

# ---- Configuration ----
source("code/R/paths.R")
con <- dbConnect(
  duckdb::duckdb(),
  dbdir = file.path(DATA, "census_finances_test.db")
)

raw_dir <- file.path(DATA, "census_gov_finances/raw")

# Item-code prefixes kept from the individual-unit file.
#   T : own-source taxes
#   B : federal intergovernmental revenue
#   C : state intergovernmental revenue
#   D : local intergovernmental revenue
#   U : charges & misc general revenue
#   A : enterprise / utility revenue (water/electric/gas/transit/liquor)
#   E : direct expenditure (current operation), by function
#   F : capital outlay -- construction, by function
#   G : capital outlay -- other (equipment/land), by function
#   I : interest on debt, by function
#   J : assistance & subsidies (incl. J67/J68 welfare cash)
#   W : cash and security holdings (W01/W31/W61, non-insurance-trust funds)
PREFIX_CODES <- c("T", "B", "C", "D", "U", "A",
                  "E", "F", "G", "I", "J", "W")

# Multi-character debt item codes (do not start with a single letter prefix).
DEBT_CODES <- c("19T", "19U", "24T", "29U", "34T", "39U",
                "44T", "49U", "61V", "64V")

# Codes pivoted to named columns in the per-unit table.
KEY_CODES <- c(
  "T01", "T09", "T10", "T13", "T15", "T16", "T19", "T20", "T22",
  "T24", "T29", "T40",
  "U01", "U11", "U20", "U21", "U30", "U41", "U95", "U99",
  "E12", "E18", "E24", "E32", "E36", "E44", "E50", "E52", "E61",
  "E62", "E74", "E75", "E77", "E79", "E80", "E81", "E89",
  "J67", "J68",
  "I89",
  "19T", "19U", "24T", "29U", "34T", "39U",
  "44T", "49U", "61V", "64V",
  "W01", "W31", "W61"
)

# Tax codes are T-prefixed and not in DEBT_CODES.
T_PREFIX_TAX <- function(itemcode) {
  stringr::str_sub(itemcode, 1L, 1L) == "T" & !itemcode %in% DEBT_CODES
}

# ---- Crosswalk readers ----
# For 2012-2016 the id_code positions 1-2 / 4-6 are Census-internal codes,
# NOT FIPS. The actual FIPS state and FIPS county appear in the GID file
# at positions 114-115 and 116-118. We read all government types.
read_gid_all_types <- function(year_dir) {
  f <- list.files(year_dir, pattern = "^Fin_GID_", full.names = TRUE,
                  recursive = TRUE)
  if (length(f) == 0) return(NULL)
  df <- readr::read_fwf(
    f[1],
    col_positions = readr::fwf_widths(
      c(14, 99, 2, 3, 30),
      col_names = c("id_code", "skip", "state_fips_chr",
                    "county_fips_chr", "rest")
    ),
    col_types = "ccccc",
    show_col_types = FALSE
  )
  df |>
    select(id_code, state_fips_chr, county_fips_chr) |>
    mutate(govt_type = str_sub(id_code, 3, 3))
}

# For 2017+ the id_code positions 1-2 and 4-6 ARE FIPS codes, so the GID/PID
# lookup is unnecessary. This stub is kept only to make the year branch
# symmetric; it parses id_code directly when called.
crosswalk_from_idcode <- function(id_codes) {
  tibble::tibble(
    id_code         = id_codes,
    state_fips_chr  = str_sub(id_codes, 1, 2),
    county_fips_chr = str_sub(id_codes, 4, 6),
    govt_type       = str_sub(id_codes, 3, 3)
  ) |>
    distinct()
}

# ---- Read one year's individual unit file ----
read_alfin_file <- function(year_dir, year) {
  f <- list.files(year_dir,
                  pattern = "[0-9]{4}FinEstDAT.*\\.txt$",
                  full.names = TRUE)
  if (length(f) == 0) {
    f <- list.files(year_dir,
                    pattern = "[0-9]{4}FinEstDAT.*\\.txt$",
                    full.names = TRUE, recursive = TRUE)
  }
  if (length(f) == 0) {
    stop(glue::glue("No FinEstDAT file found in {year_dir}"))
  }

  first_line <- readLines(f[1], n = 1, warn = FALSE)
  rec_len <- nchar(trimws(first_line, which = "right"))
  if (rec_len == 34) {
    id_len <- 14L
  } else if (rec_len == 32) {
    id_len <- 12L
  } else {
    stop(glue::glue(
      "Year {year}: unexpected record length {rec_len} (expected 34 or 32)."
    ))
  }

  message(glue::glue("  Year {year}: rec_len={rec_len}, id_len={id_len}, ",
                     "file={basename(f[1])}"))

  df <- readr::read_fwf(
    f[1],
    col_positions = readr::fwf_widths(
      c(id_len, 3, 12, 4, 1),
      col_names = c("id_code", "itemcode", "amount", "year_col", "flag")
    ),
    col_types = "ccccc",
    show_col_types = FALSE
  )

  df <- df |>
    filter(str_sub(itemcode, 1, 1) %in% PREFIX_CODES |
             itemcode %in% DEBT_CODES) |>
    mutate(
      year   = as.integer(year),
      flag   = if_else(is.na(flag) | flag == " ", "A", flag),
      amount = if_else(
        flag == "W" | trimws(amount) %in% c("", "."),
        NA_real_,
        as.numeric(trimws(amount))
      )
    )

  # Attach FIPS state/county/govt_type
  if (year <= 2016L) {
    cw <- read_gid_all_types(year_dir)
    if (is.null(cw) || nrow(cw) == 0) {
      stop(glue::glue("No GID crosswalk found for year {year}."))
    }
  } else {
    cw <- crosswalk_from_idcode(unique(df$id_code))
  }

  df |>
    inner_join(cw, by = "id_code") |>
    filter(govt_type != "0",
           !is.na(state_fips_chr), nchar(state_fips_chr) == 2,
           !is.na(county_fips_chr), county_fips_chr != "000",
           nchar(county_fips_chr) == 3) |>
    mutate(
      state_fips  = as.integer(state_fips_chr),
      county_fips = state_fips * 1000L + as.integer(county_fips_chr)
    ) |>
    select(-state_fips_chr, -county_fips_chr)
}

# ---- Per-unit aggregation ----
aggregate_per_unit <- function(df) {
  df_agg <- df |>
    group_by(id_code, state_fips, govt_type, county_fips, year) |>
    summarise(
      total_tax_rev =
        sum(amount[T_PREFIX_TAX(itemcode)], na.rm = FALSE),
      federal_ig_rev =
        sum(amount[str_sub(itemcode, 1, 1) == "B"], na.rm = FALSE),
      state_ig_rev =
        sum(amount[str_sub(itemcode, 1, 1) == "C"], na.rm = FALSE),
      local_ig_rev =
        sum(amount[str_sub(itemcode, 1, 1) == "D"], na.rm = FALSE),
      other_own_source =
        sum(amount[str_sub(itemcode, 1, 1) == "U"], na.rm = FALSE),
      enterprise_rev =
        sum(amount[str_sub(itemcode, 1, 1) == "A"], na.rm = FALSE),
      total_expenditure =
        sum(amount[str_sub(itemcode, 1, 1) == "E"], na.rm = FALSE),
      capital_outlay_construction =
        sum(amount[str_sub(itemcode, 1, 1) == "F"], na.rm = FALSE),
      capital_outlay_other =
        sum(amount[str_sub(itemcode, 1, 1) == "G"], na.rm = FALSE),
      # Combined F + G capital outlay. In 2022 the Census public-use file
      # collapsed the G prefix into F, so summing F+G across all years gives
      # the only series that is comparable over time.
      total_capital_outlay =
        sum(amount[str_sub(itemcode, 1, 1) %in% c("F", "G")],
            na.rm = FALSE),
      assistance_subsidies =
        sum(amount[str_sub(itemcode, 1, 1) == "J"], na.rm = FALSE),
      interest_on_debt =
        sum(amount[str_sub(itemcode, 1, 1) == "I"], na.rm = FALSE),
      lt_debt_outstanding =
        sum(amount[itemcode %in% c("44T", "49U")], na.rm = TRUE),
      lt_debt_issued =
        sum(amount[itemcode %in% c("24T", "29U")], na.rm = TRUE),
      lt_debt_retired =
        sum(amount[itemcode %in% c("34T", "39U")], na.rm = TRUE),
      st_debt_outstanding =
        sum(amount[itemcode == "64V"], na.rm = TRUE),
      cash_securities =
        sum(amount[itemcode %in% c("W01", "W31", "W61")], na.rm = TRUE),
      .groups = "drop"
    )

  df_key <- df |>
    filter(itemcode %in% KEY_CODES) |>
    select(id_code, year, itemcode, amount, flag) |>
    pivot_wider(
      id_cols     = c(id_code, year),
      names_from  = itemcode,
      values_from = c(amount, flag),
      names_glue  = "{.value}_{itemcode}"
    ) |>
    rename_with(tolower)

  df_agg |>
    left_join(df_key, by = c("id_code", "year"))
}

# ---- Find year directories ----
year_dirs <- list.dirs(raw_dir, recursive = FALSE, full.names = TRUE)
years     <- suppressWarnings(as.integer(basename(year_dirs)))
valid     <- !is.na(years) & years >= 2012 & years <= 2022
year_dirs <- year_dirs[valid]
years     <- years[valid]

if (length(years) == 0) {
  stop(glue::glue(
    "No year directories in {raw_dir}. Run 01_download_census_finances.sh first."
  ))
}
message("Processing years: ", paste(sort(years), collapse = ", "))

process_year <- function(year_dir, year) {
  df <- read_alfin_file(year_dir, year)
  aggregate_per_unit(df)
}

result_list <- purrr::map2(year_dirs, years, process_year)
df_all <- data.table::rbindlist(result_list, use.names = TRUE, fill = TRUE)

# Rename legacy column names so existing analysis code still works.
df_all <- df_all |>
  rename_with(~ case_match(.,
    "amount_t01" ~ "property_tax_rev",
    "amount_t09" ~ "general_sales_tax_rev",
    "amount_t40" ~ "ind_income_tax_rev",
    "flag_t01"   ~ "flag_property_tax",
    "flag_t09"   ~ "flag_general_sales_tax",
    "flag_t40"   ~ "flag_ind_income_tax",
    .default = .
  ))

# ---- Diagnostics ----
cat("\nUnit counts per year and government type:\n")
df_all |>
  group_by(year, govt_type) |>
  summarise(n = n(), .groups = "drop") |>
  pivot_wider(names_from = govt_type, values_from = n,
              names_prefix = "type_") |>
  arrange(year) |>
  print(n = Inf)

cat("\nCounties (govt_type==1) per year:\n")
df_all |>
  filter(govt_type == "1") |>
  group_by(year) |>
  summarise(
    n_counties = n(),
    pct_property_tax_present = round(100 * mean(!is.na(property_tax_rev)), 1),
    .groups = "drop"
  ) |>
  arrange(year) |>
  print(n = Inf)

cat("\nTotal rows (all govt types):", nrow(df_all), "\n")

# ---- County-only table (govt_type == 1, preserves legacy schema) ----
df_county <- df_all |> filter(govt_type == "1")
dbWriteTable(con, "govt_finances_county",
             as.data.frame(df_county), overwrite = TRUE)
cat("\nWrote 'govt_finances_county' (", nrow(df_county), " rows)\n", sep = "")

# ---- County-area aggregate (sum across all government types in each county) ----
amount_cols <- c(
  "total_tax_rev", "federal_ig_rev", "state_ig_rev", "local_ig_rev",
  "other_own_source", "enterprise_rev",
  "total_expenditure", "capital_outlay_construction", "capital_outlay_other",
  "total_capital_outlay",
  "assistance_subsidies", "interest_on_debt",
  "lt_debt_outstanding", "lt_debt_issued", "lt_debt_retired",
  "st_debt_outstanding", "cash_securities",
  "property_tax_rev", "general_sales_tax_rev", "ind_income_tax_rev",
  grep("^amount_", names(df_all), value = TRUE)
) |> unique()
amount_cols <- intersect(amount_cols, names(df_all))

df_area <- df_all |>
  select(state_fips, county_fips, year, govt_type, all_of(amount_cols)) |>
  group_by(state_fips, county_fips, year) |>
  summarise(
    across(all_of(amount_cols), ~ sum(.x, na.rm = TRUE)),
    n_units = n(),
    n_county_govts = sum(govt_type == "1"),
    n_municipal = sum(govt_type == "2"),
    n_township = sum(govt_type == "3"),
    n_special_dist = sum(govt_type == "4"),
    n_school_dist = sum(govt_type == "5"),
    .groups = "drop"
  )

dbWriteTable(con, "govt_finances_county_area",
             as.data.frame(df_area), overwrite = TRUE)
cat("Wrote 'govt_finances_county_area' (", nrow(df_area), " rows)\n",
    sep = "")

# ---- School-district-only aggregate, summed to containing county ----
# Independent school districts (govt_type == 5) are surveyed at >=98% in
# every year, including non-COG years. Summing them within each county-area
# yields an annual outcome usable for an event study at county-level
# exposure.
df_sd <- df_all |>
  filter(govt_type == "5") |>
  select(state_fips, county_fips, year, all_of(amount_cols)) |>
  group_by(state_fips, county_fips, year) |>
  summarise(
    across(all_of(amount_cols), ~ sum(.x, na.rm = TRUE)),
    n_school_dist = n(),
    .groups = "drop"
  )

dbWriteTable(con, "govt_finances_sd_by_county",
             as.data.frame(df_sd), overwrite = TRUE)
cat("Wrote 'govt_finances_sd_by_county' (", nrow(df_sd), " rows)\n",
    sep = "")

# County-area / county-only sanity ratios.
cat("\nCounty-area / county-only revenue ratios (median across counties):\n")
df_county |>
  select(state_fips, county_fips, year,
         total_tax_rev_co = total_tax_rev,
         property_tax_rev_co = property_tax_rev) |>
  inner_join(
    df_area |>
      select(state_fips, county_fips, year,
             total_tax_rev_ca = total_tax_rev,
             property_tax_rev_ca = property_tax_rev),
    by = c("state_fips", "county_fips", "year")
  ) |>
  mutate(
    ratio_total = total_tax_rev_ca / total_tax_rev_co,
    ratio_property = property_tax_rev_ca / property_tax_rev_co
  ) |>
  group_by(year) |>
  summarise(
    median_ratio_total = round(median(ratio_total, na.rm = TRUE), 2),
    median_ratio_property = round(median(ratio_property, na.rm = TRUE), 2),
    .groups = "drop"
  ) |>
  arrange(year) |>
  print(n = Inf)

dbDisconnect(con, shutdown = TRUE)
