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
# Use a separate test database so the main database.db is not touched.
# Change dbdir to "../../../data/database.db" when merging.
con <- dbConnect(
  duckdb::duckdb(),
  dbdir = "../../../data/census_finances_test.db"
)

raw_dir <- "../../../data/census_gov_finances/raw"

# Item code prefixes to keep (revenue-related codes only).
# Excludes debt (19T/29U/...), expenditures (E/F/G), wages (Z), etc.
# T = own-source taxes
# B = federal intergovernmental revenue
# C = state intergovernmental revenue
# D = local intergovernmental revenue
# U = other own-source revenue (charges, fees, misc)
REVENUE_PREFIXES <- c("T", "B", "C", "D", "U")

# Key individual tax items to keep as named columns
KEY_CODES <- c(
  "T01",  # Property taxes (primary outcome)
  "T09",  # General sales taxes
  "T40"   # Individual income taxes
)

# ---- Helper: read GID (2012-2016) to get county FIPS crosswalk ----
# GID layout (153-char records, 14-char ID):
#   pos 1-14:   ID code
#   pos 114-115: FIPS state code (2 chars)
#   pos 116-118: FIPS county code (3 chars)
read_gid <- function(year_dir) {
  f <- list.files(year_dir, pattern = "^Fin_GID_", full.names = TRUE)
  if (length(f) == 0) return(NULL)
  df <- readr::read_fwf(
    f[1],
    col_positions = readr::fwf_widths(
      c(14, 99, 2, 3, 34),  # ID, skip name+county fields, state, county, rest
      col_names = c("id_code", "skip", "state_fips_gid", "county_fips_3", "rest")
    ),
    col_types = "ccccc",
    show_col_types = FALSE
  )
  df |>
    select(id_code, state_fips_gid, county_fips_3) |>
    filter(str_sub(id_code, 3, 3) == "1")  # county type = 1
}

# ---- Helper: read PID (2017+) to get county FIPS crosswalk ----
# PID layout (146-char records, 12-char ID):
#   pos 1-12:   ID code
#   pos 112-116: FIPS place code (5 chars), = "99" + county FIPS for counties
read_pid <- function(year_dir) {
  f <- list.files(year_dir, pattern = "^Fin_PID_", full.names = TRUE,
                  recursive = TRUE)
  if (length(f) == 0) return(NULL)
  df <- readr::read_fwf(
    f[1],
    col_positions = readr::fwf_widths(
      c(12, 99, 5, 30),  # ID, skip, FIPS place code, rest
      col_names = c("id_code", "skip", "fips_place", "rest")
    ),
    col_types = "cccc",
    show_col_types = FALSE
  )
  df |>
    select(id_code, fips_place) |>
    filter(str_sub(id_code, 3, 3) == "1") |>  # county type = 1
    mutate(
      state_fips_gid  = str_sub(id_code, 1, 2),
      # "99" + 3-digit county FIPS for county govts
      county_fips_3 = str_sub(fips_place, 3, 5)
    ) |>
    select(id_code, state_fips_gid, county_fips_3)
}

# ---- Helper: read one year's individual unit file ----
# Record layouts:
#   2012-2016 (34-char): ID(14) + itemcode(3) + amount(12) + year(4) + flag(1)
#   2017-2022 (32-char): ID(12) + itemcode(3) + amount(12) + year(4) + flag(1)
read_alfin_file <- function(year_dir, year) {
  f <- list.files(
    year_dir,
    pattern = "[0-9]{4}FinEstDAT.*\\.txt$",
    full.names = TRUE
  )
  # 2022 is in a subdirectory
  if (length(f) == 0) {
    f <- list.files(
      year_dir,
      pattern = "[0-9]{4}FinEstDAT.*\\.txt$",
      full.names = TRUE,
      recursive = TRUE
    )
  }
  if (length(f) == 0) {
    stop(glue::glue("No FinEstDAT file found in {year_dir}"))
  }

  # Determine ID length from record length (first line, strip \r\n)
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

  message(glue::glue(
    "  Year {year}: rec_len={rec_len}, id_len={id_len}, ",
    "file={basename(f[1])}"
  ))

  df <- readr::read_fwf(
    f[1],
    col_positions = readr::fwf_widths(
      c(id_len, 3, 12, 4, 1),
      col_names = c("id_code", "itemcode", "amount", "year_col", "flag")
    ),
    col_types = "ccccc",
    show_col_types = FALSE
  )

  # Filter to revenue codes and county govts (type = 1)
  df |>
    filter(
      str_sub(id_code, 3, 3) == "1",   # county type
      str_sub(itemcode, 1, 1) %in% REVENUE_PREFIXES
    ) |>
    mutate(year = as.integer(year))
}

# ---- Helper: get county FIPS crosswalk for a year ----
get_crosswalk <- function(year_dir, year) {
  if (year <= 2016) {
    cw <- read_gid(year_dir)
  } else {
    cw <- read_pid(year_dir)
  }
  if (is.null(cw) || nrow(cw) == 0) {
    stop(glue::glue("No GID/PID crosswalk found for year {year}."))
  }
  cw
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

# ---- Process each year ----
process_year <- function(year_dir, year) {
  df <- read_alfin_file(year_dir, year)
  cw <- get_crosswalk(year_dir, year)

  # Join county FIPS from crosswalk
  df <- df |>
    left_join(cw, by = "id_code") |>
    filter(!is.na(county_fips_3))

  # Convert amount; W-flagged = suppressed -> NA
  df <- df |>
    mutate(
      flag   = if_else(is.na(flag) | flag == " ", "A", flag),
      amount = if_else(
        flag == "W" | trimws(amount) %in% c("", "."),
        NA_real_,
        as.numeric(trimws(amount))
      ),
      state_fips  = as.integer(state_fips_gid),
      county_fips = state_fips * 1000L + as.integer(county_fips_3)
    )

  # Compute aggregate columns before pivoting
  df_agg <- df |>
    group_by(id_code, county_fips, state_fips, county_fips_3, year) |>
    summarise(
      total_tax_rev      = sum(amount[str_sub(itemcode, 1, 1) == "T"],
                               na.rm = FALSE),
      federal_ig_rev     = sum(amount[str_sub(itemcode, 1, 1) == "B"],
                               na.rm = FALSE),
      state_ig_rev       = sum(amount[str_sub(itemcode, 1, 1) == "C"],
                               na.rm = FALSE),
      local_ig_rev       = sum(amount[str_sub(itemcode, 1, 1) == "D"],
                               na.rm = FALSE),
      other_own_source   = sum(amount[str_sub(itemcode, 1, 1) == "U"],
                               na.rm = FALSE),
      .groups = "drop"
    )

  # Pivot key codes wide and join aggregates
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

result_list <- purrr::map2(year_dirs, years, process_year)
df <- data.table::rbindlist(result_list, use.names = TRUE, fill = TRUE)

# ---- Rename key code columns ----
df <- df |>
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
cat("\nCounties per year:\n")
df |>
  group_by(year) |>
  summarise(
    n_counties = n(),
    pct_property_tax_present = round(
      100 * mean(!is.na(property_tax_rev)), 1
    ),
    .groups = "drop"
  ) |>
  arrange(year) |>
  print(n = Inf)

# Suppression summary
cat("\nSuppression rate for property_tax_rev by year:\n")
df |>
  group_by(year) |>
  summarise(
    n = n(),
    pct_suppressed = round(100 * mean(is.na(property_tax_rev)), 1),
    .groups = "drop"
  ) |>
  print(n = Inf)

cat("\nTotal rows:", nrow(df), "\n")

# ---- Write to DuckDB ----
dbWriteTable(con, "govt_finances_county", as.data.frame(df), overwrite = TRUE)
cat("\nWrote 'govt_finances_county' to census_finances_test.db\n")

dbDisconnect(con, shutdown = TRUE)
