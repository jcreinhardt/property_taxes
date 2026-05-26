gc()
rm(list = ls())

library(duckdb)
library(dplyr)
library(tidyr)

# Connect to database
con <- dbConnect(duckdb::duckdb(), dbdir = "../../../data/database.db", read_only = TRUE)

county_returns_agi <- dbGetQuery(con, "SELECT * FROM returns_county_agi WHERE year = 2017")
state_returns      <- dbGetQuery(con, "SELECT * FROM returns_state WHERE year = 2017")

dbDisconnect(con, shutdown = TRUE)

# ---- Stub midpoints (in $000s, matching SOI amount units) ----
# County data has stubs 1, 3, 4, 5, 6, 8 (2→3 and 7→8 merged on import)
stub_midpoints <- c("1" = 0, "3" = 25, "4" = 62.5, "5" = 87.5, "6" = 150, "8" = 300)

# ---- Step 1: County-level stats ----
# A18425 (state_and_local_income_taxes_amount) is only observed for itemizers,
# so avg_state_inc_tax_pp understates total state taxes paid. However,
# cross-state differences in the effective rate remain informative.

county_stats <- county_returns_agi |>
    mutate(stub_midpoint = stub_midpoints[as.character(agi_stub)]) |>
    reframe(
        origin_state          = first(statefips),
        county_name           = first(countyname),
        avg_agi_pp            = sum(stub_midpoint * returns, na.rm = TRUE) /
                                sum(returns, na.rm = TRUE),
        avg_state_inc_tax_pp  = sum(state_and_local_income_taxes_amount, na.rm = TRUE) /
                                sum(returns, na.rm = TRUE),
        .by = county
    ) |>
    mutate(origin_effective_rate = avg_state_inc_tax_pp / avg_agi_pp) |>
    rename(county_fips = county)

cat("Counties:", nrow(county_stats), "\n")

# ---- Step 2: State-level effective income tax rates ----
# State data has stubs 1–8 (2 and 7 not pre-merged); merge to match county stubs.
state_rates <- state_returns |>
    mutate(agi_stub = case_when(
        agi_stub == 2L ~ 3L,
        agi_stub >= 8L ~ 8L,
        TRUE ~ agi_stub
    )) |>
    reframe(
        state_and_local_income_taxes_amount = sum(state_and_local_income_taxes_amount, na.rm = TRUE),
        returns = sum(returns, na.rm = TRUE),
        .by = c(state, agi_stub)
    ) |>
    mutate(stub_midpoint = stub_midpoints[as.character(agi_stub)]) |>
    reframe(
        dest_effective_rate = sum(state_and_local_income_taxes_amount, na.rm = TRUE) /
                              sum(stub_midpoint * returns, na.rm = TRUE),
        dest_inc_tax_pp     = sum(state_and_local_income_taxes_amount, na.rm = TRUE) /
                              sum(returns, na.rm = TRUE),
        .by = state
    ) |>
    rename(dest_state = state)

cat("States:", nrow(state_rates), "\n")

# ---- Step 3: Cross-join counties × states, exclude same-state moves ----
result <- county_stats |>
    cross_join(state_rates) |>
    filter(dest_state != origin_state) |>
    mutate(
        rate_diff             = origin_effective_rate - dest_effective_rate,
        tax_savings_thousands = avg_agi_pp * rate_diff
    ) |>
    select(
        county_fips,
        county_name,
        origin_state,
        dest_state,
        avg_agi_pp_thousands         = avg_agi_pp,
        avg_state_inc_tax_pp_thousands = avg_state_inc_tax_pp,
        origin_effective_rate,
        dest_effective_rate,
        rate_diff,
        tax_savings_thousands
    ) |>
    arrange(county_fips, dest_state)

cat("Output rows:", nrow(result), "\n")

# ---- Verification checks ----
cat("\n--- Verification ---\n")

# State-level effective rates (sorted)
cat("\nEffective state income tax rates (destination states), sorted:\n")
state_rates |>
    arrange(desc(dest_effective_rate)) |>
    print(n = 15)

# Sanity: CA county savings moving to TX
ca_to_tx <- result |>
    filter(origin_state == 6, dest_state == 48) |>
    arrange(desc(tax_savings_thousands))
cat("\nTop CA counties: savings moving to TX ($000s):\n")
print(head(ca_to_tx |> select(county_fips, county_name, avg_agi_pp_thousands,
                               origin_effective_rate, rate_diff, tax_savings_thousands), 5))

# Sanity: TX county savings moving to CA (should be negative)
tx_to_ca <- result |>
    filter(origin_state == 48, dest_state == 6)
cat("\nTX counties: savings moving to CA ($000s) — expect negative:\n")
print(head(tx_to_ca |> select(county_fips, county_name, avg_agi_pp_thousands,
                               origin_effective_rate, rate_diff, tax_savings_thousands), 5))

# ---- Write output ----
output_path <- "../../../data/county_to_state_tax_savings_2017.csv"
write.csv(result, output_path, row.names = FALSE)
cat("\nSaved to:", output_path, "\n")
