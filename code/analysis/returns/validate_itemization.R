gc()
rm(list = ls())

library(duckdb)
library(DBI)
library(dplyr)
library(ggplot2)

con <- dbConnect(duckdb::duckdb(), dbdir = "../../../data/database.db", read_only = TRUE)

# 2-letter abbreviation → FIPS (for adding statefip to SOI side)
state_fips_map <- tibble::tibble(
  state    = c("AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL",
               "GA","HI","ID","IL","IN","IA","KS","KY","LA","ME",
               "MD","MA","MI","MN","MS","MO","MT","NE","NV","NH",
               "NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI",
               "SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"),
  statefip = c( 1L,  2L,  4L,  5L,  6L,  8L,  9L, 10L, 11L, 12L,
               13L, 15L, 16L, 17L, 18L, 19L, 20L, 21L, 22L, 23L,
               24L, 25L, 26L, 27L, 28L, 29L, 30L, 31L, 32L, 33L,
               34L, 35L, 36L, 37L, 38L, 39L, 40L, 41L, 42L, 44L,
               45L, 46L, 47L, 48L, 49L, 50L, 51L, 53L, 54L, 55L, 56L)
)

# ============================================================
# ACS: weighted itemization rates
# Use imp_itemizes_2017 (uncapped SALT) for pre-2018 years, where actual
# law had no SALT cap; imp_itemizes_2018 (SALT capped, doubled std_ded) for 2018+.
# ============================================================

# State level
acs_state <- dbGetQuery(con, "
  SELECT
    year,
    statefip,
    SUM(
      CASE WHEN year <= 2017 THEN imp_itemizes_2017
                             ELSE imp_itemizes_2018 END
      * hhwt
    ) / SUM(hhwt) AS acs_itemize_rate,
    COUNT(*) AS n_hh
  FROM acs_microdata
  WHERE pernum = 1
    AND gq IN (1, 2, 5)
    AND imp_itemizes_2017 IS NOT NULL
    AND year BETWEEN 2013 AND 2022
  GROUP BY year, statefip
") |>
  as_tibble()

# County level
acs_county <- dbGetQuery(con, "
  SELECT
    year,
    statefip,
    countyfip,
    SUM(
      CASE WHEN year <= 2017 THEN imp_itemizes_2017
                             ELSE imp_itemizes_2018 END
      * hhwt
    ) / SUM(hhwt) AS acs_itemize_rate,
    COUNT(*) AS n_hh
  FROM acs_microdata
  WHERE pernum = 1
    AND gq IN (1, 2, 5)
    AND countyfip > 0
    AND imp_itemizes_2017 IS NOT NULL
    AND year BETWEEN 2013 AND 2022
  GROUP BY year, statefip, countyfip
") |>
  as_tibble() |>
  mutate(county_fips5 = statefip * 1000L + countyfip)

# ============================================================
# SOI: actual itemization rates (sum across AGI stubs)
# ============================================================

# State level
soi_state <- dbGetQuery(con, "
  SELECT
    year,
    state,
    SUM(returns_w_items)::DOUBLE / NULLIF(SUM(returns), 0) AS soi_itemize_rate,
    SUM(returns) AS soi_returns
  FROM returns_state
  WHERE agi_stub > 0
  GROUP BY year, state
") |> as_tibble()

# County level (returns_county_agi; agi_stub > 0 already filtered at import)
soi_county <- dbGetQuery(con, "
  SELECT
    year,
    statefips,
    countyfips,
    (1000 * statefips + countyfips) AS county_fips5,
    SUM(returns_w_items)::DOUBLE / NULLIF(SUM(returns), 0) AS soi_itemize_rate,
    SUM(returns) AS soi_returns
  FROM returns_county_agi
  WHERE agi_stub > 0
    AND countyfips > 0
  GROUP BY year, statefips, countyfips
") |> as_tibble()

dbDisconnect(con, shutdown = TRUE)

# SOI's "state" column is actually the numeric FIPS code — rename and join
# state_fips_map to recover the 2-letter abbreviation for labeling
soi_state <- soi_state |>
  rename(statefip = state) |>
  mutate(statefip = as.integer(statefip)) |>
  left_join(state_fips_map, by = "statefip")

# ============================================================
# Merge
# ============================================================

state_comp <- acs_state |>
  inner_join(soi_state, by = c("statefip", "year")) |>
  filter(!is.na(acs_itemize_rate), !is.na(soi_itemize_rate)) |>
  mutate(period = if_else(year <= 2017, "2013–2017 (pre-TCJA)", "2018–2022 (post-TCJA)"))

county_comp <- acs_county |>
  inner_join(
    soi_county |> select(year, county_fips5, soi_itemize_rate, soi_returns),
    by = c("county_fips5", "year")
  ) |>
  filter(!is.na(acs_itemize_rate), !is.na(soi_itemize_rate), soi_returns >= 1000) |>
  mutate(period = if_else(year <= 2017, "2013–2017 (pre-TCJA)", "2018–2022 (post-TCJA)"))

# ============================================================
# Correlations
# ============================================================

cat("\nState-level correlation (ACS vs. SOI itemization rate):\n")
print(
  state_comp |>
    group_by(period) |>
    summarise(
      n      = n(),
      cor    = round(cor(acs_itemize_rate, soi_itemize_rate), 3),
      r2     = round(cor(acs_itemize_rate, soi_itemize_rate)^2, 3),
      mean_acs_rate = round(100 * mean(acs_itemize_rate), 1),
      mean_soi_rate = round(100 * mean(soi_itemize_rate), 1),
      .groups = "drop"
    ) |> as.data.frame()
)

cat("\nCounty-level correlation (ACS vs. SOI itemization rate, soi_returns >= 1000):\n")
print(
  county_comp |>
    group_by(period) |>
    summarise(
      n      = n(),
      cor    = round(cor(acs_itemize_rate, soi_itemize_rate), 3),
      r2     = round(cor(acs_itemize_rate, soi_itemize_rate)^2, 3),
      mean_acs_rate = round(100 * mean(acs_itemize_rate), 1),
      mean_soi_rate = round(100 * mean(soi_itemize_rate), 1),
      .groups = "drop"
    ) |> as.data.frame()
)

# ============================================================
# Plots
# ============================================================

state_labels <- state_comp |>
  group_by(state) |>
  summarise(
    acs_itemize_rate = mean(acs_itemize_rate),
    soi_itemize_rate = mean(soi_itemize_rate),
    .groups = "drop"
  ) |>
  filter(acs_itemize_rate > 0.35 | soi_itemize_rate > 0.35 |
         abs(acs_itemize_rate - soi_itemize_rate) > 0.10)

p_state <- ggplot(state_comp, aes(soi_itemize_rate, acs_itemize_rate, color = period)) +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "grey50") +
  geom_point(alpha = 0.6, size = 1.8) +
  ggrepel::geom_text_repel(
    data = state_labels,
    aes(label = state),
    color = "black", size = 3, inherit.aes = FALSE
  ) +
  scale_x_continuous(labels = scales::percent_format(1)) +
  scale_y_continuous(labels = scales::percent_format(1)) +
  labs(
    title = "Itemization rate: ACS imputed vs. IRS SOI actual",
    subtitle = "State × year; 45° line = perfect fit",
    x = "SOI actual itemization rate",
    y = "ACS imputed itemization rate",
    color = NULL
  ) +
  theme_bw() +
  theme(legend.position = "bottom")

p_county <- ggplot(county_comp, aes(soi_itemize_rate, acs_itemize_rate, color = period)) +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "grey50") +
  geom_point(alpha = 0.25, size = 0.8) +
  scale_x_continuous(labels = scales::percent_format(1)) +
  scale_y_continuous(labels = scales::percent_format(1)) +
  labs(
    title = "Itemization rate: ACS imputed vs. IRS SOI actual",
    subtitle = "County × year (SOI returns ≥ 1,000); 45° line = perfect fit",
    x = "SOI actual itemization rate",
    y = "ACS imputed itemization rate",
    color = NULL
  ) +
  theme_bw() +
  theme(legend.position = "bottom")

# Itemization rate over time: national average ACS vs. SOI
time_comp <- state_comp |>
  group_by(year) |>
  summarise(
    acs = weighted.mean(acs_itemize_rate, n_hh),
    soi = weighted.mean(soi_itemize_rate, soi_returns),
    .groups = "drop"
  ) |>
  tidyr::pivot_longer(c(acs, soi), names_to = "source", values_to = "itemize_rate")

p_time <- ggplot(time_comp, aes(year, itemize_rate, color = source, linetype = source)) +
  geom_vline(xintercept = 2017.5, linetype = "dotted", color = "grey40") +
  geom_line(linewidth = 1) +
  geom_point(size = 2) +
  scale_y_continuous(labels = scales::percent_format(1), limits = c(0, NA)) +
  scale_color_manual(values = c(acs = "#2166ac", soi = "#d6604d"),
                     labels = c(acs = "ACS imputed", soi = "IRS SOI actual")) +
  scale_linetype_manual(values = c(acs = "solid", soi = "dashed"),
                        labels = c(acs = "ACS imputed", soi = "IRS SOI actual")) +
  annotate("text", x = 2017.7, y = 0.02, label = "TCJA", hjust = 0, size = 3.5) +
  labs(
    title = "National itemization rate over time",
    subtitle = "Weighted average across states; ACS imputed vs. IRS SOI actual",
    x = NULL, y = "Share of households itemizing",
    color = NULL, linetype = NULL
  ) +
  theme_bw() +
  theme(legend.position = "bottom")

print(p_time)
print(p_state)
print(p_county)
