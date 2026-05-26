gc()
rm(list = ls())

library(tidyverse)
library(duckdb)
library(fixest)
library(broom)

con <- dbConnect(duckdb::duckdb(), dbdir = "../../../data/database.db", read_only = TRUE)

# ============================================================
# Section 1: Pull movers and stayers
# ============================================================

message("Pulling movers...")
movers_raw <- dbGetQuery(con, "
  SELECT
    year, statefip, puma,
    migrate1, migplac1, migpuma1,
    imp_salt, imp_proptx, imp_stateinctax, age, marst, nchild, ownershp, valueh, hhwt,
    incwage, inctot,
    CAST(imp_totalwages    AS INTEGER) AS imp_totalwages,
    CAST(imp_itemizable_2017 AS DOUBLE) AS imp_itemizable_2017,
    CAST(imp_itemizable_2018 AS DOUBLE) AS imp_itemizable_2018
  FROM acs_microdata
  WHERE pernum    = 1
    AND gq        IN (1, 2, 5)
    AND migrate1  >= 2
    AND migpuma1  > 0
    AND migpuma1  IS NOT NULL
    AND migplac1  IS NOT NULL
    AND year BETWEEN 2013 AND 2023
") |> as_tibble()

message(sprintf("Pulled %d movers", nrow(movers_raw)))

message("Pulling stayers...")
stayers_raw <- dbGetQuery(con, "
  SELECT
    year, statefip, migpumanow,
    imp_salt, imp_stateinctax, imp_proptx,
    age, marst, nchild, ownershp, valueh,
    incwage, inctot,
    CAST(imp_totalwages AS INTEGER) AS imp_totalwages
  FROM acs_microdata
  WHERE pernum   = 1
    AND gq       IN (1, 2, 5)
    AND migrate1 = 1
    AND year BETWEEN 2013 AND 2023
") |> as_tibble()

message(sprintf("Pulled %d stayers", nrow(stayers_raw)))

# Coverage check: household movers (gq IN 1,2,5) before any PUMA filters
mover_coverage <- dbGetQuery(con, "
  SELECT
    COUNT(*)                                                                     AS n_total,
    SUM(CASE WHEN migplac1 IS NULL OR migplac1 = 0 THEN 1 ELSE 0 END)          AS n_miss_origin_state,
    SUM(CASE WHEN migpuma1 IS NULL OR migpuma1 = 0 THEN 1 ELSE 0 END)          AS n_miss_origin_puma,
    SUM(CASE WHEN imp_salt IS NULL                 THEN 1 ELSE 0 END)           AS n_miss_dest_salt,
    SUM(CASE WHEN migplac1 IS NULL OR migplac1 = 0
              OR migpuma1 IS NULL OR migpuma1 = 0  THEN 1 ELSE 0 END)           AS n_miss_either_origin
  FROM acs_microdata
  WHERE pernum = 1 AND gq IN (1, 2, 5) AND migrate1 >= 2 AND year BETWEEN 2013 AND 2023
") |> as_tibble() |>
  mutate(across(starts_with("n_miss"),
                list(pct = ~ round(100 * . / n_total, 2))))

cat("\nMover coverage (household movers, gq IN 1,2,5, before PUMA filters):\n")
print(as.data.frame(mover_coverage))

dbDisconnect(con, shutdown = TRUE)

# ============================================================
# Section 2: Feature engineering
# ============================================================

make_features <- function(df) {
  df |>
    mutate(
      pwages     = pmax(0L, if_else(incwage == 9999999L, 0L, as.integer(incwage))),
      swages     = pmax(0L, coalesce(imp_totalwages, pwages) - pwages),
      othinc     = pmax(0L, if_else(inctot  == 9999999L, 0L, as.integer(inctot)) -
                            if_else(incwage == 9999999L, 0L, as.integer(incwage))),
      log_pwages = log(pwages + 1L),
      log_swages = log(swages + 1L),
      log_othinc = log(othinc + 1L),
      age        = as.numeric(age),
      is_mfj     = as.integer(marst == 1L),
      is_hoh     = as.integer(marst != 1L & nchild > 0L),
      owns_home  = as.integer(ownershp == 1L),
      log_valueh = if_else(
        ownershp == 1L & !is.na(valueh) & valueh < 9999999L,
        log(pmax(as.numeric(valueh), 1)),
        0
      )
    )
}


movers  <- make_features(movers_raw) |>
  filter(migpuma1 != puma | migrate1 == 3L)   # cross-PUMA movers only
stayers <- make_features(stayers_raw)

# ============================================================
# Section 3: Coverage check
# ============================================================

cat("\nMover count by migrate1:\n")
print(count(movers, migrate1) |> mutate(pct = round(100 * n / sum(n), 1)))

cat("\nShare of movers with non-NA destination SALT:", round(100 * mean(!is.na(movers$imp_salt)), 1), "%\n")

cat("\nMove type by PUMA geography:\n")
movers |>
  mutate(move_type = case_when(
    migrate1 == 5L                                           ~ "Abroad",
    migplac1 != statefip                                     ~ "Cross-state",
    migplac1 == statefip & migpuma1 != puma                  ~ "Within state, different PUMA",
    migplac1 == statefip & migpuma1 == puma                  ~ "Same PUMA",
    TRUE                                                     ~ "Unknown"
  )) |>
  count(move_type) |>
  mutate(pct = round(100 * n / sum(n), 1)) |>
  arrange(desc(n)) |>
  print()

# ============================================================
# Section 4: Origin SALT imputation
# ============================================================
#
# For each mover, look up the average SALT paid by stayers in the
# same origin state, filing status, and household income bracket.

INCOME_BREAKS <- c(-Inf, 25e3, 50e3, 75e3, 100e3, 150e3, 200e3, 300e3, Inf)

fstatus_cat <- function(df) {
  df |> mutate(fstatus_cat = case_when(
    is_mfj == 1L                       ~ "MFJ",
    marst != 1L & nchild > 0L          ~ "HoH",
    TRUE                               ~ "Single"
  ))
}

stayer_salt_lookup <- stayers |>
  fstatus_cat() |>
  mutate(hhinc     = pwages + swages + othinc,
         inc_bin   = cut(hhinc, breaks = INCOME_BREAKS, include.lowest = TRUE)) |>
  filter(!is.na(imp_salt)) |>
  group_by(year, statefip, fstatus_cat, inc_bin) |>
  summarise(origin_salt = mean(imp_salt, na.rm = TRUE),
            n_stayers   = n(),
            .groups     = "drop")

# State-level fallback (collapse over income bins)
stayer_salt_state <- stayer_salt_lookup |>
  group_by(year, statefip, fstatus_cat) |>
  summarise(origin_salt_state = weighted.mean(origin_salt, n_stayers),
            .groups = "drop")

cat(sprintf("\nSALT lookup table: %d cells (state × filing × income bracket × year)\n",
            nrow(stayer_salt_lookup)))

movers_out <- movers |>
  fstatus_cat() |>
  mutate(hhinc   = pwages + swages + othinc,
         inc_bin = cut(hhinc, breaks = INCOME_BREAKS, include.lowest = TRUE)) |>
  left_join(stayer_salt_lookup |> rename(migplac1 = statefip),
            by = c("year", "migplac1", "fstatus_cat", "inc_bin")) |>
  left_join(stayer_salt_state |> rename(migplac1 = statefip),
            by = c("year", "migplac1", "fstatus_cat")) |>
  mutate(origin_salt = coalesce(origin_salt, origin_salt_state))

cat(sprintf("  origin_salt missing: %.1f%%\n",
            100 * mean(is.na(movers_out$origin_salt))))

# ============================================================
# Section 5: SALT direction summary — shares over time
# ============================================================

salt_dir <- movers_out |>
  filter(!is.na(imp_salt), !is.na(origin_salt), migrate1 %in% c(2L, 3L)) |>
  mutate(
    salt_diff = imp_salt - origin_salt,
    move_type = if_else(migrate1 == 2L, "Within-state", "Cross-state")
  )

salt_time <- salt_dir |>
  group_by(year, move_type) |>
  summarise(
    n           = n(),
    p_higher    = mean(salt_diff >  500),
    p_lower     = mean(salt_diff < -500),
    p_similar   = mean(abs(salt_diff) <= 500),
    .groups     = "drop"
  ) |>
  tidyr::pivot_longer(c(p_higher, p_lower, p_similar),
                      names_to = "direction", values_to = "share") |>
  mutate(
    share  = 100 * share,
    ci_lo  = 100 * (share/100 - 1.96 * sqrt((share/100) * (1 - share/100) / n)),
    ci_hi  = 100 * (share/100 + 1.96 * sqrt((share/100) * (1 - share/100) / n)),
    direction = factor(direction,
      levels = c("p_higher", "p_lower", "p_similar"),
      labels = c("Higher SALT (>$500)", "Lower SALT (>$500)", "Similar SALT (±$500)"))
  )

cat("\nSALT direction by year and move type (n obs):\n")
print(
  salt_time |>
    filter(direction == "Higher SALT (>$500)") |>
    select(year, move_type, n) |>
    tidyr::pivot_wider(names_from = move_type, values_from = n) |>
    as.data.frame()
)
cat("\nSALT direction shares:\n")
print(
  salt_time |>
    select(year, move_type, direction, share) |>
    tidyr::pivot_wider(names_from = direction, values_from = share) |>
    mutate(across(where(is.numeric), \(x) round(x, 1))) |>
    as.data.frame()
)

# Linear trend fitted on 2013-2017, extrapolated over all years
all_years <- data.frame(year = sort(unique(salt_time$year)))

salt_trend <- salt_time |>
  filter(year <= 2017) |>
  group_by(direction, move_type) |>
  group_modify(function(d, ...) {
    fit <- lm(share ~ year, data = d)
    data.frame(all_years, trend = predict(fit, newdata = all_years))
  }) |>
  ungroup()

p_salt_dir <- ggplot(salt_time, aes(x = year, y = share, color = direction, fill = direction)) +
  geom_vline(xintercept = 2017.5, linetype = "dotted", color = "grey40") +
  geom_ribbon(aes(ymin = ci_lo, ymax = ci_hi), alpha = 0.15, color = NA) +
  geom_line(aes(linetype = direction), linewidth = 0.9) +
  geom_point(size = 1.8) +
  geom_line(data = salt_trend, aes(y = trend), linetype = "dashed", linewidth = 0.5, alpha = 0.7) +
  annotate("text", x = 2017.7, y = 1, label = "TCJA", hjust = 0, size = 3, color = "grey40") +
  scale_color_manual(values = c(
    "Higher SALT (>$500)"  = "#2c5f8a",
    "Lower SALT (>$500)"   = "#c06028",
    "Similar SALT (±$500)" = "grey50"
  )) +
  scale_fill_manual(values = c(
    "Higher SALT (>$500)"  = "#2c5f8a",
    "Lower SALT (>$500)"   = "#c06028",
    "Similar SALT (±$500)" = "grey50"
  )) +
  scale_linetype_manual(values = c(
    "Higher SALT (>$500)"  = "solid",
    "Lower SALT (>$500)"   = "solid",
    "Similar SALT (±$500)" = "dashed"
  )) +
  scale_y_continuous(labels = scales::label_number(suffix = "%")) +
  facet_wrap(~move_type) +
  labs(
    title    = "Share of movers by SALT change direction, over time",
    subtitle = "Dashed lines: linear trend fitted on 2013–2017, extrapolated; shading = 95% CI",
    x        = NULL, y = "Share of movers",
    color    = NULL, fill = NULL, linetype = NULL
  ) +
  theme_bw() +
  theme(legend.position = "bottom")

print(p_salt_dir)
ggsave("../../../output/Cormac/salt_direction.png", p_salt_dir, width = 10, height = 5, dpi = 150)

# Average salt_diff over time by move type
salt_avg <- salt_dir |>
  group_by(year, move_type) |>
  summarise(
    n         = n(),
    mean_diff = mean(salt_diff),
    se_diff   = sd(salt_diff) / sqrt(n()),
    ci_lo     = mean_diff - 1.96 * se_diff,
    ci_hi     = mean_diff + 1.96 * se_diff,
    .groups   = "drop"
  )

cat("\nAverage SALT diff by year and move type (n obs):\n")
print(
  salt_avg |>
    select(year, move_type, n, mean_diff) |>
    mutate(mean_diff = round(mean_diff)) |>
    tidyr::pivot_wider(names_from = move_type,
                       values_from = c(n, mean_diff),
                       names_glue = "{move_type}_{.value}") |>
    as.data.frame()
)

salt_avg_trend <- salt_avg |>
  filter(year <= 2017) |>
  group_by(move_type) |>
  group_modify(function(d, ...) {
    fit <- lm(mean_diff ~ year, data = d)
    data.frame(all_years, trend = predict(fit, newdata = all_years))
  }) |>
  ungroup()

p_salt_avg <- ggplot(salt_avg, aes(x = year, y = mean_diff, color = move_type, fill = move_type)) +
  geom_vline(xintercept = 2017.5, linetype = "dotted", color = "grey40") +
  geom_hline(yintercept = 0, linetype = "solid", color = "grey70") +
  geom_ribbon(aes(ymin = ci_lo, ymax = ci_hi), alpha = 0.15, color = NA) +
  geom_line(linewidth = 0.9) +
  geom_point(size = 1.8) +
  geom_line(data = salt_avg_trend, aes(y = trend), linetype = "dashed",
            linewidth = 0.5, alpha = 0.7) +
  annotate("text", x = 2017.7, y = -Inf, label = "TCJA",
           hjust = 0, vjust = -0.5, size = 3, color = "grey40") +
  scale_color_manual(values = c("Within-state" = "#2c5f8a", "Cross-state" = "#c06028")) +
  scale_fill_manual(values  = c("Within-state" = "#2c5f8a", "Cross-state" = "#c06028")) +
  scale_y_continuous(labels = scales::dollar_format()) +
  facet_wrap(~move_type, scales = "free_y") +
  labs(
    title    = "Average SALT change at move (destination − origin), over time",
    subtitle = "Dashed lines: linear trend fitted on 2013–2017, extrapolated; shading = 95% CI",
    x        = NULL, y = "Mean destination SALT − origin SALT",
    color    = NULL, fill = NULL
  ) +
  theme_bw() +
  theme(legend.position = "none")

print(p_salt_avg)
ggsave("../../../output/Cormac/salt_avg_diff.png", p_salt_avg, width = 10, height = 5, dpi = 150)

# Average total SALT at origin vs destination
tax_levels <- movers_out |>
  filter(migrate1 %in% c(2L, 3L), !is.na(origin_salt), !is.na(imp_salt)) |>
  mutate(move_type = if_else(migrate1 == 2L, "Within-state", "Cross-state")) |>
  group_by(year, move_type) |>
  summarise(
    n            = n(),
    origin_mean  = mean(origin_salt, na.rm = TRUE),
    dest_mean    = mean(imp_salt,    na.rm = TRUE),
    origin_se    = sd(origin_salt, na.rm = TRUE) / sqrt(n()),
    dest_se      = sd(imp_salt,    na.rm = TRUE) / sqrt(n()),
    .groups = "drop"
  ) |>
  tidyr::pivot_longer(c(origin_mean, dest_mean, origin_se, dest_se),
                      names_to = c("location", ".value"),
                      names_pattern = "(origin|dest)_(mean|se)") |>
  mutate(
    location = if_else(location == "origin", "Origin", "Destination"),
    ci_lo    = mean - 1.96 * se,
    ci_hi    = mean + 1.96 * se
  )

cat("\nAverage SALT at origin vs destination (n obs per year/move type):\n")
print(
  tax_levels |>
    filter(location == "Origin") |>
    select(year, move_type, n) |>
    tidyr::pivot_wider(names_from = move_type, values_from = n) |>
    as.data.frame()
)

p_tax_levels <- ggplot(tax_levels,
                       aes(x = year, y = mean, color = location, fill = location)) +
  geom_vline(xintercept = 2017.5, linetype = "dotted", color = "grey40") +
  geom_ribbon(aes(ymin = ci_lo, ymax = ci_hi), alpha = 0.15, color = NA) +
  geom_line(linewidth = 0.9) +
  geom_point(size = 1.8) +
  annotate("text", x = 2017.7, y = -Inf, label = "TCJA",
           hjust = 0, vjust = -0.5, size = 3, color = "grey40") +
  scale_color_manual(values = c("Origin" = "#c06028", "Destination" = "#2c5f8a")) +
  scale_fill_manual(values  = c("Origin" = "#c06028", "Destination" = "#2c5f8a")) +
  scale_y_continuous(labels = scales::dollar_format()) +
  facet_wrap(~move_type, ncol = 2) +
  labs(
    title    = "Average SALT: origin vs destination",
    subtitle = "Shading = 95% CI",
    x        = NULL, y = "Average SALT ($)",
    color    = NULL, fill = NULL
  ) +
  theme_bw() +
  theme(legend.position = "bottom")

print(p_tax_levels)
ggsave("../../../output/Cormac/tax_levels_origin_dest.png", p_tax_levels,
       width = 10, height = 5, dpi = 150)

# ============================================================
# Section 6: Itemization classification + SALT diff outcome
# ============================================================

movers_out <- movers_out |>
  mutate(
    salt_diff = imp_salt - origin_salt,
    fstatus   = case_when(
      is_mfj == 1L ~ "MFJ",
      is_hoh == 1L ~ "HoH",
      TRUE         ~ "Single"
    )
  )

std_ded_2017_map <- c("MFJ" = 12700, "Single" = 6350, "HoH" = 9350)
tcja_std_ded_map <- c("MFJ" = 24000, "Single" = 12000, "HoH" = 18000)
BAND <- 0.30

movers_anal <- movers_out |>
  filter(!is.na(salt_diff), fstatus %in% c("MFJ", "Single")) |>
  mutate(
    std_2017      = std_ded_2017_map[fstatus],
    std_tcja      = tcja_std_ded_map[fstatus],
    itemizes_2017 = as.integer(!is.na(imp_itemizable_2017) & imp_itemizable_2017 > std_2017),
    tcja_itemizes = as.integer(!is.na(imp_itemizable_2018) & imp_itemizable_2018 > std_tcja),
    group = case_when(
      itemizes_2017 == 1L & tcja_itemizes == 0L ~ "Stopped itemizing (TCJA)",
      itemizes_2017 == 0L & tcja_itemizes == 0L ~ "Never itemized",
      itemizes_2017 == 1L & tcja_itemizes == 1L ~ "Always itemized",
      TRUE                                       ~ NA_character_
    )
  ) |>
  filter(!is.na(group))

cat("\nMover group counts by filing status:\n")
print(count(movers_anal, fstatus, group) |> arrange(fstatus, group) |> as.data.frame())

# ============================================================
# Section 7: Event study — SALT sorting outcomes
# ============================================================

# Build PUMA-level stayer averages for all area-level outcomes
stayers_hhinc <- stayers |>
  mutate(hhinc = pmax(pwages + swages + othinc, 1))

puma_lookup <- stayers_hhinc |>
  group_by(year, statefip, migpumanow) |>
  summarise(
    puma_proptax_rate  = median(if_else(ownershp == 1L & valueh > 0L & valueh < 9999999L,
                                        imp_proptx / as.numeric(valueh), NA_real_),
                                na.rm = TRUE),
    puma_inctax_rate   = mean(imp_stateinctax / hhinc, na.rm = TRUE),
    puma_proptax_paid  = mean(imp_proptx,      na.rm = TRUE),
    puma_inctax_paid   = mean(imp_stateinctax, na.rm = TRUE),
    puma_salt_paid     = mean(imp_salt,        na.rm = TRUE),
    .groups = "drop"
  )

movers_anal <- movers_anal |>
  mutate(hhinc = pmax(pwages + swages + othinc, 1)) |>
  # Destination PUMA averages
  left_join(puma_lookup |> rename(puma = migpumanow) |>
              rename_with(~ paste0("dest_", .), -c(year, statefip, puma)),
            by = c("year", "statefip", "puma")) |>
  # Origin PUMA averages
  left_join(puma_lookup |> rename(migplac1 = statefip, migpuma1 = migpumanow) |>
              rename_with(~ paste0("orig_", .), -c(year, migplac1, migpuma1)),
            by = c("year", "migplac1", "migpuma1")) |>
  mutate(
    diff_proptax_rate = 100 * (dest_puma_proptax_rate - orig_puma_proptax_rate),
    diff_inctax_rate  = 100 * (dest_puma_inctax_rate  - orig_puma_inctax_rate),
    diff_proptax_paid = dest_puma_proptax_paid - orig_puma_proptax_paid,
    diff_inctax_paid  = dest_puma_inctax_paid  - orig_puma_inctax_paid,
    diff_salt_paid    = dest_puma_salt_paid     - orig_puma_salt_paid,
    # Outcome 6: individual SALT — salt_diff already computed in section 6
  )

OUTCOMES <- c(
  diff_proptax_rate = "Property tax rate (PUMA avg, pp)",
  diff_inctax_rate  = "State income tax rate (PUMA avg, pp)",
  diff_proptax_paid = "Property taxes paid (PUMA avg, $)",
  diff_inctax_paid  = "State income taxes paid (PUMA avg, $)",
  diff_salt_paid    = "SALT paid (PUMA avg, $)",
  salt_diff         = "Individual SALT (dest − orig, $)"
)

treat_labels    <- c("Stopped itemizing (TCJA)", "Always itemized")
es_group_colors <- c(
  "Stopped itemizing (TCJA)" = "#c06028",
  "Always itemized"          = "#2c5f8a"
)

COMPARISONS <- list(
  list(treat = "Stopped itemizing (TCJA)", control = "Never itemized"),
  list(treat = "Always itemized",          control = "Stopped itemizing (TCJA)")
)

run_es <- function(outcome, treat_label, control_label, fs, data) {
  dat <- data |>
    filter(fstatus == fs,
           group %in% c(treat_label, control_label),
           !is.na(.data[[outcome]])) |>
    mutate(treated = as.integer(group == treat_label))

  if (nrow(dat) < 50L) {
    message(sprintf("  Too few obs for %s vs %s, %s — skipping", treat_label, control_label, fs))
    return(NULL)
  }

  feols(
    as.formula(paste0(
      "`", outcome, "` ~ treated + i(year, treated, ref = 2017) + ",
      "log_pwages + log_swages + log_othinc + age + owns_home + log_valueh | ",
      "year + statefip"
    )),
    data = dat, vcov = ~statefip
  )
}

es_results <- bind_rows(lapply(names(OUTCOMES), function(oc) {
  bind_rows(lapply(COMPARISONS, function(cmp) {
    bind_rows(lapply(c("MFJ", "Single"), function(fs) {
      mod <- run_es(oc, cmp$treat, cmp$control, fs, movers_anal)
      if (is.null(mod)) return(tibble())
      tidy(mod) |>
        filter(str_detect(term, "year::")) |>
        mutate(
          year       = as.integer(str_extract(term, "\\d{4}")),
          outcome    = oc,
          comparison = cmp$treat,
          fstatus    = fs
        )
    }))
  }))
})) |>
  bind_rows(
    expand.grid(
      year       = 2017L,
      estimate   = 0, std.error = 0,
      outcome    = names(OUTCOMES),
      comparison = sapply(COMPARISONS, `[[`, "treat"),
      fstatus    = c("MFJ", "Single"),
      stringsAsFactors = FALSE
    )
  ) |>
  mutate(
    outcome    = factor(outcome,    levels = names(OUTCOMES), labels = unname(OUTCOMES)),
    comparison = factor(comparison, levels = treat_labels),
    fstatus    = factor(fstatus,    levels = c("MFJ", "Single")),
    ci_lo      = estimate - 1.96 * std.error,
    ci_hi      = estimate + 1.96 * std.error
  ) |>
  arrange(outcome, comparison, fstatus, year)

theme_set(theme_bw(base_size = 11))
tcja_line  <- geom_vline(xintercept = 2017.5, linetype = "dashed", color = "grey50", linewidth = 0.7)
year_scale <- scale_x_continuous(breaks = seq(2013, 2023, 2))

make_es_plot <- function(oc_level, oc_label) {
  dat <- es_results |> filter(outcome == oc_level)
  ggplot(dat, aes(x = year, y = estimate, color = comparison, fill = comparison)) +
    tcja_line +
    geom_hline(yintercept = 0, linetype = "dotted", color = "grey60") +
    geom_ribbon(aes(ymin = ci_lo, ymax = ci_hi), alpha = 0.15, color = NA) +
    geom_line(linewidth = 0.9) +
    geom_point(size = 2) +
    facet_wrap(~fstatus, ncol = 2) +
    year_scale +
    scale_color_manual(values = es_group_colors) +
    scale_fill_manual(values  = es_group_colors) +
    labs(
      title    = oc_label,
      subtitle = "DiD relative to 2017; dashed = TCJA",
      x        = "Survey year", y = oc_label,
      color    = NULL, fill = NULL
    ) +
    theme(legend.position = "bottom")
}

for (oc in names(OUTCOMES)) {
  p <- make_es_plot(OUTCOMES[[oc]], OUTCOMES[[oc]])
  print(p)
  fname <- paste0("../../../output/Cormac/cond_mig_es_", oc, ".png")
  ggsave(fname, p, width = 9, height = 5, dpi = 150)
}

message("Done.")
