# Formerly code/paper/fig1.R (moved in the 2026-10-27 restructuring).
#!/usr/bin/env Rscript

# Figure 1 (Paper) and appendix:
# Average change in effective SALT refunds (deduction × marginal federal
# income tax rate) under TCJA, binned by itemizable total under 2017 or
# 2018 rules (MFJ filers).

gc()
rm(list = ls())

library(duckdb)
library(dplyr)
library(tidyr)
library(ggplot2)
library(scales)

# Notes:
# - This script uses relative paths assuming you run it from this file's
#   directory (i.e., `code/paper/`).
# - Output is written to `output/paper/fig1/`.

source("code/R/paths.R")
DB_PATH <- file.path(DATA, "database.db")

OUTPUT_DIR <- file.path(ROOT, "output/paper/fig1")
if (!dir.exists(OUTPUT_DIR)) dir.create(OUTPUT_DIR, recursive = TRUE)

con <- dbConnect(duckdb::duckdb(), dbdir = DB_PATH, read_only = TRUE)

refund_change_raw <- dbGetQuery(con, "
    SELECT
        CAST(imp_salt_ded_2017       AS DOUBLE) AS salt_ded_2017,
        CAST(imp_salt_ded_2018       AS DOUBLE) AS salt_ded_2018,
        CAST(imp_itemizable_2017     AS DOUBLE) AS imp_itemizable_2017,
        CAST(imp_itemizable_2018     AS DOUBLE) AS imp_itemizable_2018,
        CAST(imp_marginal_fed_rate   AS DOUBLE) AS mtr
    FROM acs_microdata
    WHERE pernum = 1 AND gq IN (1, 2, 5)
      AND marst = 1
      AND imp_salt_ded_2017      IS NOT NULL
      AND imp_salt_ded_2018      IS NOT NULL
      AND imp_marginal_fed_rate  IS NOT NULL
      AND year BETWEEN 2013 AND 2023
") |>
  as_tibble() |>
  mutate(refund_diff = (salt_ded_2017 - salt_ded_2018) * mtr)

dbDisconnect(con, shutdown = TRUE)

bin_size <- 500L
x_max    <- 30000L

cut_2017      <- 12700L
cut_2017_lab  <- "2017 std ded ($12.7k)"
cut_tcja      <- 24000L
cut_tcja_lab  <- "TCJA std ded ($24k)"

diff_sum_2017 <- refund_change_raw |>
  filter(
    imp_itemizable_2017 >= 0,
    imp_itemizable_2017 <= x_max
  ) |>
  mutate(
    x_bin = floor(imp_itemizable_2017 / bin_size) * bin_size +
      bin_size / 2L + 250L
  ) |>
  group_by(x_bin) |>
  summarise(mean_diff = mean(refund_diff, na.rm = TRUE), .groups = "drop")

diff_sum_2018 <- refund_change_raw |>
  filter(
    imp_itemizable_2018 >= 0,
    imp_itemizable_2018 <= x_max
  ) |>
  mutate(
    x_bin = floor(imp_itemizable_2018 / bin_size) * bin_size +
      bin_size / 2L + 250L
  ) |>
  group_by(x_bin) |>
  summarise(mean_diff = mean(refund_diff, na.rm = TRUE), .groups = "drop")

theme_10j <- theme(
  panel.background = element_rect(fill = "white", color = NA),
  plot.background  = element_rect(fill = "white", color = NA),
  panel.border     = element_blank(),
  panel.grid.major = element_line(color = "grey90"),
  panel.grid.minor = element_blank(),
  axis.line.x      = element_line(color = "grey30"),
  axis.line.y      = element_line(color = "grey30"),
  axis.text        = element_text(color = "grey20"),
  text             = element_text(color = "grey20")
)

x_scale <- scale_x_continuous(
  labels = scales::dollar_format(scale = 1e-3, suffix = "k"),
  breaks = seq(0, x_max, by = 5000),
  limits = c(0, x_max)
)

y_scale <- scale_y_continuous(labels = scales::dollar_format())

plot_1 <- ggplot(diff_sum_2017, aes(x_bin, mean_diff)) +
  annotate(
    "rect",
    xmin = cut_2017 * 0.9,
    xmax = cut_2017 * 1.1,
    ymin = -Inf,
    ymax = Inf,
    fill = "#4a7c82",
    alpha = 0.12
  ) +
  geom_hline(yintercept = 0, color = "grey70") +
  geom_vline(xintercept = cut_2017, linetype = "dotted", color = "#4a7c82",
             linewidth = 0.7) +
  geom_line(linewidth = 0.9, color = "#23373b") +
  geom_point(size = 1.5, color = "#23373b") +
  annotate(
    "text",
    x = cut_2017,
    y = Inf,
    label = cut_2017_lab,
    hjust = -0.07,
    vjust = 1.3,
    size = 3.8,
    color = "#4a7c82"
  ) +
  x_scale + y_scale +
  labs(
    x = "Itemizable expenses under pre-TCJA rules ($)",
    y = "Mean reduction in benefit",
    title = "Change in SALT tax benefit under TCJA, by itemizable total"
  ) +
  theme_10j

ggsave(
  file.path(OUTPUT_DIR, "fig1.png"),
  plot_1,
  width = 7,
  height = 5,
  dpi = 150
)

plot_appendix <- ggplot(diff_sum_2018, aes(x_bin, mean_diff)) +
  annotate(
    "rect",
    xmin = cut_tcja * 0.9,
    xmax = cut_tcja * 1.1,
    ymin = -Inf,
    ymax = Inf,
    fill = "#c06028",
    alpha = 0.12
  ) +
  geom_hline(yintercept = 0, color = "grey70") +
  geom_vline(xintercept = cut_tcja, linetype = "dashed", color = "#c06028",
             linewidth = 0.7) +
  geom_line(linewidth = 0.9, color = "#23373b") +
  geom_point(size = 1.5, color = "#23373b") +
  annotate(
    "text",
    x = cut_tcja,
    y = Inf,
    label = cut_tcja_lab,
    hjust = -0.07,
    vjust = 1.3,
    size = 3.8,
    color = "#c06028"
  ) +
  x_scale + y_scale +
  labs(
    x = "Itemizable expenses under TCJA rules ($)",
    y = "Mean reduction in benefit",
    title = "Change in SALT tax benefit under TCJA, by itemizable total"
  ) +
  theme_10j

ggsave(
  file.path(OUTPUT_DIR, "fig1_appendix.png"),
  plot_appendix,
  width = 7,
  height = 5,
  dpi = 150
)

