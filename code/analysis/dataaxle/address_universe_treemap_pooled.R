# L-shaped "treemap" of the street-address universes pooled over 2012-2021: CoreLogic tax (x) vs Data Axle (y).
# Matched addresses (same loose key in both sources, in any year, not necessarily the same) form the square in the corner.
# Tax-only addresses extend it to the right (widths add up to the tax count), DA-only addresses extend it upwards
# (heights add up to the DA count). Each box is one cause; boxes are ordered by size, largest next to the square.
# Inputs (Bouchet, tm_pooled.sql / tm_pooled_tax.sql): tm_totals.csv, tm_da_classes.csv, tm_tax_classes.csv.
# Loose key = ZIP5 + street name + house number. 2019-only version: address_universe_treemap.R. Run from the repo root.

# Clean up workspace
gc()
rm(list = ls())

# Load libraries
library(tidyverse)

# Per-script output folder
source("code/R/paths.R")
OUTPUT_DIR <- file.path(ROOT, "output/analysis/dataaxle/address_universe_treemap")
if (!dir.exists(OUTPUT_DIR)) dir.create(OUTPUT_DIR, recursive = TRUE)

tot <- read_csv(file.path(OUTPUT_DIR, "tm_totals.csv"), show_col_types = FALSE)
MATCHED <- tot$n_both / 1e6

# Cause groups (colours), shared across both sides
GROUPS <- c("Tax roll does not cover it"                         = "#8C8C8C",
            "Same property, recorded differently"                = "#4C72B0",
            "Not residential, empty, or not a primary residence" = "#C44E52",
            "New construction"                                   = "#8172B3",
            "Unexplained / not testable"                         = "#CCB974")

# Data Axle addresses not in tax --------------------------------------------------------------
da_raw <- read_csv(file.path(OUTPUT_DIR, "tm_da_classes.csv"), show_col_types = FALSE)
da_only <- da_raw |>
    mutate(label = case_when(
               fcls == "1_county_not_in_tax" ~ "County not in tax roll",
               str_detect(fcls, "no_parcel_50m|^7a_|^8_") ~ "Parcel or area missing from roll\n(no parcel ≤ 50 m, unnumbered street, ZIP absent)",
               str_detect(fcls, "^2_|^5_|multi_number|corner") ~ "Other number on same parcel / street\n(1 char off, multi-number, corner, range)",
               str_detect(fcls, "^3_|^4_") ~ "ZIP or street-name near miss",
               str_detect(fcls, "no_link") ~ "Parcel nearby, no link found",
               str_detect(fcls, "not_testable") ~ "Coarse geocode, not testable"),
           group = case_when(
               str_detect(label, "^County|^Parcel or area") ~ "Tax roll does not cover it",
               str_detect(label, "^Other number|near miss") ~ "Same property, recorded differently",
               TRUE ~ "Unexplained / not testable"),
           evidence = case_when(
               str_detect(label, "^County|^Parcel or area") ~ "direct",
               str_detect(label, "^Other number|near miss") ~ "candidate",
               str_detect(label, "no link") ~ "ambiguous",
               TRUE ~ "unverified")) |>
    summarise(m = sum(n) / 1e6, .by = c(label, group, evidence))

# Tax addresses not in Data Axle -------------------------------------------------------------
tax_raw <- read_csv(file.path(OUTPUT_DIR, "tm_tax_classes.csv"), show_col_types = FALSE)
tax_only <- tax_raw |>
    mutate(label = case_when(
               str_detect(cls, "^2_|^3_|^5_|^6_") ~ "Non-residential, no structure, absentee / company owner,\nor USPS vacant / undeliverable",
               str_detect(cls, "^1_") ~ "DA address 1 char off / DA point ≤ 15 m",
               str_detect(cls, "^4_") ~ "Built 2020+",
               cls == "7_other" & pic == 10 ~ "Other single-family",
               cls == "7_other" ~ "Other condo, 2–4 unit, apartment"),
           group = case_when(
               str_detect(cls, "^2_|^3_|^5_|^6_") ~ "Not residential, empty, or not a primary residence",
               str_detect(cls, "^1_") ~ "Same property, recorded differently",
               str_detect(cls, "^4_") ~ "New construction",
               TRUE ~ "Unexplained / not testable"),
           evidence = case_when(
               str_detect(cls, "^1_") ~ "candidate",
               str_detect(cls, "^2_|^3_|^5_|^6_") ~ "direct / correlate",
               str_detect(cls, "^4_") ~ "direct",
               TRUE ~ "unexplained")) |>
    summarise(m = sum(n) / 1e6, .by = c(label, group, evidence))

# Composition of the big tax-only box, for the doc
big_parts <- tax_raw |> filter(str_detect(cls, "^2_|^3_|^5_|^6_")) |> summarise(m = sum(n) / 1e6, .by = cls) |> arrange(desc(m))
write_csv(big_parts, file.path(OUTPUT_DIR, "tax_only_big_group_parts.csv"))

# Order by size, largest next to the matched square
da_only  <- da_only  |> arrange(desc(m)) |> mutate(xmin = 0, xmax = MATCHED, ymax = MATCHED + cumsum(m), ymin = ymax - m)
tax_only <- tax_only |> arrange(desc(m)) |> mutate(ymin = 0, ymax = MATCHED, xmax = MATCHED + cumsum(m), xmin = xmax - m)
stopifnot(abs(sum(da_only$m) - tot$n_da_only / 1e6) < 1e-6, abs(sum(tax_only$m) - tot$n_tax_only / 1e6) < 1e-6)
write_csv(bind_rows(da_only |> mutate(side = "da_only"), tax_only |> mutate(side = "tax_only")),
          file.path(OUTPUT_DIR, "address_universe_boxes_2012_2021.csv"))

lab <- function(df) sprintf("%s  %.1fm (%s)", df$label, df$m, df$evidence)

# lo = where both axes start. lo = 0 shows the full matched square; lo > 0 cuts it so the strips are readable
# (box sizes stay to scale with each other).
plot_l <- function(lo, txt) {
    xt <- MATCHED + sum(tax_only$m); yt <- MATCHED + sum(da_only$m)
    brk <- function(top) sort(unique(c(if (lo == 0) c(0, 25, 50, 75) else seq(lo, MATCHED - 5, by = 5), MATCHED, top)))
    ggplot() +
        geom_rect(aes(xmin = lo, xmax = MATCHED, ymin = lo, ymax = MATCHED), fill = "grey92", colour = "white") +
        annotate("text", x = (lo + MATCHED) / 2, y = (lo + MATCHED) / 2, size = 6,
                 label = sprintf("Matched (any year)\n%.1fm addresses\n(%.0f%% of Data Axle, %.0f%% of tax)",
                                 MATCHED, 100 * tot$n_both / tot$n_da, 100 * tot$n_both / tot$n_tax)) +
        geom_rect(data = da_only, aes(xmin = lo, xmax = xmax, ymin = ymin, ymax = ymax, fill = group), colour = "white", linewidth = 0.4) +
        geom_rect(data = tax_only, aes(xmin = xmin, xmax = xmax, ymin = lo, ymax = ymax, fill = group), colour = "white", linewidth = 0.4) +
        # DA strip: horizontal labels inside; one-line label for thin boxes
        geom_text(data = da_only, aes(x = lo + 0.4, y = (ymin + ymax) / 2,
                                      label = if_else(m > 1.2 | lo > 0, lab(da_only), gsub("\n.*", "", lab(da_only)))),
                  hjust = 0, size = txt, lineheight = 0.85) +
        # Tax strip: vertical labels inside
        geom_text(data = tax_only, aes(x = (xmin + xmax) / 2, y = lo + 0.4, label = if_else(m >= 0.3, lab(tax_only), "")),
                  angle = 90, hjust = 0, size = txt, lineheight = 0.85) +
        scale_fill_manual(values = GROUPS, breaks = names(GROUPS), drop = FALSE) +
        scale_x_continuous(breaks = brk(xt), labels = function(b) sprintf("%g", round(b, 1))) +
        scale_y_continuous(breaks = brk(yt), labels = function(b) sprintf("%g", round(b, 1))) +
        coord_equal(xlim = c(lo, xt), ylim = c(lo, yt), expand = FALSE) +
        labs(x = sprintf("CoreLogic tax addresses (millions)%s", if (lo > 0) sprintf(", axis starts at %gm", lo) else ""),
             y = sprintf("Data Axle addresses (millions)%s", if (lo > 0) sprintf(", axis starts at %gm", lo) else ""), fill = NULL,
             title = "Street addresses seen in any year 2012–2021: CoreLogic tax roll vs Data Axle",
             subtitle = sprintf(paste("Matched = same address in both sources in any year (%.1fm also in a common year).",
                                      "Boxes to the right: in tax, never in Data Axle (%.1fm). Boxes on top: in Data Axle, never in tax (%.1fm).",
                                      "Box length = count; boxes ordered by size.", sep = "\n"),
                                tot$n_both_same_year / 1e6, tot$n_tax_only / 1e6, tot$n_da_only / 1e6),
             caption = paste("Address = ZIP5 + street name + house number; attributes from the latest year seen. Evidence in brackets: direct = the field or",
                             "coverage fact is the cause; candidate = a plausible counterpart record exists; correlate = attributes consistent with the cause",
                             "(absentee / company owner); ambiguous/unverified = no test possible.",
                             paste("Not labelled (thin boxes, far right):", paste(lab(tax_only |> filter(m < 0.3)), collapse = "; ")), sep = "\n")) +
        theme_minimal(base_size = 11) +
        theme(panel.grid = element_blank(), legend.position = "right", plot.caption = element_text(hjust = 0, size = 8),
              axis.ticks = element_line())
}

ggsave(file.path(OUTPUT_DIR, "address_universe_treemap_2012_2021.png"), plot_l(0, 2.6),
       width = 13, height = 11, dpi = 170, bg = "white", device = ragg::agg_png)
ggsave(file.path(OUTPUT_DIR, "address_universe_treemap_2012_2021_zoom.png"), plot_l(75, 3.2),
       width = 14, height = 12, dpi = 170, bg = "white", device = ragg::agg_png)

print(da_only |> select(label, m, evidence), n = 20)
print(tax_only |> select(label, m, evidence), n = 20)
print(big_parts)
