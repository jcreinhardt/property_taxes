# L-shaped "treemap" of the 2019 street-address universes: CoreLogic tax (x) vs Data Axle (y).
# Matched addresses form the square in the corner. Tax-only addresses extend it to the right (their widths add up
# to the tax count), DA-only addresses extend it upwards (heights add up to the DA count). Each box is one cause.
# Counts (millions, loose key = ZIP5 + street name + house number) come from the "Breakdown with evidence levels"
# tables in text/data_exploration/dataaxle.md (§4.7): addr_2019_full.sql, resid_2019_full.sql, geo_2019_full.sql,
# taxonly_sfr_2019_full.sql. Run from the repo root.

# Clean up workspace
gc()
rm(list = ls())

# Load libraries
library(tidyverse)

# Per-script output folder
source("code/R/paths.R")
OUTPUT_DIR <- file.path(ROOT, "output/validation/dataaxle/address_universe_treemap")
if (!dir.exists(OUTPUT_DIR)) dir.create(OUTPUT_DIR, recursive = TRUE)

MATCHED <- 76.5  # in both; DA 92.1m, tax 98.6m

# Cause groups, shared across both sides
GROUPS <- c("Tax roll does not cover it"              = "#8C8C8C",
            "Same property, recorded differently"      = "#4C72B0",
            "Not a residence"                          = "#C44E52",
            "Not a primary residence / empty"          = "#DD8452",
            "Data Axle timing (other year, new build)" = "#8172B3",
            "Unexplained / not testable"               = "#CCB974")

# Data Axle addresses not in tax (15.6m)
da_only <- tribble(
    ~label,                                          ~m,   ~group,                                     ~evidence,
    "County not in tax roll",                        5.43, "Tax roll does not cover it",               "direct",
    "Parcel or area missing from roll\n(no parcel ≤ 50 m, unnumbered street, ZIP absent)",
                                                     1.01, "Tax roll does not cover it",               "direct",
    "Other number on same parcel / street\n(1 char off, multi-number, corner, range)",
                                                     3.51, "Same property, recorded differently",      "candidate",
    "ZIP or street-name near miss",                  0.45, "Same property, recorded differently",      "candidate",
    "Parcel nearby, no link found",                  0.99, "Unexplained / not testable",               "ambiguous",
    "Coarse geocode, not testable",                  4.21, "Unexplained / not testable",               "unverified")

# Tax addresses not in Data Axle (22.1m)
tax_only <- tribble(
    ~label,                                          ~m,   ~group,                                     ~evidence,
    "Vacant land",                                   3.46, "Not a residence",                          "direct",
    "Commercial",                                    1.32, "Not a residence",                          "direct",
    "Agricultural",                                  0.88, "Not a residence",                          "direct",
    "Other non-residential",                         2.47, "Not a residence",                          "direct",
    "SFR without structure",                         1.65, "Not a residence",                          "direct",
    "SFR, USPS vacant / undeliverable",              0.53, "Not a primary residence / empty",          "direct",
    "SFR, absentee or company owner",                2.86, "Not a primary residence / empty",          "correlate",
    "SFR, in DA 2017 or 2021 only",                  4.52, "Data Axle timing (other year, new build)", "candidate",
    "SFR built 2018+",                               0.23, "Data Axle timing (other year, new build)", "direct",
    "SFR, DA address 1 char off / ≤ 15 m",           1.76, "Same property, recorded differently",      "candidate",
    "SFR, other (42% PO-box mail)",                  1.33, "Unexplained / not testable",               "unexplained",
    "Condo, 2–4 unit, apartment",                    1.04, "Unexplained / not testable",               "not characterised")

# Order boxes by group so colours sit together; DA strip stacked upwards, tax strip left to right
ord <- function(df) df |> mutate(group = factor(group, levels = names(GROUPS))) |> arrange(group, desc(m))
da_only  <- ord(da_only)  |> mutate(xmin = 0, xmax = MATCHED, ymax = MATCHED + cumsum(m), ymin = ymax - m)
tax_only <- ord(tax_only) |> mutate(ymin = 0, ymax = MATCHED, xmax = MATCHED + cumsum(m), xmin = xmax - m)
boxes <- bind_rows(
    tibble(label = "Matched: in both", m = MATCHED, group = NA, evidence = NA, xmin = 0, xmax = MATCHED, ymin = 0, ymax = MATCHED),
    da_only |> mutate(side = "da"), tax_only |> mutate(side = "tax"))
write_csv(boxes, file.path(OUTPUT_DIR, "address_universe_boxes.csv"))

lab <- function(df) sprintf("%s  %.1fm (%s)", df$label, df$m, df$evidence)

# lo = where both axes start. lo = 0 shows the full matched square; lo > 0 cuts it so the strips are readable
# (box sizes stay to scale with each other).
plot_l <- function(lo, txt) {
    xt <- MATCHED + sum(tax_only$m); yt <- MATCHED + sum(da_only$m)
    brk <- function(top) sort(unique(c(if (lo == 0) c(0, 25, 50) else seq(lo, MATCHED - 5, by = 5), MATCHED, top)))
    ggplot() +
        geom_rect(aes(xmin = lo, xmax = MATCHED, ymin = lo, ymax = MATCHED), fill = "grey92", colour = "white") +
        annotate("text", x = (lo + MATCHED) / 2, y = (lo + MATCHED) / 2, size = 6,
                 label = sprintf("Matched\n%.1fm addresses\n(83%% of Data Axle, 78%% of tax)", MATCHED)) +
        geom_rect(data = da_only, aes(xmin = lo, xmax = xmax, ymin = ymin, ymax = ymax, fill = group), colour = "white", linewidth = 0.4) +
        geom_rect(data = tax_only, aes(xmin = xmin, xmax = xmax, ymin = lo, ymax = ymax, fill = group), colour = "white", linewidth = 0.4) +
        # DA strip: horizontal labels inside; one-line label for thin boxes
        geom_text(data = da_only, aes(x = lo + 0.4, y = (ymin + ymax) / 2,
                                      label = if_else(m > 1.2 | lo > 0, lab(da_only), gsub("\n.*", "", lab(da_only)))),
                  hjust = 0, size = txt, lineheight = 0.85) +
        # Tax strip: vertical labels inside
        geom_text(data = tax_only, aes(x = (xmin + xmax) / 2, y = lo + 0.4, label = lab(tax_only)), angle = 90, hjust = 0, size = txt) +
        scale_fill_manual(values = GROUPS, breaks = names(GROUPS), drop = FALSE) +
        scale_x_continuous(breaks = brk(xt), labels = function(b) sprintf("%g", round(b, 1))) +
        scale_y_continuous(breaks = brk(yt), labels = function(b) sprintf("%g", round(b, 1))) +
        coord_equal(xlim = c(lo, xt), ylim = c(lo, yt), expand = FALSE) +
        labs(x = sprintf("CoreLogic tax addresses (millions)%s", if (lo > 0) sprintf(", axis starts at %gm", lo) else ""),
             y = sprintf("Data Axle addresses (millions)%s", if (lo > 0) sprintf(", axis starts at %gm", lo) else ""), fill = NULL,
             title = "Street addresses, 2019: CoreLogic tax roll vs Data Axle",
             subtitle = paste("Boxes to the right: in tax, not in Data Axle (22.1m). Boxes on top: in Data Axle, not in tax (15.6m).",
                              "Box length = count.", sep = "\n"),
             caption = paste("Address = ZIP5 + street name + house number. Evidence in brackets: direct = the field or coverage fact is the cause;",
                             "candidate = a plausible counterpart record exists; correlate = attributes consistent with the cause; ambiguous/unverified = no test.",
                             "SFR = single-family parcel (property indicator 10).", sep = "\n")) +
        theme_minimal(base_size = 11) +
        theme(panel.grid = element_blank(), legend.position = "right", plot.caption = element_text(hjust = 0, size = 8),
              axis.ticks = element_line())
}

ggsave(file.path(OUTPUT_DIR, "address_universe_treemap.png"), plot_l(0, 2.6),
       width = 13, height = 11, dpi = 170, bg = "white", device = ragg::agg_png)
ggsave(file.path(OUTPUT_DIR, "address_universe_treemap_zoom.png"), plot_l(55, 3.2),
       width = 14, height = 12, dpi = 170, bg = "white", device = ragg::agg_png)
