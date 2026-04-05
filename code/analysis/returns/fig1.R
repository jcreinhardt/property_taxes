# Clean and load
gc()
rm(list = ls())
getwd()

# Load libraries
library(tidyverse)
library(stringr)
library(ggplot2)
library(tigris)
library(duckdb)
library(fixest)
library(viridis)

# Load from database
con <- dbConnect(duckdb::duckdb(), dbdir = "../../../data/database.db")
state_returns <- dbGetQuery(con, "select * from returns_state")

# Compute share of itemizers, share of SALT deducters 
# and share of capped SALT deductors over time
state_returns |> 
    reframe( 
        share_itemizers = 100 * sum(returns_w_items) / sum(returns),
        share_state_income_taxes = 100 * sum(state_and_local_income_taxes_returns) / sum(returns),
        share_state_sales_taxes = 100 * sum(state_and_local_sales_tax_returns, na.rm = TRUE) / sum(returns),
        share_property_taxes = 100 * sum(property_taxes_returns) / sum(returns),
        .by = c(year)
    ) |> 
    pivot_longer(cols = c(share_itemizers, share_state_income_taxes, share_property_taxes), names_to = "type", values_to = "share") |> 
    ggplot(aes(x = year, y = share, color = type, alpha = type, group = type)) +
    geom_line() +
    geom_point() +
    scale_alpha_manual(values = c("share_itemizers" = 1, "share_state_income_taxes" = 0.85, "share_property_taxes" = 0.95), guide = "none") +
    geom_vline(xintercept = 2017.5, linetype = "dashed", color = "grey30") +
    scale_color_manual(
        values = c(
            "share_itemizers" = "#23373b",
            "share_state_income_taxes" = "#14B03D",
            "share_property_taxes" = "#eb811b"
        ),
        labels = c(
            "share_itemizers" = "Itemized",
            "share_state_income_taxes" = "Income Tax",
            "share_property_taxes" = "Property Tax"
        )
    ) +
    labs(
        x = "",
        y = "Percent of returns (%)",
        color = NULL
    ) +
    theme(
        panel.background = element_rect(fill = "#fafafa", color = NA),
        plot.background = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        axis.line.x = element_line(color = "grey30"),
        axis.line.y = element_line(color = "grey30"),
        legend.position = "right",
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.box.background = element_rect(fill = "#fafafa", color = "grey30"),
        legend.key = element_rect(fill = "#fafafa", color = NA),
        legend.text = element_text(size = 7),
        axis.text.x = element_text(color = "grey20"),
        axis.text.y = element_text(color = "grey20"),
        text = element_text(color = "grey20"),
        ) +
    scale_x_continuous(breaks = seq(2012, 2022, by = 2), limits = c(2012, 2022)) +
    scale_y_continuous(limits = c(0, 40), expand = expansion(mult = c(0, 0.05)))
ggsave("../../../output/fig1.png", width = 16, height = 8, units = "cm")

