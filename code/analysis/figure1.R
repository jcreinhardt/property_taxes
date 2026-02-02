# Clean up
gc()
rm(list = ls())
getwd()

# Load libraries
library(duckdb)
library(tidycensus)
library(data.table)
library(dplyr)
library(tidyr)
library(tidyverse)
library(purrr)
library(ggplot2)
library(fixest)
library(stringr)
library(rdrobust)
library(binsreg)
library(sf)
library(tigris)

# Connect to corelogic DB
db_path <- "/gpfs/gibbs/pi/lapoint/corelogic/database/corelogic.db"
con <- dbConnect(duckdb::duckdb(), db_path, read_only = TRUE)
dbExecute(con, "set temp_directory = '../../temp/'")

dbGetQuery(con, "select current_setting('threads') as threads,current_setting('memory_limit') as memory_limit;")

# Query panel of properties with 
# - taxes due
# - transactions
# - years 2014 through March 2020
q <- "
with transacted_properties as (
    select clip, year(sale_derived_date) as year
    from ownertransfer
    where 
        (year between 2014 and 2019 or year = 2020 and month(sale_derived_date) <= 3) and
        fips_code // 1000 = 33 
    group by clip, year
)
select 
    t.clip,
    t.tax_year,
    t.fips_code,
    t.total_tax_amount,
    case when tp.year is not null then 1 else 0 end as transacted
from tax t
left join transacted_properties tp
    on t.clip = tp.clip and t.tax_year = tp.year
where 
    ((t.tax_year between 2014 and 2019 or t.tax_year = 2020 and month(t.sale_derived_date) <= 3) and
    t.property_indicator_code = 10 and
    t.owner_occupancy_code = 'O' and
    t.total_tax_amount > 0 and
    t.fips_code // 1000 = 33
"
df <- dbGetQuery(con, q)




############################################################################################################################
# - Method 1: RDD over time - ##############################################################################################
############################################################################################################################

# For an RDD object, extract coefficients and confidence intervals
extract_coefs <- function(results) {
    c(
        coef = results$Estimate[1], 
        lower = results$ci[1,1],
        upper = results$ci[1,2]
    )
}

# Estimate RDD for each year
estimate_rd <- function(data, year) {
    rdrobust(
        y = 100 * data$transacted[data$tax_year == year],
        x = data$total_tax_amount[data$tax_year == year],
        c = 10000
    ) |> extract_coefs()
}

# Loop over years and store results
years <- 2014:2024
estimates <- map_df(years, function(year) {
    c(year = year, estimate_rd(df, year))
})

# Check
estimates

# Plot
ggplot(estimates, aes(x = year, y = coef)) +
    geom_point() +
    geom_errorbar(aes(ymin = lower, ymax = upper), width = 0.2) +
    geom_hline(yintercept = 0, linetype = "solid", color = "black") +
    geom_vline(xintercept = 2017.5, linetype = "dashed", color = "red") +
    labs(
        title = "RDD estimate at deductibility cutoff ($10,000)",
        x = "Year",
        y = "Change in transaction probability (%)"
    ) +
    theme_classic() +
    scale_x_continuous(breaks = seq(2014, 2024, by = 2)) +
    annotate("text", x = 2015, y = max(estimates$upper) * 0.9, 
             label = "Baseline outcome: 2%", 
             hjust = 0, vjust = 1, 
             size = 3, 
             box.colour = "black", 
             box.padding = unit(0.3, "lines"))
ggsave("../../output/RDD.png")





############################################################################################################################
# - Method 2: DiD or two-dimensional binscatter - ##############################################################################################
############################################################################################################################

values <- df |> 
    mutate(
        transacted = 100 * transacted,
        rank = percent_rank(total_tax_amount),
        .by = tax_year
    ) |> 
    mutate(
        bin = rank %/% 0.01
    ) |>
    reframe(
        avg_tax = mean(total_tax_amount),
        .by = c(bin, tax_year)
    )

pctile_coefs <- df |> 
    mutate(
        transacted = 100 * transacted,
        period = ifelse(tax_year <= 2017, "Pre-2018", "Post-2018"),
        rank = percent_rank(total_tax_amount),
        .by = tax_year
    ) |>
    filter(rank >= .76) |>
    mutate(
        bin = 4 * (rank %/% 0.04),
        transacted = transacted - mean(transacted),
        .by = period
    ) |>
    feols(
        transacted ~ i(bin, period) - 1
    ) |> 
    coeftable() |>
    as.data.frame() |>
    mutate(
        lower = Estimate - 1.96 * `Std. Error`,
        upper = Estimate + 1.96 * `Std. Error`
    ) |>
    tibble::rownames_to_column("coef_name") |>
    mutate(
        bin = as.numeric(str_extract(coef_name, "(?<=bin::)\\d+")),
        period = str_extract(coef_name, "(?<=period::)[^:]+")
    ) 

pctile_coefs |>
    mutate(bin = bin + ifelse(period == "Post-2018", .2, 0)) |> 
    ggplot(aes(x = bin, y = Estimate, color = period)) +
        geom_point() +
        geom_errorbar(aes(ymin = lower, ymax = upper), width = 0.2) +
        annotate("rect", xmin = 89, xmax = 91, ymin = -Inf, ymax = Inf, alpha = 0.8, fill = "gray") +
        labs(
            x = "Property tax percentile",
            y = "Transaction probability (%)"
        ) +
        theme_classic(base_family = "", base_size = 11) +
        theme(
            legend.position = "bottom",
            plot.background = element_rect(fill = "#fafafa", color = NA),
            legend.background = element_rect(fill = "#fafafa", color = NA),
            panel.background = element_rect(fill = "#fafafa", color = NA)
        ) +
        scale_color_manual(values = c("Pre-2018" = "#eb811b", "Post-2018" = "#23373b"))
ggsave("../../output/pctiles.png", width = 15, height = 10, units = "cm" )


pctile_coefs <- df |> 
    mutate(
        transacted = 100 * transacted,
        period = case_when(
            tax_year < 2016 ~ "Pre-2016",
            tax_year >= 2016 & tax_year <= 2017 ~ "2016-2017",
            tax_year >= 2018 ~ "Post-2018"
        ),
        rank = percent_rank(total_tax_amount),
        .by = tax_year
    ) |>
    filter(rank >= .76) |>
    mutate(
        bin = 4 * (rank %/% 0.04),
        transacted = transacted - mean(transacted),
        .by = period
    ) |>
    feols(
        transacted ~ i(bin, period) - 1
    ) |> 
    coeftable() |>
    as.data.frame() |>
    mutate(
        lower = Estimate - 1.96 * `Std. Error`,
        upper = Estimate + 1.96 * `Std. Error`
    ) |>
    tibble::rownames_to_column("coef_name") |>
    mutate(
        bin = as.numeric(str_extract(coef_name, "(?<=bin::)\\d+")),
        period = str_extract(coef_name, "(?<=period::)[^:]+")
    ) 

pctile_coefs |>
    mutate(bin = bin + case_when(
        period == "2016-2017" ~ .2,
        period == "Post-2018" ~ .4,
        TRUE ~ 0
    )) |>
    ggplot(aes(x = bin, y = Estimate, color = period)) +
        geom_point() +
        geom_errorbar(aes(ymin = lower, ymax = upper), width = 0.2) +
        annotate("rect", xmin = 89, xmax = 91, ymin = -Inf, ymax = Inf, alpha = 0.8, fill = "gray") +
        labs(
            x = "Property tax percentile",
            y = "Transaction probability (%)"
        ) +
        theme_classic(base_family = "", base_size = 11) +
        theme(
            legend.position = "bottom",
            plot.background = element_rect(fill = "#fafafa", color = NA),
            legend.background = element_rect(fill = "#fafafa", color = NA),
            panel.background = element_rect(fill = "#fafafa", color = NA)
        ) +
        scale_color_manual(values = c("Pre-2016" = "#eb811b", "2016-2017" = "#14B03D", "Post-2018" = "#23373b"))
ggsave("../../output/pctiles_pretrend.png", width = 15, height = 10, units = "cm" )







############################################################################################################################
# - Method 3: IV for property prices - ##############################################################################################
############################################################################################################################

# Share of variation in taxes due to rates vs. assessments
q <- " with tax_rates as (
    select 
        tax_year,
        fips_code,
        tax_rate_area_code,
        median(total_tax_amount / calculated_total_value) as tax_rate
    from tax
    where 
        tax_year between 2016 and 2017 and
        property_indicator_code = 10 and
        total_tax_amount > 0 and
        fips_code = 4005
    group by tax_year, fips_code, tax_rate_area_code
)
select 
    t.tax_year,
    t.total_tax_amount as tax_t0,
    lag(t.total_tax_amount) over (partition by t.clip order by t.tax_year) as tax_t1,
    lag(tr.tax_rate) over (partition by t.clip order by t.tax_year) as rate_t0,
    tr.tax_rate as rate_t1,
    lag(t.calculated_total_value) over (partition by t.clip order by t.tax_year) as value_t0,
    t.calculated_total_value as value_t1,
from tax t
inner join tax_rates tr
    on t.tax_year = tr.tax_year and t.fips_code = tr.fips_code and t.tax_rate_area_code = tr.tax_rate_area_code
where 
    t.tax_year between 2016 and 2017 and
    t.property_indicator_code = 10 and
    t.total_tax_amount > 0 and
    t.fips_code = 4005
"

df <- dbGetQuery(con, q) |> filter(tax_year == 2017)

head(df)

df |> 
    filter(!is.na(delta_tax) & !is.na(rate_t0) & !is.na(rate_t1) & !is.na(value_t0) & !is.na(value_t1)) |>
    reframe(
        total_diff = sum(delta_tax, na.rm = TRUE),
        rate_change = sum((rate_t1 - rate_t0) * value_t0, na.rm = TRUE),
        assessment_change = sum((value_t1 - value_t0) * rate_t1, na.rm = TRUE),
        diff = rate_change + assessment_change
    )























############################################################################################################################
# - Method 4: Spatial RDD at state borders - ##############################################################################################
############################################################################################################################

# See files in border_rd/ for implementation

# Load results
rd <- readRDS("../../output/border_rd_results.rds")

# Load income tax rates
rates <- read.csv("../../data/state_income_tax_rates_2018.csv")

# Calculate difference for each state pair
rate_diff <- expand_grid(
    state1 = rates$Abbreviation,
    state2 = rates$Abbreviation
) |>
    left_join(
        rates %>% select(Abbreviation, Top_Marginal_Rate_Pct) %>% rename(rate1 = Top_Marginal_Rate_Pct),
        by = c("state1" = "Abbreviation")
    ) |>    
    left_join(
        rates %>% select(Abbreviation, Top_Marginal_Rate_Pct) %>% rename(rate2 = Top_Marginal_Rate_Pct),
        by = c("state2" = "Abbreviation")
    ) |>
    mutate(
        rate_diff = rate1 - rate2,
        log_diff = ifelse(rate1 != 0 & rate2 != 0, log(rate2) - log(rate1), NA)
    ) |>
    select(state1, state2, rate1, rate2, rate_diff, log_diff)

# Merge
rd <- rd %>%
    left_join(
        rate_diff,
        by = c("state1", "state2")
    ) 

# Avg. estimate for pos. vs. negative tax differentials by year
rd |> 
    filter(abs(coef) < 5 & rate1 != 0 & rate2 != 0) |>
    mutate(dummy = sum(year == 2017), .by = border_idx) |>
    filter(dummy > 0) |>
    mutate(coef = coef - coef[year == 2017], .by = border_idx) |>
    mutate(
        group = ifelse(rate_diff < 0, "Negative tax differential", "Positive tax differential")
    ) |>
    feols(coef ~ i(year, group) - 1) |>
    coeftable() |>
    as.data.frame() |>
    tibble::rownames_to_column("coef_name") |>
    mutate(
        year = as.numeric(str_extract(coef_name, "(?<=year::)\\d+")),
        group = str_extract(coef_name, "(?<=group::)[^:]+"),
        lower = Estimate - 1.96 * `Std. Error`,
        upper = Estimate + 1.96 * `Std. Error`
    ) |>
    ggplot(aes(x = year, y = Estimate, color = group)) +
        geom_point(position = position_dodge(width = 0.3)) +
        geom_errorbar(aes(ymin = lower, ymax = upper), width = 0.2, position = position_dodge(width = 0.3)) +
        geom_hline(yintercept = 0, linetype = "dashed", color = "black") +
        geom_vline(xintercept = 2017.5, linetype = "dashed", color = "red") +
        labs(
            title = "Event study: RDD estimates by tax differential group",
            x = "Year",
            y = "RDD estimate (% change in probability)",
            color = "Group"
        ) +
        theme_classic() +
        theme(legend.position = "bottom") +
        scale_color_manual(values = c("Negative tax differential" = "#0C7BDC", "Positive tax differential" = "#FFC20A"))
ggsave("../../output/border_rd_event_study_groups.png")













# Plot pre-reform RD estimates vs. state income tax rates
rd |> 
    filter(year < 2018) |>
    ggplot(aes(x = rate_diff, y = coef)) +
        geom_point() +
        geom_smooth(method = "lm", se = FALSE, color = "blue") +
        geom_hline(yintercept = 0, linetype = "solid", color = "black") +
        labs(
            title = "Border RDD estimates vs. state income tax rate differentials",
            x = "State income tax rate differential (pct. points)",
            y = "RDD estimate (% change in probability)"
        ) +
        theme_classic() +
        scale_x_continuous(breaks = seq(-5, 5, by = 1)) +
        xlim(-5, 5) 
ggsave("../../output/border_rd_scatter_pre.png")

# Plot post-reform RD estimates vs. state income tax rates
rd |> 
    filter(year >= 2018) |>
    ggplot(aes(x = rate_diff, y = coef)) +
        geom_point() +
        geom_smooth(method = "lm", se = FALSE, color = "blue") +
        geom_hline(yintercept = 0, linetype = "solid", color = "black") +
        labs(
            title = "Border RDD estimates vs. state income tax rate differentials",
            x = "State income tax rate differential (pct. points)",
            y = "RDD estimate (% change in probability)"
        ) +
        theme_classic() +
        scale_x_continuous(breaks = seq(-5, 5, by = 1)) +
        xlim(-5, 5) 
ggsave("../../output/border_rd_scatter_post.png")

# Plot both with two separate lines
rd |> 
    mutate(
        period = ifelse(year < 2018, "Pre-2018", NA)
    ) |>
    filter(!is.na(period)) |>
    reframe(
        coef = mean(coef),
        rate_diff = mean(rate_diff),
        .by = c(border_idx, period)
    ) |>
    ggplot(aes(x = rate_diff, y = coef, color = period, shape = period, group = period)) +
        geom_point() +
        geom_smooth(method = "lm", se = FALSE, aes(group = period, color = period)) +
        geom_hline(yintercept = 0, linetype = "dashed", color = "#14B03D") +
        annotate("text", x = -9.6, y = 3.55, label = "IA-SD", color = "black", size = 3, hjust = 0, vjust = 0) +
        annotate("text", x = 6.75, y = -4.25, label = "NV-ID", color = "black", size = 3, hjust = 0, vjust = 0) +
        annotate("text", x = -9.5, y = -3.3, label = "CA-AZ", color = "black", size = 3, hjust = 0, vjust = 0) +
        labs(
            x = "State income tax rate differential (pct. points)",
            y = "RDD estimate (% change in probability)",
            color = "Period",
            shape = "Period"
        ) +
        theme_classic() +
        theme(
            legend.position = "bottom",
            plot.background = element_rect(fill = "#fafafa", color = NA),
            legend.background = element_rect(fill = "#fafafa", color = NA),
            panel.background = element_rect(fill = "#fafafa", color = NA)
        ) +
        scale_color_manual(values = c("Pre-2018" = "#eb811b", "Post-2018" = "#23373b"), labels = c("Pre-2018", "Post-2018")) +
        scale_shape_manual(values = c("Pre-2018" = 16, "Post-2018" = 17), labels = c("Pre-2018", "Post-2018"))
ggsave("../../output/border_rd_scatter_pre.png", width = 15, height = 10, units = "cm" )

rd |> 
    mutate(
        period = ifelse(year < 2018, "Pre-2018", "Post-2018")
    ) |>
    reframe(
        coef = mean(coef),
        rate_diff = mean(rate_diff),
        .by = c(border_idx, period)
    ) |>
    ggplot(aes(x = rate_diff, y = coef, color = period, shape = period, group = period)) +
        geom_point() +
        geom_smooth(method = "lm", se = FALSE, aes(group = period, color = period)) +
        geom_hline(yintercept = 0, linetype = "dashed", color = "#14B03D") +
        annotate("text", x = -9.6, y = 3.55, label = "IA-SD", color = "black", size = 3, hjust = 0, vjust = 0) +
        annotate("text", x = 6.75, y = -4.25, label = "NV-ID", color = "black", size = 3, hjust = 0, vjust = 0) +
        annotate("text", x = -9.5, y = -3.3, label = "CA-AZ", color = "black", size = 3, hjust = 0, vjust = 0) +
        labs(
            x = "State income tax rate differential (pct. points)",
            y = "RDD estimate (% change in probability)",
            color = "Period",
            shape = "Period"
        ) +
        theme_classic() +
        theme(
            legend.position = "bottom",
            plot.background = element_rect(fill = "#fafafa", color = NA),
            legend.background = element_rect(fill = "#fafafa", color = NA),
            panel.background = element_rect(fill = "#fafafa", color = NA)
        ) +
        scale_color_manual(values = c("Pre-2018" = "#eb811b", "Post-2018" = "#23373b"), labels = c("Pre-2018", "Post-2018")) +
        scale_shape_manual(values = c("Pre-2018" = 16, "Post-2018" = 17), labels = c("Pre-2018", "Post-2018"))
ggsave("../../output/border_rd_scatter.png", width = 15, height = 10, units = "cm" )


rd |> 
    mutate(
        period = case_when(
            year < 2016 ~ "Pre-2016",
            year >= 2016 & year <= 2017 ~ "2016-2017",
            year >= 2018 ~ "Post-2018"
        )
    ) |>
    reframe(
        coef = mean(coef),
        rate_diff = mean(rate_diff),
        .by = c(border_idx, period)
    ) |>
    ggplot(aes(x = rate_diff, y = coef, color = period, shape = period, group = period)) +
        geom_point() +
        geom_smooth(method = "lm", se = FALSE, aes(group = period, color = period)) +
        geom_hline(yintercept = 0, linetype = "dashed", color = "black") +
        labs(
            x = "State income tax rate differential (pct. points)",
            y = "RDD estimate (% change in probability)",
            color = "Period",
            shape = "Period"
        ) +
        theme_classic() +
        theme(
            legend.position = "bottom",
            plot.background = element_rect(fill = "#fafafa", color = NA),
            legend.background = element_rect(fill = "#fafafa", color = NA),
            panel.background = element_rect(fill = "#fafafa", color = NA)
        ) +
        scale_color_manual(values = c("Pre-2016" = "#eb811b", "2016-2017" = "#6a9fb5", "Post-2018" = "#23373b"), labels = c("Pre-2016", "2016-2017", "Post-2018")) +
        scale_shape_manual(values = c("Pre-2016" = 16, "2016-2017" = 17, "Post-2018" = 18), labels = c("Pre-2016", "2016-2017", "Post-2018"))
ggsave("../../output/border_rd_scatter_pretrend.png", width = 15, height = 10, units = "cm" )




# Plot both distributions
rd |> 
    mutate(
        period = ifelse(year < 2018, "Pre-2018", "Post-2018"),
        group = ifelse(rate_diff < 0, "Negative tax differential", "Positive tax differential")
    ) |>
    reframe(
        coef = mean(coef),
        rate_diff = mean(rate_diff),
        group = first(group),
        .by = c(border_idx, period)
    ) |>
    pivot_wider(
        names_from = period,
        values_from = coef
    ) |>
    mutate(
        diff = `Post-2018` - `Pre-2018`
    ) |>
    ggplot(aes(x = diff, color = group, fill = group)) +
        geom_histogram(position = "dodge", alpha = 0.5, bins = 15) +
        labs(
            title = "Border RDD estimates vs. state income tax rate differentials",
            x = "Difference in transaction probability (%)",
            y = "Density",
            color = "Period",
            fill = "Period"
        ) +
        theme_classic() +
        scale_color_manual(values = c("Negative tax differential" = "#0C7BDC", "Positive tax differential" = "#FFC20A"), labels = c("Negative tax differential", "Positive tax differential")) +
        scale_fill_manual(values = c("Negative tax differential" = "#0C7BDC", "Positive tax differential" = "#FFC20A"), labels = c("Negative tax differential", "Positive tax differential"))
ggsave("../../output/border_rd_histogram.png")


rd |> 
    mutate(
        period = ifelse(year < 2018, "Pre-2018", "Post-2018")
    ) |>
    reframe(
        coef = mean(coef),
        rate_diff = mean(rate_diff),
        .by = c(border_idx, period)
    ) |>
    pivot_wider(
        names_from = period,
        values_from = coef
    ) |>
    mutate(
        diff = `Post-2018` - `Pre-2018`
    ) |>
    ggplot(aes(x = rate_diff, y = diff)) +
        geom_point() +
        geom_smooth(method = "lm", se = FALSE, color = "blue") +
        geom_hline(yintercept = 0, linetype = "solid", color = "black") +
        labs(
            title = "Change in Border RDD estimates vs. state income tax rate differentials",
            x = "State income tax rate differential (pct. points)",
            y = "Change in RDD estimate (% change in probability)"
        ) +
        theme_classic() +
        scale_x_continuous(breaks = seq(-10, 10, by = 2)) 
        # scale_y_continuous(breaks = seq(-5, 5, by = 1)) +
        # ylim(-5, 5) +
ggsave("../../output/border_rd_diff_scatter.png")

# Group into 1 tax-rate bins
rd |> 
    filter(abs(coef) < 5 & rate1 != 0 & rate2 != 0) |>
    mutate(
        rate_bin = rate_diff %/% 1,
        period = ifelse(year < 2018, "Pre-2018", "Post-2018")
    ) |>
    reframe(coef = mean(coef), .by = c(rate_bin, period)) |>
    ggplot(aes(x = rate_bin, y = coef, color = period)) +
        geom_point() +
        geom_smooth(method = "lm", se = TRUE, aes(group = period, color = period)) +
        geom_hline(yintercept = 0, linetype = "solid", color = "black") +   
        labs(
            title = "Border RDD estimates vs. state income tax rate differentials",
            x = "State income tax rate differential (pct. points)",
            y = "RDD estimate (% change in probability)",
            color = "Period"
        ) +
        theme_classic() +
        scale_x_continuous(breaks = seq(-10, 10, by = 2)) +
        scale_y_continuous(breaks = seq(-5, 5, by = 1)) +
        ylim(-5, 5) +
        scale_color_manual(values = c("Pre-2018" = "#0C7BDC", "Post-2018" = "#FFC20A"), labels = c("Pre-2018", "Post-2018"))
ggsave("../../output/border_rd_binned_scatter.png")

coefs <- rd |> 
    filter(abs(coef) < 5 & rate1 != 0 & rate2 != 0) |>
    mutate(
        rate_bin = rate_diff %/% 1,
        period = ifelse(year < 2018, "Pre-2018", "Post-2018")
    ) |>
    feols(coef ~ i(rate_bin, period) - 1) |>
    coeftable()

coefs |> 
    as.data.frame() |>
    tibble::rownames_to_column("coef_name") |>
    mutate(
        rate_bin = as.numeric(str_extract(coef_name, "(?<=rate_bin::)\\-?\\d+")),
        period = str_extract(coef_name, "(?<=period::)[^:]+"),
        lower = Estimate - 1.96 * `Std. Error`,
        upper = Estimate + 1.96 * `Std. Error`
    ) |>
    ggplot(aes(x = rate_bin, y = Estimate, color = period)) +
        geom_point() +
        geom_errorbar(aes(ymin = lower, ymax = upper), width = 0.2) +
        geom_hline(yintercept = 0, linetype = "solid", color = "black") +   
        labs(
            title = "Border RDD estimates vs. state income tax rate differentials",
            x = "State income tax rate differential (pct. points)",
            y = "RDD estimate (% change in probability)",
            color = "Period"
        ) +
        theme_classic() +
        scale_x_continuous(breaks = seq(-10, 10, by = 2)) +
        scale_y_continuous(breaks = seq(-5, 5, by = 1)) +
        ylim(-5, 5) +
        scale_color_manual(values = c("Pre-2018" = "#0C7BDC", "Post-2018" = "#FFC20A"), labels = c("Pre-2018", "Post-2018"))
ggsave("../../output/border_rd_binned_coefs.png")


# Plot both in a joint binscatter
rd |>
    mutate(
        period = ifelse(year < 2018, "Pre-2018", "Post-2018")
    ) |> 
    binsreg(
        y = coef, 
        x = rate_diff,
        by = period,
        nbins = 7,
        samebinsby = TRUE,
        ci = FALSE,
        data = _,
        plotxrange = c(-5, 5),
        plotyrange = c(-5, 5)
    )
ggsave("../../output/border_rd_binscatter.png")




rd |> 
    feols(coef ~ rate_diff) |>
    summary()
rd |> 
    filter(abs(coef) < 5) |>
    feols(coef ~ rate_diff) |>
    summary()
rd |> 
    filter(abs(coef) < 5) |>
    mutate(post_2018 = ifelse(year >= 2018, 1, 0)) |>
    feols(coef ~ i(post_2018, rate_diff)) |>
    summary()
rd |> 
    filter(abs(coef) < 5) |>
    feols(coef ~ i(year, rate_diff)) |>
    summary()

rd |> 
    filter(abs(coef) < 5) |> 
    feols(coef ~ i(year, rate_diff) | border_idx) |>
    summary()
rd |> 
    filter(abs(coef) < 5) |> 
    feols(coef ~ i(year, rate_diff) | state1 + state2) |>
    summary()
rd |> 
    filter(abs(coef) < 5) |> 
    feols(coef ~ i(year, rate_diff) | border_idx + year) |>
    summary()
rd |> 
    filter(abs(coef) < 5) |>
    mutate(post_2018 = ifelse(year >= 2018, 1, 0)) |>
    feols(coef ~ i(post_2018, rate_diff) | border_idx + post_2018) |>
    summary()

rd |> 
    feols(coef ~ log_diff) |>
    summary()
rd |> 
    filter(abs(coef) < 5) |>
    feols(coef ~ log_diff) |>
    summary()
rd |> 
    filter(abs(coef) < 5) |>
    mutate(post_2018 = ifelse(year >= 2018, 1, 0)) |>
    feols(coef ~ i(post_2018, log_diff)) |>
    summary()
rd |> 
    filter(abs(coef) < 5) |>
    feols(coef ~ i(year, log_diff)) |>
    summary()

# Event study of correlation over time
rd |> 
    filter(abs(coef) < 5) |>
    feols(coef ~ i(year, rate_diff)) |>
    coeftable() |>
    as.data.frame() |>
    tibble::rownames_to_column("coef_name") |>
    mutate(
        year = as.numeric(str_extract(coef_name, "(?<=year::)\\d+")),
        lower = Estimate - 1.96 * `Std. Error`,
        upper = Estimate + 1.96 * `Std. Error`
    ) |>
    ggplot(aes(x = year, y = Estimate)) +
        geom_point() +
        geom_errorbar(aes(ymin = lower, ymax = upper), width = 0.2) +
        geom_hline(yintercept = 0, linetype = "solid", color = "black") +   
        labs(
            title = "Event study: Correlation between changes in log tax rate and changes in RDD estimate",
            x = "Year",
            y = "Change in RDD estimate (% change in probability)"
        ) +
        theme_classic() +
        ylim(-.3, .3) +
        scale_y_continuous(breaks = seq(-.3, .3, by = .1))
ggsave("../../output/border_rd_event_study.png")











############################################################################################################################
# - Method 5: Variation in re-assessment timing - ##############################################################################################
############################################################################################################################




############################################################################################################################
# - Method 6: Correlation between changes in taxes and changes in transactions - ##############################################################################################
############################################################################################################################


# # Query total county-level housing stock
# q <- "
# select 
#     fips_code,
#     count(1) as n,
#     sum(calculated_total_value) as stock,
#     median(total_tax_amount / calculated_total_value) as tax_rate
# from tax
# where 
#     tax_year = 2022 and
#     property_indicator_code = 10 
# group by fips_code
# "
# df <- dbGetQuery(con, q) 

# # Query census data on county populations
# pop <- get_estimates(
#   geography = "county",
#   product = "population",
#   vintage = 2022,
#   geometry = FALSE  
# ) |>
#     mutate(GEOID = as.numeric(GEOID)) |>
#     filter(variable == "POPESTIMATE") |>
#     as.data.frame()

# # Merge the two datasets
# setdiff(df$fips_code, pop$GEOID)  
# setdiff(pop$GEOID, df$fips_code)

# df <- df %>%
#   inner_join(
#     pop,
#     by = c("fips_code" = "GEOID")
#   ) %>% 
#   mutate(
#     stock_pc = stock / n,
#     housing_pc = n / value
#   )

# # Calculate correlations
# df |>
#     filter(tax_rate <= .1) |>
#     filter(stock_pc < quantile(df$stock_pc, .99)) |>
#     feols(
#         stock_pc ~ tax_rate, 
#         weights = ~ value
#     )

# df |> 
#     filter(tax_rate <= .1) |>
#     filter(stock_pc < quantile(df$stock_pc, .99)) |>
#     feols(
#         housing_pc ~ tax_rate, 
#         weights = ~ value
#     )

# df |> 
#     filter(tax_rate <= .1) |>
#     mutate(group = ntile(tax_rate, 10)) |>
#     filter(stock_pc < quantile(df$stock_pc, .99)) |>
#     reframe(
#         rate = mean(tax_rate),
#         stock = mean(stock_pc),
#         housing = mean(housing_pc),
#         .by = group
#     ) |>
#     arrange(group) 

# # Compare changes in tax rate between 2013 and 2017
# # to changes in the stock and housing rate
# q <- "
# select 
#     fips_code,
#     tax_year,
#     count(1) as n,
#     sum(calculated_total_value) as stock,
#     100 * median(total_tax_amount / calculated_total_value) as tax_rate
# from tax
# where 
#     tax_year in (2013, 2017, 2018, 2022) and
#     property_indicator_code = 10 
# group by fips_code, tax_year
# "
# df <- dbGetQuery(con, q) 

# # Merge population data
# pop <- get_estimates(
#   geography = "county",
#   product = "population",
#   vintage = c(2013, 2017, 2018, 2022),
#   geometry = FALSE  
# ) |>
#     mutate(
#         GEOID = as.numeric(GEOID),
#         year = 2017
#     ) |>
#     filter(variable == "POPESTIMATE") |>
#     as.data.frame()

# df <- df %>%
#   inner_join(
#     pop,
#     by = c("fips_code" = "GEOID")
#   ) %>% 
#   mutate(
#     stock_pc = stock / n,
#     housing_pc = n / value
#   )

# # Calculate differences
# df <- df |>
#     pivot_wider(
#         names_from = tax_year,
#         values_from = c(n, stock, tax_rate),
#         names_sep = "_"
#     ) |>
#     mutate(
#         delta_stock_pre = stock_2017 - stock_2013,
#         delta_stock_post = stock_2022 - stock_2018,
#         delta_housing_pre = n_2017 - n_2013,
#         delta_housing_post = n_2022 - n_2018,
#         delta_tax_rate_pre = tax_rate_2017 - tax_rate_2013,
#         delta_tax_rate_post = tax_rate_2022 - tax_rate_2018,
#         n_pre = (n_2017 + n_2013) / 2,
#         n_post = (n_2022 + n_2018) / 2
#     ) |> 
#     select(
#         fips_code, 
#         delta_stock_pre, delta_stock_post, 
#         delta_housing_pre, delta_housing_post, 
#         delta_tax_rate_pre, delta_tax_rate_post,
#         n_pre, n_post
#     ) |>
#     filter(!is.na(delta_tax_rate_post) & !is.na(delta_stock_post) & !is.na(delta_housing_post)) |>
#     filter(!is.na(delta_tax_rate_pre) & !is.na(delta_stock_pre) & !is.na(delta_housing_pre))

# # Break out by tax change bin
# df |> 
#     mutate(group = ntile(delta_tax_rate_pre, 10)) |>
#     reframe(
#         delta_rate_pre = mean(delta_tax_rate_pre),
#         delta_stock_pre = mean(delta_stock_pre),
#         delta_housing_pre = mean(delta_housing_pre),
#         .by = group
#     ) |>
#     arrange(group)

# df |>
#     mutate(group = ntile(delta_tax_rate_post, 10)) |>
#     reframe(
#         delta_rate_post = mean(delta_tax_rate_post),
#         delta_stock_post = mean(delta_stock_post),
#         delta_housing_post = mean(delta_housing_post),
#         .by = group
#     ) |>
#     arrange(group)

# feols(delta_stock_pre ~ delta_tax_rate_pre, data = df, weights = ~ n_pre)
# feols(delta_stock_post ~ delta_tax_rate_post, data = df, weights = ~ n_post)
# feols(delta_housing_pre ~ delta_tax_rate_pre, data = df, weights = ~ n_pre)
# feols(delta_housing_post ~ delta_tax_rate_post, data = df, weights = ~ n_post)

