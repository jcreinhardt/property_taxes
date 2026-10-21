# Key-results figure for the overnight report: (1) district border DDD on log property tax (F-33), (2) exposure event study of
# school-district property tax (CoG by county, weighted), (3) Data Axle move-outs in the border DDD, (4) parcel bills and
# nominal rates in the border DDD (CoreLogic 1%). All relative to 2017.
suppressPackageStartupMessages({library(data.table); library(ggplot2); library(patchwork)})
ROOT <- "/Users/jr2728/PhD/projects/property_taxes"; O <- file.path(ROOT, "output/analysis/salt_local")
pp <- function(d, title, ylab) ggplot(d, aes(y, coef, colour = lab)) + geom_hline(yintercept = 0, colour = "grey60") +
  geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey60") +
  geom_pointrange(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se), position = position_dodge(.4), size = .15) +
  scale_x_continuous(breaks = seq(2012, 2025, 2)) + labs(title = title, x = NULL, y = ylab, colour = NULL) + theme_minimal(base_size = 8) + theme(legend.position = "bottom")
r1 <- fread(file.path(O, "a3_district_border_ddd_robust.csv"))[sample == "all"][, lab := cutoff]
r2 <- fread(file.path(O, "a1_revenue_event_study.csv"))[outcome == "ptax" & sample %in% c("school", "county_govt") & weighted == TRUE][, lab := sample]
r3 <- fread(file.path(O, "d1_moveout_ddd.csv"))[spec %in% c("border DDD near x high", "county exposure (per SD)")][, lab := spec]
r4 <- fread(file.path(O, "c2_parcel_panel.csv"))[design == "border DDD (near x high)" & outcome %in% c("log bill", "log nominal rate", "log AV") & y >= 2014][, lab := outcome]
g <- (pp(r1, "(a) Border DDD: log property tax, school districts (F-33)", "near x high-side, vs 2017") |
      pp(r2, "(a) Exposure: log property tax per SD SALT exposure (CoG, weighted)", "per SD exposure, vs 2017")) /
     (pp(r4, "(a)/(c) Border DDD: constant parcels (CoreLogic 1%)", "near x high-side, vs 2017") |
      pp(r3, "Migration: Data Axle annualised move-out rate (pp)", "vs 2017")) +
  plot_annotation(title = "SALT cap and local jurisdictions: key results", subtitle = "95% CIs clustered by county; dashed line between 2017 and 2018")
ggsave(file.path(O, "figures/c3_key_results.png"), g, width = 12, height = 8, dpi = 150)
