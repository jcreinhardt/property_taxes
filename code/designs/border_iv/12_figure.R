# Formerly code/analysis/salt_local/c1_iv_figure.R (moved in the 2026-10-27 restructuring).
# Figure: directional border IV (R/26), pooled near (< 10 mi) x high-side coefficients in SD units of each outcome, T = 30 min:
# first stage (cheaper cross-border option, tax gap), school access, and own-tract balance.
suppressPackageStartupMessages({library(data.table); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_iv")
r <- fread(file.path(OUT, "iv_high_side.csv"))[term == "near" & T == 30]
lab <- c(x_cheap = "cheaper (>= 0.5 pp) tract across state line within 30 min", tax_gap = "own rate - p10 of reachable tracts",
         sh_x = "share of reachable tracts across state line", sch_gap = "p90 reachable achievement - own", x_better_sch = "better (>= 0.25 SD) district across line",
         ach = "own district achievement (SEDA)", ses = "own district SES (SEDA)", linc = "log median household income", lhval = "log median home value",
         sh_ba = "share BA+", sh_white = "share white", sh_own = "owner share", yrbuilt = "median year built", lpop = "log population")
grp <- c(x_cheap = "first stage", tax_gap = "first stage", sh_x = "first stage", sch_gap = "school access", x_better_sch = "school access")
r[, `:=`(est = Estimate / sd_y, lo = (Estimate - 1.96 * `Std. Error`) / sd_y, hi = (Estimate + 1.96 * `Std. Error`) / sd_y,
         lab = factor(lab[outcome], rev(lab)), grp = factor(fifelse(outcome %in% names(grp), grp[outcome], "own-tract balance"), c("first stage", "school access", "own-tract balance")))]
g <- ggplot(r, aes(est, lab, colour = grp)) + geom_vline(xintercept = 0, colour = "grey60") + geom_pointrange(aes(xmin = lo, xmax = hi), size = .3) +
  facet_grid(grp ~ ., scales = "free_y", space = "free_y") + scale_colour_manual(values = c("first stage" = "#b2182b", "school access" = "#2166ac", "own-tract balance" = "grey30"), guide = "none") +
  labs(x = "near (< 10 mi) x high-tax side, in SD of the outcome (95% CI, SE by county)", y = NULL,
       title = "Directional border instrument: first stage, school access and balance",
       subtitle = "55.9k tracts, 70 state-border pairs; pair x side FE + pair x distance-bin FE") + theme_minimal(base_size = 9)
ggsave(file.path(OUT, "figures/c1_iv_first_stage_balance.png"), g, width = 8, height = 5.5, dpi = 150)
