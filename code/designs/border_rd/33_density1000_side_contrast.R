# Every outcome run with the density-selected border design (tracts < 1,000 per sq mi within 50 mi, both sides; 29-32) as yearly
# event studies, shown for the low-tax side, the high-tax side and their contrast high minus low (difference of the two
# side-specific coefficients, SE = sqrt(se_h^2 + se_l^2); the sides are disjoint samples). Two terms:
#   main:        Z x year (Z = -distance / 10 mi; log density x year, unit FE + pair x year FE, 2017 = 0, SE by county) — the core
#                11 from the reduced form of 29, the 75 district / local-government outcomes from the main term of 31 (E x year also
#                in that model; the only yearly series for them), the 14 composition outcomes of 32;
#   interaction: Z x E x year (E = own lost SALT subsidy per owner HH, standardised; Z x year, E x year, density x year in the
#                model) — all 100 from 31 and 32. Balance caveat of 23/36: Z x E is unbalanced in 2016 prices, income, education.
# No new estimation. SD units of the outcome per 10 mi closer (main) or per 10 mi per SD of E (interaction). Writes
# c45_side_contrast{,_interaction}_{series,post_pre}.csv, c45_side_contrast{,_interaction}.txt and figures
# c45_side_contrast{,_interaction}_{core,composition,district}.png.
suppressPackageStartupMessages({library(data.table); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_rd"); invisible(out_dir("designs/border_rd/figures")); REF <- 2017
a <- fread(file.path(OUT, "c41_density1000_series.csv"))[design == "near x year" & Z == "per 10 mi closer" & density == TRUE & sample != "both sides pooled",
       .(term = "main", y, sample, src = "core (29)", outcome, b = b_sd, se = se_sd, n)]
b <- fread(file.path(OUT, "c43_exposure_interaction_density1000_series.csv"), colClasses = list(character = c("y", "coef")))
b[!grepl("^\\d+$", y), c("y", "coef") := .(coef, y)]   # census-year rows of 31 have y and coef swapped
b <- unique(b)[, y := as.integer(y)][sample != "both sides pooled" & is.finite(y) & (coef == "interaction" | src != "core"),
       .(term = coef, y, sample, src = fifelse(src == "core", "core (31)", paste0(src, " (31)")), outcome, b = b_sd, se = se_sd, n)]
d <- fread(file.path(OUT, "c44_composition_series.csv"))[grepl("^border", design) & sample != "both sides pooled",
       .(term = fifelse(design == "border: Z x year", "main", "interaction"), y, sample, src = paste0("composition: ", src, " (32)"), outcome, b = b_sd, se = se_sd, n)]
s <- rbind(a, b, d)[, side := fifelse(sample == "low-tax side", "low", "high")][, sample := NULL]
w <- dcast(s, term + src + outcome + y ~ side, value.var = c("b", "se", "n"))[is.finite(b_low) & is.finite(b_high)]
w[, `:=`(b_diff = b_high - b_low, se_diff = sqrt(se_high^2 + se_low^2))]
ser <- melt(w, id.vars = c("term", "src", "outcome", "y"), measure.vars = list(b = c("b_low", "b_high", "b_diff"), se = c("se_low", "se_high", "se_diff")), variable.name = "series")
ser[, series := factor(series, 1:3, c("low-tax side", "high-tax side", "high minus low"))]
pp <- ser[, { pre <- .SD[y < REF & y >= 2012]; post <- .SD[y > REF]
  .(post_min_pre = mean(post$b) - mean(pre$b), se = sqrt(mean(post$se^2) + mean(pre$se^2) / nrow(pre)), years = sprintf("%d-%d", min(y), max(y))) }, by = .(term, src, outcome, series)]
pp[, t := post_min_pre / se]
plot_es <- function(x, title, file, h, ncol = 4, unit) {
  x <- copy(x)[, outcome := factor(outcome, unique(outcome))]
  g <- ggplot(x, aes(y, b, colour = series)) + geom_hline(yintercept = 0, colour = "grey50") + geom_vline(xintercept = REF + .5, linetype = 3, colour = "grey50") +
    geom_ribbon(aes(ymin = b - 1.96 * se, ymax = b + 1.96 * se, fill = series), alpha = .12, colour = NA) + geom_line(linewidth = .4) + geom_point(size = .8) +
    facet_wrap(~outcome, scales = "free_y", ncol = ncol) + scale_colour_manual(NULL, values = c("#2a64a8", "#c0392b", "black")) + scale_fill_manual(NULL, values = c("#2a64a8", "#c0392b", "black")) +
    labs(x = NULL, y = paste0("SD units of the outcome ", unit, "; 2017 = 0; 95% CI, SE by county"), title = title,
         subtitle = "density-selected sample (tracts < 1,000 per sq mi within 50 mi); unit FE + pair x year FE, log density x year; high minus low = difference of the side-specific coefficients") +
    theme_minimal(base_size = 8) + theme(legend.position = "bottom")
  ggsave(file.path(OUT, "figures", file), g, width = 13, height = h, dpi = 150) }
for (tm in c("main", "interaction")) { sfx <- ifelse(tm == "main", "", "_interaction"); unit <- ifelse(tm == "main", "per 10 mi closer to the border", "per 10 mi closer per SD of SALT exposure")
  tl <- ifelse(tm == "main", "Z x year", "Z x exposure x year"); S <- ser[term == tm]; P <- pp[term == tm]
  fwrite(S, file.path(OUT, paste0("c45_side_contrast", sfx, "_series.csv"))); fwrite(P, file.path(OUT, paste0("c45_side_contrast", sfx, "_post_pre.csv")))
  sink(file.path(OUT, paste0("c45_side_contrast", sfx, ".txt")), split = TRUE)
  cat("Density-selected border design (tracts < 1,000 per sq mi within 50 mi):", tl, "event studies by side and the contrast high minus low.\n",
      "Post - pre = mean of 2018+ coefficients minus mean of 2012-16, SD units of the outcome", unit, "; t in brackets.\n",
      if (tm == "main") "Core 11 from 29 (reduced form with density x year); 75 district / local-government outcomes from the main term of 31 (E x year also in the model);\n14 composition outcomes from 32."
      else "All from the Z x E x year term of 31 (core 11 + 75 district / local-government outcomes) and 32 (14 composition outcomes). Z x E is unbalanced (23/36).",
      "\nSides are separate regressions on disjoint samples; SE of the contrast = sqrt(se_h^2 + se_l^2).\n\n")
  tab <- dcast(P, src + outcome + years ~ series, value.var = c("post_min_pre", "t"))
  tab <- tab[, .(src, outcome, years, low = sprintf("%+.3f (%.1f)", `post_min_pre_low-tax side`, `t_low-tax side`), high = sprintf("%+.3f (%.1f)", `post_min_pre_high-tax side`, `t_high-tax side`),
                 high_minus_low = sprintf("%+.3f (%.1f)", `post_min_pre_high minus low`, `t_high minus low`))][order(!grepl("^core", src), src, outcome)]
  for (g in unique(tab$src)) { cat("==", g, "\n"); print(as.data.frame(tab[src == g, -"src"]), row.names = FALSE); cat("\n") }
  cat("Counts of |t| > 1.96 by series and source:\n"); print(as.data.frame(P[, .(k = .N, sig = sum(abs(t) > 1.96), median_abs = round(median(abs(post_min_pre)), 3)), by = .(src, series)][order(src, series)]), row.names = FALSE)
  cat("\nAll outcomes:\n"); print(as.data.frame(P[, .(k = .N, sig = sum(abs(t) > 1.96), median_abs = round(median(abs(post_min_pre)), 3)), by = series]), row.names = FALSE)
  sink()
  plot_es(S[grepl("^core", src)], paste("Core outcomes:", tl, "by side and the high-minus-low contrast"), paste0("c45_side_contrast", sfx, "_core.png"), 7.5, unit = unit)
  plot_es(S[grepl("^composition", src)], paste("Household composition and age:", tl, "by side and the high-minus-low contrast"), paste0("c45_side_contrast", sfx, "_composition.png"), 8.5, unit = unit)
  plot_es(S[grepl("^(F-33|SEDA|CoG)", src)][, outcome := paste0(outcome, "\n", sub(" \\(31.*", "", src))], paste("District and local-government outcomes (31):", tl, "by side and the high-minus-low contrast"), paste0("c45_side_contrast", sfx, "_district.png"), 26, ncol = 6, unit = unit) }
# ---- the c43 key outcomes, difference only: low-tax side minus high-tax side, both terms (c45_side_contrast_low_minus_high.png)
key <- c("effective tax rate", "sales per 100 parcels", "log median sale price", "log population (ACS)", "log district property tax per pupil", "log district spending per pupil", "log district enrolment", "total revenue", "state formula aid (C01)", "instruction", "capital outlay", "district test scores (SEDA, SD)")
kd <- ser[series == "high minus low" & outcome %in% key][, `:=`(b = -b, outcome = factor(outcome, key), term = factor(term, c("interaction", "main"), c("Z x exposure x year (per 10 mi per SD of E)", "Z x year at mean exposure (per 10 mi)")))]
g <- ggplot(kd, aes(y, b, colour = term)) + geom_hline(yintercept = 0, colour = "grey50") + geom_vline(xintercept = REF + .5, linetype = 3, colour = "grey50") +
  geom_pointrange(aes(ymin = b - 1.96 * se, ymax = b + 1.96 * se), position = position_dodge(width = .5), size = .25, linewidth = .35) + geom_line(position = position_dodge(width = .5), linewidth = .35) +
  facet_wrap(~outcome, scales = "free_y", ncol = 4) + scale_colour_manual(NULL, values = c("#c0392b", "grey40")) +
  labs(x = NULL, y = "low-tax side minus high-tax side, SD units of the outcome; 2017 = 0; 95% CI", title = "Difference between the sides: low-tax side minus high-tax side, key outcomes of c43",
       subtitle = "density-selected sample; each side estimated separately (unit FE + pair x year FE, Z x year + Z x E x year + E x year + log density x year); SE of the difference = sqrt(se_low^2 + se_high^2)") +
  theme_minimal(base_size = 8) + theme(legend.position = "bottom")
ggsave(file.path(OUT, "figures/c45_side_contrast_low_minus_high.png"), g, width = 11, height = 7.5, dpi = 150)
# ---- c43's event-study figure redrawn without the pooled series: low-tax side and high-tax side only (c43_exposure_interaction_es_sides.png)
pl <- ser[term == "interaction" & series != "high minus low" & outcome %in% key][, `:=`(outcome = factor(outcome, key), series = factor(series, c("low-tax side", "high-tax side")))]
g <- ggplot(pl, aes(y, b, colour = series)) + geom_hline(yintercept = 0, colour = "grey70") + geom_vline(xintercept = REF + .5, linetype = "22", colour = "grey50") +
  geom_pointrange(aes(ymin = b - 1.96 * se, ymax = b + 1.96 * se), position = position_dodge(width = .6), size = .18, linewidth = .3) + geom_line(position = position_dodge(width = .6), linewidth = .35) +
  facet_wrap(~ outcome, scales = "free_y", ncol = 4, labeller = label_wrap_gen(30)) + scale_x_continuous(breaks = seq(2012, 2024, 2)) + scale_colour_manual(NULL, values = c("#2a64a8", "#c0392b")) +
  labs(x = NULL, y = "Z x exposure x year, SD units of the outcome per 10 mi closer per SD of exposure, 2017 = 0; 95% CI, SE by county", title = "Distance x SALT exposure x year, density-selected sample: event studies for key outcomes, by side",
       subtitle = "as c43 (exposure x year and log density x year included; unit FE + pair x year FE), pooled series omitted") +
  theme_minimal(base_size = 8) + theme(legend.position = "bottom", strip.text = element_text(size = 6.5))
ggsave(file.path(OUT, "figures/c43_exposure_interaction_es_sides.png"), g, width = 11, height = 7.5, dpi = 150)
