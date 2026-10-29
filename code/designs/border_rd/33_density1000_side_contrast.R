# Every outcome run with the density-selected border design (tracts < 1,000 per sq mi within 50 mi, both sides; 29-32) as yearly
# event studies of Z x year (Z = -distance / 10 mi, log density x year, unit FE + pair x year FE, 2017 = 0, SE by county), shown
# for the low-tax side, the high-tax side and their contrast high minus low (difference of the two side-specific coefficients,
# SE = sqrt(se_h^2 + se_l^2); the sides are disjoint samples). No new estimation: reads the series of 29 (core 11, reduced form
# with density), 31 (main term at mean exposure for the 75 district / local-government outcomes; the only yearly series for them)
# and 32 (14 composition outcomes, border Z x year). SD units of the outcome. Writes c45_side_contrast_{series,post_pre}.csv,
# c45_side_contrast.txt and figures c45_side_contrast_{core,composition,district}.png.
suppressPackageStartupMessages({library(data.table); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_rd"); invisible(out_dir("designs/border_rd/figures")); REF <- 2017
a <- fread(file.path(OUT, "c41_density1000_series.csv"))[design == "near x year" & Z == "per 10 mi closer" & density == TRUE & sample != "both sides pooled",
       .(y, sample, src = "core (29)", outcome, b = b_sd, se = se_sd, n)]
b <- fread(file.path(OUT, "c43_exposure_interaction_density1000_series.csv"), colClasses = list(character = c("y", "coef")))
b[!grepl("^\\d+$", y), c("y", "coef") := .(coef, y)]   # census-year rows of 31 have y and coef swapped
b <- unique(b)[, y := as.integer(y)][coef == "main" & src != "core" & sample != "both sides pooled" & is.finite(y),
       .(y, sample, src = paste0(src, " (31, main term)"), outcome, b = b_sd, se = se_sd, n)]
d <- fread(file.path(OUT, "c44_composition_series.csv"))[design == "border: Z x year" & sample != "both sides pooled",
       .(y, sample, src = paste0("composition: ", src, " (32)"), outcome, b = b_sd, se = se_sd, n)]
s <- rbind(a, b, d)[, y := as.integer(y)][, side := fifelse(sample == "low-tax side", "low", "high")][, sample := NULL]
w <- dcast(s, src + outcome + y ~ side, value.var = c("b", "se", "n"))[is.finite(b_low) & is.finite(b_high)]
w[, `:=`(b_diff = b_high - b_low, se_diff = sqrt(se_high^2 + se_low^2))]
ser <- melt(w, id.vars = c("src", "outcome", "y"), measure.vars = list(b = c("b_low", "b_high", "b_diff"), se = c("se_low", "se_high", "se_diff")), variable.name = "series")
ser[, series := factor(series, 1:3, c("low-tax side", "high-tax side", "high minus low"))]
fwrite(ser, file.path(OUT, "c45_side_contrast_series.csv"))
pp <- ser[, { pre <- .SD[y < REF & y >= 2012]; post <- .SD[y > REF]
  .(post_min_pre = mean(post$b) - mean(pre$b), se = sqrt(mean(post$se^2) + mean(pre$se^2) / nrow(pre)), years = sprintf("%d-%d", min(y), max(y))) }, by = .(src, outcome, series)]
pp[, t := post_min_pre / se]
fwrite(pp, file.path(OUT, "c45_side_contrast_post_pre.csv"))
sink(file.path(OUT, "c45_side_contrast.txt"), split = TRUE)
cat("Density-selected border design (tracts < 1,000 per sq mi within 50 mi): Z x year event studies by side and the contrast high minus low.\n",
    "Post - pre = mean of 2018+ coefficients minus mean of 2012-16, SD units of the outcome per 10 mi closer to the border; t in brackets.\n",
    "Core 11 from 29 (reduced form with density x year); 75 district / local-government outcomes from the main term of 31 (E x year also in the model);\n",
    "14 composition outcomes from 32. Sides are separate regressions on disjoint samples; SE of the contrast = sqrt(se_h^2 + se_l^2).\n\n")
tab <- dcast(pp, src + outcome + years ~ series, value.var = c("post_min_pre", "t"))
tab <- tab[, .(src, outcome, years, low = sprintf("%+.3f (%.1f)", `post_min_pre_low-tax side`, `t_low-tax side`), high = sprintf("%+.3f (%.1f)", `post_min_pre_high-tax side`, `t_high-tax side`),
               high_minus_low = sprintf("%+.3f (%.1f)", `post_min_pre_high minus low`, `t_high minus low`))]
tab <- tab[order(src != "core (29)", src, outcome)]
for (g in unique(tab$src)) { cat("==", g, "\n"); print(as.data.frame(tab[src == g, -"src"]), row.names = FALSE); cat("\n") }
cat("Counts of |t| > 1.96 by series and source:\n"); print(as.data.frame(pp[, .(k = .N, sig = sum(abs(t) > 1.96), median_abs = round(median(abs(post_min_pre)), 3)), by = .(src, series)][order(src, series)]), row.names = FALSE)
sink()
plot_es <- function(x, title, file, h, ncol = 4) {
  x <- copy(x)[, outcome := factor(outcome, unique(outcome))]
  g <- ggplot(x, aes(y, b, colour = series)) + geom_hline(yintercept = 0, colour = "grey50") + geom_vline(xintercept = REF + .5, linetype = 3, colour = "grey50") +
    geom_ribbon(aes(ymin = b - 1.96 * se, ymax = b + 1.96 * se, fill = series), alpha = .12, colour = NA) + geom_line(linewidth = .4) + geom_point(size = .8) +
    facet_wrap(~outcome, scales = "free_y", ncol = ncol) + scale_colour_manual(NULL, values = c("#2a64a8", "#c0392b", "black")) + scale_fill_manual(NULL, values = c("#2a64a8", "#c0392b", "black")) +
    labs(x = NULL, y = "SD units of the outcome per 10 mi closer to the border; 2017 = 0; 95% CI, SE by county", title = title,
         subtitle = "density-selected sample (tracts < 1,000 per sq mi within 50 mi); Z x year + log density x year, unit FE + pair x year FE; high minus low = difference of the side-specific coefficients") +
    theme_minimal(base_size = 8) + theme(legend.position = "bottom")
  ggsave(file.path(OUT, "figures", file), g, width = 13, height = h, dpi = 150) }
plot_es(ser[src == "core (29)"], "Core outcomes: Z x year by side and the high-minus-low contrast", "c45_side_contrast_core.png", 7.5)
plot_es(ser[grepl("^composition", src)], "Household composition and age: Z x year by side and the high-minus-low contrast", "c45_side_contrast_composition.png", 8.5)
plot_es(ser[grepl("^(F-33|SEDA|CoG)", src)][, outcome := paste0(outcome, "\n", sub(" \\(31.*", "", src))], "District and local-government outcomes (main term of 31): Z x year by side and the high-minus-low contrast", "c45_side_contrast_district.png", 26, ncol = 6)
