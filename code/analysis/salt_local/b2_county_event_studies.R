# (a)-(c) at the county level from the CoreLogic panel (b1): event studies of outcome ~ exposure (2016 SALT above $10k per
# return, SD units) x year, ref 2017, county + state x year FE, SE by county; unweighted and weighted by 2016 returns.
# Roll outcomes use counties whose residential parcel count is >= 80% of their max in every year 2013-2024 (roll gaps);
# sales outcomes need >= 50 sales in the county-year. Years 2013-2024 (2012 roll is thin).
suppressPackageStartupMessages({library(data.table); library(fixest); library(ggplot2)})
ROOT <- "/Users/jr2728/PhD/projects/property_taxes"; OUT <- file.path(ROOT, "output/analysis/salt_local")
cy <- readRDS(file.path(ROOT, "data/local_juris/county_year.rds"))[y >= 2013 & y <= 2024]
ex <- fread(file.path(ROOT, "data/local_juris/salt_exposure_county2016.csv"))[, fips := sprintf("%05d", county)]
cy <- merge(cy, ex[, .(fips, ex_z, returns)], by = "fips")[, st := substr(fips, 1, 2)]
cy[, ok_roll := uniqueN(y[n_res >= .8 * max(n_res, na.rm = TRUE)]) == 12, by = fips]
cy[, `:=`(l_levy = log(bill_res), l_levy_pp = log(bill_res / n_res), l_av_pp = log(av_res / n_res), l_p50 = log(p50), l_ratio = log(ratio50),
          eff_pct = 100 * eff50, nom_pct = 100 * nom_w, l_nom = log(nom_w))]
spec <- list(l_levy = "roll", l_levy_pp = "roll", l_av_pp = "roll", l_nom = "roll", sd_log_nom_area = "roll",
             l_p50 = "sales", eff_pct = "sales", l_ratio = "sales", regr_slope_ratio = "sales", prd = "sales", sd_eff_tract = "sales")
res <- rbindlist(lapply(names(spec), \(v) rbindlist(lapply(c(FALSE, TRUE), \(wt) {
  d <- if (spec[[v]] == "roll") cy[ok_roll == TRUE] else cy[n_sales >= 50]
  d <- d[is.finite(get(v))]
  f <- feols(as.formula(sprintf("%s ~ i(y, ex_z, ref = 2017) | fips + st^y", v)), d, cluster = ~fips, weights = if (wt) ~returns else NULL)
  ct <- as.data.table(coeftable(f), keep.rownames = "term")
  rbind(data.table(y = 2017L, coef = 0, se = 0), ct[, .(y = as.integer(sub("y::(\\d+):ex_z", "\\1", term)), coef = Estimate, se = `Std. Error`)])[,
    `:=`(outcome = v, weighted = wt, counties = uniqueN(d$fips), mean_2017 = d[y == 2017, mean(get(v), na.rm = TRUE)])] }))))
fwrite(res, file.path(OUT, "b2_county_event_studies.csv"))
sink(file.path(OUT, "b2_county_event_studies.txt"))
cat("County event studies: +1 SD SALT exposure x year (ref 2017), county + state x year FE, SE by county\n")
for (k in split(res, by = c("outcome", "weighted"))) {
  cat("\n==", k$outcome[1], "| weighted:", k$weighted[1], "| counties:", k$counties[1], "| 2017 mean:", signif(k$mean_2017[1], 4), "\n")
  print(as.data.frame(dcast(k, . ~ y, value.var = "coef")[, -1][, lapply(.SD, round, 4)]), row.names = FALSE)
  print(as.data.frame(dcast(k, . ~ y, value.var = "se")[, -1][, lapply(.SD, round, 4)]), row.names = FALSE) }
sink(); cat(readLines(file.path(OUT, "b2_county_event_studies.txt")), sep = "\n")
lab <- c(l_levy = "log residential levy", l_levy_pp = "log levy per parcel", l_av_pp = "log AV per parcel", l_nom = "log nominal rate (bill/AV)",
         sd_log_nom_area = "SD log nominal rate across tax areas", l_p50 = "log median sale price", eff_pct = "median effective rate (%)",
         l_ratio = "log median assessment ratio", regr_slope_ratio = "regressivity: slope log(AV/P) on log P", prd = "PRD", sd_eff_tract = "SD of tract eff. rates (pp)")
res[, lab := factor(lab[outcome], lab)]
g <- ggplot(res, aes(y, coef, colour = weighted)) + geom_hline(yintercept = 0, colour = "grey60") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey60") +
  geom_pointrange(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se), position = position_dodge(.45), size = .15) +
  facet_wrap(~ lab, scales = "free_y", ncol = 3) + scale_x_continuous(breaks = seq(2013, 2024, 2)) +
  scale_colour_manual(values = c(`FALSE` = "black", `TRUE` = "#e34a33"), labels = c("unweighted", "weighted by returns")) +
  labs(x = NULL, y = "effect of +1 SD exposure (vs 2017)", colour = NULL, title = "SALT-cap exposure and county outcomes (CoreLogic roll and sales)",
       subtitle = "County + state x year FE; 95% CI by county") + theme_minimal(base_size = 9) + theme(legend.position = "bottom")
ggsave(file.path(OUT, "figures/b2_county_event_studies.png"), g, width = 11, height = 10, dpi = 150)
