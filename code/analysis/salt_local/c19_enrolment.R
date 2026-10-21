# Pupils per district as a migration outcome. F-33 fall enrolment (V33), FY2012-2023, districts with enrolment > 0 in all
# 12 years. Public-school membership as reported by the district (charter pupils are included where charters belong to the
# district's reporting unit, which varies by state).
# (1) SALT exposure event study: log enrolment ~ i(y, ex_z, ref 2017) | district + state x year; county exposure (2016 SOI
#     SALT above $10k per return, SD units); SE by county; unweighted and enrolment-weighted (2017 enrolment).
# (2) Border DDD (as a3): district near x high = majority of its tracts within 10 mi of the border on the high-tax side.
#     log enrolment ~ i(y, nh, ref 2017) | district + pair^high^y + pair^dbin^y; and the same with nh x exposure.
suppressPackageStartupMessages({library(data.table); library(fixest); library(ggplot2)})
ROOT <- "/Users/jr2728/PhD/projects/property_taxes"; OUT <- file.path(ROOT, "output/analysis/salt_local")
p <- readRDS(file.path(ROOT, "data/local_juris/district_finance.rds"))[enr > 0]
p[, bal := uniqueN(y) == 12, by = leaid]; p <- p[bal == TRUE][, lenr := log(enr)]
p[, w17 := enr[y == 2017], by = leaid]
ex <- fread(file.path(ROOT, "data/local_juris/salt_exposure_county2016.csv"))[, conum := sprintf("%05d", county)]
p <- merge(p, ex[, .(conum, ex_z)], by = "conum")[, st := substr(conum, 1, 2)]
ivt <- unique(readRDS(file.path(ROOT, "data/local_juris/iv_tract_side.rds"))[, .(tract, leaid, pair, high, dbin, near)])
dv <- ivt[!is.na(leaid), .(near = as.integer(mean(near) > .5), high = as.integer(names(which.max(table(high)))),
                           pair = names(which.max(table(pair))), dbin = names(which.max(table(dbin)))), by = leaid]
dd <- merge(p, dv, by = "leaid")[, nh := near * high]
tidy <- \(m, lab, pat = "^y::") as.data.table(coeftable(m), keep.rownames = "term")[grepl(pat, term)][,
  .(spec = lab, y = as.integer(sub("^y::(\\d+).*", "\\1", term)), b = Estimate, se = `Std. Error`)]
add0 <- \(x) rbind(x, unique(x[, .(spec)])[, `:=`(y = 2017L, b = 0, se = 0)])[order(spec, y)]
res <- add0(rbind(
  tidy(feols(lenr ~ i(y, ex_z, ref = 2017) | leaid + st^y, p, cluster = ~conum), "(1) exposure x year, unweighted"),
  tidy(feols(lenr ~ i(y, ex_z, ref = 2017) | leaid + st^y, p, cluster = ~conum, weights = ~w17), "(1) exposure x year, enrolment-weighted"),
  tidy(feols(lenr ~ i(y, nh, ref = 2017) | leaid + pair^high^y + pair^dbin^y, dd, cluster = ~conum), "(2) border DDD: near x high"),
  tidy(feols(lenr ~ i(y, I(nh * ex_z), ref = 2017) + i(y, nh, ref = 2017) | leaid + pair^high^y + pair^dbin^y, dd, cluster = ~conum),
       "(2) border DDD: near x high x exposure", "ex_z")))
fwrite(res, file.path(OUT, "c19_enrolment.csv"))
# pooled post effects (2018-2023 vs 2013-2017 relative to 2017 normalisation): simple mean of post coefficients
sink(file.path(OUT, "c19_enrolment.txt"), split = TRUE)
cat("Log district enrolment (F-33 V33), balanced FY2012-2023: districts", uniqueN(p$leaid), "| border districts", uniqueN(dd$leaid),
    "| near-high", uniqueN(dd[nh == 1, leaid]), "\n")
cat("Coefficients x 100 (log points), SE in parentheses; ref 2017\n")
print(dcast(res[, .(spec, y, cell = sprintf("%.2f (%.2f)", 100 * b, 100 * se))], y ~ spec, value.var = "cell"))
sink()
g <- ggplot(res, aes(y, 100 * b)) + geom_hline(yintercept = 0, colour = "grey60") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey40") +
  geom_ribbon(aes(ymin = 100 * (b - 1.96 * se), ymax = 100 * (b + 1.96 * se)), fill = "#2a64a8", alpha = .15) +
  geom_line(colour = "#2a64a8") + geom_point(colour = "#2a64a8", size = 1) + facet_wrap(~ spec, scales = "free_y", ncol = 2) +
  scale_x_continuous(breaks = seq(2012, 2023, 2)) +
  labs(x = "fiscal year (enrolment counted in the fall of the school year)", y = "log enrolment x 100, relative to 2017",
       title = "Pupils per district: SALT exposure and the border design",
       subtitle = "(1) per SD of county exposure, district + state x year FE. (2) near-border high-tax-side districts, district + pair x side x year + pair x distance x year FE. 95% CI, SE by county.") +
  theme_minimal(base_size = 9)
ggsave(file.path(OUT, "figures/c19_enrolment.png"), g, width = 10, height = 6.5, dpi = 150)
