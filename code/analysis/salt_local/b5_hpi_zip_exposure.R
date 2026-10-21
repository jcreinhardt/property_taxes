# FHFA tract HPI in the border design with tract-level (ZIP-based, winsorised) SALT exposure: near x high x exposure and the
# low-side counterpart near x low x exposure in one model; tract + pair x side x year + pair x distance-bin x year FE, plus
# exposure x year; SE by county. Near cutoffs 5 / 10 / 20 mi. The outside-option story predicts nhx < 0 and nlx >= 0.
suppressPackageStartupMessages({library(data.table); library(fixest); library(ggplot2)})
ROOT <- "/Users/jr2728/PhD/projects/property_taxes"; OUT <- file.path(ROOT, "output/analysis/salt_local")
h <- fread(file.path(ROOT, "data/external/fhfa/hpi_at_tract.csv"), colClasses = c(tract = "character"))[year >= 2012]
h[, hpi := suppressWarnings(as.numeric(hpi))]; h <- h[is.finite(hpi) & hpi > 0]
iv <- unique(readRDS(file.path(ROOT, "data/local_juris/iv_tract_side.rds"))[, .(tract, pair, high, dbin, dist_mi, cty)])
tz <- fread(file.path(ROOT, "data/local_juris/salt_exposure_tract_zip2016.csv"), colClasses = c(tract = "character"))[, .(tract, exz_z)]
d <- merge(merge(h, iv, by = "tract"), tz, by = "tract")[, `:=`(y = year, lh = log(hpi))]
NY <- uniqueN(d$y); d <- d[, bal := uniqueN(y) == NY, by = tract][bal == TRUE]
res <- rbindlist(lapply(c(5, 10, 20), \(cut) {
  x <- copy(d)[, nr := as.integer(dist_mi < cut)][, `:=`(nh = nr * high, nhx = nr * high * exz_z, nlx = nr * (1 - high) * exz_z)]
  m <- feols(lh ~ i(y, nh, ref = 2017) + i(y, nhx, ref = 2017) + i(y, nlx, ref = 2017) + i(y, exz_z, ref = 2017) | tract + pair^high^y + pair^dbin^y, x, cluster = ~cty)
  ct <- as.data.table(coeftable(m), keep.rownames = "term")[grepl(":nhx|:nlx", term)]
  ct[, .(y = as.integer(sub("y::(\\d+):.*", "\\1", term)), which = sub("y::\\d+:", "", term), coef = Estimate, se = `Std. Error`, cutoff = cut)] }))
res <- rbind(res, unique(res[, .(which, cutoff)])[, `:=`(y = 2017L, coef = 0, se = 0)])
fwrite(res, file.path(OUT, "b5_hpi_zip_exposure.csv"))
sink(file.path(OUT, "b5_hpi_zip_exposure.txt"))
cat("FHFA tract HPI; near x high x ZIP exposure (nhx) and near x low x ZIP exposure (nlx), per SD; tracts:", uniqueN(d$tract), "\n")
print(as.data.frame(dcast(res, cutoff + which ~ y, value.var = "coef")[, lapply(.SD, \(v) if (is.numeric(v)) round(v, 4) else v)]), row.names = FALSE)
cat("SE:\n"); print(as.data.frame(dcast(res, cutoff + which ~ y, value.var = "se")[, lapply(.SD, \(v) if (is.numeric(v)) round(v, 4) else v)]), row.names = FALSE)
sink(); cat(readLines(file.path(OUT, "b5_hpi_zip_exposure.txt")), sep = "\n")
res[, lab := ifelse(which == "nhx", "high side (has cheaper option)", "low side (is the cheaper option)")]
g <- ggplot(res, aes(y, coef, colour = lab)) + geom_hline(yintercept = 0, colour = "grey60") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey60") +
  geom_pointrange(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se), position = position_dodge(.45), size = .15) +
  facet_wrap(~ paste0("near < ", cutoff, " mi"), nrow = 1) + scale_colour_manual(values = c("#b2182b", "#2166ac")) + scale_x_continuous(breaks = seq(2012, 2025, 3)) +
  labs(x = NULL, y = "log HPI per SD ZIP SALT exposure x near (vs 2017)", colour = NULL,
       title = "House prices near state borders by SALT exposure: high-tax vs low-tax side (FHFA tract HPI)",
       subtitle = "tract + pair x side x year + pair x distance-bin x year FE + exposure x year; 95% CI by county") + theme_minimal(base_size = 9) + theme(legend.position = "bottom")
ggsave(file.path(OUT, "figures/b5_hpi_zip_exposure.png"), g, width = 11, height = 4.5, dpi = 150)
