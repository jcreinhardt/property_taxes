# Formerly code/analysis/salt_local/b4_hpi_border_ddd.R (moved in the 2026-10-27 restructuring).
# (c) House prices in the border-IV design: FHFA annual tract HPI (data/external/fhfa/hpi_at_tract.csv, 2010 tracts).
# Triple difference: log HPI ~ i(y, near x high, ref 2017) | tract + pair^high^y + pair^dbin^y, SE by county; near < 10 mi.
# Also near x high x county SALT exposure, and the high- vs low-exposure split.
suppressPackageStartupMessages({library(data.table); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_iv"); invisible(out_dir("designs/border_iv/figures"))
h <- fread(file.path(DATA, "external/fhfa/hpi_at_tract.csv"), colClasses = c(tract = "character"))[year >= 2010]
h[, hpi := suppressWarnings(as.numeric(hpi))]; h <- h[is.finite(hpi) & hpi > 0]
iv <- unique(readRDS(file.path(DATA, "built/geo/iv_tract_side.rds"))[, .(tract, pair, high, dbin, near, cty, dist_mi)])
ex <- fread(file.path(DATA, "built/exposure/salt_exposure_county2016.csv"))[, cty := sprintf("%05d", county)]
d <- merge(merge(h, iv, by = "tract"), ex[, .(cty, ex_z)], by = "cty")[, `:=`(y = year, lh = log(hpi), nh = near * high)]
d <- d[y >= 2012]; NY <- uniqueN(d$y); d <- d[, bal := uniqueN(y) == NY, by = tract][bal == TRUE]
medx <- median(unique(d[, .(cty, ex_z)])$ex_z)
cat("years:", range(d$y), "| tracts:", uniqueN(d$tract), "| near-high:", uniqueN(d[nh == 1, tract]), "\n")
run <- function(dd, lab) { m <- feols(lh ~ i(y, nh, ref = 2017) | tract + pair^high^y + pair^dbin^y, dd, cluster = ~cty)
  as.data.table(coeftable(m), keep.rownames = "term")[, .(y = as.integer(sub("y::(\\d+):nh", "\\1", term)), coef = Estimate, se = `Std. Error`)][, sample := lab] }
res <- rbind(run(d, "all"), run(d[ex_z > medx], "high exposure"), run(d[ex_z <= medx], "low exposure"))
m3 <- feols(lh ~ i(y, nh, ref = 2017) + i(y, I(nh * ex_z), ref = 2017) | tract + pair^high^y + pair^dbin^y, d, cluster = ~cty)
tri <- as.data.table(coeftable(m3), keep.rownames = "term")[grepl("ex_z", term)]
res <- rbind(res, unique(res[, .(sample)])[, `:=`(y = 2017L, coef = 0, se = 0)])
fwrite(res, file.path(OUT, "b4_hpi_border_ddd.csv"))
sink(file.path(OUT, "b4_hpi_border_ddd.txt"))
cat("FHFA tract HPI, border triple difference (near < 10 mi x high side x year), ref 2017; tracts", uniqueN(d$tract), "near-high", uniqueN(d[nh == 1, tract]), "\n")
print(as.data.frame(dcast(res, sample ~ y, value.var = "coef")[, lapply(.SD, \(x) if (is.numeric(x)) round(x, 4) else x)]), row.names = FALSE)
cat("SE:\n"); print(as.data.frame(dcast(res, sample ~ y, value.var = "se")[, lapply(.SD, \(x) if (is.numeric(x)) round(x, 4) else x)]), row.names = FALSE)
cat("\nnear x high x exposure (per SD):\n"); print(as.data.frame(tri[, .(term, est = round(Estimate, 4), se = round(`Std. Error`, 4))]), row.names = FALSE)
sink(); cat(readLines(file.path(OUT, "b4_hpi_border_ddd.txt")), sep = "\n")
g <- ggplot(res, aes(y, coef, colour = sample)) + geom_hline(yintercept = 0, colour = "grey60") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey60") +
  geom_pointrange(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se), position = position_dodge(.5), size = .15) +
  scale_colour_manual(values = c(all = "black", `high exposure` = "#e34a33", `low exposure` = "#2b8cbe")) +
  labs(x = NULL, y = "log HPI, near x high-side tract (vs 2017)", colour = NULL, title = "House prices in tracts with a cheap cross-border option (FHFA tract HPI)",
       subtitle = "Triple difference: tract, pair x side x year, pair x distance-bin x year FE; 95% CI by county") + theme_minimal(base_size = 9) + theme(legend.position = "bottom")
ggsave(file.path(OUT, "figures/b4_hpi_border_ddd.png"), g, width = 8, height = 4.5, dpi = 150)
# Robustness: near cutoffs 5/10/20 mi; low-side placebo (near x low x exposure) in the same model; exposure averaged at the
# state level (state of the tract, return-weighted) instead of the county.
ivr <- unique(readRDS(file.path(DATA, "built/geo/iv_tract_side.rds"))[, .(tract, dist_mi)])
d <- merge(d, ivr[, .(tract, dm = dist_mi)], by = "tract")
exs <- merge(ex, unique(d[, .(cty)]), by = "cty")[, .(ex_st = weighted.mean(excess_pp, returns)), by = .(st = substr(cty, 1, 2))]
exs[, ex_st_z := (ex_st - mean(ex_st)) / sd(ex_st)]
d <- merge(d[, st := substr(tract, 1, 2)], exs[, .(st, ex_st_z)], by = "st")
rb <- rbindlist(lapply(c(5, 10, 20), \(cut) {
  x <- copy(d)[, nr := as.integer(dm < cut)][, `:=`(nhx = nr * high * ex_z, nlx = nr * (1 - high) * ex_z, nhs = nr * high * ex_st_z, nh2 = nr * high)]
  m1 <- feols(lh ~ i(y, nh2, ref = 2017) + i(y, nhx, ref = 2017) + i(y, nlx, ref = 2017) | tract + pair^high^y + pair^dbin^y, x, cluster = ~cty)
  m2 <- feols(lh ~ i(y, nh2, ref = 2017) + i(y, nhs, ref = 2017) | tract + pair^high^y + pair^dbin^y, x, cluster = ~cty)
  rbind(as.data.table(coeftable(m1), keep.rownames = "term")[grepl("nhx|nlx", term)], as.data.table(coeftable(m2), keep.rownames = "term")[grepl("nhs", term)])[,
    `:=`(cutoff = cut, y = as.integer(sub("y::(\\d+):.*", "\\1", term)), which = sub("y::\\d+:", "", term))] }))
fwrite(rb, file.path(OUT, "b4_hpi_border_ddd_robust.csv"))
sink(file.path(OUT, "b4_hpi_border_ddd.txt"), append = TRUE)
cat("\n== Robustness (per SD exposure): nhx = near x high x county exposure; nlx = near x LOW x county exposure (placebo / inflow);\n   nhs = near x high x state-level exposure. Rows: cutoff x term; columns: years\n")
print(as.data.frame(dcast(rb, cutoff + which ~ y, value.var = "Estimate")[, lapply(.SD, \(v) if (is.numeric(v)) round(v, 4) else v)]), row.names = FALSE)
cat("SE:\n"); print(as.data.frame(dcast(rb, cutoff + which ~ y, value.var = "Std. Error")[, lapply(.SD, \(v) if (is.numeric(v)) round(v, 4) else v)]), row.names = FALSE)
sink()
