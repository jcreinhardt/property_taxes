# (b) Dispersion of local tax levels across school districts within metro areas (CBSA, from the F-33 district files) and its
# response to SALT exposure. Outcomes per CBSA-year (balanced districts, CBSAs with >= 10 districts): SD and p90-p10 of log
# property tax per pupil, enrollment-weighted SD. Exposure: return-weighted mean of 2016 county excess SALT per return, SD units
# across CBSAs. Event study ~ i(y, exposure, ref 2017) | cbsa + state x year (state of the CBSA's largest district), SE by CBSA.
# Prediction if the cap pressures high-tax jurisdictions most: compression (negative coefficients) after 2017.
suppressPackageStartupMessages({library(data.table); library(fixest); library(ggplot2)})
ROOT <- "/Users/jr2728/PhD/projects/property_taxes"; OUT <- file.path(ROOT, "output/analysis/salt_local"); E <- file.path(ROOT, "data/external/elsec")
p <- readRDS(file.path(ROOT, "data/local_juris/district_finance.rds"))
cb <- rbindlist(lapply(c(12:21, 23), \(yy) { fn <- file.path(E, sprintf("elsec%02d.txt", yy)); hd <- gsub('"', "", strsplit(readLines(fn, 1), ",")[[1]])
  x <- fread(fn, skip = 1, header = FALSE, colClasses = "character", fill = TRUE); setnames(x, seq_along(hd), hd); x[, .(leaid = NCESID, cbsa = CBSA)] }))
cb <- cb[cbsa != "" & cbsa != "N", .(cbsa = names(which.max(table(cbsa)))), by = leaid]
ex <- fread(file.path(ROOT, "data/local_juris/salt_exposure_county2016.csv"))[, conum := sprintf("%05d", county)]
p <- merge(merge(p, cb, by = "leaid"), ex[, .(conum, excess_pp, returns)], by = "conum")
p <- p[ptax > 0 & enr >= 100][, bal := uniqueN(y) == 12, by = leaid][bal == TRUE][, lptpp := log(ptax / enr)]
excb <- unique(p[, .(cbsa, conum, excess_pp, returns)])[, .(ex = weighted.mean(excess_pp, returns)), by = cbsa][, ex_z := (ex - mean(ex)) / sd(ex)]
stc <- p[y == 2017, .(st = substr(conum[which.max(enr)], 1, 2)), by = cbsa]
dsp <- p[, .(n_d = .N, sd = sd(lptpp), p9010 = diff(quantile(lptpp, c(.1, .9))),
             wsd = sqrt(sum(enr * (lptpp - weighted.mean(lptpp, enr))^2) / sum(enr))), by = .(cbsa, y)][n_d >= 10]
dsp <- merge(merge(dsp, excb, by = "cbsa"), stc, by = "cbsa")
res <- rbindlist(lapply(c("sd", "p9010", "wsd"), \(v) { m <- feols(as.formula(sprintf("%s ~ i(y, ex_z, ref = 2017) | cbsa + st^y", v)), dsp, cluster = ~cbsa)
  rbind(data.table(y = 2017L, coef = 0, se = 0), as.data.table(coeftable(m), keep.rownames = "term")[, .(y = as.integer(sub("y::(\\d+):ex_z", "\\1", term)), coef = Estimate, se = `Std. Error`)])[,
    `:=`(outcome = v, cbsas = uniqueN(dsp$cbsa), mean17 = dsp[y == 2017, mean(get(v))])] }))
fwrite(res, file.path(OUT, "a4_cbsa_dispersion.csv"))
sink(file.path(OUT, "a4_cbsa_dispersion.txt"))
cat("Within-CBSA dispersion of log property tax per pupil across school districts; CBSAs with >= 10 balanced districts:", uniqueN(dsp$cbsa), "\n")
for (k in split(res, by = "outcome")) { cat("\n==", k$outcome[1], "| 2017 mean:", round(k$mean17[1], 3), "\n")
  print(as.data.frame(dcast(k, . ~ y, value.var = "coef")[, -1][, lapply(.SD, round, 4)]), row.names = FALSE)
  print(as.data.frame(dcast(k, . ~ y, value.var = "se")[, -1][, lapply(.SD, round, 4)]), row.names = FALSE) }
cat("\nMean dispersion by year (all CBSAs):\n"); print(dsp[, .(sd = round(mean(sd), 3), p9010 = round(mean(p9010), 3), wsd = round(mean(wsd), 3)), by = y][order(y)])
sink(); cat(readLines(file.path(OUT, "a4_cbsa_dispersion.txt")), sep = "\n")
g <- ggplot(res, aes(y, coef)) + geom_hline(yintercept = 0, colour = "grey60") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey60") +
  geom_pointrange(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se), size = .2) + facet_wrap(~ outcome, nrow = 1, scales = "free_y") +
  scale_x_continuous(breaks = seq(2012, 2023, 2)) + labs(x = "fiscal year", y = "effect of +1 SD CBSA exposure (vs 2017)",
  title = "(b) Dispersion of property tax per pupil across school districts within metros, by SALT exposure",
  subtitle = "CBSA + state x year FE; 95% CI by CBSA; sd, p90-p10 and enrollment-weighted sd of log property tax per pupil") + theme_minimal(base_size = 9)
ggsave(file.path(OUT, "figures/a4_cbsa_dispersion.png"), g, width = 10, height = 3.8, dpi = 150)
