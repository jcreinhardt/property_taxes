# School-district finance panel FY2012-2023 (Census Annual Survey of School System Finances, district files elsecYY.txt in
# data/external/elsec/): property tax T06, total local revenue TLOCREV, state revenue TSTREV, current spending TCURELSC,
# enrollment V33, keyed by NCESID (= SEDA / TIGER LEAID). Amounts in $ thousands.
# (a) District revenue event study on county SALT exposure: log T06 ~ i(y, ex_z) | district + state x year.
# (b) Dispersion: within-county and within-state SD of log property tax per pupil across districts, by year; event study of
#     the within-county SD on exposure (county + state x year FE).
# Border IV at the district level: district near x high = majority of its tracts (R/26). DDD on log T06 and log T06 per pupil:
#     i(y, nh) | district + pair^high^y + pair^dbin^y.
suppressPackageStartupMessages({library(data.table); library(fixest); library(ggplot2)})
ROOT <- "/Users/jr2728/PhD/projects/property_taxes"; OUT <- file.path(ROOT, "output/analysis/salt_local"); E <- file.path(ROOT, "data/external/elsec")
rd <- function(yy) { if (yy == 22) x <- as.data.table(readxl::read_excel(file.path(E, "elsec22.xlsx"), col_types = "text")) else {  # 2022 txt is truncated
  fn <- file.path(E, sprintf("elsec%02d.txt", yy)); hd <- gsub('"', "", strsplit(readLines(fn, 1), ",")[[1]])
  x <- fread(fn, skip = 1, header = FALSE, colClasses = "character", fill = TRUE); setnames(x, seq_along(hd), hd) }
  x[, .(y = 2000L + yy, leaid = NCESID, conum = CONUM, name = NAME, enr = as.numeric(V33), ptax = as.numeric(T06), loc = as.numeric(TLOCREV),
        strev = as.numeric(TSTREV), spend = as.numeric(TCURELSC), schlev = SCHLEV)] }
p <- rbindlist(lapply(12:23, rd))
p <- p[nchar(leaid) == 7 & leaid != "" & enr > 0]
cat("district-years:", nrow(p), "| districts:", uniqueN(p$leaid), "\n"); print(p[, .N, by = y][order(y)])
saveRDS(p, file.path(ROOT, "data/local_juris/district_finance.rds"))
ex <- fread(file.path(ROOT, "data/local_juris/salt_exposure_county2016.csv"))[, conum := sprintf("%05d", county)]
p <- merge(p, ex[, .(conum, ex_z, returns)], by = "conum")[, st := substr(conum, 1, 2)]
p[, bal := uniqueN(y) == 12, by = leaid]
es <- function(d, f, lab) { m <- feols(f, d, cluster = ~conum); ct <- as.data.table(coeftable(m), keep.rownames = "term")[grepl("^y::", term)]
  ct[, .(y = as.integer(sub("y::(\\d+):.*", "\\1", term)), coef = Estimate, se = `Std. Error`, spec = lab, n = nobs(m))] }
d <- p[bal == TRUE & ptax > 0][, `:=`(lpt = log(ptax), lptpp = log(ptax / enr), lspend = log(spend / enr), lst = log(pmax(strev, 1) / enr))]
res <- rbind(es(d, lpt ~ i(y, ex_z, ref = 2017) | leaid + st^y, "(a) log property tax, district, unweighted"),
             es(d, lpt ~ i(y, ex_z, ref = 2017) | leaid + st^y, "(a) log property tax, district, enrollment-weighted")[0],
             es(d, lptpp ~ i(y, ex_z, ref = 2017) | leaid + st^y, "(a) log property tax per pupil"),
             es(d, lspend ~ i(y, ex_z, ref = 2017) | leaid + st^y, "log current spending per pupil"),
             es(d, lst ~ i(y, ex_z, ref = 2017) | leaid + st^y, "log state revenue per pupil"))
mw <- feols(lpt ~ i(y, ex_z, ref = 2017) | leaid + st^y, d, cluster = ~conum, weights = ~enr)
res <- rbind(res, as.data.table(coeftable(mw), keep.rownames = "term")[, .(y = as.integer(sub("y::(\\d+):.*", "\\1", term)), coef = Estimate,
  se = `Std. Error`, spec = "(a) log property tax, district, enrollment-weighted", n = nobs(mw))])
# (b) dispersion across districts within county and within state
disp_c <- d[, .(n_d = .N, sd_c = sd(lptpp), enr = sum(enr)), by = .(conum, st, y)][n_d >= 3]
disp_c <- merge(disp_c, ex[, .(conum, ex_z)], by = "conum")[, bal := uniqueN(y) == 12, by = conum]
res <- rbind(res, es(disp_c[bal == TRUE], sd_c ~ i(y, ex_z, ref = 2017) | conum + st^y, "(b) within-county SD of log property tax per pupil"))
disp_s <- d[, .(sd_s = sd(lptpp), p90p10 = diff(quantile(lptpp, c(.1, .9))), n_d = .N), by = .(st, y)]
fwrite(res, file.path(OUT, "a3_district_event_studies.csv")); fwrite(disp_s, file.path(OUT, "a3_state_dispersion.csv"))
# border IV at the district level
ivt <- unique(readRDS(file.path(ROOT, "data/local_juris/iv_tract_side.rds"))[, .(tract, leaid, pair, high, dbin, near)])
dv <- ivt[!is.na(leaid), .(near = as.integer(mean(near) > .5), high = high[which.max(tabulate(match(high, high)))],
                            pair = names(which.max(table(pair))), dbin = names(which.max(table(dbin)))), by = leaid]
dd <- merge(d, dv, by = "leaid")[, nh := near * high]
ddd <- rbindlist(lapply(c("lpt", "lptpp"), \(v) {
  m <- feols(as.formula(sprintf("%s ~ i(y, nh, ref = 2017) | leaid + pair^high^y + pair^dbin^y", v)), dd, cluster = ~conum)
  m2 <- feols(as.formula(sprintf("%s ~ i(y, I(nh * ex_z), ref = 2017) + i(y, nh, ref = 2017) | leaid + pair^high^y + pair^dbin^y", v)), dd, cluster = ~conum)
  rbind(as.data.table(coeftable(m), keep.rownames = "term")[, spec := paste("DDD near x high:", v)],
        as.data.table(coeftable(m2), keep.rownames = "term")[grepl("ex_z", term)][, spec := paste("DDD near x high x exposure:", v)])[,
        `:=`(n_dist = uniqueN(dd$leaid), n_nh = uniqueN(dd[nh == 1, leaid]))] }))
fwrite(ddd, file.path(OUT, "a3_district_border_ddd.csv"))
sink(file.path(OUT, "a3_district_panel.txt"))
cat("School-district finance panel (Census F-33 district files FY2012-2023); balanced districts:", uniqueN(d$leaid), "\n")
for (k in split(res, by = "spec")) { cat("\n==", k$spec[1], "| obs:", k$n[1], "\n")
  print(as.data.frame(dcast(rbind(k, data.table(y = 2017L, coef = 0, se = 0, spec = k$spec[1], n = k$n[1])), . ~ y, value.var = "coef")[, -1][, lapply(.SD, round, 4)]), row.names = FALSE)
  print(as.data.frame(dcast(rbind(k, data.table(y = 2017L, coef = 0, se = 0, spec = k$spec[1], n = k$n[1])), . ~ y, value.var = "se")[, -1][, lapply(.SD, round, 4)]), row.names = FALSE) }
cat("\n== Within-state dispersion of log property tax per pupil across districts, national mean of state SDs by year\n")
print(disp_s[n_d >= 20, .(mean_sd = round(mean(sd_s), 3), mean_p90p10 = round(mean(p90p10), 3)), by = y][order(y)])
cat("\n== District border DDD (districts:", ddd$n_dist[1], "| near-high:", ddd$n_nh[1], ")\n")
print(as.data.frame(ddd[, .(spec, term, est = round(Estimate, 4), se = round(`Std. Error`, 4))]), row.names = FALSE)
sink(); cat(readLines(file.path(OUT, "a3_district_panel.txt")), sep = "\n")
# Figure + robustness: district border DDD by distance cutoff (near = majority of tracts < 5 / < 10 / < 20 mi) and by
# county exposure (above / below median of the sample's districts)
rob <- rbindlist(lapply(c(5, 10, 20), \(cut) {
  ivc <- unique(readRDS(file.path(ROOT, "data/local_juris/iv_tract_side.rds"))[, .(tract, leaid, pair, high, dbin, dist_mi)])
  dvc <- ivc[!is.na(leaid), .(nearc = as.integer(mean(dist_mi < cut) > .5), high = names(which.max(table(high))), pair = names(which.max(table(pair))),
                               dbin = names(which.max(table(dbin)))), by = leaid][, high := as.integer(high)]
  x <- merge(d, dvc, by = "leaid")[, nh := nearc * high]
  medx <- median(unique(x[, .(leaid, ex_z)])$ex_z)
  rbindlist(lapply(c("all", "high exposure", "low exposure"), \(s) {
    xs <- if (s == "all") x else if (s == "high exposure") x[ex_z > medx] else x[ex_z <= medx]
    m <- feols(lpt ~ i(y, nh, ref = 2017) | leaid + pair^high^y + pair^dbin^y, xs, cluster = ~conum)
    as.data.table(coeftable(m), keep.rownames = "term")[, .(y = as.integer(sub("y::(\\d+):nh", "\\1", term)), coef = Estimate, se = `Std. Error`)][,
      `:=`(cutoff = paste0("near < ", cut, " mi"), sample = s, n_nh = uniqueN(xs[nh == 1, leaid]))] })) }))
rob <- rbind(rob, unique(rob[, .(cutoff, sample, n_nh)])[, `:=`(y = 2017L, coef = 0, se = 0)], fill = TRUE)
fwrite(rob, file.path(OUT, "a3_district_border_ddd_robust.csv"))
g <- ggplot(rob, aes(y, coef, colour = sample)) + geom_hline(yintercept = 0, colour = "grey60") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey60") +
  geom_pointrange(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se), position = position_dodge(.5), size = .15) + facet_wrap(~ cutoff, nrow = 1) +
  scale_colour_manual(values = c(all = "black", `high exposure` = "#e34a33", `low exposure` = "#2b8cbe")) + scale_x_continuous(breaks = seq(2012, 2023, 2)) +
  labs(x = "fiscal year", y = "log property-tax revenue, near x high-side district (vs 2017)", colour = NULL,
       title = "School districts with a cheaper district across a nearby state line: property-tax revenue",
       subtitle = "Triple difference: district, pair x side x year, pair x distance-bin x year FE; 95% CI by county; Census F-33 FY2012-2023") +
  theme_minimal(base_size = 9) + theme(legend.position = "bottom")
ggsave(file.path(OUT, "figures/a3_district_border_ddd.png"), g, width = 11, height = 4.5, dpi = 150)
sink(file.path(OUT, "a3_district_panel.txt"), append = TRUE)
cat("\n== Robustness: district border DDD on log property tax by near cutoff and exposure split\n")
print(as.data.frame(dcast(rob, cutoff + sample + n_nh ~ y, value.var = "coef")[, lapply(.SD, \(x) if (is.numeric(x)) round(x, 3) else x)]), row.names = FALSE)
cat("SE:\n"); print(as.data.frame(dcast(rob, cutoff + sample ~ y, value.var = "se")[, lapply(.SD, \(x) if (is.numeric(x)) round(x, 3) else x)]), row.names = FALSE)
sink()
