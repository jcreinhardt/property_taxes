# Income of movers vs stayers by county SALT exposure (SOI county files 2012-2022; flow year = y2 of the file, ref 2017).
# Per county-year: AGI per return of non-migrants (stayers), out-movers (all US destinations), in-movers (all US origins),
# and net AGI flow (in - out) as % of stayers' AGI. Outcomes: log AGI per return of each group, and movers relative to
# stayers (log ratio). Event study ~ i(y, ex_z, ref 2017) | county + state x year, SE by county.
suppressPackageStartupMessages({library(data.table); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- file.path(ROOT, "output/analysis/salt_local"); M <- file.path(ROOT, "data/SOI/migration_county")
rd <- \(kind, yy) { x <- fread(file.path(M, sprintf("county%s%02d%02d.csv", kind, yy - 1, yy)), colClasses = "character", encoding = "Latin-1")
  setnames(x, tolower(names(x))); x[, y := 2000L + yy][] }
g <- rbindlist(lapply(12:22, \(yy) {
  o <- rd("outflow", yy); i <- rd("inflow", yy)
  o[, c := sprintf("%02d%03d", as.integer(y1_statefips), as.integer(y1_countyfips))]
  i[, c := sprintf("%02d%03d", as.integer(y2_statefips), as.integer(y2_countyfips))]
  st <- o[grepl("Non-migrants", y2_countyname), .(c, y, n_st = as.numeric(n1), a_st = as.numeric(agi))]
  ou <- o[as.integer(y2_statefips) == 97 & as.integer(y2_countyfips) == 0, .(c, n_out = as.numeric(n1), a_out = as.numeric(agi))]
  ix <- i[as.integer(y1_statefips) == 97 & as.integer(y1_countyfips) == 0, .(c, n_in = as.numeric(n1), a_in = as.numeric(agi))]
  Reduce(\(a, b) merge(a, b, by = "c"), list(st, ou, ix)) }))
for (k in c("n_st", "a_st", "n_out", "a_out", "n_in", "a_in")) set(g, which(g[[k]] <= 0), k, NA)
g[, `:=`(l_st = log(a_st / n_st), l_out = log(a_out / n_out), l_in = log(a_in / n_in))]
g[, `:=`(rel_out = l_out - l_st, rel_in = l_in - l_st, net_agi = 100 * (a_in - a_out) / a_st, net_ret = 100 * (n_in - n_out) / n_st)]
ex <- fread(file.path(ROOT, "data/local_juris/salt_exposure_county2016.csv"))[, c := sprintf("%05d", county)]
g <- merge(g, ex[, .(c, ex_z)], by = "c")[, st := substr(c, 1, 2)][, bal := uniqueN(y) == 11, by = c][bal == TRUE]
v <- c(l_st = "log AGI per return, stayers", l_out = "log AGI per return, out-movers", l_in = "log AGI per return, in-movers",
       rel_out = "out-movers relative to stayers (log)", rel_in = "in-movers relative to stayers (log)",
       net_ret = "net migration, returns (% of stayers)", net_agi = "net AGI flow (% of stayers' AGI)")
res <- rbindlist(lapply(names(v), \(k) { m <- feols(as.formula(sprintf("%s ~ i(y, ex_z, ref = 2017) | c + st^y", k)), g[is.finite(get(k))], cluster = ~c)
  as.data.table(coeftable(m), keep.rownames = "term")[, .(var = k, label = v[k], y = as.integer(sub("y::(\\d+):ex_z", "\\1", term)), b = Estimate, se = `Std. Error`)] }))
res <- rbind(res, data.table(var = names(v), label = v, y = 2017L, b = 0, se = 0))[order(var, y)]
fwrite(res, file.path(OUT, "c23_soi_agi_in_out.csv"))
sink(file.path(OUT, "c23_soi_agi_in_out.txt"), split = TRUE)
cat("Counties:", uniqueN(g$c), "| per SD of county exposure, ref 2017; county + state x year FE; SE by county\n")
cat("Means: net migration", round(mean(g$net_ret, na.rm = TRUE), 2), "% of stayers; out-movers / stayers log AGI", round(mean(g$rel_out, na.rm = TRUE), 3),
    "; in-movers", round(mean(g$rel_in, na.rm = TRUE), 3), "\n")
print(dcast(res[, .(var, y, cell = sprintf("%.3f (%.3f)", b, se))], y ~ var, value.var = "cell"))
sink()
res[, label := factor(label, v)]
gg <- ggplot(res, aes(y, b)) + geom_hline(yintercept = 0, colour = "grey60") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey40") +
  geom_ribbon(aes(ymin = b - 1.96 * se, ymax = b + 1.96 * se), fill = "#2a64a8", alpha = .15) + geom_line(colour = "#2a64a8") + geom_point(colour = "#2a64a8", size = 1) +
  facet_wrap(~ label, scales = "free_y", ncol = 2) + scale_x_continuous(breaks = 2012:2022) +
  labs(x = "migration year (address change between tax years y-1 and y)", y = "coefficient per SD of county SALT exposure, relative to 2017",
       title = "Movers' and stayers' incomes and net flows by county SALT exposure (IRS SOI)", subtitle = "county + state x year FE; 95% CI, SE by county") +
  theme_minimal(base_size = 9)
ggsave(file.path(OUT, "figures/c23_soi_agi_in_out.png"), gg, width = 10, height = 10, dpi = 150)
