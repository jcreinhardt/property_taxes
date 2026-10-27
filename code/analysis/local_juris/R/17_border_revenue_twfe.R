# TWFE on border-side revenue (side-year panel from R/16: 1% parcel panel x 100, constant-coverage counties, sides ordered
# by 2013-17 median effective rate). Outcomes in logs: total revenue, mean bill per parcel, parcels (log rev = log bill + log n).
# (1) Differential: y_spt = a_sp + d_pt + sum_t b_t 1[lower side] 1[year t], 2017 = 0 -> growth of lower- relative to
#     higher-tax side since 2017 (pair x year FE absorb anything common to both sides). SE clustered by pair.
# (2) By side: y_spt = a_sp + g_t, separately for lower and higher sides, 2017 = 0.
# Panels: full (pair-years with both sides) and balanced (both sides in all years).
suppressPackageStartupMessages({library(data.table); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- file.path(ROOT, "output/analysis/local_juris")
d <- fread(file.path(ROOT, "data/local_juris/border_revenue_side_year.csv"))
d[, `:=`(lrev = log(rev), lbill = log(mbill), ln = log(n), low = as.integer(side == "lower-tax side"), sid = paste(pair, st_a))]
d <- d[, if (.N == 2) .SD, by = .(pair, y)]; NY <- uniqueN(d$y)
d[, balanced := .N == 2 * NY, by = pair]
tidy <- function(f, lab, v, panel, n) { ct <- as.data.table(coeftable(f), keep.rownames = "term")
  rbind(data.table(y = 2017L, coef = 0, se = 0), ct[, .(y = as.integer(sub("^y::(\\d+).*", "\\1", term)), coef = Estimate, se = `Std. Error`)])[,
    `:=`(spec = lab, outcome = v, panel = panel, n_pairs = n)] }
res <- rbindlist(lapply(c("full", "balanced"), \(p) { dd <- if (p == "full") d else d[balanced == TRUE]; np <- uniqueN(dd$pair)
  rbindlist(lapply(c("lrev", "lbill", "ln"), \(v) rbind(
    tidy(feols(as.formula(sprintf("%s ~ i(y, low, ref = 2017) | sid + pair^y", v)), dd, cluster = ~pair),
         "lower minus higher side (pair x year FE)", v, p, np),
    tidy(feols(as.formula(sprintf("%s ~ i(y, ref = 2017) | sid", v)), dd[low == 1], cluster = ~pair), "lower-tax side (side FE)", v, p, np),
    tidy(feols(as.formula(sprintf("%s ~ i(y, ref = 2017) | sid", v)), dd[low == 0], cluster = ~pair), "higher-tax side (side FE)", v, p, np)))) }))
res[, outcome := factor(outcome, c("lrev", "lbill", "ln"), c("log revenue", "log mean bill", "log parcels"))]
fwrite(res, file.path(OUT, "border_revenue_twfe.csv"))
sink(file.path(OUT, "border_revenue_twfe.txt"))
for (k in split(res, by = c("spec", "panel"))) {
  cat("\n==", k$spec[1], "|", k$panel[1], "| pairs:", k$n_pairs[1], "\n")
  print(as.data.frame(dcast(k, y ~ outcome, value.var = c("coef", "se"))[, lapply(.SD, round, 3)]), row.names = FALSE)
}
sink(); cat(readLines(file.path(OUT, "border_revenue_twfe.txt")), sep = "\n")
pl <- res[panel == "full" & outcome != "log parcels"]
pl[, spec := factor(spec, c("lower minus higher side (pair x year FE)", "higher-tax side (side FE)", "lower-tax side (side FE)"))]
pl[, facet := ifelse(spec == "lower minus higher side (pair x year FE)", "lower minus higher side (pair x year FE)", "each side separately (side FE)")]
pl[, facet := factor(facet, c("each side separately (side FE)", "lower minus higher side (pair x year FE)"))]
g <- ggplot(pl, aes(y, coef, colour = spec, shape = outcome)) + geom_hline(yintercept = 0, colour = "grey60") +
  geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey60") +
  geom_pointrange(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se), position = position_dodge(width = .6), size = .25) +
  facet_wrap(~ facet, ncol = 1, scales = "free_y") + scale_x_continuous(breaks = 2013:2021) +
  scale_colour_manual(values = c("lower minus higher side (pair x year FE)" = "black", "higher-tax side (side FE)" = "#e34a33",
                                 "lower-tax side (side FE)" = "#2b8cbe")) +
  labs(x = NULL, y = "change vs 2017 (log points)", colour = NULL, shape = NULL,
       title = "Border revenue TWFE: lower- vs higher-tax side (2017 = 0)",
       subtitle = sprintf("%d pairs, full panel; 1%% parcel panel within 20 mi, constant-coverage counties; 95%% CIs clustered by pair",
                          res[panel == "full", n_pairs[1]])) +
  theme_minimal(base_size = 10) + theme(legend.position = "bottom", legend.box = "vertical")
ggsave(file.path(OUT, "figures/border_revenue_twfe.png"), g, width = 7.5, height = 7, dpi = 150)
