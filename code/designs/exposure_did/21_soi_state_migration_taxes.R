# Are changes in states' net migration rates related to changes in their effective income tax rates? State x year panel
# 2012-2022: net interstate migration rates from 20_soi_state_net_migration.R (SOI state files; % of filing population,
# individuals; also AGI of in- minus out-migrants in % of state AGI, and gross in and out rates) against three effective
# state income tax rate measures from build/exposure/07_state_income_tax_rates.R (% of income):
#   eff_law (fixed 2016 national household sample through each state-year's law, TAXSIM: law only), eff_law_top (the same for
#   the top 10% of households), eff_stc (Census collections / SOI AGI), eff_acs (TAXSIM on each year's ACS households).
# Specifications, weights = 2016 filing population, SE clustered by state:
#   (a) levels, state + year FE:         net_sy = a_s + g_y + b eff_sy
#   (b) with a lag:                       net_sy = a_s + g_y + b0 eff_sy + b1 eff_s,y-1
#   (c) first differences, year FE:      d net_sy = g_y + b d eff_sy
#   (d) long differences across the 51 states: mean 2018-19 minus mean 2012-16, and 2020-22 minus 2012-16, net on eff.
# Migration year y = file (y-1, y) = moves during ~y; the tax rate is that of tax year y. File 1617 (2017) codes ~30% too many
# movers, so net rates in 2017 are inflated; (a)-(c) are repeated without 2017.
suppressPackageStartupMessages({library(data.table); library(fixest); library(ggplot2)}); options(width = 200)
source("code/R/paths.R"); OUT <- out_dir("designs/exposure_did"); invisible(out_dir("designs/exposure_did/figures"))
m <- fread(file.path(OUT, "20_soi_state_net_migration_panel.csv"))
t <- fread(file.path(DATA, "built/exposure/state_income_tax_rates.csv"))
p <- merge(m, t[, .(state, y, eff_law, eff_law_top, mtr_law_top, eff_stc, eff_acs)], by = c("state", "y"))
stopifnot(nrow(p) == 51 * 11)
p <- merge(p, p[y == 2016, .(state, w = pop)], by = "state")[order(state, y)]
p[, `:=`(l_eff_law = shift(eff_law), l_eff_law_top = shift(eff_law_top), l_eff_stc = shift(eff_stc), l_eff_acs = shift(eff_acs),
         d_net_us = net_us - shift(net_us), d_net_agi = net_agi - shift(net_agi), d_in = in_rate - shift(in_rate), d_out = out_rate - shift(out_rate),
         d_eff_law = eff_law - shift(eff_law), d_eff_law_top = eff_law_top - shift(eff_law_top), d_eff_stc = eff_stc - shift(eff_stc), d_eff_acs = eff_acs - shift(eff_acs)), by = state]
X <- c(eff_law = "law only (fixed sample)", eff_law_top = "law only, top 10% households", eff_stc = "collections / AGI", eff_acs = "ACS households x law")
Y <- c(net_us = "net in-migration (pp of pop.)", net_agi = "net AGI inflow (% of AGI)", in_rate = "in-rate (pp)", out_rate = "out-rate (pp)")
fmt <- \(m, k) { ct <- coeftable(m); sprintf("%+.3f (%.3f)", ct[k, 1], ct[k, 2]) }
run <- \(yv, xv, d = p) rbindlist(list(
  data.table(spec = "levels | state + year", b = fmt(feols(as.formula(sprintf("%s ~ %s | state + y", yv, xv)), d, weights = ~w, cluster = ~state), xv)),
  data.table(spec = "levels + lag: current", b = fmt(mm <- feols(as.formula(sprintf("%s ~ %s + l_%s | state + y", yv, xv, xv)), d[!is.na(get(paste0("l_", xv)))], weights = ~w, cluster = ~state), xv)),
  data.table(spec = "levels + lag: lag", b = fmt(mm, paste0("l_", xv))),
  data.table(spec = "first differences | year", b = fmt(feols(as.formula(sprintf("d_%s ~ d_%s | y", sub("_rate", "", yv), xv)), d[!is.na(get(paste0("d_", xv)))], weights = ~w, cluster = ~state), paste0("d_", xv)))))
res <- rbindlist(lapply(names(Y), \(yv) rbindlist(lapply(names(X), \(xv) run(yv, xv)[, `:=`(y = yv, x = xv)]))))
res17 <- rbindlist(lapply(names(Y), \(yv) rbindlist(lapply(names(X), \(xv) run(yv, xv, p[y != 2017])[, `:=`(y = yv, x = xv)]))))
# long differences
ld <- p[, .(pre = mean(net_us[y %in% 2012:2016]), cap = mean(net_us[y %in% 2018:2019]), cov = mean(net_us[y %in% 2020:2022]),
            pre_agi = mean(net_agi[y %in% 2012:2016]), cap_agi = mean(net_agi[y %in% 2018:2019]), cov_agi = mean(net_agi[y %in% 2020:2022]),
            e_pre = mean(eff_law[y %in% 2012:2016]), e_cap = mean(eff_law[y %in% 2018:2019]), e_cov = mean(eff_law[y %in% 2020:2022]),
            s_pre = mean(eff_stc[y %in% 2012:2016]), s_cap = mean(eff_stc[y %in% 2018:2019]), s_cov = mean(eff_stc[y %in% 2020:2022]),
            t_pre = mean(eff_law_top[y %in% 2012:2016]), t_cap = mean(eff_law_top[y %in% 2018:2019]), t_cov = mean(eff_law_top[y %in% 2020:2022]), w = w[1]), by = state]
ld[, `:=`(dn_cap = cap - pre, dn_cov = cov - pre, da_cap = cap_agi - pre_agi, da_cov = cov_agi - pre_agi, de_cap = e_cap - e_pre, de_cov = e_cov - e_pre,
          ds_cap = s_cap - s_pre, ds_cov = s_cov - s_pre, dt_cap = t_cap - t_pre, dt_cov = t_cov - t_pre)]
ldr <- rbindlist(lapply(c("dn", "da"), \(o) rbindlist(lapply(c("de", "dt", "ds"), \(x) rbindlist(lapply(c("cap", "cov"), \(per) {
  d <- ld[!is.na(get(paste0(x, "_", per)))]; f <- as.formula(sprintf("%s_%s ~ %s_%s", o, per, x, per))
  data.table(outcome = c(dn = "net rate", da = "net AGI")[o], x = c(de = "eff_law", dt = "eff_law_top", ds = "eff_stc")[x],
             period = c(cap = "2018-19 vs 2012-16", cov = "2020-22 vs 2012-16")[per], n = nrow(d),
             weighted = fmt(feols(f, d, weights = ~w, vcov = "hetero"), 2), unweighted = fmt(feols(f, d, vcov = "hetero"), 2)) }))))))
fwrite(rbind(res[, .(sample = "2012-22", y, x, spec, b)], res17[, .(sample = "excl. 2017", y, x, spec, b)]), file.path(OUT, "21_soi_state_migration_taxes.csv"))
fwrite(ldr, file.path(OUT, "21_soi_state_migration_taxes_longdiff.csv"))

sink(file.path(OUT, "21_soi_state_migration_taxes.txt"), split = TRUE)
cat("Net interstate migration (pp of SOI filing population per year) on effective state income tax rates (% of income), 51 states x 2012-2022.\n",
    "Coefficients = pp of net migration per 1 pp of effective rate; weights = 2016 filing population; SE clustered by state.\n\n", sep = "")
cat("Variation in the tax measures: SD of within-state year-to-year changes (pp) and share of state-years with |change| > 0.1 pp\n")
print(p[!is.na(d_eff_law), .(sd_law = round(sd(d_eff_law), 3), big_law = round(mean(abs(d_eff_law) > .1), 2), sd_law_top = round(sd(d_eff_law_top), 3), big_law_top = round(mean(abs(d_eff_law_top) > .1), 2),
                           sd_stc = round(sd(d_eff_stc, na.rm = TRUE), 3), big_stc = round(mean(abs(d_eff_stc) > .1, na.rm = TRUE), 2), sd_acs = round(sd(d_eff_acs), 3))])
cat("\nLevels 2012-22, cross-section of state means: net rate vs effective rate (law only) -- the familiar correlation\n")
cs <- p[, .(net = mean(net_us), agi = mean(net_agi), eff = mean(eff_law), eff_top = mean(eff_law_top), w = w[1]), by = state]
cat(sprintf("  corr(net, eff_law) = %.2f, weighted slope %s pp per pp | corr(net AGI, eff_law_top) = %.2f, slope %s\n", cor(cs$net, cs$eff), fmt(feols(net ~ eff, cs, weights = ~w, vcov = "hetero"), 2),
            cor(cs$agi, cs$eff_top), fmt(feols(agi ~ eff_top, cs, weights = ~w, vcov = "hetero"), 2)))
cat("\n(a)-(c) Panel, 2012-2022:\n"); print(dcast(res[, .(outcome = Y[y], measure = X[x], spec, b)], outcome + measure ~ spec, value.var = "b"))
cat("\n(a)-(c) Panel without 2017 (file 1617):\n"); print(dcast(res17[, .(outcome = Y[y], measure = X[x], spec, b)], outcome + measure ~ spec, value.var = "b"))
cat("\n(d) Long differences across states (period means), net rate / net AGI on effective rate, SE robust:\n"); print(ldr)
cat("\nThe largest law changes and the state's net migration around them (eff_law change > 0.3 pp; net rate pp, years y-2..y+2):\n")
big <- p[abs(d_eff_law) > 0.3][order(-abs(d_eff_law))]
for (i in seq_len(nrow(big))) { st <- big$state[i]; yy <- big$y[i]; q <- p[state == st & y %in% (yy - 2):(yy + 2)]
  cat(sprintf("  %s %d: eff_law %+.2f pp (%.2f -> %.2f), top %+.2f | net rate: %s\n", st, yy, big$d_eff_law[i], big$eff_law[i] - big$d_eff_law[i], big$eff_law[i], big$d_eff_law_top[i],
              paste(sprintf("%d %+.2f", q$y, q$net_us), collapse = ", "))) }
sink()

# Figure: long differences, net rate change vs law-only effective rate change, both periods, plus the levels cross-section
pl <- rbind(ld[, .(state, w, x = de_cap, yv = dn_cap, panel = "2018-19 vs 2012-16")], ld[, .(state, w, x = de_cov, yv = dn_cov, panel = "2020-22 vs 2012-16")])
g1 <- ggplot(pl, aes(x, yv)) + geom_hline(yintercept = 0, colour = "grey70") + geom_vline(xintercept = 0, colour = "grey70") +
  geom_smooth(aes(weight = w), method = "lm", se = TRUE, colour = "#b2182b", fill = "#b2182b", alpha = .15, linewidth = .6) +
  geom_point(aes(size = w), colour = "#2166ac", alpha = .6) + geom_text(aes(label = state), size = 2.3, vjust = -.7) + scale_size_area(max_size = 6, guide = "none") +
  facet_wrap(~ panel) + labs(x = "change in effective state income tax rate, law only (pp of income)", y = "change in net interstate migration (pp of population per year)",
       title = "Changes in net migration vs changes in state income tax law, 51 states", subtitle = "Period means from the SOI state migration files; tax rate = fixed 2016 national household sample through each state's law (TAXSIM). Line: weighted OLS, 95% CI.") + theme_ptax(9)
g2 <- ggplot(cs, aes(eff, net)) + geom_hline(yintercept = 0, colour = "grey70") + geom_smooth(aes(weight = w), method = "lm", colour = "#b2182b", fill = "#b2182b", alpha = .15, linewidth = .6) +
  geom_point(aes(size = w), colour = "#2166ac", alpha = .6) + geom_text(aes(label = state), size = 2.3, vjust = -.7) + scale_size_area(max_size = 6, guide = "none") +
  labs(x = "effective state income tax rate, law only (% of income), 2012-22 mean", y = "net interstate migration (pp of population per year), 2012-22 mean", title = "Levels: states with higher income taxes lose people") + theme_ptax(9)
ggsave(file.path(OUT, "figures/21_soi_state_migration_taxes.png"), g1, width = 10, height = 5, dpi = 150)
ggsave(file.path(OUT, "figures/21_soi_state_migration_taxes_levels.png"), g2, width = 6, height = 5, dpi = 150)
