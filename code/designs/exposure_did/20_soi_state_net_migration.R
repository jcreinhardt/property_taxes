# States ranked by net migration as a share of population, IRS SOI state migration files 1112-2122 (years 2012-2022;
# file YYZZ = address change between the TY20YY and TY20ZZ returns, labelled by the second year).
# Per state-year, from the outflow and inflow files: individuals (n2, exemptions) and returns (n1) of
#   out-migrants to other states ("Total Migration-US"), to abroad ("Total Migration-Foreign"), in-migrants from other
#   states and from abroad, same-state movers, non-migrants.
# Population = the state's filers in year y-1 as the files report them: non-migrants + out-migrants (US + foreign).
# Same-state movers (about 3% of filers) are left out because files 1112 and 1213 do not report them; this is the SOI
# filing population, roughly 80% of the Census estimate. Rates in % of that population:
#   net interstate = (in from other states - out to other states) / pop; net total adds the foreign flows.
# Rankings by period means (2012-17 pre-cap, 2018-19 cap pre-COVID, 2020-22 COVID) and over all years. File 1617 (our
# 2017) codes ~30% too many movers in every state (see notebook 2026-10-27-diop-green-reconciliation), which scales
# in- and out-flows alike, so its net rates are inflated in absolute value by about the same factor; the pre-cap mean
# is reported with and without it.
suppressPackageStartupMessages({library(data.table); library(ggplot2)}); options(width = 220)
source("code/R/paths.R"); OUT <- out_dir("designs/exposure_did"); invisible(out_dir("designs/exposure_did/figures")); M <- file.path(DATA, "SOI/migration_state")
yrs <- 12:22
rd <- \(kind, yy) { x <- fread(file.path(M, sprintf("state%s%02d%02d.csv", kind, yy - 1, yy)), colClasses = "character", encoding = "Latin-1")
  setnames(x, tolower(names(x))); x[, y := 2000L + yy][] }
num <- \(z) as.numeric(z)
p <- rbindlist(lapply(yrs, \(yy) {
  o <- rd("outflow", yy); i <- rd("inflow", yy)
  o[, `:=`(s = as.integer(y1_statefips), d = as.integer(y2_statefips), nm = y2_state_name)]
  i[, `:=`(s = as.integer(y2_statefips), d = as.integer(y1_statefips), nm = y1_state_name)]
  pick <- \(x, cond, pre) x[eval(cond), setNames(.(s, num(n1), num(n2), num(agi)), c("s", paste0(pre, c("_n1", "_n2", "_agi"))))]
  Reduce(\(a, b) merge(a, b, by = "s", all.x = TRUE), list(
    pick(o, quote(d == s & grepl("Non-migrants", nm)), "nonmig"),
    pick(o, quote(d == 97 & grepl("Same State", nm)), "same"),
    pick(o, quote(d == 97 & grepl("Migration.US$", nm)), "out_us"),        # 1112/1213 spell "Total Migration US"
    pick(o, quote(d == 98 & grepl("Foreign$", nm)), "out_for"),
    pick(i, quote(d == 97 & grepl("Migration.US$", nm)), "in_us"),
    pick(i, quote(d == 98 & grepl("Foreign$", nm)), "in_for")))[, y := 2000L + yy][] }))[s <= 56]
stopifnot(p[, uniqueN(y), by = s][, all(V1 == length(yrs))], !anyNA(p[, !c("same_n1", "same_n2", "same_agi")]), p[is.na(same_n2), all(y <= 2013 | s == 11)])
fips <- data.table(s = c(1,2,4,5,6,8,9,10,11,12,13,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,44,45,46,47,48,49,50,51,53,54,55,56),
                   state = c("AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"))
p <- merge(p, fips, by = "s")
p[, `:=`(pop = nonmig_n2 + out_us_n2 + out_for_n2, ret = nonmig_n1 + out_us_n1 + out_for_n1)]
p[, `:=`(net_us  = 100 * (in_us_n2 - out_us_n2) / pop,                      # individuals, interstate only
         net_tot = 100 * (in_us_n2 + in_for_n2 - out_us_n2 - out_for_n2) / pop,
         net_ret = 100 * (in_us_n1 - out_us_n1) / ret,                      # returns, interstate
         in_rate = 100 * in_us_n2 / pop, out_rate = 100 * out_us_n2 / pop,
         net_agi = 100 * (in_us_agi - out_us_agi) / (nonmig_agi + out_us_agi + out_for_agi))]  # AGI of in- minus out-migrants, % of year-1 AGI
fwrite(p[order(state, y), .(state, s, y, pop, ret, same_n2, in_us_n2, out_us_n2, in_for_n2, out_for_n2, net_us, net_tot, net_ret, in_rate, out_rate, net_agi)],
       file.path(OUT, "20_soi_state_net_migration_panel.csv"))

per <- \(k, ys) p[y %in% ys, setNames(.(mean(get(k))), k), by = state]
r <- Reduce(\(a, b) merge(a, b, by = "state"), list(
  p[y %in% 2012:2017, .(pre = mean(net_us), pre_ex17 = mean(net_us[y != 2017]), pre_tot = mean(net_tot), pre_agi = mean(net_agi)), by = state],
  p[y %in% 2018:2019, .(cap = mean(net_us), cap_tot = mean(net_tot), cap_agi = mean(net_agi)), by = state],
  p[y %in% 2020:2022, .(covid = mean(net_us), covid_tot = mean(net_tot), covid_agi = mean(net_agi)), by = state],
  p[, .(all = mean(net_us), all_tot = mean(net_tot), all_ret = mean(net_ret), all_agi = mean(net_agi), pop_2016 = pop[y == 2016],
        cum_2012_22 = 100 * sum(in_us_n2 - out_us_n2) / pop[y == 2012]), by = state]))
r[, `:=`(d_cap = cap - pre_ex17, d_covid = covid - pre_ex17)]
r <- r[order(-all)][, rank_all := .I][order(-pre_ex17)][, rank_pre := .I][order(-cap)][, rank_cap := .I][order(-covid)][, rank_covid := .I][order(-all)]
fwrite(r, file.path(OUT, "20_soi_state_net_migration.csv"))

f <- \(x) sprintf("%+.2f", x)
sink(file.path(OUT, "20_soi_state_net_migration.txt"), split = TRUE)
cat("Net migration as % of SOI filing population (individuals = exemptions on returns; population = non-migrants + out-migrants in year y-1; same-state movers excluded, not in files 1112/1213).\n",
    "SOI state files 1112-2122, labelled by second year (file 1718 = 2018). Net = in-migrants from other states - out-migrants to other states, per year.\n",
    "Periods: pre = 2012-17 (pre_ex17 drops file 1617, which codes ~30% too many movers); cap = 2018-19; covid = 2020-22; all = 2012-22.\n",
    "tot adds foreign in/out flows; ret uses returns instead of individuals; agi = AGI of in- minus out-migrants, % of state AGI; cum = cumulative 2012-22 net, % of 2012 pop.\n\n", sep = "")
cat("Ranking by mean annual net interstate migration rate, 2012-2022 (all 50 states + DC):\n")
print(r[, .(rank_all, state, all = f(all), pre_ex17 = f(pre_ex17), cap = f(cap), covid = f(covid), d_cap = f(d_cap), d_covid = f(d_covid),
            all_tot = f(all_tot), all_ret = f(all_ret), all_agi = f(all_agi), cum_2012_22 = f(cum_2012_22), pop_2016_m = round(pop_2016 / 1e6, 1))], nrows = 60)
cat("\nRank correlations between periods (Spearman): pre_ex17 vs cap", round(cor(r$pre_ex17, r$cap, method = "spearman"), 2),
    "| pre_ex17 vs covid", round(cor(r$pre_ex17, r$covid, method = "spearman"), 2), "| cap vs covid", round(cor(r$cap, r$covid, method = "spearman"), 2),
    "| all: individuals vs returns", round(cor(r$all, r$all_ret, method = "spearman"), 2), "| all: interstate vs incl. foreign", round(cor(r$all, r$all_tot, method = "spearman"), 2), "\n")
cat("\nFile 1617 check: mean |net rate| across states by year (the 2017 value is scaled by the mover-coding artifact):\n")
print(p[, .(mean_abs_net = round(mean(abs(net_us)), 3), mean_in = round(mean(in_rate), 2), mean_out = round(mean(out_rate), 2)), by = y][order(y)])
cat("\nLargest changes 2018-19 vs 2012-16 (pp of population per year):\n")
print(r[order(d_cap)][c(1:8, (.N - 7):.N), .(state, pre_ex17 = f(pre_ex17), cap = f(cap), d_cap = f(d_cap))])
cat("\nYear-by-year net interstate rate (%), selected states:\n")
print(dcast(p[state %in% c("FL","TX","NV","AZ","ID","SC","NC","TN","CA","NY","NJ","IL","CT","MA","PA","MD","DC"), .(state, y, v = f(net_us))], state ~ y, value.var = "v")[order(-as.numeric(sub("\\+", "", `2019`)))])
sink()

# Figure: ranked dot plot, three period means per state
long <- melt(r[, .(state, all, `2012-16 (pre-cap, excl. 1617)` = pre_ex17, `2018-19 (cap, pre-COVID)` = cap, `2020-22 (COVID)` = covid)],
             id.vars = c("state", "all"), variable.name = "period", value.name = "rate")
long[, state := factor(state, r[order(all), state])]
g <- ggplot(long, aes(rate, state)) + geom_vline(xintercept = 0, colour = "grey60") +
  geom_segment(data = r, aes(x = 0, xend = all, y = factor(state, levels(long$state)), yend = factor(state, levels(long$state))), colour = "grey80", inherit.aes = FALSE) +
  geom_point(aes(colour = period, shape = period), size = 1.8) +
  scale_colour_manual(values = c("grey45", "#2166ac", "#b2182b")) + scale_shape_manual(values = c(16, 17, 15)) +
  labs(x = "net interstate migration, % of SOI filing population per year (individuals)", y = NULL, colour = NULL, shape = NULL,
       title = "States ranked by net migration as a share of population (IRS SOI state migration files)",
       subtitle = "Ordered by the 2012-22 mean (grey bar). Net = in-migrants from other states minus out-migrants to other states, per year.") +
  theme_ptax(9) + theme(legend.position = "bottom", panel.grid.major.y = element_blank())
ggsave(file.path(OUT, "figures/20_soi_state_net_migration.png"), g, width = 8, height = 9.5, dpi = 150)
