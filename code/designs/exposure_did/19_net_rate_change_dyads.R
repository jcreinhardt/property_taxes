# Treatment = the policy-induced change in the net-of-federal-deduction state income tax rate, 2016 -> 2018 (2026-10-29).
# For each state and tax year, the effective state income tax rate paid after itemisation:
#   r_net(s,t) = eff_stc(s,t) - m_t x dd(s,t), with eff_stc = state individual income tax collections / SOI AGI (build/exposure/07),
#   dd = federally deducted state and local income taxes / AGI from Historic Table 2 (A18425 through TY2017; from TY2018 the limited
#   SALT A18460 times the income-tax share A18425 / A18300 of the amounts claimed), m_t = federal marginal rate of itemisers,
#   0.28 through 2017 and 0.24 from 2018 (statutory brackets where the median itemiser sits; the constant-0.25 variant isolates the cap).
# Treatment intensities, fixed over time: D_sub(s) = -(m18 dd(s,2018) - m16 dd(s,2016)) = lost federal subsidy in pp of AGI (the cap and
#   the bracket cut), D_net(s) = r_net(s,2018) - r_net(s,2016) (adds the state's own rate change), D_z1 = TAXSIM cap burden of 2016 owner
#   households / income (build/exposure/06, z1_cap_inc, pp). For a state pair, D_ij = D_i - D_j.
# Regressions: dyads (build/soi/10; y = second file year, 2012-2022): net_ijt (thousands of returns, Diop & Green's outcome) and
#   net per 1,000 origin returns (weighted by 2016 origin returns) ~ i(y, D_ij, ref 2017) | dyad + year, SE by origin (two-way origin + destination in the csv); samples
#   all 2,450 dyads, among CA NY FL TX, big four <-> other 46, among the other 46. States: net out-migration in thousands of returns
#   (unweighted) and the rate (2016-return weights) ~ i(y, D_s) | s + y as in 17, SE by state. Post-period summaries: mean 2018-19 and 2020-22 coefficients.
# Outputs: output/designs/exposure_did/c19_net_rate_change_{rates,treat,dyads,states}.csv, c19_net_rate_change.txt,
#   figures/c19_net_rate_change_{rates,dyads}.png.
suppressPackageStartupMessages({library(data.table); library(fixest); library(ggplot2); library(patchwork)})
source("code/R/paths.R"); OUT <- out_dir("designs/exposure_did"); FIG <- out_dir("designs/exposure_did/figures"); R <- file.path(DATA, "SOI/state_returns")
B4 <- c(6L, 36L, 12L, 48L); nm4 <- c(`6` = "CA", `36` = "NY", `12` = "FL", `48` = "TX")
# ---- 1. net effective state income tax rate by state-year
sp <- fread(file.path(DATA, "built/exposure/soi_state_salt_panel.csv"))[st != "DC"]
lim <- rbindlist(lapply(2018:2022, \(ty) { f <- list.files(R, sprintf("^%02din5[45]cm.*\\.csv$", ty - 2000), full.names = TRUE); stopifnot(length(f) == 1)
  x <- fread(f); setnames(x, toupper(names(x))); x[, A18460 := as.numeric(gsub(",", "", A18460))]
  x[AGI_STUB > 0 & STATE %in% state.abb, .(ty = ty, salt_lim = sum(A18460)), by = .(st = STATE)] }))
sp <- merge(sp, lim, by = c("st", "ty"), all.x = TRUE)
sp[, ded_inc := fifelse(ty <= 2017, salt_inc, salt_lim * salt_inc / salt_tot)]       # deducted state+local income taxes
rt <- fread(file.path(DATA, "built/exposure/state_income_tax_rates.csv"))[, .(st = state, ty = y, eff_stc)]
sp <- merge(sp, rt, by = c("st", "ty"))
sp[, `:=`(m = fifelse(ty <= 2017, 0.28, 0.24), dd = 100 * ded_inc / agi, item = n_item / n1)]
sp[, `:=`(sub = m * dd, sub25 = 0.25 * dd)][, `:=`(r_net = eff_stc - sub, r_net25 = eff_stc - sub25)]
fwrite(sp[, .(st, s = fips, ty, eff_stc, dd, item, sub, r_net, r_net25)], file.path(OUT, "c19_net_rate_change_rates.csv"))
tr <- dcast(sp[ty %in% c(2016, 2018)], st + fips ~ ty, value.var = c("sub", "sub25", "r_net", "eff_stc"))
tr[, `:=`(D_sub = sub_2016 - sub_2018, D_sub25 = sub25_2016 - sub25_2018, D_net = r_net_2018 - r_net_2016, D_stc = eff_stc_2018 - eff_stc_2016)]
z1 <- fread(file.path(DATA, "built/exposure/simulated_instrument_state.csv"))[, .(st = ab, D_z1 = 100 * z1_cap_inc)]
tr <- merge(tr, z1, by = "st"); setnames(tr, "fips", "s")
ec <- fread(file.path(DATA, "built/exposure/exposure_components_state.csv"), colClasses = c(geo = "character"))[, .(s = as.integer(geo), ex_ret = d_salt, returns16)]
tr <- merge(tr, ec, by = "s")
fwrite(tr, file.path(OUT, "c19_net_rate_change_treat.csv"))
# ---- 2. dyads
dy <- fread(file.path(DATA, "built/soi/state_dyads.csv"))[o <= 56 & o != 11 & d <= 56 & d != 11, .(o, d, y = y2, n1)]
dy <- merge(dy, dy[, .(o = d, d = o, y, n1_rev = n1)], by = c("o", "d", "y"))
Ds <- c("D_sub", "D_sub25", "D_net", "D_z1")
for (k in Ds) { dy <- merge(dy, tr[, .(o = s, x = get(k))], by = "o"); setnames(dy, "x", paste0(k, "_o")); dy <- merge(dy, tr[, .(d = s, x = get(k))], by = "d"); setnames(dy, "x", paste0(k, "_d")); dy[, (k) := get(paste0(k, "_o")) - get(paste0(k, "_d"))] }
dy <- merge(dy, tr[, .(o = s, ret_o = returns16)], by = "o")
dy[, `:=`(dyad = paste(o, d), net_k = (n1 - n1_rev) / 1e3, net_r = 1e3 * (n1 - n1_rev) / ret_o, big_o = o %in% B4, big_d = d %in% B4)]
dy[, grp := fcase(big_o & big_d, "among CA NY FL TX", big_o | big_d, "big four <-> other 46", default = "among the other 46")]
stopifnot(dy[, .N] == 2450 * 11)
smp <- list("all 2,450 dyads" = quote(TRUE), "among CA NY FL TX" = quote(grp == "among CA NY FL TX"), "big four <-> other 46" = quote(grp == "big four <-> other 46"), "among the other 46" = quote(grp == "among the other 46"))
ys <- c(net_k = "net out-migration, thousands of returns per dyad", net_r = "net out-migration per 1,000 origin returns (weighted by 2016 origin returns)")
run_d <- \(k, x, sname) { d <- dy[eval(smp[[sname]])]
  m <- feols(as.formula(sprintf("%s ~ i(y, %s, ref = 2017) | dyad + y", k, x)), d, weights = if (k == "net_r") ~ret_o else NULL, cluster = ~o); m2 <- summary(m, vcov = ~o + d)
  ct <- as.data.table(coeftable(m), keep.rownames = "t")[, .(outcome = k, treat = x, sample = sname, y = as.integer(sub("y::(\\d+):.*", "\\1", t)), b = Estimate, se = `Std. Error`, se_od = coeftable(m2)[, 2], dyads = uniqueN(d$dyad))]
  rbind(ct, ct[1, .(outcome, treat, sample, y = 2017L, b = 0, se = 0, se_od = 0, dyads)])[order(y)] }
g <- CJ(k = names(ys), x = Ds, s = names(smp), sorted = FALSE); dser <- rbindlist(Map(run_d, g$k, g$x, g$s))
fwrite(dser, file.path(OUT, "c19_net_rate_change_dyads.csv"))
# ---- 3. states (as 17)
M <- file.path(DATA, "SOI/migration_state"); rd <- \(kind, yy) { x <- fread(file.path(M, sprintf("state%s%02d%02d.csv", kind, yy - 1, yy)), colClasses = "character", encoding = "Latin-1"); setnames(x, tolower(names(x))); x[, y := 2000L + yy][] }
p <- rbindlist(lapply(12:22, \(yy) { o <- rd("outflow", yy); i <- rd("inflow", yy)
  o[, `:=`(s = as.integer(y1_statefips), d = as.integer(y2_statefips))]; i[, `:=`(s = as.integer(y2_statefips), d = as.integer(y1_statefips))]
  Reduce(\(a, b) merge(a, b, by = "s"), list(o[d == s & grepl("Non-migrants", y2_state_name), .(s, y, nonmig = as.numeric(n1))], o[d == 97 & grepl("Migration.US$", y2_state_name), .(s, out = as.numeric(n1))], i[d == 97 & grepl("Migration.US$", y1_state_name), .(s, inn = as.numeric(n1))])) }))[s <= 56 & s != 11]
p[, base := nonmig + out][, `:=`(r_net_out = 100 * (out - inn) / base, net_k = (out - inn) / 1e3)]; p <- merge(p, p[y == 2016, .(s, w = base)], by = "s"); p <- merge(p, tr[, c("s", Ds), with = FALSE], by = "s"); p[, big4 := s %in% B4]
ssmp <- list("all 50 states" = quote(TRUE), "big four (CA NY FL TX)" = quote(big4 == TRUE), "other 46 states" = quote(big4 == FALSE))
sys <- c(net_k = "net out-migration, thousands of returns (unweighted)", r_net_out = "net out-migration rate, pp of returns (2016-return weights)")
run_s <- \(k, x, sname) { d <- p[eval(ssmp[[sname]])]; few <- uniqueN(d$s) < 10
  m <- feols(as.formula(sprintf("%s ~ i(y, %s, ref = 2017) | s + y", k, x)), d, weights = if (k == "r_net_out") ~w else NULL, vcov = if (few) "hetero" else ~s)
  ct <- as.data.table(coeftable(m), keep.rownames = "t")[, .(outcome = k, treat = x, sample = sname, y = as.integer(sub("y::(\\d+):.*", "\\1", t)), b = Estimate, se = `Std. Error`)]
  rbind(ct, ct[1, .(outcome, treat, sample, y = 2017L, b = 0, se = 0)])[order(y)] }
g <- CJ(k = names(sys), x = Ds, s = names(ssmp), sorted = FALSE); sser <- rbindlist(Map(run_s, g$k, g$x, g$s)); fwrite(sser, file.path(OUT, "c19_net_rate_change_states.csv"))
# ---- 4. text
summ <- \(z, by) z[, .(pre = mean(b[y %in% 2013:2016]), p1819 = mean(b[y %in% 2018:2019]), p2022 = mean(b[y %in% 2020:2022]), se1819 = sqrt(mean(se[y %in% 2018:2019]^2)), se2022 = sqrt(mean(se[y %in% 2020:2022]^2)),
                        slope = coef(lm(b ~ y, data.frame(b = b[y <= 2017], y = y[y <= 2017])))[2]), by = by][, `:=`(d1819 = p1819 - pre, d2022 = p2022 - pre)]
f <- \(b, s) sprintf("%7.3f (%6.3f)", b, s)
sink(file.path(OUT, "c19_net_rate_change.txt"), split = TRUE)
cat("Net-of-deduction effective state income tax rate, r_net = STC collections / AGI - m x deducted income taxes / AGI (pp); m = 0.28 to 2017, 0.24 from 2018.\n\n")
cat("Big four and the extremes, TY2016 and TY2018 (pp of AGI):\n")
print(as.data.frame(tr[order(-D_sub), .(st, eff_stc16 = round(eff_stc_2016, 2), sub16 = round(sub_2016, 2), sub18 = round(sub_2018, 2), r_net16 = round(r_net_2016, 2), r_net18 = round(r_net_2018, 2), D_sub = round(D_sub, 3), D_net = round(D_net, 3), D_z1 = round(D_z1, 3), ex_ret = round(ex_ret))][st %in% nm4 | D_sub >= sort(D_sub, TRUE)[6] | D_sub <= sort(D_sub)[3]]), row.names = FALSE)
cat(sprintf("\nAcross the 50 states: SD of D_sub %.3f pp, D_net %.3f, D_z1 %.3f; corr(D_sub, D_net) %.2f, corr(D_sub, D_z1) %.2f, corr(D_sub, lost subsidy per return) %.2f, corr(D_sub, SALT/AGI 2017) n/a\n",
            sd(tr$D_sub), sd(tr$D_net), sd(tr$D_z1), cor(tr$D_sub, tr$D_net), cor(tr$D_sub, tr$D_z1), cor(tr$D_sub, tr$ex_ret)))
cat("\n== Dyads: net_ijt ~ i(year, D_ij, ref 2017) | dyad + year, SE by origin. Cells: mean 2018-19 minus mean 2013-16 (SE) | mean 2020-22 minus mean 2013-16 (SE) | pre slope per year. Per pp of D_ij.\n")
ds <- summ(dser, c("outcome", "treat", "sample"))[, cell := sprintf("%s | %s | %+.3f", f(d1819, se1819), f(d2022, se2022), slope)]
for (k in names(ys)) for (x in Ds) { cat(sprintf("\n-- %s on %s\n", ys[k], x)); print(as.data.frame(ds[outcome == k & treat == x, .(sample, cell)]), row.names = FALSE) }
cat("\n== States: y ~ i(year, D_s, ref 2017) | state + year, SE by state (hetero for the big four). Per pp of D_s.\n")
ss <- summ(sser, c("outcome", "treat", "sample"))[, cell := sprintf("%s | %s | %+.3f", f(d1819, se1819), f(d2022, se2022), slope)]
for (k in names(sys)) for (x in Ds) { cat(sprintf("\n-- %s on %s\n", sys[k], x)); print(as.data.frame(ss[outcome == k & treat == x, .(sample, cell)]), row.names = FALSE) }
cat("\nYear by year, net_k on D_sub (thousands per dyad per pp):\n"); print(as.data.frame(dcast(dser[outcome == "net_k" & treat == "D_sub"], y ~ sample, value.var = "b", fun.aggregate = \(v) round(v, 2))), row.names = FALSE)
sink()
# ---- 5. figures
pr <- sp[st %in% nm4 | st %in% c("NJ", "CT", "MA", "OR", "MN", "WA", "NV")]
g1 <- ggplot(pr, aes(ty, r_net, colour = st)) + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey60") + geom_line() + geom_point(size = 1) + labs(x = NULL, y = "net rate, pp of AGI", title = "Effective state income tax rate after the federal deduction, r_net") + theme_minimal(base_size = 8)
g2 <- ggplot(pr, aes(ty, sub, colour = st)) + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey60") + geom_line() + geom_point(size = 1) + labs(x = NULL, y = "m x deducted income taxes / AGI, pp", title = "Federal subsidy of the state income tax") + theme_minimal(base_size = 8)
g3 <- ggplot(tr, aes(ex_ret, D_sub, label = st)) + geom_text(size = 2.5) + labs(x = "lost SALT subsidy per return (SOI ZIP components, $)", y = "D_sub: lost subsidy 2016->18, pp of AGI", title = "Treatment vs the project's exposure") + theme_minimal(base_size = 8)
ggsave(file.path(FIG, "c19_net_rate_change_rates.png"), (g1 | g2) / g3 + plot_layout(heights = c(1, 1)), width = 11, height = 8, dpi = 150, bg = "white")
pl <- dser[treat %in% c("D_sub", "D_z1")][, `:=`(sample = factor(sample, names(smp)), ol = factor(ys[outcome], ys))]
g4 <- ggplot(pl, aes(y, b, colour = treat)) + facet_grid(ol ~ sample, scales = "free_y") + annotate("rect", xmin = 2019.5, xmax = 2022.5, ymin = -Inf, ymax = Inf, alpha = .06) + geom_hline(yintercept = 0, colour = "grey50") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey60") +
  geom_ribbon(aes(ymin = b - 1.96 * se, ymax = b + 1.96 * se, fill = treat), alpha = .12, colour = NA) + geom_line() + geom_point(size = 1) + scale_x_continuous(breaks = seq(2012, 2022, 2)) + scale_colour_brewer(NULL, palette = "Dark2") + scale_fill_brewer(NULL, palette = "Dark2") +
  labs(x = "SOI file year (second year)", y = "per pp of D_ij = D_i - D_j, ref 2017; 95% CI by origin state", title = "Dyad net flows on the 2016->18 change in the net-of-deduction rate gap, dyad + year FE", subtitle = "D_sub: lost federal subsidy of state income taxes (SOI aggregates); D_z1: TAXSIM cap burden of 2016 owner households / income") +
  theme_minimal(base_size = 8) + theme(legend.position = "bottom", strip.text.y = element_text(angle = 0))
ggsave(file.path(FIG, "c19_net_rate_change_dyads.png"), g4, width = 12, height = 7, dpi = 150, bg = "white")
