# State-level DiDs: is the migration response to lost SALT subsidies confined to CA, NY, FL and TX? (2026-10-29)
# The dyad-subset result in 16 (Diop & Green Table 7 by subset) found the dSALT x year effect on net flows among the big four and in
# the mixed pairs, and nothing among the other 46 states. Here the same question in the project's state-level event-study form:
#   y_st ~ i(year, E_s, ref = 2017) | state + year, weighted by 2016 returns, SE clustered by state (hetero SE where < 10 clusters).
# Outcomes per state-year from the SOI state files (files 1112-2122 -> years 2012-2022, y = second year of the file, moves during ~y):
#   net out-migration rate (out - in, pp of non-migrants + out-migrants), out-of-state and in-from-other-state rates, and the net rate
#   split by destination group from the dyads (build/soi/10): to the big four (CA NY FL TX) and to the other 46 states.
# Exposure E (per SD across the 50 states, fixed): lost SALT subsidy per return and per owner HH (build/exposure/05, AK HI have no
#   owner count -> dropped in the per-owner spec), Diop & Green SALT income tax / AGI at TY2017 (build/soi/11), and within the big
#   four the CA/NY vs FL/TX dummy. Samples: all 50 states; the big four; the other 46; all 50 with separate year FE for the big four.
# Outputs: output/designs/exposure_did/c17_state_did_big_four{,_series}.csv, .txt, figures/c17_state_did_big_four.png.
suppressPackageStartupMessages({library(data.table); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/exposure_did"); FIG <- out_dir("designs/exposure_did/figures"); M <- file.path(DATA, "SOI/migration_state")
B4 <- c(6L, 36L, 12L, 48L); yrs <- 12:22
rd <- \(kind, yy) { x <- fread(file.path(M, sprintf("state%s%02d%02d.csv", kind, yy - 1, yy)), colClasses = "character", encoding = "Latin-1"); setnames(x, tolower(names(x))); x[, y := 2000L + yy][] }
p <- rbindlist(lapply(yrs, \(yy) { o <- rd("outflow", yy); i <- rd("inflow", yy)
  o[, `:=`(s = as.integer(y1_statefips), d = as.integer(y2_statefips))]; i[, `:=`(s = as.integer(y2_statefips), d = as.integer(y1_statefips))]
  st  <- o[d == s & grepl("Non-migrants", y2_state_name), .(s, y, nonmig = as.numeric(n1))]
  out <- o[d == 97 & grepl("Migration.US$", y2_state_name), .(s, out = as.numeric(n1))]
  inn <- i[d == 97 & grepl("Migration.US$", y1_state_name), .(s, inn = as.numeric(n1))]
  Reduce(\(a, b) merge(a, b, by = "s"), list(st, out, inn)) }))[s <= 56 & s != 11]
dy <- fread(file.path(DATA, "built/soi/state_dyads.csv"))[o <= 56 & o != 11 & d <= 56 & d != 11, .(s = o, d, y = y2, n1)]
dy <- merge(dy, dy[, .(s = d, d = s, y, n1_rev = n1)], by = c("s", "d", "y"))[, grp := fifelse(d %in% B4, "b4", "o46")]
dy <- dcast(dy[, .(net = sum(n1 - n1_rev), out = sum(n1)), by = .(s, y, grp)], s + y ~ grp, value.var = c("net", "out"))
p <- merge(p, dy, by = c("s", "y")); p[, base := nonmig + out]
p[, `:=`(r_net = 100 * (out - inn) / base, r_out = 100 * out / base, r_in = 100 * inn / base, r_net_b4 = 100 * net_b4 / base, r_net_o46 = 100 * net_o46 / base)]
p <- merge(p, p[y == 2016, .(s, w = base)], by = "s"); stopifnot(p[, uniqueN(s)] == 50, p[, .N] == 50 * length(yrs))
# ---- exposure
ec <- fread(file.path(DATA, "built/exposure/exposure_components_state.csv"), colClasses = c(geo = "character"))[, .(s = as.integer(geo), ex_ret = d_salt, ex_own = fifelse(is.finite(d_salt_own), d_salt_own, NA_real_))]
sp <- fread(file.path(DATA, "built/exposure/soi_state_salt_panel.csv"))[ty == 2017, .(s = fips, salt17 = salt_inc / agi, item17 = n_item / n1)]
p <- merge(p, merge(ec, sp, by = "s"), by = "s"); p[, big4 := s %in% B4][, hi := as.integer(s %in% c(6L, 36L))]
for (k in c("ex_ret", "ex_own", "salt17")) p[, (k) := (get(k) - mean(get(k)[y == 2017], na.rm = TRUE)) / sd(get(k)[y == 2017], na.rm = TRUE)]   # SD over states
cat("Exposure per SD, big four:\n"); print(p[y == 2017 & big4, .(s, ex_ret = round(ex_ret, 2), ex_own = round(ex_own, 2), salt17 = round(salt17, 2), item17 = round(item17, 3))])
# ---- specs
ys <- c(r_net = "net out-migration rate (pp of returns)", r_net_b4 = "net out-migration to CA NY FL TX (pp)", r_net_o46 = "net out-migration to the other 46 (pp)", r_out = "out-of-state rate (pp)", r_in = "in-from-other-state rate (pp)")
xs <- c(ex_ret = "lost SALT subsidy per return (SD)", ex_own = "lost SALT subsidy per owner HH (SD)", salt17 = "SALT income tax / AGI, TY2017 (SD)", hi = "CA NY vs FL TX")
smp <- list("all 50 states" = quote(TRUE), "big four (CA NY FL TX)" = quote(big4), "other 46 states" = quote(!big4), "all 50, big four x year FE" = quote(TRUE))
es <- \(k, x, sname) { d <- p[eval(smp[[sname]]) & is.finite(get(x))]; if (x == "hi" && sname != "big four (CA NY FL TX)") return(NULL)
  fe <- if (sname == "all 50, big four x year FE") "s + y^big4" else "s + y"; few <- uniqueN(d$s) < 10
  m <- feols(as.formula(sprintf("%s ~ i(y, %s, ref = 2017) | %s", k, x, fe)), d, weights = ~w, vcov = if (few) "hetero" else ~s)
  ct <- as.data.table(coeftable(m), keep.rownames = "t")[, .(outcome = k, exposure = x, sample = sname, y = as.integer(sub("y::(\\d+):.*", "\\1", t)), b = Estimate, se = `Std. Error`, n_states = uniqueN(d$s), se_type = if (few) "hetero" else "cluster state")]
  rbind(ct, ct[1, .(outcome, exposure, sample, y = 2017L, b = 0, se = 0, n_states, se_type)])[order(y)] }
grid <- CJ(k = names(ys), x = names(xs), sname = names(smp), sorted = FALSE)
ser <- rbindlist(Map(es, grid$k, grid$x, grid$sname))
ser[, `:=`(outcome_l = factor(ys[outcome], ys), exposure_l = factor(xs[exposure], xs), sample = factor(sample, names(smp)))]
fwrite(ser, file.path(OUT, "c17_state_did_big_four_series.csv"))
sm <- ser[, .(pre = mean(b[y %in% 2013:2016]), post1819 = mean(b[y %in% 2018:2019]), post2022 = mean(b[y %in% 2020:2022]), se1819 = sqrt(mean(se[y %in% 2018:2019]^2)), se2022 = sqrt(mean(se[y %in% 2020:2022]^2)),
             pre_slope = coef(lm(b ~ y, data.frame(b = b[y %in% 2012:2017], y = y[y %in% 2012:2017])))[2], n_states = n_states[1], se_type = se_type[1]), by = .(outcome, exposure, sample)]
sm[, `:=`(d1819 = post1819 - pre, d2022 = post2022 - pre)]
# deviation from the 2012-17 linear pre-trend extrapolated (trend fitted on the 2012-17 coefficients, ref 2017)
tr <- ser[y <= 2017, .(a = coef(lm(b ~ y))[1], g = coef(lm(b ~ y))[2]), by = .(outcome, exposure, sample)]
sm <- merge(sm, tr, by = c("outcome", "exposure", "sample"))[, `:=`(dt1819 = post1819 - (a + g * 2018.5), dt2022 = post2022 - (a + g * 2021))][, c("a", "g") := NULL]
fwrite(sm, file.path(OUT, "c17_state_did_big_four.csv"))
f <- \(b, s) sprintf("%6.3f (%5.3f)", b, s)
sink(file.path(OUT, "c17_state_did_big_four.txt"), split = TRUE)
cat("State-level event studies y_st ~ i(year, E, ref 2017) | state + year, weighted by 2016 returns, SE by state (hetero where < 10 states), SOI state files 2012-2022.\n",
    "Cells: mean coefficient 2018-19 minus mean 2013-16 (SE of the 2018-19 mean) | mean 2020-22 minus mean 2013-16 (SE) | pre-period slope 2012-17 per year | 2018-19 and 2020-22 means as deviations from that trend. pp of returns per SD of E.\n\n")
for (x in names(xs)) { cat("==", xs[x], "\n"); w <- sm[exposure == x][, cell := sprintf("%s | %s | %+.3f | %+.3f | %+.3f", f(d1819, se1819), f(d2022, se2022), pre_slope, dt1819, dt2022)]
  print(as.data.frame(dcast(w, outcome ~ sample, value.var = "cell")[match(names(ys), outcome)][, outcome := ys[outcome]]), row.names = FALSE); cat("\n") }
cat("Year-by-year, net out-migration rate on lost SALT subsidy per return (SD):\n")
print(as.data.frame(dcast(ser[outcome == "r_net" & exposure == "ex_ret"], y ~ sample, value.var = "b", fun.aggregate = \(v) round(v, 3))), row.names = FALSE)
cat("\nRaw net out-migration rates (pp), big four:\n"); print(dcast(p[(big4), .(s = c(`6` = "CA", `36` = "NY", `12` = "FL", `48` = "TX")[as.character(s)], y, r = round(r_net, 2))], y ~ s, value.var = "r"))
sink()
g <- ggplot(ser[exposure != "hi"], aes(y, b, colour = exposure_l)) + facet_grid(outcome_l ~ sample, scales = "free_y", labeller = label_wrap_gen(28)) +
  annotate("rect", xmin = 2019.5, xmax = 2022.5, ymin = -Inf, ymax = Inf, alpha = 0.06) + geom_hline(yintercept = 0, colour = "grey50") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey60") +
  geom_ribbon(aes(ymin = b - 1.96 * se, ymax = b + 1.96 * se, fill = exposure_l), alpha = 0.12, colour = NA) + geom_line() + geom_point(size = 1) +
  scale_x_continuous(breaks = seq(2012, 2022, 2)) + scale_colour_brewer(NULL, palette = "Dark2") + scale_fill_brewer(NULL, palette = "Dark2") +
  labs(x = "SOI file year (second year; moves during ~y)", y = "pp of returns per SD of exposure, ref 2017; 95% CI (state clusters; hetero for the big four)",
       title = "State-level SALT exposure x year: all states, the big four, the other 46", subtitle = "state + year FE, weighted by 2016 returns; shaded = 2020-22 files") +
  theme_minimal(base_size = 8) + theme(legend.position = "bottom", strip.text.y = element_text(angle = 0))
ggsave(file.path(FIG, "c17_state_did_big_four.png"), g, width = 12, height = 10, dpi = 150, bg = "white")
