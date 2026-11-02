# The state-level SALT exposure DiDs of 17 on Data Axle moves instead of SOI returns (2026-10-29).
# Panel from the 1% family sample on Bouchet (output/validation/dataaxle/soi_vs_dataaxle/da_state_pairs_1pct.{sql,csv}): families with
# the same family_id in consecutive annual files, origin x destination state; strict move = state changed and length_of_residence reset
# at the destination. State-year rates (pp of linked families): out-of-state, in-from-other-state, net out (out - in), and net out to the
# big four / to the other 46. Year = destination file (2012-2021; the two-year 2021->23 pair is dropped). Year FE absorb the uneven
# file intervals. Same specification, exposure measures and samples as 17: y ~ i(y, E, ref 2017) | s + y, weights = 2016 linked families
# (DA_W=soi: 2016 SOI returns as in 17), SE by state (hetero for the big four).
# Outputs: output/designs/exposure_did/c18_state_did_big_four_dataaxle{,_series}.csv, .txt, figures/c18_state_did_big_four_dataaxle.png.
suppressPackageStartupMessages({library(data.table); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/exposure_did"); FIG <- out_dir("designs/exposure_did/figures"); B4 <- c(6L, 36L, 12L, 48L)
ab <- unique(as.data.table(tigris::fips_codes)[, .(ab = state, s = as.integer(state_code))])[s <= 56 & s != 11]
x <- fread(file.path(OUTPUT, "validation/dataaxle/soi_vs_dataaxle/da_state_pairs_1pct.csv"))[y1 - y0 == 1 & y1 <= 2021]
x <- merge(x, ab[, .(o = ab, s = s)], by = "o"); x <- merge(x, ab[, .(d = ab, sd = s)], by = "d")          # 50 states, drops DC / territories / blanks
x[, grp := fifelse(sd %in% B4, "b4", "o46")]
st <- x[s == sd, .(stay = sum(fam)), by = .(s, y = y1)]
out <- x[s != sd, .(out = sum(fam_strict), out_b4 = sum(fam_strict[grp == "b4"]), out_o46 = sum(fam_strict[grp == "o46"])), by = .(s, y = y1)]
inn <- x[s != sd, .(inn = sum(fam_strict), in_b4 = sum(fam_strict[s %in% B4]), in_o46 = sum(fam_strict[!s %in% B4])), by = .(s = sd, y = y1)]
p <- Reduce(\(a, b) merge(a, b, by = c("s", "y")), list(st, out, inn)); p[, base := stay + out]
p[, `:=`(r_net = 100 * (out - inn) / base, r_out = 100 * out / base, r_in = 100 * inn / base, r_net_b4 = 100 * (out_b4 - in_b4) / base, r_net_o46 = 100 * (out_o46 - in_o46) / base)]
stopifnot(p[, uniqueN(s)] == 50, p[, .N] == 500)
if (Sys.getenv("DA_W", "da") == "soi") { M <- file.path(DATA, "SOI/migration_state"); o16 <- fread(file.path(M, "stateoutflow1516.csv"), colClasses = "character")
  setnames(o16, tolower(names(o16))); w <- o16[as.integer(y1_statefips) == as.integer(y2_statefips), .(s = as.integer(y1_statefips), w = as.numeric(n1))]; p <- merge(p, w, by = "s")
} else p <- merge(p, p[y == 2016, .(s, w = base)], by = "s")
cat("Linked families per state-year (1% sample), 2016: total", p[y == 2016, sum(base)], "; strict out-of-state moves per year:", p[, sum(out)/10], "\n")
# ---- exposure and specs, as in 17
ec <- fread(file.path(DATA, "built/exposure/exposure_components_state.csv"), colClasses = c(geo = "character"))[, .(s = as.integer(geo), ex_ret = d_salt, ex_own = fifelse(is.finite(d_salt_own), d_salt_own, NA_real_))]
sp <- fread(file.path(DATA, "built/exposure/soi_state_salt_panel.csv"))[ty == 2017, .(s = fips, salt17 = salt_inc / agi)]
p <- merge(p, merge(ec, sp, by = "s"), by = "s"); p[, big4 := s %in% B4][, hi := as.integer(s %in% c(6L, 36L))]
for (k in c("ex_ret", "ex_own", "salt17")) p[, (k) := (get(k) - mean(get(k)[y == 2017], na.rm = TRUE)) / sd(get(k)[y == 2017], na.rm = TRUE)]
ys <- c(r_net = "net out-migration rate (pp of families)", r_net_b4 = "net out-migration to CA NY FL TX (pp)", r_net_o46 = "net out-migration to the other 46 (pp)", r_out = "out-of-state rate (pp)", r_in = "in-from-other-state rate (pp)")
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
fwrite(ser, file.path(OUT, "c18_state_did_big_four_dataaxle_series.csv"))
sm <- ser[, .(pre = mean(b[y %in% 2013:2016]), post1819 = mean(b[y %in% 2018:2019]), post2021 = mean(b[y %in% 2020:2021]), se1819 = sqrt(mean(se[y %in% 2018:2019]^2)), se2021 = sqrt(mean(se[y %in% 2020:2021]^2)),
             pre_slope = coef(lm(b ~ y, data.frame(b = b[y %in% 2012:2017], y = y[y %in% 2012:2017])))[2], n_states = n_states[1], se_type = se_type[1]), by = .(outcome, exposure, sample)]
tr <- ser[y <= 2017, .(a = coef(lm(b ~ y))[1], g = coef(lm(b ~ y))[2]), by = .(outcome, exposure, sample)]
sm <- merge(sm, tr, by = c("outcome", "exposure", "sample"))[, `:=`(d1819 = post1819 - pre, d2021 = post2021 - pre, dt1819 = post1819 - (a + g * 2018.5), dt2021 = post2021 - (a + g * 2020.5))][, c("a", "g") := NULL]
fwrite(sm, file.path(OUT, "c18_state_did_big_four_dataaxle.csv"))
f <- \(b, s) sprintf("%6.3f (%5.3f)", b, s)
sink(file.path(OUT, "c18_state_did_big_four_dataaxle.txt"), split = TRUE)
cat("Data Axle (1% family sample) state-level event studies y_st ~ i(year, E, ref 2017) | state + year, weights", Sys.getenv("DA_W", "da"), "2016, SE by state (hetero where < 10 states), 2012-2021.\n",
    "Cells: mean 2018-19 minus mean 2013-16 (SE) | mean 2020-21 minus mean 2013-16 (SE) | pre slope 2012-17 | the two post means as deviations from that trend. pp of linked families per SD of E.\n\n")
for (x in names(xs)) { cat("==", xs[x], "\n"); w <- sm[exposure == x][, cell := sprintf("%s | %s | %+.3f | %+.3f | %+.3f", f(d1819, se1819), f(d2021, se2021), pre_slope, dt1819, dt2021)]
  print(as.data.frame(dcast(w, outcome ~ sample, value.var = "cell")[match(names(ys), outcome)][, outcome := ys[outcome]]), row.names = FALSE); cat("\n") }
cat("Year-by-year, net out-migration rate on lost SALT subsidy per return (SD):\n")
print(as.data.frame(dcast(ser[outcome == "r_net" & exposure == "ex_ret"], y ~ sample, value.var = "b", fun.aggregate = \(v) round(v, 3))), row.names = FALSE)
cat("\nRaw Data Axle net out-migration rates (pp of linked families), big four:\n"); print(dcast(p[(big4), .(s = c(`6` = "CA", `36` = "NY", `12` = "FL", `48` = "TX")[as.character(s)], y, r = round(r_net, 2))], y ~ s, value.var = "r"))
sink()
g <- ggplot(ser[exposure != "hi"], aes(y, b, colour = exposure_l)) + facet_grid(outcome_l ~ sample, scales = "free_y", labeller = label_wrap_gen(28)) +
  annotate("rect", xmin = 2019.5, xmax = 2021.5, ymin = -Inf, ymax = Inf, alpha = 0.06) + geom_hline(yintercept = 0, colour = "grey50") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey60") +
  geom_ribbon(aes(ymin = b - 1.96 * se, ymax = b + 1.96 * se, fill = exposure_l), alpha = 0.12, colour = NA) + geom_line() + geom_point(size = 1) +
  scale_x_continuous(breaks = seq(2012, 2022, 2)) + scale_colour_brewer(NULL, palette = "Dark2") + scale_fill_brewer(NULL, palette = "Dark2") +
  labs(x = "Data Axle destination file year", y = "pp of linked families per SD of exposure, ref 2017; 95% CI (state clusters; hetero for the big four)",
       title = "Data Axle moves (1% family sample): state-level SALT exposure x year, all states, the big four, the other 46", subtitle = "same specification as the SOI version (17); state + year FE; shaded = 2020-21 files") +
  theme_minimal(base_size = 8) + theme(legend.position = "bottom", strip.text.y = element_text(angle = 0))
ggsave(file.path(FIG, "c18_state_did_big_four_dataaxle.png"), g, width = 12, height = 10, dpi = 150, bg = "white")
