# Diop & Green (2026, SSRN) Table 7 on our SOI files, without their controls (2026-10-27).
# Their equation: Migration_ijt = gamma dSALT_ijt + sum_{t != ref} beta_t dSALT_ijt x 1[year = t] + year FE + origin FE
#   (+ controls: income, population, HPI, unemployment, GDP growth of origin and destination, no-sales-tax dummy),
#   White SEs on origin x destination x year observations; Migration in thousands of households (SOI returns),
#   emigration (column 1) and net emigration = emigration - immigration (column 2); 2001-2019, ref 2001.
# dSALT_ijt = SALT_it - SALT_jt with SALT = itemized state and local income tax deductions / AGI (their Table 1 moments,
#   mean 0.026, SD 0.015, max 0.068, match income taxes only; income + real estate would be 0.045). Their year t is SOI
#   file t(t+1) (the reconciliation entry), i.e. our label t+1.
# Here: files 1112-1920 (their 2011-2019), reference 2011, no controls; also 2011-2021, the Table 6 Post x dSALT form,
#   dSALT fixed at its 2017 value (TCJA halved the itemizer pool, so the time-varying dSALT shrinks by half in 2018+ for
#   every dyad and the post-2017 interactions are measured on a compressed regressor), and SEs clustered by origin state
#   and two-way origin + destination next to White.
# Inputs: data/built/soi/state_dyads.csv (build/soi/10), data/built/exposure/soi_state_salt_panel.csv (build/soi/11).
# Outputs: output/designs/exposure_did/c16_diop_green_table7.{csv,txt}, figures/c16_diop_green_table7.png.
suppressPackageStartupMessages({library(data.table); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/exposure_did"); FIG <- out_dir("designs/exposure_did/figures")

dy <- fread(file.path(DATA, "built/soi/state_dyads.csv"))[o <= 56 & o != 11 & d <= 56 & d != 11]            # 50 states, 49 destinations
sp <- fread(file.path(DATA, "built/exposure/soi_state_salt_panel.csv"))[, .(fips, ty, salt = salt_inc / agi, salt2 = (salt_inc + salt_re) / agi)]
dy <- merge(dy, sp[, .(o = fips, y1 = ty, salt_o = salt, salt2_o = salt2)], by = c("o", "y1"))
dy <- merge(dy, sp[, .(d = fips, y1 = ty, salt_d = salt, salt2_d = salt2)], by = c("d", "y1"))
dy <- merge(dy, sp[ty == 2017, .(o = fips, salt17_o = salt)], by = "o"); dy <- merge(dy, sp[ty == 2017, .(d = fips, salt17_d = salt)], by = "d")
dy <- merge(dy, dy[, .(o = d, d = o, y1, n1_rev = n1)], by = c("o", "d", "y1"), all.x = TRUE)
dy[, `:=`(emig = n1 / 1e3, net = (n1 - n1_rev) / 1e3, dsalt = salt_o - salt_d, dsalt2 = salt2_o - salt2_d, dsalt17 = salt17_o - salt17_d, post = as.integer(y1 >= 2018))]
stopifnot(!anyNA(dy$net), dy[, .N] == 2450 * 11)

desc <- dy[y1 <= 2019, .(obs = .N, emig_mean = mean(emig), emig_sd = sd(emig), net_sd = sd(net), salt_mean = mean(salt_o), salt_sd = sd(salt_o), salt_max = max(salt_o), dsalt_sd = sd(dsalt), dsalt17_sd = sd(dsalt17))]
dsalt_by_year <- dy[, .(dsalt_sd = sd(dsalt), salt_mean = mean(salt_o)), by = y1]

# event-study form (Table 7) and Post form (Table 6), three SEs each
xs <- c(dsalt = "dSALT (income tax / AGI, time-varying)", dsalt17 = "dSALT fixed at 2017", dsalt2 = "dSALT incl. real estate taxes")
ys <- c(emig = "out-migration (k households)", net = "net out-migration (k)")
se3 <- function(m) { v <- list(white = "hetero", o = ~o, od = ~o + d)
  r <- lapply(names(v), \(k) as.data.table(coeftable(m, vcov = v[[k]]), keep.rownames = "term")[, .(term, b = Estimate, se = `Std. Error`, vc = k)])
  dcast(rbindlist(r), term + b ~ vc, value.var = "se")[, .(term, b, se_white = white, se_o = o, se_od = od)] }
t7 <- function(y, x, ref, yrs, dat = dy) { m <- feols(as.formula(sprintf("%s ~ %s + i(y1, %s, ref = %d) | y1 + o", y, x, x, ref)), dat[y1 %in% yrs])
  se3(m)[, `:=`(y = y, x = x, ref = ref, sample = paste(range(yrs), collapse = "-"), form = "year x dSALT", year = as.integer(sub(sprintf("y1::(\\d+):%s", x), "\\1", term)))] }
t6 <- function(y, x, yrs, yfe) { m <- feols(as.formula(sprintf("%s ~ %s + %s post:%s | o %s", y, x, if (yfe) "" else "post +", x, if (yfe) "+ y1" else "")), dy[y1 %in% yrs])
  se3(m)[, `:=`(y = y, x = x, ref = NA_integer_, sample = paste(range(yrs), collapse = "-"), form = if (yfe) "Post x dSALT, year FE" else "Post x dSALT", year = NA_integer_)] }
grid <- CJ(y = names(ys), x = names(xs))
res <- rbind(grid[, t7(y, x, 2011L, 2011:2019), by = .(y, x)][, -(1:2)], grid[, t7(y, x, 2011L, 2011:2021), by = .(y, x)][, -(1:2)],
             grid[, t7(y, x, 2017L, 2011:2021), by = .(y, x)][, -(1:2)],
             grid[, t6(y, x, 2011:2019, FALSE), by = .(y, x)][, -(1:2)], grid[, t6(y, x, 2011:2019, TRUE), by = .(y, x)][, -(1:2)])
res[grepl("^y1::", term) & is.na(year), year := as.integer(sub("y1::(\\d+):.*", "\\1", term))]
fwrite(res, file.path(OUT, "c16_diop_green_table7.csv"))

dg <- data.table(year = 2011:2019, dg_out = c(-1.503, -0.619, 0.153, -1.086, 0.641, 4.478, 1.165, 16.886, 23.654), dg_out_se = c(4.562, 4.592, 4.440, 4.004, 4.479, 5.415, 4.651, 7.520, 8.195),
                 dg_net = c(-4.806, -3.301, 0.887, -3.184, 1.697, 8.104, 2.833, 16.296, 31.405), dg_net_se = c(1.977, 2.134, 2.362, 2.126, 2.576, 3.099, 2.618, 4.419, 5.652))   # Table 7, ref 2001
fmt <- \(b, s) sprintf("%7.2f (%5.2f)", b, s)
sink(file.path(OUT, "c16_diop_green_table7.txt"))
cat("Diop & Green (2026) Table 7 on SOI files 1112-1920 (their years 2011-2019), no controls. y ~ dSALT + dSALT x year | year + origin FE.\n")
cat("Units: thousands of households (returns) per origin x destination x year; dSALT = SALT_o - SALT_d, SALT = itemized state+local income tax / AGI.\n")
cat("SEs: White (theirs) | clustered by origin | two-way origin + destination.\n\n")
cat("Descriptives 2011-2019 (their Table 1: emig mean 1.297 sd 2.706, net sd 1.166, SALT mean 0.026 sd 0.015 max 0.068, dSALT sd 0.020):\n"); print(desc[, lapply(.SD, signif, 3)])
cat("\nSALT mean and dSALT SD by year (TCJA halves the itemizer pool from 2018):\n"); print(dsalt_by_year[, lapply(.SD, round, 4), by = y1])
for (xx in names(xs)) for (yy in names(ys)) {
  r <- res[y == yy & x == xx & form == "year x dSALT" & ref == 2011 & sample == "2011-2019"]
  cat(sprintf("\n== %s on %s, ref 2011, 2011-2019 ==\n", ys[yy], xs[xx]))
  cat(sprintf("  %-10s %s\n", "dSALT", fmt(r[is.na(year), b], r[is.na(year), se_white])))
  m <- merge(r[!is.na(year), .(year, b, se_white, se_o, se_od)], dg, by = "year", all.x = TRUE)
  cat(sprintf("  %s   ours: %s [cl. origin %5.2f] [two-way %5.2f]   D&G (ref 2001): out %s  net %s\n", m$year, fmt(m$b, m$se_white), m$se_o, m$se_od, fmt(m$dg_out, m$dg_out_se), fmt(m$dg_net, m$dg_net_se)), sep = "")
  r2 <- res[y == yy & x == xx & form == "year x dSALT" & ref == 2017 & sample == "2011-2021" & !is.na(year)]
  cat("  ref 2017, 2011-2021: ", paste(sprintf("%d %s", r2$year, fmt(r2$b, r2$se_white)), collapse = "; "), "\n")
  for (f in c("Post x dSALT", "Post x dSALT, year FE")) { r3 <- res[y == yy & x == xx & form == f]
    cat(sprintf("  %-22s %s\n", f, paste(sprintf("%s %s [cl %5.2f]", r3$term, fmt(r3$b, r3$se_white), r3$se_o), collapse = "; "))) } }
cat("\nD&G Table 6 (with controls, 2001-2019): out dSALT 23.27 (1.14), Post x dSALT 19.58 (5.26); net dSALT 10.67 (0.56), Post x dSALT 24.48 (3.45).\n")
sink()
cat(readLines(file.path(OUT, "c16_diop_green_table7.txt")), sep = "\n")

pl <- res[form == "year x dSALT" & ref == 2011 & sample == "2011-2021" & !is.na(year) & x != "dsalt2"]
pl <- rbind(pl, CJ(y = names(ys), x = c("dsalt", "dsalt17"))[, .(term = "", b = 0, se_white = 0, se_o = 0, se_od = 0, y, x, ref = 2011L, sample = "2011-2021", form = "", year = 2011L)])
pl[, `:=`(yl = factor(ys[y], ys), xl = factor(xs[x], xs))]
g <- ggplot(pl, aes(year, b)) + facet_grid(yl ~ xl, scales = "free_y") +
  annotate("rect", xmin = 2019.5, xmax = 2021.5, ymin = -Inf, ymax = Inf, alpha = 0.08) + geom_hline(yintercept = 0, colour = "grey50") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey50") +
  geom_errorbar(aes(ymin = b - 1.96 * se_o, ymax = b + 1.96 * se_o), width = 0, colour = COL_SIDE[1], alpha = 0.4, linewidth = 1.2) +
  geom_errorbar(aes(ymin = b - 1.96 * se_white, ymax = b + 1.96 * se_white), width = 0.2, colour = COL_SIDE[1]) + geom_point(colour = COL_SIDE[1], size = 2) +
  scale_x_continuous(breaks = 2011:2021) + labs(x = "year (Diop & Green label = SOI file t/t+1; shaded = beyond their sample)", y = "coefficient on dSALT x year, thousands of households per dyad (ref 2011)",
  title = "Diop & Green Table 7 on SOI files 1112-2122, no controls: 95% CIs White (thin) and clustered by origin state (thick)") + theme_ptax()
ggsave(file.path(FIG, "c16_diop_green_table7.png"), g, width = 11, height = 7, dpi = 150)

# ---- dyad subsamples: among CA FL NY TX (12 dyads), among the other 46 (2,070), and the 368 mixed pairs ----------------
big <- c(6L, 12L, 36L, 48L); dy[, grp := fifelse(o %in% big & d %in% big, "among CA FL NY TX", fifelse(!o %in% big & !d %in% big, "among the other 46", "big four <-> other 46"))]
grps <- c("among CA FL NY TX", "among the other 46", "big four <-> other 46")
sub <- rbindlist(lapply(grps, \(g) CJ(y = names(ys), x = c("dsalt", "dsalt17"))[, t7(y, x, 2011L, 2011:2021, dy[grp == g]), by = .(y, x)][, -(1:2)][, grp := g]))
fwrite(sub, file.path(OUT, "c16_diop_green_subsamples.csv"))
sink(file.path(OUT, "c16_diop_green_subsamples.txt"))
cat("Same specification (y ~ dSALT + dSALT x year | year + origin FE, ref 2011, 2011-2021, thousands of households per dyad) on dyad subsets.\n")
cat("dyads per year:"); print(dy[y1 == 2017, .N, by = grp]); cat("SEs: White [clustered by origin; 4 clusters in the first group, so White only there].\n")
for (g in grps) for (xx in c("dsalt", "dsalt17")) for (yy in names(ys)) { r <- sub[grp == g & x == xx & y == yy]
  cat(sprintf("\n== %s: %s on %s ==\n  dSALT %s\n", g, ys[yy], xs[xx], fmt(r[is.na(year), b], r[is.na(year), se_white])))
  r <- r[!is.na(year)]; cat(paste(sprintf("  %d %s%s", r$year, fmt(r$b, r$se_white), if (g == grps[1]) "" else sprintf(" [%5.2f]", r$se_o)), collapse = "\n"), "\n") }
sink(); cat(readLines(file.path(OUT, "c16_diop_green_subsamples.txt")), sep = "\n")
ps <- rbind(sub[!is.na(year)], CJ(y = names(ys), x = c("dsalt", "dsalt17"), grp = grps)[, .(term = "", b = 0, se_white = 0, se_o = 0, se_od = 0, y, x, ref = 2011L, sample = "", form = "", year = 2011L, grp)])
ps[, `:=`(yl = factor(ys[y], ys), xl = factor(xs[x], xs), grp = factor(grp, grps))]
g2 <- ggplot(ps, aes(year, b, colour = xl)) + facet_wrap(vars(yl, grp), scales = "free_y", nrow = 2) +
  geom_hline(yintercept = 0, colour = "grey50") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey50") +
  geom_errorbar(aes(ymin = b - 1.96 * se_white, ymax = b + 1.96 * se_white), width = 0.2, position = position_dodge(0.5)) + geom_point(size = 2, position = position_dodge(0.5)) +
  scale_colour_manual(values = unname(COL_SIDE[1:2]), name = NULL) + scale_x_continuous(breaks = 2011:2021) +
  labs(x = "year (Diop & Green label = SOI file t/t+1)", y = "coefficient on dSALT x year, thousands of households per dyad (ref 2011), 95% White CI",
       title = "Diop & Green Table 7, no controls, by dyad subset: CA FL NY TX among themselves, the other 46 among themselves, and the mixed pairs") + theme_ptax() + theme(legend.position = "bottom")
ggsave(file.path(FIG, "c16_diop_green_subsamples.png"), g2, width = 12, height = 7, dpi = 150)
