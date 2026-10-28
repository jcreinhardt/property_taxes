# Distance x SALT exposure x year on the density-selected sample of 29 (tracts < 1,000 per sq mi within 50 mi, both sides):
#   y = sum_t b_t Z 1[t] + sum_t g_t Z x E 1[t] + sum_t d_t E 1[t] + sum_t l_t log density 1[t] + unit FE + pair x year FE
# Z = -distance / 10 mi, E = own lost SALT subsidy per owner household (ZIP, standardised), 2017 = 0, SE by county; by side
# and pooled (pair x side x year FE). Outcomes: the core 11 of 29 and the 75 district / local-government outcomes of 18.
# Reported: post - pre of g_t (per 10 mi closer per SD of exposure) and b_t, SD units; tiles; event studies for key items.
# Caveat from 23 / 36: Z x E is unbalanced in 2016 prices, turnover, income and education in this sample too.
suppressPackageStartupMessages({library(data.table); library(DBI); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_rd"); invisible(out_dir("designs/border_rd/figures")); REF <- 2017
source("code/designs/border_rd/nb_panels.R")
tz <- fread(file.path(DATA, "built/exposure/salt_exposure_tract_zip2016.csv"), colClasses = c(tract = "character", zip = "character"))[, .(tract, zip)]
cz <- fread(file.path(DATA, "built/exposure/exposure_components_zip.csv"), colClasses = c(geo = "character"))[returns16 >= 100 & is.finite(d_salt_own), .(zip = geo, d_salt_own)]
d <- fread(file.path(OUT, "c41_sample_competition.csv"), colClasses = c(tract = "character", pair = "character"))
d <- merge(d, rbindlist(lapply(c("low", "high"), \(s) readRDS(file.path(DATA, sprintf("built/geo/%s_side_balance_tracts.rds", s)))[, .(tract, leaid, cty, pop)])), by = "tract")
d <- merge(d, merge(tz, cz, by = "zip")[, .(tract, d_salt_own)], by = "tract")[, `:=`(z = -dist_mi / 10, ps = paste(pair, side))]
d[, ex := (d_salt_own - mean(d_salt_own)) / sd(d_salt_own)][, zx := z * ex]
cat(sprintf("sample with exposure: %d tracts (%d low, %d high); median lost subsidy $%.0f\n", nrow(d), sum(d$side == "low"), sum(d$side == "high"), median(d$d_salt_own)))
samples <- list(`low-tax side` = d[side == "low"], `high-tax side` = d[side == "high"], `both sides pooled` = copy(d)[, pair := ps])
core <- list(cl = c(lp50 = "log median sale price", lbill = "log tax bill per parcel", eff50 = "effective tax rate", turn = "sales per 100 parcels"), ac = c(lhval = "log median home value (ACS)", lpop = "log population (ACS)"), mv = c(moveout = "move-out rate (Data Axle)"),
             f = c(lptax = "log district property tax per pupil", lspend = "log district spending per pupil", lenr = "log district enrolment"), sd = c(lptax = "log county school property tax (CoG)"))
cunits <- c(cl = "tract", ac = "tract", mv = "tract", f = "leaid", sd = "cty")
pp <- function(r) { pre <- r[y < REF & y >= 2012]; post <- r[y > REF]; if (!nrow(pre) || !nrow(post)) return(NULL); data.table(post_min_pre = mean(post$b) - mean(pre$b), se = sqrt(mean(post$se^2) + mean(pre$se^2) / nrow(pre))) }
both <- function(p, fe_unit) { x <- p[is.finite(yv)]; if (uniqueN(x$y) < 3) return(NULL)
  m <- feols(as.formula(sprintf("yv ~ i(y, z, ref = %d) + i(y, zx, ref = %d) + i(y, ex, ref = %d) + i(y, ldens, ref = %d) | %s + pair^y", REF, REF, REF, REF, fe_unit)), x, cluster = ~cty)
  ct <- as.data.table(coeftable(m), keep.rownames = "term")[grepl("^y::\\d+:(z|zx)$", term)]
  ct[, `:=`(y = as.integer(sub("y::(\\d+):.*", "\\1", term)), coef = ifelse(grepl(":zx$", term), "interaction", "main"))][, .(y, coef, b = Estimate, se = `Std. Error`)][, n := nobs(m)] }
census <- function(p, fe_unit) { x <- p[is.finite(yv)]; x[, `:=`(post = as.integer(y == 2022), pre = as.integer(y == 2012))]
  m <- feols(yv ~ z:post + z:pre + zx:post + zx:pre + ex:post + ex:pre + ldens:post + ldens:pre | cty + pair^y, x, cluster = ~cty); ct <- coeftable(m); rn <- rownames(ct)
  g <- function(v, s, j) { i <- which(rn == paste0(v, ":", s) | rn == paste0(s, ":", v)); if (length(i)) ct[i[1], j] else NA_real_ }
  rbindlist(lapply(c(main = "z", interaction = "zx"), \(v) data.table(y = c(2012, 2017, 2022), b = c(g(v, "pre", 1), 0, g(v, "post", 1)), se = c(g(v, "pre", 2), 0, g(v, "post", 2)))), idcol = "coef")[, n := nobs(m)] }
res <- list(); ser <- list()
for (s in names(samples)) { P <- nb_panels(samples[[s]], c("z", "zx", "ex", "ldens")); E <- nb_extra_panels(P$dis, P$cnt)
  run <- function(p, k, lab, src, unit, cens = FALSE) { p <- copy(p)[, yv := get(k)]; fy <- if (cens) 2017 else 2016; sdy <- sd(p[y == fy, yv], na.rm = TRUE); if (!is.finite(sdy) || sdy == 0) return(NULL)
    r <- if (cens) census(p, unit) else both(p, unit); if (is.null(r)) return(NULL)
    r <- rbind(r, data.table(y = REF, coef = c("main", "interaction"), b = 0, se = 0, n = r$n[1]))[, `:=`(sample = s, src = src, outcome = lab, b_sd = b / sdy, se_sd = se / sdy)]; ser[[length(ser) + 1]] <<- r
    for (cc in c("main", "interaction")) { q <- pp(r[coef == cc]); if (!is.null(q)) res[[length(res) + 1]] <<- q[, `:=`(coef = cc, sample = s, src = src, outcome = lab, b_sd = post_min_pre / sdy, se_sd = se / sdy, n = r$n[1])] } }
  for (tb in names(core)) for (k in names(core[[tb]])) run(P[[tb]], k, core[[tb]][k], "core", cunits[tb])
  for (tb in names(E$panels)) { p0 <- E$panels[[tb]]; if (tb == "se") p0 <- p0[is.finite(z)]; for (k in names(E$vars[[tb]])) run(p0, k, E$vars[[tb]][k], E$src[tb], E$units[tb], cens = tb == "ar") } }
res <- rbindlist(res)[, t := b_sd / se_sd]; ser <- rbindlist(ser)
fwrite(res, file.path(OUT, "c43_exposure_interaction_density1000.csv")); fwrite(ser, file.path(OUT, "c43_exposure_interaction_density1000_series.csv"))
sink(file.path(OUT, "c43_exposure_interaction_density1000.txt"), split = TRUE)
cat("Z x year + Z x exposure x year + exposure x year + log density x year | unit FE + pair x year FE; density-selected sample. post - pre in SD units: interaction per 10 mi closer per SD of exposure, main per 10 mi closer at mean exposure. Cells: estimate (t).\n")
for (cc in c("interaction", "main")) { cat("\n==", cc, "\n"); w <- dcast(res[coef == cc, .(src, outcome, sample, cell = sprintf("%.3f (%.1f)", b_sd, t))], src + outcome ~ sample, value.var = "cell"); print(as.data.frame(w), row.names = FALSE) }
cat("\nsummary: cells with |t| > 1.96 and share positive, by coefficient, source and sample\n"); print(res[, .(k = .N, sig = sum(abs(t) > 1.96), share_pos = round(mean(b_sd > 0), 2), median_abs = round(median(abs(b_sd)), 3)), by = .(coef, src = ifelse(src == "core", "core", "district / local govt"), sample)][order(coef, src, sample)])
sink()
tl <- copy(res)[, `:=`(outcome = factor(outcome, rev(unique(res$outcome))), sample = factor(sample, names(samples)), coef = factor(coef, c("interaction", "main"), c("Z x exposure x year (per 10 mi per SD of E)", "Z x year at mean exposure (per 10 mi)")), src = factor(src, c("core", unique(res[src != "core", src]))))]
g <- ggplot(tl, aes(sample, outcome, fill = pmax(pmin(t, 3), -3))) + geom_tile(colour = "white") + geom_text(aes(label = sprintf("%.2f", b_sd)), size = 2) + facet_grid(src ~ coef, scales = "free_y", space = "free_y", labeller = label_wrap_gen(28)) +
  scale_fill_gradient2("t-stat", low = "#c0392b", mid = "white", high = "#2a64a8", limits = c(-3, 3)) + labs(x = NULL, y = NULL, title = "Distance x SALT exposure x year on the density-selected sample: core and district / local-government outcomes", subtitle = "tracts < 1,000 per sq mi within 50 mi; exposure x year and log density x year included; unit FE, pair x year FE; cells = post - pre in SD units, colour = t") +
  theme_minimal(base_size = 7) + theme(axis.text.x = element_text(angle = 30, hjust = 1), strip.text.y = element_text(angle = 0, size = 6))
ggsave(file.path(OUT, "figures/c43_exposure_interaction_tiles.png"), g, width = 11, height = 15, dpi = 150)
key <- c("effective tax rate", "sales per 100 parcels", "log median sale price", "log population (ACS)", "log district property tax per pupil", "log district spending per pupil", "log district enrolment", "total revenue", "state formula aid (C01)", "instruction", "capital outlay", "district test scores (SEDA, SD)")
ser[, y := as.integer(as.character(y))]; pl <- ser[coef == "interaction" & outcome %in% key][, `:=`(outcome = factor(outcome, key), sample = factor(sample, names(samples)))]
g <- ggplot(pl, aes(y, b_sd, colour = sample)) + geom_hline(yintercept = 0, colour = "grey70") + geom_vline(xintercept = REF + .5, linetype = "22", colour = "grey50") +
  geom_pointrange(aes(ymin = b_sd - 1.96 * se_sd, ymax = b_sd + 1.96 * se_sd), position = position_dodge(width = .6), size = .18, linewidth = .3) + geom_line(position = position_dodge(width = .6), linewidth = .35) +
  facet_wrap(~ outcome, scales = "free_y", ncol = 4, labeller = label_wrap_gen(30)) + scale_x_continuous(breaks = seq(2012, 2024, 2)) + scale_colour_manual(NULL, values = c("#2a64a8", "#c0392b", "grey30")) +
  labs(x = NULL, y = "Z x exposure x year, SD units of the outcome per 10 mi closer per SD of exposure, 2017 = 0; 95% CI, SE by county", title = "Distance x SALT exposure x year, density-selected sample: event studies for key outcomes", subtitle = "exposure x year and log density x year included; unit FE + pair x year FE") +
  theme_minimal(base_size = 8) + theme(legend.position = "bottom", strip.text = element_text(size = 6.5))
ggsave(file.path(OUT, "figures/c43_exposure_interaction_es.png"), g, width = 11, height = 7.5, dpi = 150)
