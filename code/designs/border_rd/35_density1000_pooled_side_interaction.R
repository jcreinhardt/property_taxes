# The side difference of the density-selected border design (31) estimated in one pooled regression, with H = 1 on the high-tax
# side interacted with the treatment terms, so the difference high minus low gets its SE from one covariance matrix. Three pooled
# models, next to the two side-by-side regressions of 31 (differenced by hand, SE = sqrt(se_h^2 + se_l^2)):
#   saturated:  every term x H, unit FE + pair x side x year FE   -> identical point estimates to the split; the check
#   common FE:  every term x H, unit FE + pair x year FE + H x year  -> the side-year shock is assumed common across borders,
#               so H x Z x E x year is the difference of the near-vs-inland gradients within the same border-year
#   common E:   Z and Z x E x H only; E x year and log density x year common to both sides, pair x year FE + H x year
# Terms as 31: Z = -distance / 10 mi, E = own lost SALT subsidy per owner HH (standardised over the pooled sample), 2017 = 0,
# SE by county (counties never cross a state line, so the split-sample difference is independent across sides). Outcomes: the
# core 11 and the 5 further key items of c43; every coefficient scaled by the pooled 2016 SD of the outcome. Reported: the
# difference high minus low of the Z x E x year term (and of Z x year), plotted as low minus high to match c45's difference figure.
# Writes c47_pooled_side_interaction_{series,post_pre}.csv, c47_pooled_side_interaction.txt, figures/c47_pooled_side_interaction{,_main}.png.
suppressPackageStartupMessages({library(data.table); library(DBI); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_rd"); invisible(out_dir("designs/border_rd/figures")); REF <- 2017
source("code/designs/border_rd/nb_panels.R")
tz <- fread(file.path(DATA, "built/exposure/salt_exposure_tract_zip2016.csv"), colClasses = c(tract = "character", zip = "character"))[, .(tract, zip)]
cz <- fread(file.path(DATA, "built/exposure/exposure_components_zip.csv"), colClasses = c(geo = "character"))[returns16 >= 100 & is.finite(d_salt_own), .(zip = geo, d_salt_own)]
d <- fread(file.path(OUT, "c41_sample_competition.csv"), colClasses = c(tract = "character", pair = "character"))
d <- merge(d, rbindlist(lapply(c("low", "high"), \(s) readRDS(file.path(DATA, sprintf("built/geo/%s_side_balance_tracts.rds", s)))[, .(tract, leaid, cty, pop)])), by = "tract")
d <- merge(d, merge(tz, cz, by = "zip")[, .(tract, d_salt_own)], by = "tract")[, `:=`(z = -dist_mi / 10, H = as.integer(side == "high"), ps = paste(pair, side))]
d[, ex := (d_salt_own - mean(d_salt_own)) / sd(d_salt_own)][, zx := z * ex][, `:=`(zH = z * H, zxH = zx * H, exH = ex * H, ldH = ldens * H)]
cat(sprintf("pooled sample: %d tracts (%d low, %d high), %d pairs\n", nrow(d), sum(d$H == 0), sum(d$H == 1), uniqueN(d$pair)))
P <- nb_panels(d, c("z", "zx", "ex", "ldens", "H", "zH", "zxH", "exH", "ldH")); E <- nb_extra_panels(P$dis, P$cnt)
# districts / counties: H and ps are population-weighted means in nb_panels; keep units that are (nearly) on one side and re-derive the products
fix <- function(p) { p <- p[H <= .05 | H >= .95][, H := as.integer(H >= .5)][, `:=`(zH = z * H, zxH = zx * H, exH = ex * H, ldH = ldens * H, ps = paste(pair, H))]; p }
core <- list(cl = c(lp50 = "log median sale price", lbill = "log tax bill per parcel", eff50 = "effective tax rate", turn = "sales per 100 parcels"), ac = c(lhval = "log median home value (ACS)", lpop = "log population (ACS)"), mv = c(moveout = "move-out rate (Data Axle)"),
             f = c(lptax = "log district property tax per pupil", lspend = "log district spending per pupil", lenr = "log district enrolment"), sd = c(lptax = "log county school property tax (CoG)"))
cunits <- c(cl = "tract", ac = "tract", mv = "tract", f = "leaid", sd = "cty")
extra <- list(f = c(totrev = "total revenue", st_formula = "state formula aid (C01)", inst = "instruction", capout = "capital outlay"), se = c(ach = "district test scores (SEDA, SD)"))
for (tb in c("f", "sd")) P[[tb]] <- fix(P[[tb]]); for (tb in names(extra)) E$panels[[tb]] <- fix(E$panels[[tb]])
I <- function(v) sprintf("i(y, %s, ref = %d)", v, REF)
models <- list(
  `split: low side`  = list(f = paste(I("z"), "+", I("zx"), "+", I("ex"), "+", I("ldens"), "| %s + pair^y"), sub = quote(H == 0), coef = c(main = "z", interaction = "zx")),
  `split: high side` = list(f = paste(I("z"), "+", I("zx"), "+", I("ex"), "+", I("ldens"), "| %s + pair^y"), sub = quote(H == 1), coef = c(main = "z", interaction = "zx")),
  `pooled, saturated (pair x side x year FE)` = list(f = paste(I("z"), "+", I("zx"), "+", I("ex"), "+", I("ldens"), "+", I("zH"), "+", I("zxH"), "+", I("exH"), "+", I("ldH"), "| %s + ps^y"), sub = TRUE, coef = c(main = "zH", interaction = "zxH")),
  `pooled, common pair x year FE + H x year` = list(f = paste(I("z"), "+", I("zx"), "+", I("ex"), "+", I("ldens"), "+", I("zH"), "+", I("zxH"), "+", I("exH"), "+", I("ldH"), "+", I("H"), "| %s + pair^y"), sub = TRUE, coef = c(main = "zH", interaction = "zxH")),
  `pooled, common E and density slopes` = list(f = paste(I("z"), "+", I("zx"), "+", I("ex"), "+", I("ldens"), "+", I("zH"), "+", I("zxH"), "+", I("H"), "| %s + pair^y"), sub = TRUE, coef = c(main = "zH", interaction = "zxH")))
ser <- list()
run <- function(p, k, lab, unit) { p <- copy(p)[, yv := get(k)][is.finite(yv) & is.finite(ex)][, ps := paste(pair, H)]; sdy <- sd(p[y == 2016, yv], na.rm = TRUE); if (!is.finite(sdy) || sdy == 0 || uniqueN(p$y) < 3) return(NULL)
  for (mn in names(models)) { m <- models[[mn]]; x <- p[eval(m$sub)]
    fit <- feols(as.formula(sprintf(paste("yv ~", m$f), unit)), x, cluster = ~cty); ct <- as.data.table(coeftable(fit), keep.rownames = "term")
    for (cc in names(m$coef)) { r <- ct[grepl(sprintf("^y::\\d+:%s$", m$coef[cc]), term)][, .(y = as.integer(sub("y::(\\d+):.*", "\\1", term)), b = Estimate / sdy, se = `Std. Error` / sdy)]
      r <- rbind(r, data.table(y = REF, b = 0, se = 0))[, `:=`(model = mn, coef = cc, outcome = lab, n = nobs(fit))]; ser[[length(ser) + 1]] <<- r } } }
for (tb in names(core)) for (k in names(core[[tb]])) run(P[[tb]], k, core[[tb]][k], cunits[tb])
for (tb in names(extra)) for (k in names(extra[[tb]])) run(E$panels[[tb]], k, extra[[tb]][k], E$units[tb])
ser <- rbindlist(ser)
# split difference by hand: high minus low, SE = sqrt(se_h^2 + se_l^2)
sp <- dcast(ser[grepl("^split", model)], coef + outcome + y ~ model, value.var = c("b", "se"))
sp <- sp[, .(coef, outcome, y, b = `b_split: high side` - `b_split: low side`, se = sqrt(`se_split: high side`^2 + `se_split: low side`^2), model = "split: high minus low by hand", n = NA_integer_)]
all <- rbind(ser[!grepl("^split", model)], sp)[, model := factor(model, c("split: high minus low by hand", names(models)[3:5]))]
fwrite(all, file.path(OUT, "c47_pooled_side_interaction_series.csv"))
pp <- all[, { pre <- .SD[y < REF & y >= 2012]; post <- .SD[y > REF]; .(post_min_pre = mean(post$b) - mean(pre$b), se = sqrt(mean(post$se^2) + mean(pre$se^2) / nrow(pre))) }, by = .(model, coef, outcome)][, t := post_min_pre / se]
fwrite(pp, file.path(OUT, "c47_pooled_side_interaction_post_pre.csv"))
sink(file.path(OUT, "c47_pooled_side_interaction.txt"), split = TRUE)
cat("Side difference (high minus low) of the density-selected border design, pooled models with H x terms vs the split-sample difference. post - pre (2018+ minus 2012-16), SD units of the outcome (pooled 2016 SD), per 10 mi closer (main) / per 10 mi per SD of E (interaction); cells: estimate (t).\n")
for (cc in c("interaction", "main")) { cat("\n==", cc, "term, H x ... x year\n"); x <- pp[coef == cc][, cell := sprintf("%+.3f (%.1f)", post_min_pre, t)]; print(as.data.frame(dcast(x, outcome ~ model, value.var = "cell")), row.names = FALSE) }
chk <- merge(all[model == "split: high minus low by hand", .(coef, outcome, y, b1 = b, se1 = se)], all[model == names(models)[3], .(coef, outcome, y, b2 = b, se2 = se)], by = c("coef", "outcome", "y"))[y != REF]
cat(sprintf("\nCheck saturated pooled vs split-by-hand: max |b diff| = %.2e, SE ratio pooled / by hand: median %.3f, range %.3f-%.3f\n", max(abs(chk$b1 - chk$b2)), median(chk$se2 / chk$se1), min(chk$se2 / chk$se1), max(chk$se2 / chk$se1)))
cat("\nCounts of |t| > 1.96 (16 outcomes):\n"); print(as.data.frame(pp[, .(sig = sum(abs(t) > 1.96), median_abs = round(median(abs(post_min_pre)), 3)), by = .(coef, model)]), row.names = FALSE)
sink()
key <- c("effective tax rate", "sales per 100 parcels", "log median sale price", "log population (ACS)", "log district property tax per pupil", "log district spending per pupil", "log district enrolment", "total revenue", "state formula aid (C01)", "instruction", "capital outlay", "district test scores (SEDA, SD)")
plot_es <- function(cc, file, unit, title) { x <- all[coef == cc & outcome %in% key][, `:=`(b = -b, outcome = factor(outcome, key))]
  g <- ggplot(x, aes(y, b, colour = model)) + geom_hline(yintercept = 0, colour = "grey50") + geom_vline(xintercept = REF + .5, linetype = 3, colour = "grey50") +
    geom_pointrange(aes(ymin = b - 1.96 * se, ymax = b + 1.96 * se), position = position_dodge(width = .7), size = .18, linewidth = .3) + geom_line(position = position_dodge(width = .7), linewidth = .3) +
    facet_wrap(~outcome, scales = "free_y", ncol = 4, labeller = label_wrap_gen(30)) + scale_x_continuous(breaks = seq(2012, 2024, 2)) + scale_colour_manual(NULL, values = c("grey55", "#c0392b", "#2a64a8", "#2e8b57")) +
    labs(x = NULL, y = paste0("low-tax side minus high-tax side, SD units of the outcome ", unit, "; 2017 = 0; 95% CI, SE by county"), title = title,
         subtitle = "density-selected sample; grey = two side regressions differenced by hand; red = one pooled regression, every term x H, pair x side x year FE (same points, one covariance matrix);\nblue = pooled with common pair x year FE + H x year; green = also common E x year and density x year slopes") +
    theme_minimal(base_size = 8) + theme(legend.position = "bottom", strip.text = element_text(size = 6.5)) + guides(colour = guide_legend(nrow = 2))
  ggsave(file.path(OUT, "figures", file), g, width = 12, height = 8, dpi = 150) }
plot_es("interaction", "c47_pooled_side_interaction.png", "per 10 mi closer per SD of exposure", "Side difference of the Z x exposure x year term: pooled H-interacted models vs the split-sample difference")
plot_es("main", "c47_pooled_side_interaction_main.png", "per 10 mi closer", "Side difference of the Z x year term at mean exposure: pooled H-interacted models vs the split-sample difference")
