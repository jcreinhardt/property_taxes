# School-district outcomes on the density-selected sample of 29 (tracts < 1,000 per sq mi within 50 mi, both sides): the
# widened F-33 set (39 items, log per pupil), SEDA (scores, SES) and the CoG school-district aggregates by county (7, log per
# capita), as (A) Z x year with log density x year and (B) 2SLS on competition x year instrumented by Z x year, Z = -distance
# / 10 mi; by side and pooled; district / county FE + pair x year FE, SE by county. Post - pre tiles with coverage.
suppressPackageStartupMessages({library(data.table); library(DBI); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_rd"); invisible(out_dir("designs/border_rd/figures")); REF <- 2017
source("code/designs/border_rd/nb_panels.R")
d <- fread(file.path(OUT, "c41_sample_competition.csv"), colClasses = c(tract = "character", pair = "character"))
d <- merge(d, rbindlist(lapply(c("low", "high"), \(s) readRDS(file.path(DATA, sprintf("built/geo/%s_side_balance_tracts.rds", s)))[, .(tract, leaid, cty, pop)])), by = "tract")[, `:=`(z = -dist_mi / 10, ps = paste(pair, side))]
samples <- list(`low-tax side` = d[side == "low"], `high-tax side` = d[side == "high"], `both sides pooled` = copy(d)[, pair := ps])
pp <- function(r) { pre <- r[y < REF & y >= 2012]; post <- r[y > REF]; if (!nrow(pre) || !nrow(post)) return(NULL); r[, .(post_min_pre = mean(post$b) - mean(pre$b), se = sqrt(mean(post$se^2) + mean(pre$se^2) / nrow(pre)), years = uniqueN(y))] }
iv <- function(p, fe_unit) { x <- p[is.finite(yv) & is.finite(comp)]; if (uniqueN(x$y) < 3) return(NULL)
  m <- feols(as.formula(sprintf("yv ~ i(y, ldens, ref = %d) | %s + pair^y | i(y, comp, ref = %d) ~ i(y, z, ref = %d)", REF, fe_unit, REF, REF)), x, cluster = ~cty)
  ct <- as.data.table(coeftable(m), keep.rownames = "term")[grepl("^fit_y::\\d+:comp$", term)]
  rbind(ct[, .(y = as.integer(sub("fit_y::(\\d+):.*", "\\1", term)), b = Estimate, se = `Std. Error`)], data.table(y = REF, b = 0, se = 0))[, n := nobs(m)] }
res <- list(); cov <- list()
for (s in names(samples)) { P <- nb_panels(samples[[s]], c("z", "comp", "ldens")); E <- nb_extra_panels(P$dis, P$cnt)
  for (tb in names(E$panels)) { p0 <- E$panels[[tb]]; if (tb == "se") p0 <- p0[is.finite(z)]
    for (k in names(E$vars[[tb]])) { p <- copy(p0)[, yv := get(k)]; fy <- if (tb == "ar") 2017 else 2016; sdy <- sd(p[y == fy, yv], na.rm = TRUE); if (!is.finite(sdy) || sdy == 0) next
      cov[[length(cov) + 1]] <- data.table(sample = s, src = E$src[tb], outcome = E$vars[[tb]][k], units = uniqueN(p[is.finite(yv), get(E$units[tb])]), unit_years = sum(is.finite(p$yv)), share = round(mean(is.finite(p$yv)), 2), years = paste(range(p[is.finite(yv), y]), collapse = "-"))
      rf <- if (tb == "ar") NULL else es(p, "z", E$units[tb], "cty", REF, sprintf(" + i(y, ldens, ref = %d)", REF))
      if (tb == "ar") { x <- p[is.finite(yv)]; x[, `:=`(post = as.integer(y == 2022), pre = as.integer(y == 2012))]   # census years only: 2022 - 2012 vs 2017
        m <- feols(yv ~ z:post + z:pre + ldens:post + ldens:pre | cty + pair^y, x, cluster = ~cty); ct <- coeftable(m)
        rf <- data.table(y = c(2012, 2017, 2022), b = c(ct["z:pre", 1], 0, ct["z:post", 1]), se = c(ct["z:pre", 2], 0, ct["z:post", 2]), n = nobs(m))
        x <- x[is.finite(comp)]; m2 <- feols(yv ~ ldens:post + ldens:pre | cty + pair^y | comp:post + comp:pre ~ z:post + z:pre, x, cluster = ~cty); ct2 <- coeftable(m2)
        iv_r <- data.table(y = c(2012, 2017, 2022), b = c(ct2["fit_comp:pre", 1], 0, ct2["fit_comp:post", 1]), se = c(ct2["fit_comp:pre", 2], 0, ct2["fit_comp:post", 2]), n = nobs(m2)) } else iv_r <- iv(p, E$units[tb])
      for (dd in c("near x year", "IV")) { r <- if (dd == "near x year") rf else iv_r; if (is.null(r)) next; q <- pp(r); if (is.null(q)) next
        res[[length(res) + 1]] <- q[, `:=`(design = dd, sample = s, src = E$src[tb], outcome = E$vars[[tb]][k], b_sd = post_min_pre / sdy, se_sd = se / sdy, n = r$n[1])] } } } }
res <- rbindlist(res)[, t := b_sd / se_sd]; cov <- rbindlist(cov)
fwrite(res, file.path(OUT, "c42_district_outcomes.csv")); fwrite(cov, file.path(OUT, "c42_district_outcomes_coverage.csv"))
sink(file.path(OUT, "c42_district_outcomes.txt"), split = TRUE)
cat("District / local-government outcomes on the density-selected sample (29). post - pre in SD units of the outcome (2016; 2022 - 2012 for the census-year CoG area panel), per 10 mi closer (near x year) or per SD of competition (IV); log density x year, unit FE + pair x year FE, SE by county. Cells: estimate (t).\n")
for (dd in c("near x year", "IV")) { cat("\n==", dd, "\n"); w <- dcast(res[design == dd, .(src, outcome, sample, cell = sprintf("%.3f (%.1f)", b_sd, t))], src + outcome ~ sample, value.var = "cell"); print(as.data.frame(w), row.names = FALSE) }
cat("\nsummary: cells with |t| > 1.96, by design and sample\n"); print(res[, .(k = .N, sig = sum(abs(t) > 1.96), median_abs = round(median(abs(b_sd)), 3)), by = .(design, sample)])
cat("\ncoverage (low-tax side sample):\n"); print(as.data.frame(cov[sample == "low-tax side", .(src, outcome, units, unit_years, share, years)]), row.names = FALSE)
sink()
tl <- copy(res)[, `:=`(outcome = factor(outcome, rev(unique(res$outcome))), sample = factor(sample, names(samples)), design = factor(design, c("near x year", "IV"), c("Z x year, per 10 mi closer", "IV: per SD of competition")))]
g <- ggplot(tl, aes(sample, outcome, fill = pmax(pmin(t, 3), -3))) + geom_tile(colour = "white") + geom_text(aes(label = sprintf("%.2f", b_sd)), size = 2) + facet_grid(src ~ design, scales = "free_y", space = "free_y", labeller = label_wrap_gen(28)) +
  scale_fill_gradient2("t-stat", low = "#c0392b", mid = "white", high = "#2a64a8", limits = c(-3, 3)) + labs(x = NULL, y = NULL, title = "District and local-government outcomes, density-selected sample: post-2017 change near the border and per SD of competition (IV)", subtitle = "tracts < 1,000 per sq mi within 50 mi; log density x year, unit FE, pair x year FE; cells = post - pre in SD units, colour = t") +
  theme_minimal(base_size = 7) + theme(axis.text.x = element_text(angle = 30, hjust = 1), strip.text.y = element_text(angle = 0, size = 6))
ggsave(file.path(OUT, "figures/c42_district_outcomes_tiles.png"), g, width = 11, height = 14, dpi = 150)
