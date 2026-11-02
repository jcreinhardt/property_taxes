# Three house-price indices in the density-selected border design (29): tract-year median log sale price, repeat-sales index
# and hedonic index from build/corelogic/som/l_price_indices.R (data/built/corelogic/price_indices/price_indices_tract_year.parquet).
# Sample as in 31 / 35: tracts < 1,000 per sq mi within 50 mi of a ranked border, both sides, with ZIP exposure. Models by side:
#   main:        yv ~ i(y, z, ref 2017) + i(y, ldens, ref 2017) | tract + pair^y
#   interaction: yv ~ i(y, z) + i(y, zx) + i(y, ex) + i(y, ldens) | tract + pair^y          (zx = Z x E, E standardised)
# Z = -distance / 10 mi; SE by county; high minus low from the two side regressions (exact under county clustering, 35).
# Coefficients in log points x 100 (not SD units, so the three indices are on one scale). Tract-years with >= MINS sales.
# Also: coverage and the correlation of year-to-year changes between the indices on the sample.
suppressPackageStartupMessages({library(data.table); library(DBI); library(duckdb); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_rd"); invisible(out_dir("designs/border_rd/figures")); REF <- 2017; MINS <- 5
con <- dbConnect(duckdb()); px <- as.data.table(dbGetQuery(con, sprintf("SELECT * FROM '%s/data/built/corelogic/price_indices/price_indices_tract_year.parquet'", ROOT))); dbDisconnect(con, shutdown = TRUE)
px <- px[n_sales >= MINS & y <= 2024]
tz <- fread(file.path(DATA, "built/exposure/salt_exposure_tract_zip2016.csv"), colClasses = c(tract = "character", zip = "character"))[, .(tract, zip)]
cz <- fread(file.path(DATA, "built/exposure/exposure_components_zip.csv"), colClasses = c(geo = "character"))[returns16 >= 100 & is.finite(d_salt_own), .(zip = geo, d_salt_own)]
d <- fread(file.path(OUT, "c41_sample_competition.csv"), colClasses = c(tract = "character", pair = "character"))
d <- merge(d, rbindlist(lapply(c("low", "high"), \(s) readRDS(file.path(DATA, sprintf("built/geo/%s_side_balance_tracts.rds", s)))[, .(tract, cty, pop)])), by = "tract")
d <- merge(d, merge(tz, cz, by = "zip")[, .(tract, d_salt_own)], by = "tract")[, z := -dist_mi / 10]
d[, ex := (d_salt_own - mean(d_salt_own)) / sd(d_salt_own)][, zx := z * ex]
p <- merge(px, d[, .(tract, side, pair, cty, z, zx, ex, ldens)], by = "tract")
vars <- c(med_lp = "median log sale price", mean_lp = "mean log sale price", rep = "repeat-sales index", hed = "hedonic index")
cat(sprintf("sample: %d tracts (%d low, %d high); tract-years with >= %d sales: %d; with repeat index: %d\n", uniqueN(p$tract), uniqueN(p[side == "low", tract]), uniqueN(p[side == "high", tract]), MINS, nrow(p), sum(is.finite(p$rep))))
# ---- coverage and agreement between the indices
cov <- p[, .(tract_years = .N, med = sum(is.finite(med_lp)), rep = sum(is.finite(rep)), hed = sum(is.finite(hed)), sales_per_ty = round(mean(n_sales), 1), rep_sales_per_ty = round(mean(n_rep, na.rm = TRUE), 1)), by = side]
setorder(p, tract, y); ch <- p[, .(y, d_med = med_lp - shift(med_lp), d_rep = rep - shift(rep), d_hed = hed - shift(hed), gap = y - shift(y)), by = tract][gap == 1]
cm <- cor(ch[, .(d_med, d_rep, d_hed)], use = "pairwise"); sds <- ch[, .(d_med = sd(d_med, na.rm = TRUE), d_rep = sd(d_rep, na.rm = TRUE), d_hed = sd(d_hed, na.rm = TRUE))]
# ---- event studies by side
pp <- function(r) { pre <- r[y < REF & y >= 2012]; post <- r[y > REF]; if (!nrow(pre) || !nrow(post)) return(NULL); data.table(post_min_pre = mean(post$b) - mean(pre$b), se = sqrt(mean(post$se^2) + mean(pre$se^2) / nrow(pre))) }
getc <- function(m, pat, nm) { ct <- as.data.table(coeftable(m), keep.rownames = "term")[grepl(pat, term)]; rbind(ct[, .(y = as.integer(sub(".*y::(\\d+):.*", "\\1", term)), b = Estimate, se = `Std. Error`)], data.table(y = REF, b = 0, se = 0))[, `:=`(coef = nm, n = nobs(m))] }
ser <- list()
for (s in c("low", "high")) for (k in names(vars)) { x <- p[side == s & is.finite(get(k))][, yv := 100 * get(k)]
  m1 <- feols(as.formula(sprintf("yv ~ i(y, z, ref = %d) + i(y, ldens, ref = %d) | tract + pair^y", REF, REF)), x, cluster = ~cty)
  m2 <- feols(as.formula(sprintf("yv ~ i(y, z, ref = %d) + i(y, zx, ref = %d) + i(y, ex, ref = %d) + i(y, ldens, ref = %d) | tract + pair^y", REF, REF, REF, REF)), x, cluster = ~cty)
  ser[[length(ser) + 1]] <- rbind(getc(m1, "^y::\\d+:z$", "Z x year"), getc(m2, "^y::\\d+:zx$", "Z x exposure x year"))[, `:=`(side = paste0(s, "-tax side"), index = vars[k])] }
ser <- rbindlist(ser)
ctr <- merge(ser[side == "high-tax side", .(y, coef, index, b_h = b, se_h = se)], ser[side == "low-tax side", .(y, coef, index, b_l = b, se_l = se)], by = c("y", "coef", "index"))[, .(y, coef, index, side = "high minus low", b = b_h - b_l, se = sqrt(se_h^2 + se_l^2), n = NA_integer_)]
ser <- rbind(ser, ctr)[, side := factor(side, c("low-tax side", "high-tax side", "high minus low"))]
res <- ser[, pp(.SD), by = .(coef, index, side)][, t := post_min_pre / se]
fwrite(ser, file.path(OUT, "c48_price_indices_series.csv")); fwrite(res, file.path(OUT, "c48_price_indices_post_pre.csv"))
sink(file.path(OUT, "c48_price_indices.txt"), split = TRUE)
cat("Three price indices on the density-selected sample (tracts < 1,000 per sq mi within 50 mi, both sides). Log points x 100; tract FE + pair x year FE, log density x year; SE by county.\n")
cat("Z x year: per 10 mi closer to the border; Z x exposure x year: per 10 mi closer per SD of own lost SALT subsidy (E x year included). post - pre = mean 2018+ minus mean 2012-16. Cells: estimate (t).\n\n")
cat("coverage by side:\n"); print(as.data.frame(cov), row.names = FALSE)
cat("\ncorrelation of year-to-year changes between the indices (consecutive tract-years), and their SDs (log points):\n"); print(round(cm, 3)); print(round(100 * sds, 1))
for (cc in unique(res$coef)) { cat("\n==", cc, "\n"); w <- dcast(res[coef == cc, .(index, side, cell = sprintf("%.2f (%.1f)", post_min_pre, t))], index ~ side, value.var = "cell"); w <- w[match(vars, index)]; print(as.data.frame(w), row.names = FALSE) }
sink()
ser[, index := factor(index, vars)]
g <- ggplot(ser, aes(y, b, colour = index)) + geom_hline(yintercept = 0, colour = "grey70") + geom_vline(xintercept = REF + .5, linetype = "22", colour = "grey50") +
  geom_pointrange(aes(ymin = b - 1.96 * se, ymax = b + 1.96 * se), position = position_dodge(width = .7), size = .18, linewidth = .3) + geom_line(position = position_dodge(width = .7), linewidth = .35) +
  facet_grid(coef ~ side, scales = "free_y") + scale_x_continuous(breaks = seq(2012, 2024, 2)) + scale_colour_manual(NULL, values = c("grey40", "grey65", "#2a64a8", "#c0392b")) +
  labs(x = NULL, y = "log points x 100, 2017 = 0; 95% CI, SE by county", title = "House prices in the density-selected border design: median, repeat-sales and hedonic indices",
       subtitle = sprintf("tract-years with >= %d arm's-length sales; tract FE + pair x year FE, log density x year; row 1 per 10 mi closer, row 2 per 10 mi closer per SD of exposure", MINS)) +
  theme_minimal(base_size = 8) + theme(legend.position = "bottom", strip.text.y = element_text(size = 7))
ggsave(file.path(OUT, "figures/c48_price_indices_es.png"), g, width = 11, height = 6.5, dpi = 150)
