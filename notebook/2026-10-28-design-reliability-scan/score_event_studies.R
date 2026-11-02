suppressPackageStartupMessages(library(data.table))
files <- list.files("output/designs", pattern = "\\.csv$", recursive = TRUE, full.names = TRUE)
skip <- c("a3_state_dispersion","c4_iv_one_sided","c5_iv_predicted","c6_iv_predicted","iv_balance","iv_first_stage","iv_high_side","school_district_gaps",
          "c13_side_selection","c14_side_profiles","c15_border_rd","c7_gap_range","tract_diff_by","border_cl_rate","border_gap_weighted","border_pair_gaps","border_pair_sides",
          "border_revenue_low_high","border_revenue_side_order","border_sales_turnover.csv","border_sample_gaps","border_vacancy.csv","a1_revenue_trend_break","c16_border_rd_by_year")
files <- files[!Reduce(`|`, lapply(skip, grepl, x = files))]
load1 <- function(f) {
  d <- fread(f); nm <- names(d)
  if ("term" %in% nm && !"y" %in% nm) { d[, y := as.integer(sub(".*?(20[0-9]{2}).*", "\\1", term))]; d <- d[grepl("20[0-9]{2}", term)] }
  if ("term" %in% nm && "y" %in% nm) { d[, term := NULL] }
  cc <- intersect(c("coef","b","est","Estimate"), names(d))[1]; sc <- intersect(c("se","Std. Error"), names(d))[1]
  if (is.na(sc) && all(c("lo","hi") %in% names(d))) { d[, se := (hi - lo) / 3.92]; sc <- "se" }
  if (is.na(cc) || is.na(sc)) return(NULL)
  setnames(d, c(cc, sc), c("coef","se"))
  drop <- intersect(c("t value","Pr(>|t|)","p","lo","hi","n","n_eff","n_dist","n_nh","nh_tracts","tracts","counties","cbsas","mean17","mean_2017","n_cty","scale","pairs","obs","base","n_pairs","n_nh","sdy","label"), names(d))
  if ("label" %in% names(d) && "var" %in% names(d)) drop <- setdiff(drop, "label")
  d[, (drop) := NULL]
  g <- setdiff(names(d), c("y","coef","se"))
  d[, grp := if (length(g)) do.call(paste, c(.SD, sep = " | ")) else "", .SDcols = g]
  d[, .(y = as.integer(y), coef = as.numeric(coef), se = as.numeric(se), grp, file = sub("output/designs/", "", f))]
}
D <- rbindlist(lapply(files, load1), use.names = TRUE)
D <- D[!is.na(y) & !is.na(coef) & is.finite(se)]
sc <- D[, {
  pre <- .SD[y <= 2016 & se > 0]; post <- .SD[y >= 2019 & se > 0]; p18 <- .SD[y == 2018 & se > 0]
  sl <- if (nrow(pre) >= 3) { m <- lm(coef ~ y, pre, weights = 1/se^2); c(coef(m)[2], summary(m)$coefficients[2, 2]) } else c(NA, NA)
  .(n_pre = nrow(pre), n_post = nrow(post), pre_slope_t = round(sl[1]/sl[2], 1), pre_max_t = round(max(abs(pre$coef/pre$se), -Inf), 1),
    pre_mean = signif(mean(pre$coef), 2), post_mean = signif(mean(post$coef), 2), post_min_t = round(min(abs(post$coef/post$se), Inf), 1),
    post_sig = sum(abs(post$coef/post$se) > 1.96), post_sign = as.numeric(if (nrow(post)) sign(mean(post$coef)) else NA_real_),
    post_same_sign = if (nrow(post)) all(sign(post$coef) == sign(mean(post$coef))) else FALSE, last_y = max(.SD$y))
}, by = .(file, grp)]
sc[, flat_pre := !is.na(pre_slope_t) & abs(pre_slope_t) < 2 & pre_max_t < 2.5]
sc[, post_effect := post_sig >= 2 & post_same_sign]
sc[, verdict := fifelse(flat_pre & post_effect, "PASS: flat pre + post effect", fifelse(flat_pre & !post_effect, "null: flat pre, no post effect", fifelse(!flat_pre & post_effect, "pre-trend + post effect", "pre-trend, no post effect")))]
setorder(sc, verdict, file, grp)
fwrite(sc, "notebook/2026-10-28-design-reliability-scan/event_study_scores.csv")
cat("series:", nrow(sc), "\n"); print(table(sc$verdict))
cat("\n==== PASS\n"); print(sc[verdict == "PASS: flat pre + post effect", .(file, grp, n_pre, pre_slope_t, pre_max_t, post_mean, post_sig, n_post)], nrows = 500)
cat("\n==== pre-trend + post effect\n"); print(sc[verdict == "pre-trend + post effect", .(file, grp, pre_slope_t, pre_max_t, pre_mean, post_mean, post_sig, n_post)], nrows = 500)
