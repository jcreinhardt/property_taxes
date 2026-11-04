# Budget-first vs rate-first tax areas. Input: SOM n_levy_regime.sbatch -> data/built/corelogic/levyreg/levy_area_YYYY.parquet
# (residential parcels per tax area x year; stable = the same parcels, same area, in t-1 and t, bill and AV > 0).
# For each area, regress same-parcel levy growth on same-parcel AV growth over 2013-2024:
#   dlog levy_at = a + b * dlog AV_at.   b ~ 0: the levy is fixed and the rate absorbs base growth (budget-first);
#   b ~ 1: the rate is fixed and the levy follows the base (rate-first). The rate slope is b - 1.
# Same parcels in both years, so new construction and roll churn are out, and levy and AV are measured separately (no
# division bias). Classical error in area AV growth attenuates b towards 0 (towards "budget-first").
# Outputs: data/built/corelogic/levy_regime_area.parquet; output/build/corelogic/levy_regime_{state.csv,area_summary.txt},
# figures/levy_regime_states.png.
suppressPackageStartupMessages({library(data.table); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("build/corelogic"); invisible(out_dir("build/corelogic/figures"))
DIR <- file.path(DATA, "built/corelogic/levyreg")
d <- dq(sprintf("SELECT * FROM read_parquet('%s/levy_area_*.parquet') WHERE y >= 2013 AND n_st > 0", DIR))
for (v in c("n", "bill", "av", "n_st", "bill0", "bill1", "av0", "av1")) set(d, j = v, value = as.numeric(d[[v]]))
st_ab <- c(`01`="AL",`02`="AK",`04`="AZ",`05`="AR",`06`="CA",`08`="CO",`09`="CT",`10`="DE",`11`="DC",`12`="FL",`13`="GA",`15`="HI",
  `16`="ID",`17`="IL",`18`="IN",`19`="IA",`20`="KS",`21`="KY",`22`="LA",`23`="ME",`24`="MD",`25`="MA",`26`="MI",`27`="MN",`28`="MS",
  `29`="MO",`30`="MT",`31`="NE",`32`="NV",`33`="NH",`34`="NJ",`35`="NM",`36`="NY",`37`="NC",`38`="ND",`39`="OH",`40`="OK",`41`="OR",
  `42`="PA",`44`="RI",`45`="SC",`46`="SD",`47`="TN",`48`="TX",`49`="UT",`50`="VT",`51`="VA",`53`="WA",`54`="WV",`55`="WI",`56`="WY")
d[, `:=`(st = st_ab[substr(fips, 1, 2)], aid = paste(fips, area), city = startsWith(area, "city:"),
         g_levy = log(bill1 / bill0), g_av = log(av1 / av0), cov = n_st / n)]
# city fallback only where the county has (almost) no tax-area codes; otherwise city cells mix coded and uncoded parcels
d[, sh_city := sum(n * city) / sum(n), by = .(fips, y)]
raw <- nrow(d)
d <- d[!is.na(st) & n_st >= 30 & cov >= 0.7 & abs(g_av) < 0.7 & abs(g_levy) < 0.7 & (!city | sh_city > 0.9)]
cat("area-years:", raw, "->", nrow(d), "after n_st >= 30, coverage >= 70%, |growth| < 0.7, city fallback only in uncoded counties\n")
# state x year demeaned growth (parcel-weighted), for the robustness slope
d[, `:=`(g_levy_dm = g_levy - weighted.mean(g_levy, n_st), g_av_dm = g_av - weighted.mean(g_av, n_st)), by = .(st, y)]

slope <- function(y, x) {
  if (length(x) < 6 || sd(x) < 0.01) return(list(b = NA_real_, se = NA_real_))
  m <- lm.fit(cbind(1, x), y); r <- m$residuals; X <- cbind(1, x)
  v <- sum(r^2) / (length(x) - 2) * solve(crossprod(X))[2, 2]; list(b = unname(m$coefficients[2]), se = sqrt(v)) }
a <- d[, c(list(st = st[1], fips = fips[1], years = .N, n = mean(n), sd_g_av = sd(g_av), mean_g_av = mean(g_av),
                sh_rate_same = mean(sh_rate_same), sh_av_same = mean(sh_av_same)),
           slope(g_levy, g_av), setNames(slope(g_levy_dm, g_av_dm), c("b_dm", "se_dm"))), by = aid]
cls <- function(b, se) fifelse(is.na(b), "no AV variation", fifelse(se > 0.33, "imprecise",
         fifelse(b < 1/3, "budget-first", fifelse(b > 2/3, "rate-first", "mixed"))))
a[, `:=`(regime = cls(b, se), regime_dm = cls(b_dm, se_dm))]
dir.create(file.path(DATA, "built/corelogic"), showWarnings = FALSE)
con <- db_mem(); duckdb::dbWriteTable(con, "a", a)
DBI::dbExecute(con, sprintf("COPY a TO '%s' (FORMAT parquet)", file.path(DATA, "built/corelogic/levy_regime_area.parquet"))); DBI::dbDisconnect(con, shutdown = TRUE)

# state level: pooled within-area slope (area FE; + year FE = state x year), parcel weights, SE by area
pooled <- function(dd) {
  m1 <- feols(g_levy ~ g_av | aid, dd, weights = ~n_st, cluster = ~aid)
  m2 <- tryCatch(feols(g_levy ~ g_av | aid + y, dd, weights = ~n_st, cluster = ~aid), error = function(e) NULL)
  list(b_pool = coef(m1)[[1]], se_pool = se(m1)[[1]], b_pool_sy = if (is.null(m2)) NA_real_ else coef(m2)[[1]],
       se_pool_sy = if (is.null(m2)) NA_real_ else se(m2)[[1]]) }
s <- d[, c(list(areas = uniqueN(aid), parcels_m = sum(n) / uniqueN(y) / 1e6, sd_g_av = sqrt(weighted.mean((g_av - weighted.mean(g_av, n_st))^2, n_st)),
                sh_rate_same = weighted.mean(sh_rate_same, n_st)), pooled(.SD)), by = st]
sh <- dcast(a, st ~ regime, value.var = "n", fun.aggregate = sum)
cols <- setdiff(names(sh), "st"); sh[, (cols) := lapply(.SD, \(x) round(x / rowSums(sh[, ..cols]), 2)), .SDcols = cols]
s <- merge(s, sh, by = "st")[order(b_pool)]
s[, med_b := a[!is.na(b) & se <= 0.33, .(med_b = median(b)), by = st][s, on = "st", med_b]]
fwrite(s, file.path(OUT, "levy_regime_state.csv"))

nat <- pooled(d)
sink(file.path(OUT, "levy_regime_area_summary.txt"))
cat("Levy-regime slopes: dlog levy ~ dlog AV on the same parcels, tax area x year 2013-2024 (CoreLogic full roll, SOM)\n")
cat("b ~ 0 budget-first (rate absorbs base growth), b ~ 1 rate-first. Regime cut-offs 1/3 and 2/3; 'imprecise' = SE > 0.33.\n\n")
cat(sprintf("Area-years %d, areas %d. National pooled b = %.3f (%.3f); with year FE %.3f (%.3f)\n\n", nrow(d), uniqueN(d$aid),
            nat$b_pool, nat$se_pool, nat$b_pool_sy, nat$se_pool_sy))
cat("Parcel-weighted regime shares (areas with >= 6 years):\n")
print(a[, .(parcels_m = round(sum(n) / 1e6, 1), areas = .N), by = regime][, share := round(parcels_m / sum(parcels_m), 3)][order(-parcels_m)])
cat("\nSame, after removing state x year means:\n")
print(a[, .(parcels_m = round(sum(n) / 1e6, 1), areas = .N), by = regime_dm][, share := round(parcels_m / sum(parcels_m), 3)][order(-parcels_m)])
cat("\nStates, sorted by pooled slope (b_pool: area FE; b_pool_sy: + state x year FE; shares = parcel shares by area regime;\n",
    "sh_rate_same = share of stable parcels whose bill/AV is unchanged to 0.05%):\n")
print(s[, lapply(.SD, \(x) if (is.numeric(x)) round(x, 3) else x)], nrows = 100)
sink(); cat(readLines(file.path(OUT, "levy_regime_area_summary.txt")), sep = "\n")

p <- melt(s[parcels_m >= 0.05], id.vars = "st", measure.vars = list(b = c("b_pool", "b_pool_sy"), se = c("se_pool", "se_pool_sy")),
          variable.name = "spec")[, spec := c("area FE", "area + year FE")[spec]]
p[, st := factor(st, s[order(b_pool), st])]
g <- ggplot(p, aes(b, st, colour = spec)) + geom_vline(xintercept = c(0, 1), colour = "grey60") +
  geom_pointrange(aes(xmin = b - 1.96 * se, xmax = b + 1.96 * se), position = position_dodge(.5), size = .15) +
  scale_colour_manual(values = c(`area FE` = "black", `area + year FE` = "#e34a33")) + coord_cartesian(xlim = c(-0.5, 1.5)) +
  labs(x = "slope of same-parcel levy growth on assessed-value growth (0 = budget-first, 1 = rate-first)", y = NULL, colour = NULL,
       title = "How tax areas respond to growth in the assessed base, 2013-2024",
       subtitle = "Residential parcels in the same tax area in consecutive years; parcel weights; 95% CI by tax area") +
  theme_minimal(base_size = 9) + theme(legend.position = "bottom")
ggsave(file.path(OUT, "figures/levy_regime_states.png"), g, width = 7, height = 9, dpi = 150)
