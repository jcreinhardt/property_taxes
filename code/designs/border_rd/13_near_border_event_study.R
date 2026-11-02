# Distance to the border as an instrument, year by year: on the low-tax side within 50 mi of the line, rural pairs (09's
# default rule: no metro >= 0.5m with >= 45% of its population in the window), does the near-border effect on local public
# finances and tract outcomes change after 2017?  y_it = sum_t b_t near_i x 1[year = t] + unit FE + pair x year FE, b_2017 = 0,
# SE by county. near = 1(distance < 10 mi) for tracts; for districts and counties the population share of their tracts
# within 10 mi (units with >= 50% of their population in the sample). A linear version (distance / 10 mi x year) is also saved.
# Second version (no unit FE, no reference year): the cross-sectional near coefficient estimated separately in each year,
# y_i = a_t + b_t near_i + pair FE, i.e. the level difference between near and far tracts year by year, which is what the
# instrument identifies if it is valid; plotted as levels (c26_near_border_yearly_levels).
# Outcomes:
#   Census of Governments / F-33 (data/census_finances_test.db, data/built/census_finances/district_finance.rds):
#     district: log property tax, local revenue, state revenue and current spending per pupil, log enrolment (FY2012-23);
#     county, school districts aggregated (govt_finances_sd_by_county, complete every year): log property tax, total tax;
#     county area, all local governments (govt_finances_county_area): log property tax, total tax, expenditure, census
#     years 2012 / 2017 / 2022 only (survey years sample out small units: totals saw-tooth 433 / 400 ... 508 / 482 bn).
#   Tracts: CoreLogic tract_year (log median sale price, log bill and assessed value per parcel, effective rate, sales per
#     100 parcels, 2012-24), ACS tract_year (log income, home value, population, owner share, 2013-23), Data Axle move-outs
#     (annualised, 2013-21). Reads 08's tract table, 09's metro-pair list.
suppressPackageStartupMessages({library(data.table); library(DBI); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_rd"); invisible(out_dir("designs/border_rd/figures")); MX <- 50; REF <- 2017
d <- readRDS(file.path(DATA, "built/geo/low_side_balance_tracts.rds"))[dist_mi < MX]
metro <- fread(file.path(OUT, "c22_low_side_metro_pairs.csv"))$pair
d <- d[!pair %in% metro][, `:=`(near = as.integer(dist_mi < 10), d10 = dist_mi / 10)]
cat("rural low-side tracts within 50 mi:", nrow(d), "| pairs:", uniqueN(d$pair), "| near:", sum(d$near), "\n")
source("code/designs/border_rd/nb_panels.R")
P <- nb_panels(d, c("near", "d10"))
tid <- function(r, lab, unit) if (!is.null(r)) r[, `:=`(outcome = lab, unit = unit)]
lv <- function(p, ylab, unit, cl) rbindlist(lapply(sort(unique(p$y)), \(t) { x <- p[y == t & is.finite(yv)]; if (nrow(x) < 30 || uniqueN(x$pair) < 2) return(NULL)
  ct <- coeftable(feols(yv ~ near | pair, x, cluster = as.formula(paste0("~", cl)))); data.table(y = t, b = ct[1, 1], se = ct[1, 2], outcome = ylab, unit = unit, n = nrow(x)) }))
res <- list(); lev <- list()
addv <- function(p, vars, unit, fe_unit, cl) for (k in names(vars)) { p[, yv := get(k)]
  for (z in c("near", "d10")) res[[length(res) + 1]] <<- tid(es(p, z, fe_unit, cl), vars[k], unit)
  lev[[length(lev) + 1]] <<- lv(p, vars[k], unit, cl) }
addv(P$cl, c(lp50 = "log median sale price (CoreLogic)", lbill = "log tax bill per parcel (CoreLogic)", lav = "log assessed value per parcel (CoreLogic)",
             eff50 = "effective tax rate, % (CoreLogic)", turn = "sales per 100 parcels (CoreLogic)"), "tract", "tract", "cty")
addv(P$ac, c(linc = "log median HH income (ACS 5-yr)", lhval = "log median home value (ACS 5-yr)", lpop = "log population (ACS 5-yr)", sh_own = "owner share (ACS 5-yr)"), "tract", "tract", "cty")
addv(P$mv, c(moveout = "move-out rate, %/yr (Data Axle)"), "tract", "tract", "cty")
f <- P$f; cat("districts:", uniqueN(f$leaid), "| district-years:", nrow(f), "\n")
addv(f, c(lptax = "log property tax per pupil (F-33 district)", lloc = "log local revenue per pupil (F-33 district)", lst = "log state revenue per pupil (F-33 district)",
          lspend = "log current spending per pupil (F-33 district)", lenr = "log enrolment (F-33 district)"), "district", "leaid", "cty")
sd <- P$sd; cat("counties:", uniqueN(sd$cty), "| school-district county-years:", nrow(sd), "| area census-years:", nrow(P$ar), "\n")
addv(sd, c(lptax = "log property tax, school districts in county (CoG)", ltax = "log total tax, school districts in county (CoG)"), "county", "cty", "cty")
addv(P$ar, c(lptax = "log property tax, all local govts in county (CoG, census years)", ltax = "log total tax, all local govts (CoG, census years)", lexp = "log expenditure, all local govts (CoG, census years)"), "county", "cty", "cty")
res <- rbindlist(res); lev <- rbindlist(lev)
fwrite(res, file.path(OUT, "c26_near_border_event_study.csv")); fwrite(lev, file.path(OUT, "c26_near_border_yearly_levels.csv"))
# ---- post-2017 means (near): average of b_t over 2018+ vs 2013-2016, and the pre-trend
su <- res[z == "near", .(pre_mean = mean(b[y < REF & y >= 2013]), post_mean = mean(b[y > REF]), post_min_pre = mean(b[y > REF]) - mean(b[y < REF & y >= 2013]),
                         se_post = sqrt(mean(se[y > REF]^2)), years = paste(range(y), collapse = "-"), n = n[1]), by = .(unit, outcome)]
fwrite(su, file.path(OUT, "c26_near_border_event_study_summary.csv"))
sl <- lev[, .(pre_mean = mean(b[y < REF & y >= 2013]), post_mean = mean(b[y > REF]), post_min_pre = mean(b[y > REF]) - mean(b[y < REF & y >= 2013]),
              se_post = sqrt(mean(se[y > REF]^2)), se_pre = sqrt(mean(se[y < REF & y >= 2013]^2))), by = .(unit, outcome)]
sink(file.path(OUT, "c26_near_border_event_study.txt"), split = TRUE)
cat("Near-border (< 10 mi) x year on the low-tax side within 50 mi, rural pairs (", uniqueN(d$pair), "pairs,", nrow(d), "tracts,", sum(d$near), "near). Unit FE + pair x year FE, SE by county, 2017 = 0.\n")
cat("Districts / counties: near = population share within 10 mi; units with >= 50% of their population in the sample:", uniqueN(f$leaid), "districts,", uniqueN(sd$cty), "counties.\n\n")
cat("Mean of the yearly near coefficients, 2013-2016 vs 2018+, and their difference (RMS of post SEs):\n")
print(as.data.frame(su[, .(unit, outcome, years, n, pre = round(pre_mean, 3), post = round(post_mean, 3), post_minus_pre = round(post_min_pre, 3), se_post = round(se_post, 3))]), row.names = FALSE)
cat("\nYearly cross-sections (no unit FE, no reference year): level of the near coefficient, mean 2013-16 vs 2018+ (RMS SEs):\n")
print(as.data.frame(sl[, .(unit, outcome, pre = round(pre_mean, 3), se_pre = round(se_pre, 3), post = round(post_mean, 3), se_post = round(se_post, 3), post_minus_pre = round(post_min_pre, 3))]), row.names = FALSE)
cat("\nYearly levels (near):\n")
for (o in unique(lev$outcome)) { r <- lev[outcome == o][order(y)]; cat("\n==", o, "\n")
  print(as.data.frame(rbind(round(dcast(r, . ~ y, value.var = "b")[, -1], 3), round(dcast(r, . ~ y, value.var = "se")[, -1], 3))), row.names = c("b", "se")) }
cat("\nYearly coefficients (near, event study):\n")
for (o in unique(res[z == "near", outcome])) { r <- res[z == "near" & outcome == o][order(y)]; cat("\n==", o, "\n")
  print(as.data.frame(rbind(round(dcast(r, . ~ y, value.var = "b")[, -1], 3), round(dcast(r, . ~ y, value.var = "se")[, -1], 3))), row.names = c("b", "se")) }
sink()
# ---- figure (near)
pl <- res[z == "near"]; pl[, outcome := factor(outcome, unique(res$outcome))]
g <- ggplot(pl, aes(y, b)) + geom_hline(yintercept = 0, colour = "grey70") + geom_vline(xintercept = REF + .5, linetype = "22", colour = "grey50") +
  geom_ribbon(aes(ymin = b - 1.96 * se, ymax = b + 1.96 * se), fill = "#2e8b57", alpha = .15) + geom_line(colour = "#2e8b57") + geom_point(colour = "#2e8b57", size = 1) +
  facet_wrap(~ outcome, scales = "free_y", ncol = 4, labeller = label_wrap_gen(34)) + scale_x_continuous(breaks = seq(2012, 2024, 2)) +
  labs(x = NULL, y = "near-border (< 10 mi) x year coefficient, 2017 = 0; unit FE + border-pair x year FE; 95% CI, SE by county",
       title = "Does the near-border effect on the low-tax side change after 2017?  Rural pairs, within 50 miles",
       subtitle = sprintf("%d tracts in %d pairs (%d within 10 mi); districts and counties: population share within 10 mi, units with >= 50%% of their population in the sample", nrow(d), uniqueN(d$pair), sum(d$near))) +
  theme_minimal(base_size = 8) + theme(strip.text = element_text(size = 6.5))
ggsave(file.path(OUT, "figures/c26_near_border_event_study.png"), g, width = 11, height = 10, dpi = 150)
# ---- figure: yearly levels
pl <- copy(lev)[, outcome := factor(outcome, unique(res$outcome))]
g <- ggplot(pl, aes(y, b)) + geom_hline(yintercept = 0, colour = "grey70") + geom_vline(xintercept = REF + .5, linetype = "22", colour = "grey50") +
  geom_ribbon(aes(ymin = b - 1.96 * se, ymax = b + 1.96 * se), fill = "#2a64a8", alpha = .15) + geom_line(colour = "#2a64a8") + geom_point(colour = "#2a64a8", size = 1) +
  facet_wrap(~ outcome, scales = "free_y", ncol = 4, labeller = label_wrap_gen(34)) + scale_x_continuous(breaks = seq(2012, 2024, 2)) +
  labs(x = NULL, y = "near-border (< 10 mi) coefficient, estimated separately in each year; border-pair FE; 95% CI, SE by county",
       title = "Level difference between near-border and inland tracts on the low-tax side, year by year.  Rural pairs, within 50 miles",
       subtitle = sprintf("%d tracts in %d pairs (%d within 10 mi); districts and counties: population share within 10 mi, units with >= 50%% of their population in the sample. No unit FE, no reference year", nrow(d), uniqueN(d$pair), sum(d$near))) +
  theme_minimal(base_size = 8) + theme(strip.text = element_text(size = 6.5))
ggsave(file.path(OUT, "figures/c26_near_border_yearly_levels.png"), g, width = 11, height = 10, dpi = 150)
