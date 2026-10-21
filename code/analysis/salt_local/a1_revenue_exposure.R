# (a) SALT cap and local property-tax revenue. Exposure (pre-period, 2016 IRS SOI county x AGI-stub data): SALT deduction
# above $10k per return, approximated stub by stub as max(0, average SALT per SALT-itemizer - 10k) x SALT itemizers, summed
# over stubs and divided by all returns ("excess_pp", $k per return); also the share of returns in stubs whose average SALT
# exceeds $10k ("sh_capped"). Outcomes: Census of Governments / Annual Survey finance by county:
#   school districts (aggregated by county; F-33 is a census every year), county governments (balanced panel),
#   all local governments in the county area (Census years 2012/2017/2022 only).
# Event study: log(property tax revenue) ~ exposure(SD units) x year (ref 2017) | county + state x year; SE by county.
# Fiscal-year timing: FY2018 straddles the Dec-2017 enactment; FY2019 is the first fully post budget.
suppressPackageStartupMessages({library(data.table); library(duckdb); library(fixest); library(ggplot2)})
ROOT <- "/Users/jr2728/PhD/projects/property_taxes"; OUT <- file.path(ROOT, "output/analysis/salt_local"); dir.create(file.path(OUT, "figures"), recursive = TRUE, showWarnings = FALSE)
con <- dbConnect(duckdb(), file.path(ROOT, "data/database.db"), read_only = TRUE)
r <- as.data.table(dbGetQuery(con, "SELECT county::INT county, statefips, agi_stub, returns, taxes_paid_returns n_salt, taxes_paid_amount a_salt,
  property_taxes_amount a_ptax, returns_w_items n_item FROM returns_county_agi WHERE year = 2016")); dbDisconnect(con, shutdown = TRUE)
ex <- r[, .(returns = sum(returns), excess_pp = sum(pmax(0, a_salt / pmax(n_salt, 1) - 10) * n_salt) / sum(returns),
            sh_capped = sum(returns[a_salt / pmax(n_salt, 1) > 10]) / sum(returns), salt_pp = sum(a_salt) / sum(returns),
            sh_item = sum(n_item) / sum(returns)), by = county]
ex[, ex_z := (excess_pp - mean(excess_pp)) / sd(excess_pp)]
fwrite(ex, file.path(ROOT, "data/local_juris/salt_exposure_county2016.csv"))
con <- dbConnect(duckdb(), file.path(ROOT, "data/census_finances_test.db"), read_only = TRUE)
qfin <- function(tb) as.data.table(dbGetQuery(con, sprintf("SELECT county_fips::INT county, state_fips st, year::INT y, property_tax_rev ptax,
  total_tax_rev ttax, total_expenditure exp FROM %s", tb)))
fin <- list(school = qfin("govt_finances_sd_by_county"), county_govt = qfin("govt_finances_county"), county_area = qfin("govt_finances_county_area"))
dbDisconnect(con, shutdown = TRUE)
fin$county_govt <- fin$county_govt[, if (uniqueN(y) == 11) .SD, by = county]
fin$county_area <- fin$county_area[y %in% c(2012, 2017, 2022)]
est <- function(d, v, lab, w = NULL) {
  d <- merge(d, ex[, .(county, ex_z, returns)], by = "county")[get(v) > 0][, lv := log(get(v))]
  f <- feols(lv ~ i(y, ex_z, ref = 2017) | county + st^y, d, cluster = ~county, weights = w)
  ct <- as.data.table(coeftable(f), keep.rownames = "term")
  rbind(data.table(y = 2017L, coef = 0, se = 0), ct[, .(y = as.integer(sub("y::(\\d+):ex_z", "\\1", term)), coef = Estimate, se = `Std. Error`)])[,
    `:=`(sample = lab, outcome = v, weighted = !is.null(w), counties = uniqueN(d$county))] }
res <- rbindlist(c(lapply(names(fin), \(k) est(fin[[k]], "ptax", k)), lapply(names(fin), \(k) est(fin[[k]], "ptax", k, ~returns)),
                   lapply(names(fin), \(k) est(fin[[k]], "ttax", k)), lapply("school", \(k) est(fin[[k]], "exp", k))))
fwrite(res, file.path(OUT, "a1_revenue_event_study.csv"))
sink(file.path(OUT, "a1_revenue_event_study.txt"))
cat("Exposure: 2016 SALT above $10k per return (county). SD =", round(sd(ex$excess_pp), 3), "$k; mean", round(mean(ex$excess_pp), 3),
    "| top counties:", paste(head(ex[order(-excess_pp), sprintf("%05d", county)], 8), collapse = ","), "\n")
cat("Coefficients: effect of +1 SD exposure on log outcome relative to 2017; county + state x year FE; SE by county\n")
for (k in split(res, by = c("sample", "outcome", "weighted"))) {
  cat("\n==", k$sample[1], "|", k$outcome[1], "| weighted by returns:", k$weighted[1], "| counties:", k$counties[1], "\n")
  print(as.data.frame(k[order(y), .(y, coef = round(coef, 4), se = round(se, 4))]), row.names = FALSE) }
sink(); cat(readLines(file.path(OUT, "a1_revenue_event_study.txt")), sep = "\n")
pl <- res[outcome == "ptax" & sample != "county_area"]
g <- ggplot(pl, aes(y, coef, colour = weighted)) + geom_hline(yintercept = 0, colour = "grey60") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey60") +
  geom_pointrange(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se), position = position_dodge(.4), size = .25) +
  facet_wrap(~ sample, ncol = 1, scales = "free_y") + scale_x_continuous(breaks = 2012:2022) +
  scale_colour_manual(values = c(`FALSE` = "black", `TRUE` = "#e34a33"), labels = c("unweighted", "weighted by returns")) +
  labs(x = "fiscal year", y = "log property-tax revenue per +1 SD exposure (vs 2017)", colour = NULL,
       title = "SALT-cap exposure and local property-tax revenue (Census of Governments)",
       subtitle = "Exposure: 2016 SALT deduction above $10k per return (IRS SOI, county); county + state x year FE; 95% CI by county") +
  theme_minimal(base_size = 10) + theme(legend.position = "bottom")
ggsave(file.path(OUT, "figures/a1_revenue_event_study.png"), g, width = 8, height = 6.5, dpi = 150)
# Trend-break version: exposure x linear trend (all years) + exposure x post (level shift from 2018) + exposure x post x trend
# (slope change); reported for FY2018+ and, given fiscal-year timing, FY2019+ as the first post year.
tb <- rbindlist(lapply(c("school", "county_govt"), \(k) rbindlist(lapply(c(FALSE, TRUE), \(wt) rbindlist(lapply(c(2018, 2019), \(p0) {
  d <- merge(fin[[k]], ex[, .(county, ex_z, returns)], by = "county")[ptax > 0][, `:=`(lv = log(ptax), tt = y - 2017, post = as.integer(y >= p0))]
  f <- feols(lv ~ ex_z:tt + ex_z:post + ex_z:post:tt | county + st^y, d, cluster = ~county, weights = if (wt) ~returns else NULL)
  as.data.table(coeftable(f), keep.rownames = "term")[, `:=`(sample = k, weighted = wt, first_post = p0)] }))))))
fwrite(tb, file.path(OUT, "a1_revenue_trend_break.csv"))
sink(file.path(OUT, "a1_revenue_event_study.txt"), append = TRUE)
cat("\n\n== Trend break: log property tax ~ ex_z x trend + ex_z x post + ex_z x post x trend | county + state x year\n")
print(as.data.frame(tb[, .(sample, weighted, first_post, term, est = round(Estimate, 4), se = round(`Std. Error`, 4))]), row.names = FALSE)
sink()
