# Housing-stock outcomes on the county SALT exposure x year design (same specification as 04_county_outcomes.R):
# outcome ~ i(year, ex_z, ref 2017) | county + state x year, SE by county, weighted by 2016 returns. ex_z = 2016 SALT above
# $10k per return, SD units (build/exposure/01).
# Outcomes: CoreLogic roll (build/corelogic/10): log residential parcels; new-construction sales per 1,000 residential parcels
#   and as a share of arm's-length residential sales (sales incl. new builds, price > 10k); if the year-built extract
#   (som/l_yearbuilt.sbatch) is present, parcels built in the year per 1,000 parcels. Roll outcomes use counties whose residential
#   parcel count is >= 80% of its max in every year 2013-2024; sales shares need >= 50 sales.
# ACS (build/acs/07): log housing units, vacant units per 100 (all, for rent, for sale, seasonal), owner share of occupied
#   units (%), median year built; ACS 1-year (counties >= 65k, no 2020) and ACS 5-year (all counties; release year = last
#   survey year, so post-2017 values are 5-year windows that straddle the cap).
suppressPackageStartupMessages({library(data.table); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/exposure_did"); invisible(out_dir("designs/exposure_did/figures"))
ex <- fread(file.path(DATA, "built/exposure/salt_exposure_county2016.csv"))[, fips := sprintf("%05d", county)][, .(fips, ex_z, returns)]
cy <- readRDS(file.path(DATA, "built/corelogic/county_year.rds"))[y >= 2013 & y <= 2024]
cy <- merge(cy, ex, by = "fips")
cy[, ok_roll := uniqueN(y[n_res >= .8 * max(n_res, na.rm = TRUE)]) == 12, by = fips]
cy[, `:=`(l_nres = log(n_res), newcon_pk = 1000 * fcoalesce(n_newcon, 0) / n_res, sh_newcon = 100 * n_newcon / n_sales_all)]
if ("built_y" %in% names(cy)) cy[, `:=`(built_pk = 1000 * built_y / n_yb, entered_pk = 1000 * entered / n_res)]
ac <- readRDS(file.path(DATA, "built/acs/acs_county_year.rds"))
ac <- merge(ac, ex, by = "fips")
ac[, `:=`(l_hu = log(hu), vac_pct = 100 * vac / hu, vac_rent_pct = 100 * vac_rent / hu, vac_sale_pct = 100 * vac_sale / hu,
          vac_seas_pct = 100 * vac_seas / hu, own_pct = 100 * own / ten_tot)]
lab <- c(l_nres = "log residential parcels (roll)", newcon_pk = "new-construction sales per 1,000 parcels (roll)",
         sh_newcon = "new-construction share of sales, % (roll)", built_pk = "parcels built in the year per 1,000 (roll)", entered_pk = "parcels entering the roll per 1,000",
         l_hu = "log housing units", vac_pct = "vacant units per 100", vac_rent_pct = "vacant for rent per 100", vac_sale_pct = "vacant for sale per 100",
         vac_seas_pct = "seasonal vacant per 100", own_pct = "owner share of occupied units, %", yrbuilt = "median year built")
run <- function(d, v, src) {
  d <- d[is.finite(get(v))][, st := substr(fips, 1, 2)]
  if (uniqueN(d$y) < 3) return(NULL)
  f <- feols(as.formula(sprintf("%s ~ i(y, ex_z, ref = 2017) | fips + st^y", v)), d, cluster = ~fips, weights = ~returns)
  ct <- as.data.table(coeftable(f), keep.rownames = "term")
  rbind(data.table(y = 2017L, coef = 0, se = 0), ct[, .(y = as.integer(sub("y::(\\d+):ex_z", "\\1", term)), coef = Estimate, se = `Std. Error`)])[,
    `:=`(outcome = v, source = src, counties = uniqueN(d$fips), mean_2017 = d[y == 2017, weighted.mean(as.numeric(get(v)), returns)])] }
res <- rbind(
  run(cy[ok_roll == TRUE], "l_nres", "roll"), run(cy[ok_roll == TRUE & n_sales_all >= 50], "newcon_pk", "roll"), run(cy[n_sales_all >= 50], "sh_newcon", "roll"),
  if ("built_y" %in% names(cy)) rbind(run(cy[ok_roll == TRUE & n_yb >= .8 * n_res], "built_pk", "roll"), run(cy[ok_roll == TRUE], "entered_pk", "roll")),
  rbindlist(lapply(c("acs1", "acs5"), \(sv) rbindlist(lapply(c("l_hu", "vac_pct", "vac_rent_pct", "vac_sale_pct", "vac_seas_pct", "own_pct", "yrbuilt"),
    \(v) run(ac[survey == sv], v, sv))))))
res[, label := factor(lab[outcome], lab)][, source := factor(source, c("roll", "acs1", "acs5"), c("CoreLogic roll", "ACS 1-year", "ACS 5-year"))]
fwrite(res, file.path(OUT, "17_housing_stock.csv"))
sink(file.path(OUT, "17_housing_stock.txt"))
cat("Housing stock: +1 SD SALT exposure x year (ref 2017), county + state x year FE, SE by county, weighted by 2016 returns\n")
for (k in split(res, by = c("source", "outcome"), sorted = TRUE, drop = TRUE)) {
  cat("\n==", as.character(k$source[1]), "|", as.character(k$label[1]), "| counties:", k$counties[1], "| 2017 mean (weighted):", signif(k$mean_2017[1], 4), "\n")
  print(as.data.frame(dcast(k, . ~ y, value.var = "coef")[, -1][, lapply(.SD, round, 4)]), row.names = FALSE)
  print(as.data.frame(dcast(k, . ~ y, value.var = "se")[, -1][, lapply(.SD, round, 4)]), row.names = FALSE) }
sink(); cat(readLines(file.path(OUT, "17_housing_stock.txt")), sep = "\n")
g <- ggplot(res, aes(y, coef, colour = source)) + geom_hline(yintercept = 0, colour = "grey60") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey60") +
  geom_pointrange(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se), position = position_dodge(.5), size = .15) +
  facet_wrap(~ label, scales = "free_y", ncol = 3) + scale_x_continuous(breaks = seq(2013, 2024, 2)) +
  scale_colour_manual(values = c(`CoreLogic roll` = "black", `ACS 1-year` = "#e34a33", `ACS 5-year` = "#2b8cbe")) +
  labs(x = NULL, y = "effect of +1 SD exposure (vs 2017)", colour = NULL, title = "SALT-cap exposure and the housing stock",
       subtitle = "County + state x year FE, weighted by 2016 returns; 95% CI by county. ACS 5-year: release year = last survey year.") +
  theme_minimal(base_size = 9) + theme(legend.position = "bottom")
ggsave(file.path(OUT, "figures/17_housing_stock.png"), g, width = 11, height = 9, dpi = 150)
