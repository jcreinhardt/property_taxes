# (c) Repeat-sales prices and ZIP-level SALT exposure, all residential parcels with >= 2 arm's-length sales (30% of parcels),
# parcel FE + state x year FE (and CBSA-free county x year robustness: county x year FE absorb local shocks, leaving
# within-county ZIP variation); log price ~ i(y, exposure, ref 2017); SE by county. Also the 2016-binned exposure split.
suppressPackageStartupMessages({library(data.table); library(duckdb); library(fixest); library(ggplot2)})
ROOT <- "/Users/jr2728/PhD/projects/property_taxes"; OUT <- file.path(ROOT, "output/analysis/salt_local")
con <- dbConnect(duckdb())
s <- as.data.table(dbGetQuery(con, sprintf("SELECT clip, tract, y::SMALLINT y, ln(price)::FLOAT lp FROM '%s/data/local_juris/kpanel/repeat_sales.parquet'
  WHERE NOT newcon AND hash(clip) %% 1000 < 300", ROOT))); dbDisconnect(con, shutdown = TRUE)
tz <- fread(file.path(ROOT, "data/local_juris/salt_exposure_tract_zip2016.csv"), colClasses = c(tract = "character"))[, .(tract, exz_z)]
d <- merge(s, tz, by = "tract"); rm(s)
d <- d[, if (.N >= 2) .SD, by = clip][, `:=`(st = substr(tract, 1, 2), cty = substr(tract, 1, 5))]
m1 <- feols(lp ~ i(y, exz_z, ref = 2017) | clip + st^y, d, cluster = ~cty, lean = TRUE)
m2 <- feols(lp ~ i(y, exz_z, ref = 2017) | clip + cty^y, d, cluster = ~cty, lean = TRUE)
tid <- function(m, lab) rbind(data.table(y = 2017L, coef = 0, se = 0), as.data.table(coeftable(m), keep.rownames = "term")[,
  .(y = as.integer(sub("y::(\\d+):.*", "\\1", term)), coef = Estimate, se = `Std. Error`)])[, spec := lab]
r <- rbind(tid(m1, "state x year FE"), tid(m2, "county x year FE (within-county ZIP variation)"))
fwrite(r, file.path(OUT, "b8_repeat_sales_exposure.csv"))
sink(file.path(OUT, "b8_repeat_sales_exposure.txt"))
cat("Repeat-sales log price on ZIP SALT exposure (per SD, winsorised) x year; 30% of parcels; sales", nrow(d), "parcels", uniqueN(d$clip), "\n")
for (k in split(r, by = "spec")) { cat("\n==", k$spec[1], "\n"); print(as.data.frame(dcast(k, . ~ y, value.var = "coef")[, -1][, lapply(.SD, round, 4)]), row.names = FALSE)
  print(as.data.frame(dcast(k, . ~ y, value.var = "se")[, -1][, lapply(.SD, round, 4)]), row.names = FALSE) }
sink(); cat(readLines(file.path(OUT, "b8_repeat_sales_exposure.txt")), sep = "\n")
g <- ggplot(r, aes(y, coef, colour = spec)) + geom_hline(yintercept = 0, colour = "grey60") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey60") +
  geom_pointrange(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se), position = position_dodge(.4), size = .2) + scale_x_continuous(breaks = 2012:2024) +
  labs(x = NULL, y = "log price per SD ZIP SALT exposure (vs 2017)", colour = NULL, title = "(c) Repeat-sales house prices and SALT exposure (ZIP, IRS SOI 2016)",
       subtitle = "parcel FE; 95% CI by county; exposure = SALT deduction above $10k per return, winsorised at p99") + theme_minimal(base_size = 9) + theme(legend.position = "bottom")
ggsave(file.path(OUT, "figures/b8_repeat_sales_exposure.png"), g, width = 9, height = 4.8, dpi = 150)
