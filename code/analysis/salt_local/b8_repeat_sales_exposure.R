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
# Income control: ZIP share of 2016 returns with AGI >= $200k (SOI stub 6) and share $100-200k (stub 5), each x year
con <- dbConnect(duckdb(), file.path(ROOT, "data/database.db"), read_only = TRUE)
zi <- as.data.table(dbGetQuery(con, "SELECT lpad(zipcode::INT::VARCHAR, 5, '0') zip, sum(returns) FILTER (WHERE agi_stub = 6) / sum(returns) sh200,
  sum(returns) FILTER (WHERE agi_stub = 5) / sum(returns) sh100 FROM returns_zipcode_agi WHERE year = 2016 GROUP BY 1")); dbDisconnect(con, shutdown = TRUE)
tzi <- merge(fread(file.path(ROOT, "data/local_juris/salt_exposure_tract_zip2016.csv"), colClasses = c(tract = "character", zip = "character"))[, .(tract, zip)], zi, by = "zip")
d <- merge(d, tzi[, .(tract, sh200, sh100)], by = "tract")
d[, `:=`(i200 = (sh200 - mean(sh200)) / sd(sh200), i100 = (sh100 - mean(sh100)) / sd(sh100))]
cat("corr(exposure, share AGI 200k+):", round(cor(unique(d[, .(tract, exz_z, sh200)])$exz_z, unique(d[, .(tract, exz_z, sh200)])$sh200), 3), "\n")
m3 <- feols(lp ~ i(y, exz_z, ref = 2017) + i(y, i200, ref = 2017) + i(y, i100, ref = 2017) | clip + st^y, d, cluster = ~cty, lean = TRUE)
r3 <- as.data.table(coeftable(m3), keep.rownames = "term")[, .(term, est = round(Estimate, 4), se = round(`Std. Error`, 4))]
sink(file.path(OUT, "b8_repeat_sales_exposure.txt"), append = TRUE)
cat("\n== State x year FE + income-share x year controls (share AGI >= 200k, share 100-200k, SD units)\n"); print(r3)
sink()
