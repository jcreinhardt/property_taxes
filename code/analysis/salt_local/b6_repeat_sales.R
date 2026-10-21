# Repeat-sales version of the border capitalisation test (parcels with >= 2 arm's-length sales 2012-2024, no new construction;
# Bouchet r_repeat_sales.sql). log price ~ near x high x year + near x high x ZIP exposure x year + near x low x exposure x year
# + exposure x year | parcel + pair^side^year + pair^distance-bin^year; SE by county. Parcel FE make this a repeat-sales index,
# so changes in the composition of sold homes cannot drive it. Random 40% of parcels (by clip) to fit in memory.
suppressPackageStartupMessages({library(data.table); library(duckdb); library(fixest); library(ggplot2)})
ROOT <- "/Users/jr2728/PhD/projects/property_taxes"; OUT <- file.path(ROOT, "output/analysis/salt_local"); SH <- as.numeric(Sys.getenv("RS_SHARE", "0.4")); OFF <- as.numeric(Sys.getenv("RS_OFF", "0")); RSX <- Sys.getenv("RS_SFX", "")
con <- dbConnect(duckdb())
s <- as.data.table(dbGetQuery(con, sprintf("SELECT clip, tract, y, ln(price) lp FROM '%s/data/local_juris/kpanel/repeat_sales.parquet'
  WHERE NOT newcon AND hash(clip) %% 1000 >= %d AND hash(clip) %% 1000 < %d", ROOT, round(OFF * 1000), round((OFF + SH) * 1000))))
dbDisconnect(con, shutdown = TRUE)
s <- s[, if (.N >= 2) .SD, by = clip]
iv <- unique(readRDS(file.path(ROOT, "data/local_juris/iv_tract_side.rds"))[, .(tract, pair, high, dbin, near, cty)])
tz <- fread(file.path(ROOT, "data/local_juris/salt_exposure_tract_zip2016.csv"), colClasses = c(tract = "character"))[, .(tract, exz_z)]
d <- merge(merge(s, iv, by = "tract"), tz, by = "tract")[, `:=`(nh = near * high, nhx = near * high * exz_z, nlx = near * (1 - high) * exz_z)]
cat("sales:", nrow(d), "| parcels:", uniqueN(d$clip), "| near-high parcels:", uniqueN(d[nh == 1, clip]), "\n")
m1 <- feols(lp ~ i(y, nh, ref = 2017) | clip + pair^high^y + pair^dbin^y, d, cluster = ~cty)
m2 <- feols(lp ~ i(y, nh, ref = 2017) + i(y, nhx, ref = 2017) + i(y, nlx, ref = 2017) + i(y, exz_z, ref = 2017) | clip + pair^high^y + pair^dbin^y, d, cluster = ~cty)
tid <- function(m, pat) as.data.table(coeftable(m), keep.rownames = "term")[grepl(pat, term), .(y = as.integer(sub("y::(\\d+):.*", "\\1", term)),
  which = sub("y::\\d+:", "", term), coef = Estimate, se = `Std. Error`)]
r <- rbind(tid(m1, ":nh$")[, which := "near x high (no exposure terms)"], tid(m2, ":nhx|:nlx"))
r <- rbind(r, unique(r[, .(which)])[, `:=`(y = 2017L, coef = 0, se = 0)])
fwrite(r, file.path(OUT, paste0("b6_repeat_sales", RSX, ".csv")))
sink(file.path(OUT, paste0("b6_repeat_sales", RSX, ".txt")))
cat("Repeat-sales border test (parcel FE), share of parcels", SH, "; sales", nrow(d), "parcels", uniqueN(d$clip), "\n")
print(as.data.frame(dcast(r, which ~ y, value.var = "coef")[, lapply(.SD, \(v) if (is.numeric(v)) round(v, 4) else v)]), row.names = FALSE)
cat("SE:\n"); print(as.data.frame(dcast(r, which ~ y, value.var = "se")[, lapply(.SD, \(v) if (is.numeric(v)) round(v, 4) else v)]), row.names = FALSE)
sink(); cat(readLines(file.path(OUT, paste0("b6_repeat_sales", RSX, ".txt"))), sep = "\n")
g <- ggplot(r, aes(y, coef, colour = which)) + geom_hline(yintercept = 0, colour = "grey60") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey60") +
  geom_pointrange(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se), position = position_dodge(.5), size = .15) +
  scale_x_continuous(breaks = seq(2012, 2024, 2)) + labs(x = NULL, y = "log price (repeat sales), vs 2017", colour = NULL,
  title = "Repeat-sales prices near state borders", subtitle = "parcel + pair x side x year + pair x distance x year FE; exposure terms per SD of ZIP SALT exposure; 95% CI by county") +
  theme_minimal(base_size = 9) + theme(legend.position = "bottom")
ggsave(file.path(OUT, paste0("figures/b6_repeat_sales", RSX, ".png")), g, width = 9, height = 4.8, dpi = 150)
# Pooled: post (2018+) and late (2021+) interactions
d[, `:=`(post = as.integer(y >= 2018), late = as.integer(y >= 2021))]
mp <- feols(lp ~ nh:post + nhx:post + nlx:post + nhx:late + nlx:late + i(y, exz_z, ref = 2017) | clip + pair^high^y + pair^dbin^y, d, cluster = ~cty)
sink(file.path(OUT, paste0("b6_repeat_sales", RSX, ".txt")), append = TRUE)
cat("\nPooled (post = 2018+, late = additional 2021+):\n")
print(as.data.table(coeftable(mp), keep.rownames = "term")[grepl("post|late", term), .(term, est = round(Estimate, 4), se = round(`Std. Error`, 4))])
sink()
