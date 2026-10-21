# Randomisation inference for the district border DDD (a3): permute which side of each pair is "high" (coin flip per pair),
# re-estimate lpt ~ nh x post | district + pair^high^y + pair^dbin^y, 200 draws; p-value = share of |placebo| >= |actual|.
suppressPackageStartupMessages({library(data.table); library(fixest)})
ROOT <- "/Users/jr2728/PhD/projects/property_taxes"; OUT <- file.path(ROOT, "output/analysis/salt_local")
p <- readRDS(file.path(ROOT, "data/local_juris/district_finance.rds"))[ptax > 0][, bal := uniqueN(y) == 12, by = leaid][bal == TRUE]
ivt <- unique(readRDS(file.path(ROOT, "data/local_juris/iv_tract_side.rds"))[, .(tract, leaid, pair, high, dbin, near)])
dv <- ivt[!is.na(leaid), .(near = as.integer(mean(near) > .5), high = as.integer(names(which.max(table(high)))), pair = names(which.max(table(pair))),
                            dbin = names(which.max(table(dbin)))), by = leaid]
d <- merge(p, dv, by = "leaid")[, `:=`(lpt = log(ptax), post = as.integer(y >= 2018), conum = conum)]
est <- function(h) { x <- copy(d)[, high := h][, nh := near * high]; coef(feols(lpt ~ nh:post | leaid + pair^high^y + pair^dbin^y, x, notes = FALSE))[1] }
a <- est(d$high); set.seed(42); prs <- unique(d$pair)
draws <- sapply(1:200, \(i) { fl <- setNames(sample(0:1, length(prs), TRUE), prs); est(ifelse(fl[d$pair] == 1, 1L - d$high, d$high)) })
sink(file.path(OUT, "a7_district_ri.txt"))
cat("District border DDD pooled near x high x post: actual", round(a, 4), "| RI p-value (200 side flips):", mean(abs(draws) >= abs(a)),
    "| placebo sd:", round(sd(draws), 4), "| placebo quantiles 2.5/50/97.5:", round(quantile(draws, c(.025, .5, .975)), 4), "\n")
sink(); cat(readLines(file.path(OUT, "a7_district_ri.txt")), sep = "\n")
