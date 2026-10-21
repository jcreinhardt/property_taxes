# Coverage of school and neighbourhood covariates in the border-instrument tracts (iv_tract_side, T = 30), overall and
# within 50 mi of the border by side and near/far. Spending per pupil: F-33 FY2016 current spending / enrolment of the
# tract's assigned district (leaid = unified, else elementary, at the tract's mean sales location).
suppressPackageStartupMessages({library(data.table)})
ROOT <- "/Users/jr2728/PhD/projects/property_taxes"; OUT <- file.path(ROOT, "output/analysis/salt_local")
d <- readRDS(file.path(ROOT, "data/local_juris/iv_tract_side.rds"))[T == 30]
f <- readRDS(file.path(ROOT, "data/local_juris/district_finance.rds"))[y == 2016 & enr > 0, .(leaid, spend_pp = spend / enr, ptax_pp = ptax / enr, enr)]
d <- merge(d, f, by = "leaid", all.x = TRUE)
d[ptax_pp == 0, ptax_pp := NA]   # dependent systems: no district levy
v <- c(leaid = "school district assigned", ach = "test scores (SEDA achievement)", ses = "district SES (SEDA)", perfrl = "% free/reduced lunch (SEDA)",
       spend_pp = "current spending per pupil (F-33)", ptax_pp = "district property tax per pupil (F-33, > 0)",
       linc = "median HH income (ACS)", lhval = "median home value (ACS)", sh_ba = "share BA+ (ACS)", sh_white = "share white (ACS)",
       sh_own = "owner share (ACS)", yrbuilt = "median year built (ACS)", med = "median effective tax rate (CoreLogic)")
cv <- function(x) as.list(sapply(names(v), \(k) round(100 * mean(!is.na(x[[k]]) & is.finite(as.numeric(if (k == "leaid") 1 else x[[k]])) & (k != "leaid" | !is.na(x$leaid))), 1)))
r <- rbind(data.table(group = "all tracts", n = nrow(d), as.data.table(cv(d))),
           d[dist_mi <= 50, c(.(n = .N), cv(.SD)), by = .(group = paste0(ifelse(high == 1, "high", "low"), " side, ", ifelse(near == 1, "< 10 mi", "10-50 mi")))][order(group)])
out <- data.table(variable = v, t(r[, -(1:2)]))
setnames(out, c("variable", paste0(r$group, " (n=", r$n, ")")))
sink(file.path(OUT, "c12_border_covariate_coverage.txt"), split = TRUE)
cat("% of tracts with a non-missing value\n"); print(out)
sink()
