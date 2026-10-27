# Alternative SALT exposure from property taxes: county share of residential parcels (CoreLogic 1% panel) with a 2016 bill
# above $10,000 (where the cap binds on property tax alone), and the share above $5,000 (binds with typical state income
# tax). Revenue event studies as a1 (school districts by county, county governments) and a3 (district level), exposure in SD
# units; also near x high x exposure in the district border DDD.
suppressPackageStartupMessages({library(data.table); library(duckdb); library(fixest)})
source("code/R/paths.R"); OUT <- file.path(ROOT, "output/analysis/salt_local")
con <- dbConnect(duckdb())
ce <- as.data.table(dbGetQuery(con, sprintf("SELECT lpad(q.fips::VARCHAR, 5, '0') conum, count(*) n, avg((p.bill > 10000)::INT) sh10, avg((p.bill > 5000)::INT) sh5
  FROM '%s/data/salt_cap/panel/panel.parquet' p JOIN '%s/data/salt_cap/panel/parcels.parquet' q USING (clip) WHERE p.y = 2016 AND p.bill > 0 GROUP BY 1", ROOT, ROOT)))
dbDisconnect(con, shutdown = TRUE)
ce <- ce[n >= 30][, `:=`(sh10_z = (sh10 - mean(sh10)) / sd(sh10), sh5_z = (sh5 - mean(sh5)) / sd(sh5))]
ex <- fread(file.path(ROOT, "data/local_juris/salt_exposure_county2016.csv"))[, conum := sprintf("%05d", county)]
ce <- merge(ce, ex[, .(conum, ex_z, returns)], by = "conum")
cat("counties:", nrow(ce), "| mean share > 10k:", round(weighted.mean(ce$sh10, ce$n), 4), "| > 5k:", round(weighted.mean(ce$sh5, ce$n), 3),
    "| corr(sh10_z, ex_z):", round(cor(ce$sh10_z, ce$ex_z), 3), "\n")
p <- readRDS(file.path(ROOT, "data/local_juris/district_finance.rds"))[ptax > 0 & enr > 0][, bal := uniqueN(y) == 12, by = leaid][bal == TRUE]
p <- merge(p, ce, by = "conum")[, `:=`(st = substr(conum, 1, 2), lpt = log(ptax), tt = y - 2017, post = as.integer(y >= 2019))]
out <- rbindlist(lapply(c("sh10_z", "sh5_z"), \(v) rbindlist(lapply(c(FALSE, TRUE), \(wt) {
  m <- feols(as.formula(sprintf("lpt ~ i(y, %s, ref = 2017) | leaid + st^y", v)), p, cluster = ~conum, weights = if (wt) ~enr else NULL)
  m2 <- feols(as.formula(sprintf("lpt ~ %s:tt + %s:post + %s:post:tt | leaid + st^y", v, v, v)), p, cluster = ~conum, weights = if (wt) ~enr else NULL)
  rbind(as.data.table(coeftable(m), keep.rownames = "term"), as.data.table(coeftable(m2), keep.rownames = "term"))[, `:=`(exposure = v, weighted = wt)] }))))
fwrite(out, file.path(OUT, "a6_alt_exposure.csv"))
sink(file.path(OUT, "a6_alt_exposure.txt"))
cat("District log property tax (F-33, balanced) on property-tax-based exposure (SD units), district + state x year FE, SE by county\n")
print(as.data.frame(out[, .(exposure, weighted, term, est = round(Estimate, 4), se = round(`Std. Error`, 4))]), row.names = FALSE)
sink(); cat(readLines(file.path(OUT, "a6_alt_exposure.txt")), sep = "\n")
