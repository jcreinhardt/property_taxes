# County SALT-cap exposure from the 2016 IRS SOI county x AGI-stub file (split out of salt_local/a1_revenue_exposure.R,
# 2026-10-27). SALT deduction above $10k per return, approximated stub by stub as max(0, average SALT per SALT-itemizer - 10k)
# x SALT itemizers, summed over stubs and divided by all returns ("excess_pp", $k per return); also the share of returns in
# stubs whose average SALT exceeds $10k ("sh_capped"), SALT per return, itemizer share, and ex_z = excess_pp in SD units.
# Output: data/built/exposure/salt_exposure_county2016.csv (county, returns, excess_pp, sh_capped, salt_pp, sh_item, ex_z).
suppressPackageStartupMessages({library(data.table); library(duckdb)})
source("code/R/paths.R")
con <- db_open("database.db")
r <- as.data.table(dbGetQuery(con, "SELECT county::INT county, statefips, agi_stub, returns, taxes_paid_returns n_salt, taxes_paid_amount a_salt,
  property_taxes_amount a_ptax, returns_w_items n_item FROM returns_county_agi WHERE year = 2016")); dbDisconnect(con, shutdown = TRUE)
ex <- r[, .(returns = sum(returns), excess_pp = sum(pmax(0, a_salt / pmax(n_salt, 1) - 10) * n_salt) / sum(returns),
            sh_capped = sum(returns[a_salt / pmax(n_salt, 1) > 10]) / sum(returns), salt_pp = sum(a_salt) / sum(returns),
            sh_item = sum(n_item) / sum(returns)), by = county]
ex[, ex_z := (excess_pp - mean(excess_pp)) / sd(excess_pp)]
fwrite(ex, file.path(DATA, "built/exposure/salt_exposure_county2016.csv"))
cat("counties:", nrow(ex), "| SD of excess_pp:", round(sd(ex$excess_pp), 3), "$k | mean:", round(mean(ex$excess_pp), 3), "\n")
