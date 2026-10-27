# Outcome profiles across state borders, extending inland: mean of each variable by signed distance to the tract's border
# (- = low-tax side, + = high-tax side), 5-mi bins out to 100 mi, after removing border-pair means (pair FE only, so the
# level gap between sides stays visible). Bin means are regression coefficients from Y ~ i(bin) | pair (SE by county),
# re-centred so the tract-weighted mean is 0; lines = separate linear fits per side within 0-100 mi.
suppressPackageStartupMessages({library(data.table); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- file.path(ROOT, "output/analysis/salt_local"); BW <- 5; MX <- 100
d <- readRDS(file.path(ROOT, "data/local_juris/iv_tract_side.rds"))[T == 30]
f <- readRDS(file.path(ROOT, "data/local_juris/district_finance.rds"))[y == 2016 & enr > 0, .(leaid, lspend = log(spend / enr))]
d <- merge(d, f, by = "leaid", all.x = TRUE)
d[, sd_mi := ifelse(high == 1, dist_mi, -dist_mi)]
d <- d[abs(sd_mi) <= MX][, bin := factor((floor(sd_mi / BW) + .5) * BW)]
v <- c(med = "effective tax rate (%)", ach = "district test scores (SEDA, SD)", ses = "district SES (SEDA)", perfrl = "district share free/reduced lunch",
       lspend = "district log spending per pupil", linc = "tract log median HH income", lhval = "tract log median home value", sh_ba = "tract share BA+",
       sh_white = "tract share white", sh_own = "tract owner share", yrbuilt = "tract median year built", lpop = "tract log population")
res <- rbindlist(lapply(names(v), \(k) {
  m <- feols(as.formula(sprintf("%s ~ 0 + i(bin) | pair", k)), d, cluster = ~cty)
  ct <- as.data.table(coeftable(m), keep.rownames = "term")[, .(x = as.numeric(sub("bin::", "", term)), b = Estimate, se = `Std. Error`)]
  ct <- merge(ct, d[!is.na(get(k)), .N, by = .(x = as.numeric(as.character(bin)))], by = "x")
  ct[, b := b - weighted.mean(b, N)][, `:=`(var = k, label = v[k])] }))
res[, label := factor(label, v)][, side := ifelse(x > 0, "high-tax side", "low-tax side")]
fwrite(res, file.path(OUT, "c14_side_profiles.csv"))
g <- ggplot(res, aes(x, b, colour = side)) + geom_hline(yintercept = 0, colour = "grey75") + geom_vline(xintercept = 0) +
  annotate("rect", xmin = -10, xmax = 10, ymin = -Inf, ymax = Inf, alpha = .07, fill = "grey20") +
  geom_pointrange(aes(ymin = b - 1.96 * se, ymax = b + 1.96 * se), size = .12, linewidth = .3) +
  geom_smooth(aes(weight = N), method = "lm", formula = y ~ x, se = FALSE, linewidth = .5) +
  facet_wrap(~ label, scales = "free_y", ncol = 3) + scale_colour_manual(NULL, values = c("high-tax side" = "#c0392b", "low-tax side" = "#2a64a8")) +
  scale_x_continuous(breaks = seq(-MX, MX, 25)) +
  labs(x = "signed distance to the state border (miles): low-tax side < 0 < high-tax side", y = "mean, net of border-pair means",
       title = "Tract and school-district characteristics across state borders, out to 100 miles inland",
       subtitle = sprintf("%d-mile bins, pair fixed effects, 95%% CI (SE by county); lines: linear fit per side; shaded: within 10 mi", BW)) +
  theme_minimal(base_size = 8) + theme(legend.position = "bottom")
ggsave(file.path(OUT, "figures/c14_side_profiles.png"), g, width = 11, height = 10, dpi = 150)
# summary: near-border (< 10 mi) vs inland (50-100 mi) mean on each side, SD units
sds <- sapply(names(v), \(k) sd(d[[k]], na.rm = TRUE))
s <- res[, .(near = weighted.mean(b[abs(x) < 10], N[abs(x) < 10]), inland = weighted.mean(b[abs(x) > 50], N[abs(x) > 50])), by = .(var, side)]
s[, `:=`(near = near / sds[var], inland = inland / sds[var])]
sink(file.path(OUT, "c14_side_profiles.txt"), split = TRUE)
cat("Bin means net of pair means, in SD units: near (< 10 mi) and inland (50-100 mi), by side\n")
print(dcast(s, var ~ side, value.var = c("near", "inland"))[match(names(v), var)][, lapply(.SD, \(x) if (is.numeric(x)) round(x, 2) else x)])
sink()
