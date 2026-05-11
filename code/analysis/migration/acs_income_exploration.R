gc()
rm(list = ls())

library(tidyverse)
library(duckdb)
library(ggplot2)
library(glmnet)
library(Matrix)
library(ranger)
library(binsreg)
library(fixest)
library(broom)
library(stringr)
library(doParallel)

n_cores <- 10
options(ranger.num.threads = n_cores)
registerDoParallel(cores = n_cores)

con <- dbConnect(duckdb::duckdb(), dbdir = "../../../data/database.db", read_only = TRUE)

period_colors <- c(
    "2012-2014" = "#23373b",
    "2015-2017" = "#4a7c82",
    "2018-2020" = "#c06028",
    "2021-2023" = "#eb811b"
)

########################################################
# 1. Sample counts
########################################################

# Income variables:
#   hhincome : household-level (repeats on every person row; restrict to pernum = 1)
#   inctot, ftotinc, incwage : person-level
#
# Coverage (verified in section 2): all variables have full coverage within
# their respective universes — apparent missingness is entirely due to out-of-
# universe codes (children under 15/16, group quarters residents), not true NAs.

sample_counts <- dbGetQuery(con, "
    SELECT
        year,
        COUNT(*)                                                    AS n_persons,
        COUNT(DISTINCT serial)                                      AS n_households
    FROM acs_microdata
    GROUP BY year
    ORDER BY year
")

print(sample_counts)

# Primary keys:
#   Person:    year + serial + pernum  (unique; verified below)
#   Household: year + serial
# Note: works because this extract contains only single-year ACS samples.
#   If multi-year samples are added, 'sample' must be included in the key.
pk_check <- dbGetQuery(con, "
    SELECT COUNT(*) AS n_rows,
           COUNT(DISTINCT year || '_' || serial || '_' || pernum) AS n_unique
    FROM acs_microdata
")
stopifnot(pk_check$n_rows == pk_check$n_unique)

# Sample restriction used throughout:
#   gq IN (1,2,5) : housing unit residents only
#                   1 = households (1970 def), 2 = additional HHs (1990 def),
#                   5 = additional HHs (2000 def); excludes GQ institutions (3,4)
#   age >= 15     : income universe (ACS does not ask income of under-15s)
#   pernum = 1    : one row per household (for household-level variables)
hh_sample_dummy <- "pernum = 1 AND age >= 15 AND gq IN (1, 2, 5)"

########################################################
# 2. Coverage
########################################################

# Variable levels:
#   hhincome : HOUSEHOLD-level — same value on every person row; use pernum = 1
#   inctot   : person-level
#   ftotinc  : person-level
#   incwage  : person-level
#
# N/A codes:
#   inctot / hhincome / ftotinc : 9999999 (7 nines)
#   incwage                     : 999999  (6 nines) -- different!
#
# Income universe: persons age 15+, housing units (gq IN (1,2,5))

coverage <- dbGetQuery(con, "
    SELECT
        year,

        -- denominators
        SUM(CASE WHEN age >= 15 AND gq IN (1,2,5) THEN 1 ELSE 0 END)      AS n_persons_universe,
        SUM(CASE WHEN pernum = 1 AND gq IN (1,2,5) THEN 1 ELSE 0 END)     AS n_households_universe,
        -- incwage universe: age 16+ in housing units (empstat = 0 flags under-16s)
        SUM(CASE WHEN age >= 16 AND gq IN (1,2,5) THEN 1 ELSE 0 END)      AS n_incwage_universe,

        -- valid counts (raw, unweighted)
        SUM(CASE WHEN age >= 15 AND gq IN (1,2,5)
                  AND inctot != 9999999 THEN 1 ELSE 0 END)               AS n_inctot,
        SUM(CASE WHEN pernum = 1 AND gq IN (1,2,5)
                  AND hhincome != 9999999 THEN 1 ELSE 0 END)             AS n_hhincome,
        SUM(CASE WHEN age >= 15 AND gq IN (1,2,5)
                  AND ftotinc != 9999999 THEN 1 ELSE 0 END)              AS n_ftotinc,
        SUM(CASE WHEN age >= 16 AND gq IN (1,2,5)
                  AND incwage != 999999 THEN 1 ELSE 0 END)               AS n_incwage
    FROM acs_microdata
    GROUP BY year
    ORDER BY year
")

coverage <- coverage |>
    mutate(
        pct_inctot   = n_inctot   / n_persons_universe,
        pct_hhincome = n_hhincome / n_households_universe,
        pct_ftotinc  = n_ftotinc  / n_persons_universe,
        pct_incwage  = n_incwage  / n_incwage_universe
    )

print(coverage |> select(year,
    n_persons_universe, n_inctot,   pct_inctot,
    n_households_universe, n_hhincome, pct_hhincome,
    n_ftotinc, pct_ftotinc,
    n_incwage_universe, n_incwage, pct_incwage
))


########################################################
# 3. Migration variables
########################################################

# All migration variables are person-level, universe age 1+.
# N/A codes (out-of-universe = same house or non-mover):
#   migrate1   : 0
#   migrate1d  : 0
#   migplac1   : 000
#   migcounty1 : 000  -- also 000 when county not identifiable; use migrate1 to distinguish
#   migpuma1   : 00000
#   migmet131  : 00000
#
# migrate1 values: 1=same house, 2=moved within state, 3=moved between states, 4=abroad
# migcounty1 is state-dependent: must combine with migplac1 to identify county

# 3a. Distribution of migrate1 by year
migration_dist <- dbGetQuery(con, "
    SELECT
        year,
        SUM(CASE WHEN migrate1 = 1 THEN 1 ELSE 0 END)  AS same_house,
        SUM(CASE WHEN migrate1 = 2 THEN 1 ELSE 0 END)  AS moved_within_state,
        SUM(CASE WHEN migrate1 = 3 THEN 1 ELSE 0 END)  AS moved_between_states,
        SUM(CASE WHEN migrate1 = 4 THEN 1 ELSE 0 END)  AS abroad,
        SUM(CASE WHEN migrate1 = 0 THEN 1 ELSE 0 END)  AS na_under1,
        COUNT(*)                                        AS n_total
    FROM acs_microdata
    GROUP BY year ORDER BY year
")

print(migration_dist)

# 3b. Coverage of geographic migration variables among movers (migrate1 IN (2,3,4))
migration_coverage <- dbGetQuery(con, "
    SELECT
        year,
        SUM(CASE WHEN migrate1 IN (2,3,4) THEN 1 ELSE 0 END)                        AS n_movers,

        -- migplac1: state/country of prior residence
        SUM(CASE WHEN migrate1 IN (2,3,4) AND migplac1 != 0 THEN 1 ELSE 0 END)      AS n_migplac1,

        -- migcounty1: prior county; 000 = non-mover OR unidentifiable county
        SUM(CASE WHEN migrate1 IN (2,3,4) AND migcounty1 != 0 THEN 1 ELSE 0 END)    AS n_migcounty1,

        -- migpuma1: prior migration PUMA
        SUM(CASE WHEN migrate1 IN (2,3,4) AND migpuma1 != 0 THEN 1 ELSE 0 END)      AS n_migpuma1,

        -- migmet131: prior metro area (2013 delineations)
        SUM(CASE WHEN migrate1 IN (2,3,4) AND migmet131 != 0 THEN 1 ELSE 0 END)     AS n_migmet131
    FROM acs_microdata
    GROUP BY year ORDER BY year
")

migration_coverage <- migration_coverage |>
    mutate(
        pct_migplac1   = n_migplac1   / n_movers,
        pct_migcounty1 = n_migcounty1 / n_movers,
        pct_migpuma1   = n_migpuma1   / n_movers,
        pct_migmet131  = n_migmet131  / n_movers
    )

print(migration_coverage)


########################################################
# 4. Share of movers by hhincome decile (2017 vs 2018)
########################################################

mover_by_income <- dbGetQuery(con, "
    WITH heads AS (
        SELECT
            CASE
                WHEN year BETWEEN 2012 AND 2014 THEN '2012-2014'
                WHEN year BETWEEN 2015 AND 2017 THEN '2015-2017'
                WHEN year BETWEEN 2018 AND 2020 THEN '2018-2020'
                WHEN year BETWEEN 2021 AND 2023 THEN '2021-2023'
            END AS period,
            NTILE(10) OVER (PARTITION BY year ORDER BY hhincome) AS decile,
            CASE WHEN migrate1 IN (2,3,4) THEN 1 ELSE 0 END AS mover
        FROM acs_microdata
        WHERE year BETWEEN 2012 AND 2023
          AND pernum = 1 AND gq IN (1,2,5) AND hhincome != 9999999
    )
    SELECT
        period,
        decile,
        AVG(mover)   AS share_movers,
        COUNT(*)     AS n_households
    FROM heads
    GROUP BY period, decile
    ORDER BY period, decile
")

mover_by_income |>
    mutate(period = factor(period, levels = names(period_colors))) |>
    ggplot(aes(x = decile, y = share_movers, color = period, group = period)) +
    geom_line() +
    geom_point() +
    scale_color_manual(name = NULL, values = period_colors) +
    scale_x_continuous(breaks = 1:10) +
    labs(x = "Household income decile (year-specific)", y = "Share of household heads who moved") +
    theme(
        panel.background = element_rect(fill = "#fafafa", color = NA),
        plot.background  = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major = element_line(color = "grey90"),
        panel.grid.minor = element_blank(),
        axis.line.x = element_line(color = "grey30"),
        axis.line.y = element_line(color = "grey30"),
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.key = element_rect(fill = "#fafafa", color = NA),
        axis.text.x = element_text(color = "grey20"),
        axis.text.y = element_text(color = "grey20"),
        text = element_text(color = "grey20")
    )

########################################################
# 5. Share of movers by SALT decile (2017 vs 2018)
########################################################

# imp_salt is only populated for household heads in housing units (gq IN (1,2)).
# Excludes households with top-coded property taxes or 2024 (imp_salt IS NULL).

mover_by_salt <- dbGetQuery(con, "
    WITH heads AS (
        SELECT
            CASE
                WHEN year BETWEEN 2012 AND 2014 THEN '2012-2014'
                WHEN year BETWEEN 2015 AND 2017 THEN '2015-2017'
                WHEN year BETWEEN 2018 AND 2020 THEN '2018-2020'
                WHEN year BETWEEN 2021 AND 2023 THEN '2021-2023'
            END AS period,
            NTILE(10) OVER (PARTITION BY year ORDER BY imp_salt) AS decile,
            CASE WHEN migrate1 IN (2,3,4) THEN 1 ELSE 0 END AS mover
        FROM acs_microdata
        WHERE year BETWEEN 2012 AND 2023
          AND pernum = 1 AND gq IN (1, 2, 5)
          AND imp_salt IS NOT NULL
    )
    SELECT
        period,
        decile,
        AVG(mover)   AS share_movers,
        COUNT(*)     AS n_households
    FROM heads
    GROUP BY period, decile
    ORDER BY period, decile
")

mover_by_salt |>
    mutate(period = factor(period, levels = names(period_colors))) |>
    ggplot(aes(x = decile, y = share_movers, color = period, group = period)) +
    geom_line() +
    geom_point() +
    scale_color_manual(name = NULL, values = period_colors) +
    scale_x_continuous(breaks = 1:10) +
    labs(x = "SALT decile (year-specific)", y = "Share of household heads who moved") +
    theme(
        panel.background = element_rect(fill = "#fafafa", color = NA),
        plot.background  = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major = element_line(color = "grey90"),
        panel.grid.minor = element_blank(),
        axis.line.x = element_line(color = "grey30"),
        axis.line.y = element_line(color = "grey30"),
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.key = element_rect(fill = "#fafafa", color = NA),
        axis.text.x = element_text(color = "grey20"),
        axis.text.y = element_text(color = "grey20"),
        text = element_text(color = "grey20")
    )

########################################################
# 6. LASSO: biggest predictors of moving (household heads)
########################################################

heads_lasso <- dbGetQuery(con, "
    SELECT
        CASE WHEN migrate1 IN (2,3,4) THEN 1 ELSE 0 END AS mover,
        imp_proptx, imp_stateinctax, imp_totalwages,
        age, sex, marst, race, hispan,
        educ, empstat, uhrswork, wkswork1,
        ownershp, valueh, hhincome,
        metro, statefip, year, multgen,
        -- life events / family
        nchlt5, marrno, marrinyr, divinyr, widinyr, fertyr, gchouse,
        -- nativity / language
        citizen, speakeng,
        CASE WHEN bpl >= 100 THEN 1 ELSE 0 END AS foreign_born,
        yrsusa1,
        -- employment detail
        occ2010, ind, labforce, looking, occscore,
        CASE WHEN pwstate2 > 0 AND pwstate2 != statefip THEN 1 ELSE 0 END AS cross_state_commuter,
        -- housing detail
        rooms, bedrooms, builtyr2, unitsstr, mortgage,
        CAST(rent    AS DOUBLE) AS rent,
        CAST(owncost AS DOUBLE) AS owncost,
        -- education detail
        degfield, gradeatt,
        -- geography
        region, density
    FROM acs_microdata
    WHERE pernum = 1 AND gq IN (1, 2, 5) AND migrate1 != 0
      AND imp_salt IS NOT NULL AND hhincome != 9999999
      AND year BETWEEN 2013 AND 2023
      AND random() < .1
") |>
    as_tibble() |>
    mutate(
        valueh = if_else(valueh == 9999999L | ownershp != 1L, 0, as.numeric(valueh)),
        owncost = if_else(owncost >= 99999, 0, owncost),
        across(c(age, imp_proptx, imp_stateinctax, imp_totalwages,
                 hhincome, valueh, uhrswork, wkswork1,
                 nchlt5, marrno, yrsusa1, occscore,
                 rooms, bedrooms, rent, owncost, density),
               ~ as.numeric(scale(as.numeric(.))))
    ) |>
    na.omit()

# Build sparse model matrix in parallel: split predictor groups across cores,
# build each block independently, then cbind. sparse.model.matrix is
# single-threaded so this is the only way to exploit multiple cores here.
factor_blocks <- list(
    ~ 0 + factor(sex) + factor(marst) + factor(race) + factor(hispan) +
          factor(educ) + factor(empstat) + factor(ownershp) +
          factor(metro) + factor(multgen) + factor(region),
    ~ 0 + factor(marrinyr) + factor(divinyr) + factor(widinyr) +
          factor(fertyr) + factor(gchouse) +
          factor(citizen) + factor(speakeng) +
          factor(labforce) + factor(looking),
    ~ 0 + factor(occ2010),
    ~ 0 + factor(ind),
    ~ 0 + factor(builtyr2) + factor(unitsstr) + factor(mortgage) +
          factor(degfield) + factor(gradeatt),
    ~ 0 + factor(statefip) + factor(year)
)

cont_vars <- c(
    "age", "imp_proptx", "imp_stateinctax", "imp_totalwages",
    "hhincome", "valueh", "uhrswork", "wkswork1",
    "nchlt5", "marrno", "yrsusa1", "occscore",
    "rooms", "bedrooms", "rent", "owncost", "density",
    "foreign_born", "cross_state_commuter"
)

X_cont <- as(as.matrix(heads_lasso[, cont_vars]), "sparseMatrix")

X_blocks <- parallel::mclapply(
    factor_blocks,
    function(f) sparse.model.matrix(f, data = heads_lasso),
    mc.cores = min(length(factor_blocks), 10)
)

X <- do.call(cbind, c(list(X_cont), X_blocks))

y <- heads_lasso$mover

message(sprintf("LASSO matrix: %d obs × %d predictors", nrow(X), ncol(X)))

set.seed(42)
cv_fit <- cv.glmnet(X, y, family = "gaussian", alpha = 1, nfolds = 5,
                    parallel = TRUE)

cat(sprintf("\nOptimal lambda: %.6f (lambda.1se: %.6f)\n",
            cv_fit$lambda.min, cv_fit$lambda.1se))

# Non-zero coefficients at the more parsimonious lambda.1se
coefs <- coef(cv_fit, s = "lambda.1se") |>
    as.matrix() |>
    as.data.frame() |>
    setNames("coefficient") |>
    tibble::rownames_to_column("variable") |>
    filter(variable != "(Intercept)", coefficient != 0) |>
    arrange(desc(abs(coefficient)))

cat(sprintf("\n%d non-zero coefficients (lambda.1se):\n", nrow(coefs)))
print(coefs)

# Collapse factor dummies: range from lowest to highest level (including ref = 0)
coefs_summary <- coefs |>
    mutate(
        base_var  = str_replace(variable, "^factor\\(([^)]+)\\).*", "\\1"),
        is_factor = str_detect(variable, "^factor\\(")
    ) |>
    group_by(base_var) |>
    summarise(
        is_factor = any(is_factor),
        range     = max(c(0, coefficient)) - min(c(0, coefficient)),
        .groups   = "drop"
    ) |>
    arrange(desc(range))

# Variance decomposition: for each predictor group, compute its component
# of the fitted values and measure its variance as a share of Var(ŷ).
# Groups won't sum to 100% due to inter-group covariances.
selected_vars <- coefs$variable
beta_selected <- setNames(coefs$coefficient, coefs$variable)
y_hat         <- as.numeric(X[, selected_vars, drop = FALSE] %*% beta_selected)

var_decomp <- coefs |>
    mutate(base_var = str_replace(variable, "^factor\\(([^)]+)\\).*", "\\1")) |>
    group_by(base_var) |>
    group_modify(function(df, key) {
        component <- as.numeric(X[, df$variable, drop = FALSE] %*% df$coefficient)
        tibble(var_component = var(component))
    }) |>
    ungroup() |>
    mutate(
        pct_var_yhat = round(100 * var_component / var(y_hat), 1),
        pct_var_y    = round(100 * var_component / var(y),     1)
    )

predictor_summary <- coefs_summary |>
    left_join(var_decomp |> select(base_var, pct_var_yhat, pct_var_y), by = "base_var") |>
    arrange(desc(pct_var_yhat))

cat("\nPredictor summary (coefficient range + variance decomposition):\n")
print(predictor_summary, n = Inf)

predictor_summary |>
    slice_max(range, n = 30) |>
    mutate(base_var = fct_reorder(base_var, range)) |>
    ggplot(aes(x = range, y = base_var, fill = is_factor)) +
    geom_col() +
    scale_fill_manual(
        values = c("TRUE" = "#4a7c82", "FALSE" = "#eb811b"),
        labels = c("TRUE" = "categorical", "FALSE" = "continuous"),
        name   = NULL
    ) +
    labs(x = "Coefficient range (highest − lowest level)", y = NULL,
         title = "Predictor importance (LASSO, household heads 2013-2023)") +
    theme(
        panel.background = element_rect(fill = "#fafafa", color = NA),
        plot.background  = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major = element_line(color = "grey90"),
        panel.grid.minor = element_blank(),
        axis.line.x = element_line(color = "grey30"),
        axis.line.y = element_line(color = "grey30"),
        text = element_text(color = "grey20")
    )

########################################################
# 7. Partial dependence of moving on SALT (random forest)
########################################################

# Train a joint forest on (y, x, z) then integrate out z via the standard
# PDP: for each grid value of x, replace every observation's SALT with that
# value, predict, and average. The result is E_z[f(x, z)] — the marginal
# effect of SALT holding the distribution of controls fixed.

pdp_data <- dbGetQuery(con, "
    SELECT
        CASE WHEN migrate1 IN (2,3,4) THEN 1 ELSE 0 END AS mover,
        CAST(imp_salt  AS DOUBLE) AS imp_salt,
        age,
        sex, marst, race, hispan, educ, empstat, ownershp, metro, multgen,
        CAST(hhincome  AS DOUBLE) AS hhincome,
        CAST(valueh    AS DOUBLE) AS valueh,
        CAST(uhrswork  AS DOUBLE) AS uhrswork,
        year,
        -- life events / family
        nchlt5, marrno, marrinyr, divinyr, widinyr, fertyr, gchouse,
        -- nativity / language
        citizen, speakeng,
        CASE WHEN bpl >= 100 THEN 1 ELSE 0 END AS foreign_born,
        yrsusa1,
        -- employment detail
        occ2010, ind, labforce, looking, occscore,
        CASE WHEN pwstate2 > 0 AND pwstate2 != statefip THEN 1 ELSE 0 END AS cross_state_commuter,
        -- housing detail
        rooms, bedrooms, builtyr2, unitsstr, mortgage,
        CAST(rent    AS DOUBLE) AS rent,
        CAST(owncost AS DOUBLE) AS owncost,
        -- education detail
        degfield, gradeatt
    FROM acs_microdata
    WHERE pernum = 1 AND gq IN (1, 2, 5) AND migrate1 != 0
      AND imp_salt IS NOT NULL AND hhincome != 9999999
      AND year BETWEEN 2012 AND 2023
      AND random() < .1
") |>
    as_tibble() |>
    mutate(
        valueh  = if_else(valueh  == 9999999, 0, valueh),
        owncost = if_else(owncost >= 99999,   0, owncost),
        across(c(sex, marst, race, hispan, educ, empstat, ownershp, multgen,
                 marrinyr, divinyr, widinyr, fertyr, gchouse,
                 citizen, speakeng, occ2010, ind, labforce, looking,
                 builtyr2, unitsstr, mortgage, degfield, gradeatt),
               factor)
    ) |> 
    na.omit()
    
# Train on pre-TCJA period only; year excluded from formula so the forest
# generalises to post-2017 observations at prediction time.
pdp_train <- pdp_data |> filter(year %in% 2012:2017)

rf_pdp <- ranger(
    mover ~ imp_salt + age + sex + marst + race + hispan + educ +
            empstat + ownershp + multgen +
            hhincome + valueh + uhrswork +
            nchlt5 + marrno + yrsusa1 + occscore +
            rooms + bedrooms + rent + owncost +
            foreign_born + cross_state_commuter +
            marrinyr + divinyr + widinyr + fertyr + gchouse +
            citizen + speakeng + labforce + looking +
            occ2010 + ind + builtyr2 + unitsstr + mortgage +
            degfield + gradeatt,
    data          = pdp_train,
    num.trees     = 500,
    min.node.size = 50,
    seed          = 42
)

# PDP grid: 50 quantile-spaced values of imp_salt up to 99th percentile
salt_grid <- quantile(pdp_data$imp_salt, probs = seq(0, 0.99, length.out = 50))

# A subsample of 2000 rows is sufficient for accurate marginal averaging;
# draw once and reuse across all grid points.
set.seed(42)
pdp_sample <- pdp_data[sample(nrow(pdp_data), min(2000L, nrow(pdp_data))), ]

pdp_curve <- map_dbl(salt_grid, function(x_val) {
    newdata          <- pdp_sample
    newdata$imp_salt <- x_val
    mean(predict(rf_pdp, data = newdata)$predictions)
})

pdp_df <- tibble(imp_salt = salt_grid, move_rate = pdp_curve)

ggplot(pdp_df, aes(imp_salt, move_rate)) +
    geom_line(linewidth = 1, color = "#2166ac") +
    scale_x_continuous(labels = scales::dollar_format(scale = 1e-3, suffix = "k")) +
    scale_y_continuous(labels = scales::percent_format(1)) +
    labs(
        x        = "SALT (state and local taxes paid)",
        y        = "Predicted move rate",
        title    = "Partial dependence of moving on SALT",
        subtitle = "E[mover | SALT = x], integrating over marginal distribution of controls"
    ) +
    theme(
        panel.background = element_rect(fill = "#fafafa", color = NA),
        plot.background  = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major = element_line(color = "grey90"),
        panel.grid.minor = element_blank(),
        axis.line.x      = element_line(color = "grey30"),
        axis.line.y      = element_line(color = "grey30"),
        axis.text        = element_text(color = "grey20"),
        text             = element_text(color = "grey20")
    )

# Residual plot by period: predict for every observation using the pre-period
# forest (which learned the SALT-moving relationship from 2012-2017), then
# compute actual - predicted. Binning residuals by SALT and period shows
# whether high-SALT households moved more or less than the pre-period model
# expects — i.e., where the relationship shifted post-TCJA.
# Quantile cut points can repeat when many observations share the same SALT.
# Use unique breaks and gracefully fall back to NA bins when there is no spread.
salt_breaks <- stats::quantile(
    pdp_data$imp_salt,
    probs = seq(0, 1, length.out = 21),
    na.rm = TRUE
) |>
    unique()

pdp_data <- pdp_data |>
    mutate(
        predicted = predict(rf_pdp, data = pdp_data)$predictions,
        resid     = mover - predicted,
        period    = case_when(
            year %in% 2012:2014 ~ "2012-2014",
            year %in% 2015:2017 ~ "2015-2017",
            year %in% 2018:2020 ~ "2018-2020",
            year %in% 2021:2023 ~ "2021-2023"
        ) |> factor(levels = names(period_colors)),
        salt_bin  = if (length(salt_breaks) > 1) {
            cut(
                imp_salt,
                breaks = salt_breaks,
                include.lowest = TRUE,
                labels = FALSE
            )
        } else {
            NA_integer_
        }
    )

resid_by_period <- pdp_data |>
    group_by(period, salt_bin) |>
    summarise(
        mean_resid = mean(resid),
        salt_mid   = mean(imp_salt),
        n          = n(),
        .groups    = "drop"
    )

ggplot(resid_by_period, aes(salt_mid, mean_resid, color = period)) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "grey50") +
    geom_line(linewidth = 0.9) +
    geom_point(size = 1.5) +
    scale_x_continuous(labels = scales::dollar_format(scale = 1e-3, suffix = "k")) +
    scale_y_continuous(labels = scales::percent_format(1)) +
    scale_color_manual(values = period_colors, name = NULL) +
    labs(
        x        = "SALT (state and local taxes paid)",
        y        = "Actual - predicted move rate",
        title    = "Move rate residuals vs. SALT, by period",
        subtitle = "Residuals from forest trained on 2012-2017; zero = matches pre-period expectation"
    ) +
    theme(
        panel.background  = element_rect(fill = "#fafafa", color = NA),
        plot.background   = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major  = element_line(color = "grey90"),
        panel.grid.minor  = element_blank(),
        axis.line.x       = element_line(color = "grey30"),
        axis.line.y       = element_line(color = "grey30"),
        axis.text         = element_text(color = "grey20"),
        text              = element_text(color = "grey20"),
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.position   = "bottom"
    ) + 
    xlim(0, 10000)

########################################################
# 8. Moving rate vs SALT — two-sided DML (Robinson) by period
########################################################

# y = f(x, Z, e), E[e|x,Z] = 0.
# Two-sided residualization (DML): train nuisance RFs for E[Y|Z] and E[X|Z]
# on pre-policy data (2012-2017). Residualize both mover and SALT, then plot
# E[ỹ | x̃, period] by period. Changes in the ỹ-x̃ slope across periods reflect
# changes in the structural SALT→moving effect, purged of demographic selection
# into both moving and SALT levels. OOB predictions used for pre-period to
# avoid overfitting; out-of-sample predict() for post-period.

salt_rf <- dbGetQuery(con, "
    SELECT
        CASE WHEN migrate1 IN (2,3,4) THEN 1 ELSE 0 END AS mover,
        CAST(imp_salt AS DOUBLE) AS imp_salt,
        age, sex, marst, race, hispan,
        educ, metro, statefip, year, multgen
    FROM acs_microdata
    WHERE pernum = 1 AND gq IN (1, 2, 5) AND migrate1 != 0
      AND imp_salt IS NOT NULL
      AND year BETWEEN 2012 AND 2023
      AND random() < 0.1
") |>
    as_tibble() |>
    mutate(
        period = case_when(
            year %in% 2012:2014 ~ "2012-2014",
            year %in% 2015:2017 ~ "2015-2017",
            year %in% 2018:2020 ~ "2018-2020",
            year %in% 2021:2023 ~ "2021-2023"
        ) |> factor(levels = names(period_colors)),
        across(c(sex, marst, race, hispan, educ, metro, multgen, statefip), factor),
        imp_salt = as.numeric(imp_salt)
    ) |>
    na.omit()

pre_idx   <- salt_rf$year %in% 2012:2017
pre_data  <- salt_rf[pre_idx, ]
post_data <- salt_rf[!pre_idx, ]

# Nuisance RF for E[Y|Z]: mover on demographics only (no SALT, no year/period)
rf_y <- ranger(
    mover ~ age + sex + marst + race + hispan + educ + metro + statefip + multgen,
    data = pre_data, num.trees = 200, min.node.size = 50, max.depth = 6, seed = 42
)

# Nuisance RF for E[X|Z]: SALT on demographics only
rf_x <- ranger(
    imp_salt ~ age + sex + marst + race + hispan + educ + metro + statefip + multgen,
    data = pre_data, num.trees = 200, min.node.size = 50, max.depth = 6, seed = 42
)

# Residualize: OOB for pre-period, out-of-sample for post-period
salt_rf$pred_y <- NA_real_
salt_rf$pred_x <- NA_real_
salt_rf$pred_y[pre_idx]  <- rf_y$predictions
salt_rf$pred_y[!pre_idx] <- predict(rf_y, data = post_data)$predictions
salt_rf$pred_x[pre_idx]  <- rf_x$predictions
salt_rf$pred_x[!pre_idx] <- predict(rf_x, data = post_data)$predictions

salt_rf <- salt_rf |>
    mutate(
        resid_y = mover    - pred_y,
        resid_x = imp_salt - pred_x
    )


binsreg(
    y          = salt_rf$resid_y,
    by         = salt_rf$period,
    x          = salt_rf$resid_x,
    nbins      = 20,
    samebinsby = TRUE,
    plotxrange = c(-2000, 7500)
)

########################################################
# 9. Moving rate vs SALT — DML controlling for income and home value
########################################################

# Same two-sided DML as section 7 but adds household income and home value
# to the nuisance controls (Z), purging income/wealth selection from the
# SALT-moving relationship. Valueh is set to 0 for renters and top-coded
# observations (consistent with section 6 treatment).

salt_rf2 <- dbGetQuery(con, "
    SELECT
        CASE WHEN migrate1 IN (2,3,4) THEN 1 ELSE 0 END AS mover,
        CAST(imp_salt AS DOUBLE) AS imp_salt,
        age, sex, marst, race, hispan,
        educ, year, multgen,
        hhincome, valueh, ownershp
    FROM acs_microdata
    WHERE pernum = 1 AND gq IN (1, 2, 5) AND migrate1 != 0
      AND imp_salt IS NOT NULL
      AND hhincome != 9999999
      AND year BETWEEN 2012 AND 2023
      AND random() < 0.1
") |>
    as_tibble() |>
    mutate(
        valueh = if_else(valueh == 9999999L | ownershp != 1L, 0, as.numeric(valueh)),
        period = case_when(
            year %in% 2012:2014 ~ "2012-2014",
            year %in% 2015:2017 ~ "2015-2017",
            year %in% 2018:2020 ~ "2018-2020",
            year %in% 2021:2023 ~ "2021-2023"
        ) |> factor(levels = names(period_colors)),
        across(c(sex, marst, race, hispan, educ, multgen, ownershp), factor),
        imp_salt = as.numeric(imp_salt),
        hhincome = as.numeric(hhincome)
    ) |>
    na.omit()

pre_idx2   <- salt_rf2$year %in% 2012:2017
pre_data2  <- salt_rf2[pre_idx2, ]
post_data2 <- salt_rf2[!pre_idx2, ]

rf_y2 <- ranger(
    mover ~ age + sex + marst + race + hispan + educ + multgen +
             hhincome + valueh + ownershp,
    data = pre_data2, num.trees = 200, min.node.size = 50, max.depth = 6, seed = 42
)

rf_x2 <- ranger(
    imp_salt ~ age + sex + marst + race + hispan + educ + multgen +
                hhincome + valueh + ownershp,
    data = pre_data2, num.trees = 200, min.node.size = 50, max.depth = 6, seed = 42
)

salt_rf2$pred_y <- NA_real_
salt_rf2$pred_x <- NA_real_
salt_rf2$pred_y[pre_idx2]  <- rf_y2$predictions
salt_rf2$pred_y[!pre_idx2] <- predict(rf_y2, data = post_data2)$predictions
salt_rf2$pred_x[pre_idx2]  <- rf_x2$predictions
salt_rf2$pred_x[!pre_idx2] <- predict(rf_x2, data = post_data2)$predictions

salt_rf2 <- salt_rf2 |>
    mutate(
        resid_y = mover    - pred_y,
        resid_x = imp_salt - pred_x
    )

binsreg(
    y          = salt_rf2$resid_y,
    by         = salt_rf2$period,
    x          = salt_rf2$resid_x,
    nbins      = 10,
    samebinsby = TRUE,
    plotxrange = c(-5000, 7500)
)

salt_rf2 |> feols(resid_y ~ i(year,resid_x))

########################################################
# 10. Moving rates by itemization group (TCJA design)
########################################################

# Three groups defined by itemization status under each rule set:
#
#   (a) Always itemized:          itemizes under both 2017 and TCJA rules
#   (b) Stopped itemizing (TCJA): itemizes under 2017 rules but not TCJA rules
#   (c) Never itemized:           does not itemize under either rule set
#
# 2017-law classification: imp_itemizes_2017 (SALT uncapped, std_ded fixed at
#   2017 levels for all years — already stored in DB; MFJ $12,700 / HoH $9,350 /
#   Single $6,350).
# TCJA-law classification: imp_itemizable_2018 (SALT capped $10k, mort int at
#   $750k cap) vs. fixed post-TCJA std_ded. Standard deduction computed from
#   marst and nchild matching the build script (MFJ $24k / HoH $18k / Single $12k)
#   and held fixed for all years so group assignment reflects the policy change,
#   not year-to-year inflation indexing.

item_data <- dbGetQuery(con, "
    SELECT
        year,
        CASE WHEN migrate1 IN (2,3,4) THEN 1 ELSE 0 END AS mover,
        migrate1,
        imp_itemizes_2017,
        CAST(imp_itemizable_2017 AS DOUBLE)              AS imp_itemizable_2017,
        CAST(imp_itemizable_2018 AS DOUBLE)              AS imp_itemizable_2018,
        CAST(imp_salt            AS DOUBLE)              AS imp_salt,
        CASE
            WHEN marst = 1                 THEN 24000
            WHEN marst != 1 AND nchild > 0 THEN 18000
            ELSE                                12000
        END                                              AS tcja_std_ded,
        age, sex, marst, nchild, race, hispan, educ, metro, statefip, multgen
    FROM acs_microdata
    WHERE pernum = 1 AND gq IN (1, 2, 5) AND migrate1 != 0
      AND imp_itemizes_2017    IS NOT NULL
      AND imp_itemizable_2018  IS NOT NULL
      AND year BETWEEN 2012 AND 2023
      AND random() < .1
") |>
    as_tibble() |>
    mutate(
        tcja_itemizes = as.integer(imp_itemizable_2018 > tcja_std_ded),
        group = case_when(
            imp_itemizes_2017 == 1L & tcja_itemizes == 1L ~ "Always itemized",
            imp_itemizes_2017 == 1L & tcja_itemizes == 0L ~ "Stopped itemizing (TCJA)",
            imp_itemizes_2017 == 0L                        ~ "Never itemized"
        ) |> factor(levels = c("Never itemized", "Stopped itemizing (TCJA)", "Always itemized")),
        fstatus = case_when(
            marst == 1L               ~ "MFJ",
            marst != 1L & nchild > 0L ~ "HoH",
            TRUE                       ~ "Single"
        ) |> factor(levels = c("MFJ", "HoH", "Single")),
        across(c(sex, marst, race, hispan, educ, metro, multgen, statefip), factor)
    ) |>
    na.omit()

cat("\nOverall move rate by year:\n")
print(
    item_data |>
        group_by(year) |>
        summarise(move_rate = round(100 * mean(mover), 2), n = n(), .groups = "drop") |>
        as.data.frame()
)

cat("\nGroup composition:\n")
print(
    item_data |>
        count(group) |>
        mutate(pct = round(100 * n / sum(n), 1))
)

# Step 1: random forest to partial out demographics (no statefip).
# OOB predictions avoid in-sample overfitting.
rf_fit <- ranger(
    mover ~ age + sex + marst + race + hispan + educ + metro + multgen,
    data      = item_data |> filter(year %in% 2012:2017),
    num.trees = 500,
    seed      = 42
)

item_data <- item_data |>
    mutate(adj_mover = mover - rf_fit$predictions + mean(mover))

# Step 2: group × year cell means with HC1-robust SEs via a saturated feols.
cell_fit <- feols(
    adj_mover ~ 0 + group:factor(year) | statefip^year,
    data   = item_data,
    vcov   = "HC1"
)

mover_by_group_year <- tidy(cell_fit) |>
    mutate(
        group = case_when(
            grepl("Always itemized",   term) ~ "Always itemized",
            grepl("Stopped itemizing", term) ~ "Stopped itemizing (TCJA)",
            TRUE                             ~ "Never itemized"
        ) |> factor(levels = c("Never itemized", "Stopped itemizing (TCJA)", "Always itemized")),
        year = as.integer(regmatches(term, regexpr("\\d{4}", term)))
    ) |>
    select(group, year, share_movers = estimate, se = std.error)

group_colors <- c(
    "Never itemized"           = "#23373b",
    "Stopped itemizing (TCJA)" = "#c06028",
    "Always itemized"          = "#4a7c82"
)

ggplot(mover_by_group_year,
       aes(year, share_movers, color = group, fill = group, group = group)) +
    geom_vline(xintercept = 2017.5, linetype = "dashed", color = "grey60") +
    geom_ribbon(aes(ymin = share_movers - 1.96 * se,
                    ymax = share_movers + 1.96 * se),
                alpha = 0.12, color = NA) +
    geom_line(linewidth = 0.9) +
    geom_point(size = 2) +
    annotate("text", x = 2017.7, y = Inf, label = "TCJA",
             vjust = 1.5, hjust = 0, color = "grey40", size = 3.5) +
    scale_color_manual(name = NULL, values = group_colors) +
    scale_fill_manual(name  = NULL, values = group_colors) +
    scale_x_continuous(breaks = 2012:2023) +
    scale_y_continuous(labels = scales::percent_format(1)) +
    labs(
        x     = NULL,
        y     = "Share of household heads who moved",
        title = "Moving rates by itemization group"
    ) +
    theme(
        panel.background  = element_rect(fill = "#fafafa", color = NA),
        plot.background   = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major  = element_line(color = "grey90"),
        panel.grid.minor  = element_blank(),
        axis.line.x       = element_line(color = "grey30"),
        axis.line.y       = element_line(color = "grey30"),
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.key        = element_rect(fill = "#fafafa", color = NA),
        axis.text.x       = element_text(color = "grey20", angle = 45, hjust = 1),
        axis.text.y       = element_text(color = "grey20"),
        text              = element_text(color = "grey20")
    )
ggsave("../../../output/Cormac/10a_moving_rates_by_group.png", last_plot(), width = 8, height = 5, dpi = 150)

########################################################
# 10b. Event study: moving rates by itemization group (TWFE)
########################################################

# Two separate TWFE event studies vs. "Never itemized" (untreated control):
#   - "Always itemized"          (treat_always)
#   - "Stopped itemizing (TCJA)" / compliers (treat_stopped)
# Reference year = 2017. Controls: age, sex, marst, race, hispan, educ,
# metro, multgen. FE: year + statefip. Clustered SEs by state.
# item_data already in memory from Section 10 (10% sample, 2012–2023).

run_group_es <- function(treat_label, fs, data) {
    # Restrict never-itemizers to the 10% closest to the 2017 itemization
    # threshold within the same filing status: gap = std_ded_2017 - imp_itemizable_2017.
    # Computed separately per fstatus so MFJ, HoH, and Single each contribute
    # a near-threshold control group appropriate to their own standard deduction.
    std_ded_2017 <- c(MFJ = 12700, HoH = 9350, Single = 6350)

    never_close <- data |>
        filter(group == "Never itemized", fstatus == fs) |>
        mutate(gap_2017 = std_ded_2017[as.character(fstatus)] - imp_itemizable_2017) |>
        filter(gap_2017 <= quantile(gap_2017, 0.10, na.rm = TRUE))

    dat <- bind_rows(never_close, data |> filter(group == treat_label, fstatus == fs)) |>
        mutate(treated = as.integer(group == treat_label))
    feols(
        mover ~ treated + i(year, treated, ref = 2017) + age + sex + race +
            hispan + educ + metro + multgen | year + statefip,
        data = dat, vcov = ~statefip
    )
}

treat_labels <- c("Always itemized", "Stopped itemizing (TCJA)")
fstatuses    <- c("MFJ", "HoH", "Single")

es_group_results <- bind_rows(lapply(fstatuses, function(fs) {
    bind_rows(lapply(treat_labels, function(tl) {
        fit <- run_group_es(tl, fs, item_data)
        tidy(fit) |>
            filter(str_detect(term, "year::")) |>
            mutate(year  = as.integer(str_extract(term, "\\d{4}")),
                   group = tl, fstatus = fs)
    }))
})) |>
    bind_rows(
        expand.grid(year = 2017L, estimate = 0, std.error = 0,
                    group   = treat_labels,
                    fstatus = fstatuses,
                    stringsAsFactors = FALSE)
    ) |>
    mutate(
        group   = factor(group,   levels = c("Stopped itemizing (TCJA)", "Always itemized")),
        fstatus = factor(fstatus, levels = fstatuses),
        ci_lo   = estimate - 1.96 * std.error,
        ci_hi   = estimate + 1.96 * std.error
    ) |>
    arrange(fstatus, group, year)

es_group_colors <- c(
    "Always itemized"          = "#4a7c82",
    "Stopped itemizing (TCJA)" = "#c06028"
)

ggplot(es_group_results,
       aes(year, estimate, color = group, fill = group, group = group)) +
    geom_hline(yintercept = 0, color = "grey70") +
    geom_vline(xintercept = 2017.5, linetype = "dashed", color = "grey50") +
    geom_ribbon(aes(ymin = ci_lo, ymax = ci_hi), alpha = 0.10, color = NA) +
    geom_line(linewidth = 0.9) +
    geom_point(size = 2) +
    facet_wrap(~ fstatus, ncol = 3) +
    scale_x_continuous(breaks = seq(2012, 2023, by = 2)) +
    scale_y_continuous(labels = scales::percent_format(accuracy = 0.01)) +
    scale_color_manual(values = es_group_colors, name = NULL) +
    scale_fill_manual(values  = es_group_colors, name = NULL) +
    labs(
        x        = NULL,
        y        = "Additional probability of moving (pp)",
        title    = "Event study: moving rates by itemization group and filing status",
        subtitle = "vs. nearest 10% of never-itemizers by filing status; ref = 2017; controls + year + state FE; clustered by state"
    ) +
    theme(
        panel.background  = element_rect(fill = "#fafafa", color = NA),
        plot.background   = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major  = element_line(color = "grey90"),
        panel.grid.minor  = element_blank(),
        strip.background  = element_rect(fill = "grey92", color = NA),
        strip.text        = element_text(color = "grey20"),
        axis.line.x       = element_line(color = "grey30"),
        axis.line.y       = element_line(color = "grey30"),
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.key        = element_rect(fill = "#fafafa", color = NA),
        axis.text.x       = element_text(color = "grey20", angle = 45, hjust = 1),
        axis.text.y       = element_text(color = "grey20"),
        text              = element_text(color = "grey20"),
        legend.position   = "bottom"
    )
ggsave("../../../output/Cormac/10b_event_study.png", last_plot(), width = 11, height = 5, dpi = 150)

########################################################
# 10c. Event study by filing status: asymmetric matched controls
#       - Compliers vs. nearest 10% of never-itemizers (2017 threshold)
#       - Always-itemizers vs. nearest 10% of compliers (TCJA threshold)
########################################################

# run_matched_es: generic version of run_group_es where both the control group
# label and the gap variable/threshold can vary.
# gap_col:   column name of the itemizable total used to measure proximity
# threshold: the filing-status-specific standard deduction to gap against
run_matched_es <- function(treat_label, control_label, gap_col, threshold, fs, data) {
    control_close <- data |>
        filter(group == control_label, fstatus == fs) |>
        mutate(gap = threshold - .data[[gap_col]]) |>
        filter(gap <= quantile(gap, 0.10, na.rm = TRUE))

    dat <- bind_rows(control_close, data |> filter(group == treat_label, fstatus == fs)) |>
        mutate(treated = as.integer(group == treat_label))

    feols(
        mover ~ treated + i(year, treated, ref = 2017) + age + sex + race +
            hispan + educ + metro + multgen | year + statefip,
        data = dat, vcov = ~statefip
    )
}

std_ded_2017_map <- c(MFJ = 12700, HoH = 9350, Single = 6350)
tcja_std_ded_map <- c(MFJ = 24000, HoH = 18000, Single = 12000)

es_10c_results <- bind_rows(lapply(fstatuses, function(fs) {
    bind_rows(
        tidy(run_matched_es("Stopped itemizing (TCJA)", "Never itemized",
                            "imp_itemizable_2017", std_ded_2017_map[[fs]], fs, item_data)) |>
            filter(str_detect(term, "year::")) |>
            mutate(year = as.integer(str_extract(term, "\\d{4}")),
                   group = "Stopped itemizing (TCJA)", fstatus = fs),
        tidy(run_matched_es("Always itemized", "Stopped itemizing (TCJA)",
                            "imp_itemizable_2018", tcja_std_ded_map[[fs]], fs, item_data)) |>
            filter(str_detect(term, "year::")) |>
            mutate(year = as.integer(str_extract(term, "\\d{4}")),
                   group = "Always itemized", fstatus = fs)
    )
})) |>
    bind_rows(
        expand.grid(year = 2017L, estimate = 0, std.error = 0,
                    group   = treat_labels,
                    fstatus = fstatuses,
                    stringsAsFactors = FALSE)
    ) |>
    mutate(
        group   = factor(group,   levels = c("Stopped itemizing (TCJA)", "Always itemized")),
        fstatus = factor(fstatus, levels = fstatuses),
        ci_lo   = estimate - 1.96 * std.error,
        ci_hi   = estimate + 1.96 * std.error
    ) |>
    arrange(fstatus, group, year)

ggplot(es_10c_results,
       aes(year, estimate, color = group, fill = group, group = group)) +
    geom_hline(yintercept = 0, color = "grey70") +
    geom_vline(xintercept = 2017.5, linetype = "dashed", color = "grey50") +
    geom_ribbon(aes(ymin = ci_lo, ymax = ci_hi), alpha = 0.10, color = NA) +
    geom_line(linewidth = 0.9) +
    geom_point(size = 2) +
    facet_wrap(~ fstatus, ncol = 3) +
    scale_x_continuous(breaks = seq(2012, 2023, by = 2)) +
    scale_y_continuous(labels = scales::percent_format(accuracy = 0.01)) +
    scale_color_manual(values = es_group_colors, name = NULL) +
    scale_fill_manual(values  = es_group_colors, name = NULL) +
    labs(
        x        = NULL,
        y        = "Additional probability of moving (pp)",
        title    = "Event study: asymmetric matched controls, by filing status",
        subtitle = "Compliers vs. near-2017-threshold never-itemizers; always-itemizers vs. near-TCJA-threshold compliers"
    ) +
    theme(
        panel.background  = element_rect(fill = "#fafafa", color = NA),
        plot.background   = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major  = element_line(color = "grey90"),
        panel.grid.minor  = element_blank(),
        strip.background  = element_rect(fill = "grey92", color = NA),
        strip.text        = element_text(color = "grey20"),
        axis.line.x       = element_line(color = "grey30"),
        axis.line.y       = element_line(color = "grey30"),
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.key        = element_rect(fill = "#fafafa", color = NA),
        axis.text.x       = element_text(color = "grey20", angle = 45, hjust = 1),
        axis.text.y       = element_text(color = "grey20"),
        text              = element_text(color = "grey20"),
        legend.position   = "bottom"
    )
ggsave("../../../output/Cormac/10c_matched_controls.png", last_plot(), width = 11, height = 5, dpi = 150)

########################################################
# 10d. Heterogeneity by age and metro status (MFJ and Single only)
########################################################

# Uses the same asymmetric matched-control design as 10c.
# Two heterogeneity cuts, each producing a facet_grid(fstatus ~ subgroup) plot:
#   (a) Age: under 45 vs. 45 and over
#   (b) Metro status: metropolitan vs. non-metropolitan
# metro factor codes: 1=non-metro, 2=metro/central city, 3=metro/outside central city,
# 4=metro/city status unknown, 5=mixed PUMA (straddles metro boundary) — excluded.

fstatuses_2 <- c("MFJ", "Single")

build_het_results <- function(subgroup_filters, fstatuses_arg = fstatuses_2) {
    sg_names <- names(subgroup_filters)
    bind_rows(lapply(fstatuses_arg, function(fs) {
        bind_rows(lapply(sg_names, function(sg) {
            d <- subgroup_filters[[sg]](item_data)
            bind_rows(
                tidy(run_matched_es("Stopped itemizing (TCJA)", "Never itemized",
                                    "imp_itemizable_2017", std_ded_2017_map[[fs]], fs, d)) |>
                    filter(str_detect(term, "year::")) |>
                    mutate(year = as.integer(str_extract(term, "\\d{4}")),
                           group = "Stopped itemizing (TCJA)", fstatus = fs, subgroup = sg),
                tidy(run_matched_es("Always itemized", "Stopped itemizing (TCJA)",
                                    "imp_itemizable_2018", tcja_std_ded_map[[fs]], fs, d)) |>
                    filter(str_detect(term, "year::")) |>
                    mutate(year = as.integer(str_extract(term, "\\d{4}")),
                           group = "Always itemized", fstatus = fs, subgroup = sg)
            )
        }))
    })) |>
        bind_rows(expand.grid(year = 2017L, estimate = 0, std.error = 0,
                              group    = treat_labels,
                              fstatus  = fstatuses_arg,
                              subgroup = sg_names,
                              stringsAsFactors = FALSE)) |>
        mutate(
            group    = factor(group,    levels = c("Stopped itemizing (TCJA)", "Always itemized")),
            fstatus  = factor(fstatus,  levels = fstatuses_arg),
            subgroup = factor(subgroup, levels = sg_names),
            ci_lo    = estimate - 1.96 * std.error,
            ci_hi    = estimate + 1.96 * std.error
        ) |>
        arrange(fstatus, subgroup, group, year)
}

plot_het <- function(results, title, subtitle) {
    ggplot(results, aes(year, estimate, color = group, fill = group, group = group)) +
        geom_hline(yintercept = 0, color = "grey70") +
        geom_vline(xintercept = 2017.5, linetype = "dashed", color = "grey50") +
        geom_ribbon(aes(ymin = ci_lo, ymax = ci_hi), alpha = 0.10, color = NA) +
        geom_line(linewidth = 0.9) +
        geom_point(size = 2) +
        facet_grid(fstatus ~ subgroup) +
        scale_x_continuous(breaks = seq(2012, 2023, by = 2)) +
        scale_y_continuous(labels = scales::percent_format(accuracy = 0.01)) +
        scale_color_manual(values = es_group_colors, name = NULL) +
        scale_fill_manual(values  = es_group_colors, name = NULL) +
        labs(x = NULL, y = "Additional probability of moving (pp)",
             title = title, subtitle = subtitle) +
        theme(
            panel.background  = element_rect(fill = "#fafafa", color = NA),
            plot.background   = element_rect(fill = "#fafafa", color = NA),
            panel.grid.major  = element_line(color = "grey90"),
            panel.grid.minor  = element_blank(),
            strip.background  = element_rect(fill = "grey92", color = NA),
            strip.text        = element_text(color = "grey20"),
            axis.line.x       = element_line(color = "grey30"),
            axis.line.y       = element_line(color = "grey30"),
            legend.background = element_rect(fill = "#fafafa", color = NA),
            legend.key        = element_rect(fill = "#fafafa", color = NA),
            axis.text.x       = element_text(color = "grey20", angle = 45, hjust = 1),
            axis.text.y       = element_text(color = "grey20"),
            text              = element_text(color = "grey20"),
            legend.position   = "bottom"
        )
}

# --- (a) Age ---
plot_het(
    build_het_results(list(
        "Under 45"    = function(d) filter(d, age < 45),
        "45 and over" = function(d) filter(d, age >= 45)
    )),
    title    = "Heterogeneity by age: event study (MFJ and Single)",
    subtitle = "Matched controls as in 10c; ref = 2017; controls + year + state FE; clustered by state"
)
ggsave("../../../output/Cormac/10d_het_age.png", last_plot(), width = 9, height = 6, dpi = 150)

# --- (b) Metro status ---
plot_het(
    build_het_results(list(
        "Metropolitan"     = function(d) filter(d, metro %in% c("2", "3", "4")),
        "Non-metropolitan" = function(d) filter(d, metro == "1")
    )),
    title    = "Heterogeneity by metro status: event study (MFJ and Single)",
    subtitle = "Matched controls as in 10c; ref = 2017; controls + year + state FE; clustered by state"
)
ggsave("../../../output/Cormac/10d_het_metro.png", last_plot(), width = 9, height = 6, dpi = 150)

# --- (c) Number of children (MFJ only) ---
# Singles by definition have nchild = 0 (nchild > 0 maps to HoH, which is excluded).
plot_het(
    build_het_results(list(
        "No children"  = function(d) filter(d, nchild == 0),
        "Any children" = function(d) filter(d, nchild >= 1)
    ), fstatuses_arg = "MFJ"),
    title    = "Heterogeneity by number of children: event study (MFJ only)",
    subtitle = "Matched controls as in 10c; ref = 2017; controls + year + state FE; clustered by state"
)
ggsave("../../../output/Cormac/10d_het_children.png", last_plot(), width = 9, height = 5, dpi = 150)

# --- (d) Education ---
# educ codes: 0-6 = HS or less, 7-8 = some college (no degree), 10-11 = bachelor's+
# (code 9 absent in data)
plot_het(
    build_het_results(list(
        "HS or less"      = function(d) filter(d, as.integer(as.character(educ)) <= 6),
        "Some college"    = function(d) filter(d, as.integer(as.character(educ)) %in% c(7, 8)),
        "Bachelor's+"     = function(d) filter(d, as.integer(as.character(educ)) >= 10)
    )),
    title    = "Heterogeneity by education: event study (MFJ and Single)",
    subtitle = "Matched controls as in 10c; ref = 2017; controls + year + state FE; clustered by state"
)
ggsave("../../../output/Cormac/10d_het_education.png", last_plot(), width = 12, height = 6, dpi = 150)

# --- (e) Sex ---
plot_het(
    build_het_results(list(
        "Male"   = function(d) filter(d, sex == "1"),
        "Female" = function(d) filter(d, sex == "2")
    )),
    title    = "Heterogeneity by sex: event study (MFJ and Single)",
    subtitle = "Matched controls as in 10c; ref = 2017; controls + year + state FE; clustered by state"
)
ggsave("../../../output/Cormac/10d_het_sex.png", last_plot(), width = 9, height = 6, dpi = 150)

# --- (f) Race / ethnicity ---
# race and hispan stored as factors; convert to integer for comparison.
# Groups: NH White (race=1, hispan=0), NH Black (race=2, hispan=0),
#         Hispanic (hispan>0), Other NH (race>=3, hispan=0).
plot_het(
    build_het_results(list(
        "White NH"  = function(d) filter(d, as.integer(as.character(race)) == 1 &
                                             as.integer(as.character(hispan)) == 0),
        "Black NH"  = function(d) filter(d, as.integer(as.character(race)) == 2 &
                                             as.integer(as.character(hispan)) == 0),
        "Hispanic"  = function(d) filter(d, as.integer(as.character(hispan)) > 0),
        "Other NH"  = function(d) filter(d, as.integer(as.character(race)) >= 3 &
                                             as.integer(as.character(hispan)) == 0)
    )),
    title    = "Heterogeneity by race/ethnicity: event study (MFJ and Single)",
    subtitle = "Matched controls as in 10c; ref = 2017; controls + year + state FE; clustered by state"
)
ggsave("../../../output/Cormac/10d_het_race.png", last_plot(), width = 12, height = 6, dpi = 150)

# --- (g) Multigenerational household ---
# multgen codes: 0=N/A/GQ, 1=not multigenerational, 2=3 generations, 3=4+ generations
plot_het(
    build_het_results(list(
        "Not multigenerational" = function(d) filter(d, multgen == "1"),
        "Multigenerational"     = function(d) filter(d, multgen %in% c("2", "3"))
    )),
    title    = "Heterogeneity by multigenerational household: event study (MFJ and Single)",
    subtitle = "Matched controls as in 10c; ref = 2017; controls + year + state FE; clustered by state"
)
ggsave("../../../output/Cormac/10d_het_multigen.png", last_plot(), width = 9, height = 6, dpi = 150)

########################################################
# 10e. Additional federal tax from SALT cap: treatment vs. matched controls
########################################################

# For each of the two comparisons in 10c, plot mean imp_additional_fed_tax_salt
# by year and filing status:
#   Comparison 1: Stopped itemizing (TCJA) vs. near-2017-threshold never-itemizers
#   Comparison 2: Always itemized          vs. near-TCJA-threshold compliers
# Solid lines = treatment groups; dashed = matched control groups.
# Uses the full sample (no random subsample) for stable means.

exposure_raw <- dbGetQuery(con, "
    SELECT
        year,
        CAST(imp_additional_fed_tax_salt AS DOUBLE) AS additional_tax,
        CAST(imp_salt_ded_curr AS DOUBLE)           AS salt_ded_curr,
        CAST(imp_marginal_fed_rate AS DOUBLE)       AS marginal_fed_rate,
        imp_itemizes_2017,
        CAST(imp_itemizable_2017 AS DOUBLE)         AS imp_itemizable_2017,
        CAST(imp_itemizable_2018 AS DOUBLE)         AS imp_itemizable_2018,
        CASE WHEN marst = 1                 THEN 24000
             WHEN marst != 1 AND nchild > 0 THEN 18000
             ELSE                                12000
        END                                         AS tcja_std_ded,
        CASE WHEN marst = 1                 THEN 'MFJ'
             WHEN marst != 1 AND nchild > 0 THEN 'HoH'
             ELSE                                'Single'
        END                                         AS fstatus
    FROM acs_microdata
    WHERE pernum = 1 AND gq IN (1, 2, 5)
      AND imp_additional_fed_tax_salt IS NOT NULL
      AND imp_itemizes_2017           IS NOT NULL
      AND imp_itemizable_2018         IS NOT NULL
      AND year BETWEEN 2013 AND 2023
") |>
    as_tibble() |>
    mutate(
        tcja_itemizes = as.integer(imp_itemizable_2018 > tcja_std_ded),
        group = case_when(
            imp_itemizes_2017 == 1L & tcja_itemizes == 1L ~ "Always itemized",
            imp_itemizes_2017 == 1L & tcja_itemizes == 0L ~ "Stopped itemizing (TCJA)",
            TRUE                                           ~ "Never itemized"
        ),
        fstatus     = factor(fstatus, levels = c("MFJ", "HoH", "Single")),
        salt_refund = salt_ded_curr * coalesce(marginal_fed_rate, 0)
    )

# Build the four plot groups, applying the same 10th-percentile gap filter as 10c
exposure_groups <- bind_rows(
    exposure_raw |>
        filter(group == "Stopped itemizing (TCJA)") |>
        mutate(plot_group = "Stopped itemizing (TCJA)", ltype = "Treatment"),
    exposure_raw |>
        filter(group == "Always itemized") |>
        mutate(plot_group = "Always itemized", ltype = "Treatment"),
    exposure_raw |>
        filter(group == "Never itemized") |>
        group_by(fstatus) |>
        mutate(
            std_ded_2017 = c(MFJ = 12700, HoH = 9350, Single = 6350)[as.character(fstatus)],
            gap          = std_ded_2017 - imp_itemizable_2017
        ) |>
        filter(gap <= quantile(gap, 0.10, na.rm = TRUE)) |>
        ungroup() |>
        mutate(plot_group = "Near-2017-threshold\nnever-itemizers", ltype = "Control"),
    exposure_raw |>
        filter(group == "Stopped itemizing (TCJA)") |>
        group_by(fstatus) |>
        mutate(gap = tcja_std_ded - imp_itemizable_2018) |>
        filter(gap <= quantile(gap, 0.10, na.rm = TRUE)) |>
        ungroup() |>
        mutate(plot_group = "Near-TCJA-threshold\ncompliers", ltype = "Control")
)

exposure_summary <- exposure_groups |>
    group_by(year, fstatus, plot_group, ltype) |>
    summarise(mean_tax = mean(additional_tax, na.rm = TRUE), .groups = "drop") |>
    mutate(
        plot_group = factor(plot_group, levels = c(
            "Always itemized", "Near-TCJA-threshold\ncompliers",
            "Stopped itemizing (TCJA)", "Near-2017-threshold\nnever-itemizers"
        ))
    )

exposure_colors <- c(
    "Always itemized"                    = "#4a7c82",
    "Near-TCJA-threshold\ncompliers"     = "#4a7c82",
    "Stopped itemizing (TCJA)"           = "#c06028",
    "Near-2017-threshold\nnever-itemizers" = "#c06028"
)
exposure_ltypes <- c(
    "Always itemized"                    = "solid",
    "Near-TCJA-threshold\ncompliers"     = "dashed",
    "Stopped itemizing (TCJA)"           = "solid",
    "Near-2017-threshold\nnever-itemizers" = "dashed"
)

ggplot(exposure_summary,
       aes(year, mean_tax, color = plot_group, linetype = plot_group, group = plot_group)) +
    geom_vline(xintercept = 2017.5, linetype = "dashed", color = "grey50") +
    geom_line(linewidth = 0.9) +
    geom_point(size = 2) +
    facet_wrap(~ fstatus, ncol = 3) +
    scale_x_continuous(breaks = seq(2013, 2023, by = 2)) +
    scale_y_continuous(labels = scales::dollar_format()) +
    scale_color_manual(values = exposure_colors, name = NULL) +
    scale_linetype_manual(values = exposure_ltypes, name = NULL) +
    labs(
        x        = NULL,
        y        = "Mean additional federal tax from SALT cap ($)",
        title    = "TCJA SALT exposure: treatment and matched control groups",
        subtitle = "Solid = treatment; dashed = matched control (10th-percentile gap); by filing status"
    ) +
    theme(
        panel.background  = element_rect(fill = "#fafafa", color = NA),
        plot.background   = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major  = element_line(color = "grey90"),
        panel.grid.minor  = element_blank(),
        strip.background  = element_rect(fill = "grey92", color = NA),
        strip.text        = element_text(color = "grey20"),
        axis.line.x       = element_line(color = "grey30"),
        axis.line.y       = element_line(color = "grey30"),
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.key        = element_rect(fill = "#fafafa", color = NA),
        axis.text.x       = element_text(color = "grey20", angle = 45, hjust = 1),
        axis.text.y       = element_text(color = "grey20"),
        text              = element_text(color = "grey20"),
        legend.position   = "bottom"
    )
ggsave("../../../output/Cormac/10e_exposure.png", last_plot(), width = 11, height = 5, dpi = 150)

salt_ded_summary <- exposure_groups |>
    group_by(year, fstatus, plot_group, ltype) |>
    summarise(mean_salt_ded = mean(salt_ded_curr, na.rm = TRUE), .groups = "drop") |>
    mutate(
        plot_group = factor(plot_group, levels = levels(exposure_summary$plot_group))
    )

ggplot(salt_ded_summary,
       aes(year, mean_salt_ded, color = plot_group, linetype = plot_group, group = plot_group)) +
    geom_vline(xintercept = 2017.5, linetype = "dashed", color = "grey50") +
    geom_line(linewidth = 0.9) +
    geom_point(size = 2) +
    facet_wrap(~ fstatus, ncol = 3) +
    scale_x_continuous(breaks = seq(2013, 2023, by = 2)) +
    scale_y_continuous(labels = scales::dollar_format()) +
    scale_color_manual(values = exposure_colors, name = NULL) +
    scale_linetype_manual(values = exposure_ltypes, name = NULL) +
    labs(
        x        = NULL,
        y        = "Mean deductible SALT under current law ($)",
        title    = "Currently refunded SALT by group",
        subtitle = "imp_salt_ded_curr: actual SALT deducted (0 if not itemizing) | dashed line = TCJA"
    ) +
    theme(
        panel.background  = element_rect(fill = "#fafafa", color = NA),
        plot.background   = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major  = element_line(color = "grey90"),
        panel.grid.minor  = element_blank(),
        strip.background  = element_rect(fill = "grey92", color = NA),
        strip.text        = element_text(color = "grey20"),
        axis.line.x       = element_line(color = "grey30"),
        axis.line.y       = element_line(color = "grey30"),
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.key        = element_rect(fill = "#fafafa", color = NA),
        axis.text.x       = element_text(color = "grey20", angle = 45, hjust = 1),
        axis.text.y       = element_text(color = "grey20"),
        text              = element_text(color = "grey20"),
        legend.position   = "bottom"
    )
ggsave("../../../output/Cormac/10e_salt_ded.png", last_plot(), width = 11, height = 5, dpi = 150)

########################################################
# 10f. Move type event study: within-state vs. between-state (TWFE)
#       MFJ and Single filers, matched controls as in 10c
########################################################

# migrate1: 2 = moved within state, 3 = moved between states.
# Run the same matched TWFE as 10c but swap the outcome from `mover` to
# `within_state` or `between_state`. Two comparisons × two outcomes × two
# filing statuses → facet_grid(outcome_type ~ fstatus).

run_matched_es_outcome <- function(treat_label, control_label, gap_col, threshold,
                                   fs, data, outcome_col) {
    control_close <- data |>
        filter(group == control_label, fstatus == fs) |>
        mutate(gap = threshold - .data[[gap_col]]) |>
        filter(gap <= quantile(gap, 0.10, na.rm = TRUE))

    dat <- bind_rows(control_close, data |> filter(group == treat_label, fstatus == fs)) |>
        mutate(treated = as.integer(group == treat_label))

    dat[["depvar"]] <- dat[[outcome_col]]
    feols(depvar ~ treated + i(year, treated, ref = 2017) + age + sex + race +
              hispan + educ + metro + multgen | year + statefip,
          data = dat, vcov = ~statefip)
}

item_data_mig <- item_data |>
    mutate(
        within_state  = as.integer(migrate1 == 2L),
        between_state = as.integer(migrate1 == 3L)
    )

outcome_specs <- list(
    "Within-state"  = "within_state",
    "Between-state" = "between_state"
)

mig_type_results <- bind_rows(lapply(c("MFJ", "Single"), function(fs) {
    bind_rows(lapply(names(outcome_specs), function(olabel) {
        ocol <- outcome_specs[[olabel]]
        bind_rows(
            tidy(run_matched_es_outcome("Stopped itemizing (TCJA)", "Never itemized",
                                        "imp_itemizable_2017", std_ded_2017_map[[fs]],
                                        fs, item_data_mig, ocol)) |>
                filter(str_detect(term, "year::")) |>
                mutate(year = as.integer(str_extract(term, "\\d{4}")),
                       group = "Stopped itemizing (TCJA)", fstatus = fs, outcome_type = olabel),
            tidy(run_matched_es_outcome("Always itemized", "Stopped itemizing (TCJA)",
                                        "imp_itemizable_2018", tcja_std_ded_map[[fs]],
                                        fs, item_data_mig, ocol)) |>
                filter(str_detect(term, "year::")) |>
                mutate(year = as.integer(str_extract(term, "\\d{4}")),
                       group = "Always itemized", fstatus = fs, outcome_type = olabel)
        )
    }))
})) |>
    bind_rows(expand.grid(
        year         = 2017L,
        estimate     = 0,
        std.error    = 0,
        group        = treat_labels,
        fstatus      = c("MFJ", "Single"),
        outcome_type = names(outcome_specs),
        stringsAsFactors = FALSE
    )) |>
    mutate(
        group        = factor(group,        levels = c("Stopped itemizing (TCJA)", "Always itemized")),
        fstatus      = factor(fstatus,      levels = c("MFJ", "Single")),
        outcome_type = factor(outcome_type, levels = names(outcome_specs)),
        ci_lo        = estimate - 1.96 * std.error,
        ci_hi        = estimate + 1.96 * std.error
    ) |>
    arrange(fstatus, outcome_type, group, year)

ggplot(mig_type_results,
       aes(year, estimate, color = group, fill = group, group = group)) +
    geom_hline(yintercept = 0, color = "grey70") +
    geom_vline(xintercept = 2017.5, linetype = "dashed", color = "grey50") +
    geom_ribbon(aes(ymin = ci_lo, ymax = ci_hi), alpha = 0.10, color = NA) +
    geom_line(linewidth = 0.9) +
    geom_point(size = 2) +
    facet_grid(outcome_type ~ fstatus) +
    scale_x_continuous(breaks = seq(2012, 2023, by = 2)) +
    scale_y_continuous(labels = scales::percent_format(accuracy = 0.01)) +
    scale_color_manual(values = es_group_colors, name = NULL) +
    scale_fill_manual(values  = es_group_colors, name = NULL) +
    labs(
        x        = NULL,
        y        = "Additional probability of moving (pp)",
        title    = "Event study: within- vs. between-state moves (MFJ and Single)",
        subtitle = "Matched controls as in 10c; ref = 2017; controls + year + state FE; clustered by state"
    ) +
    theme(
        panel.background  = element_rect(fill = "#fafafa", color = NA),
        plot.background   = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major  = element_line(color = "grey90"),
        panel.grid.minor  = element_blank(),
        strip.background  = element_rect(fill = "grey92", color = NA),
        strip.text        = element_text(color = "grey20"),
        axis.line.x       = element_line(color = "grey30"),
        axis.line.y       = element_line(color = "grey30"),
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.key        = element_rect(fill = "#fafafa", color = NA),
        axis.text.x       = element_text(color = "grey20", angle = 45, hjust = 1),
        axis.text.y       = element_text(color = "grey20"),
        text              = element_text(color = "grey20"),
        legend.position   = "bottom"
    )
ggsave("../../../output/Cormac/10f_migration_type.png", last_plot(), width = 10, height = 7, dpi = 150)

########################################################
# 10g. Robustness: alternative control group distance bands
########################################################

# Concern: unobserved deductions (medical expenses, charitable donations, etc.)
# may cause true itemizers near the threshold to appear as "Never itemizers",
# contaminating the closest control band. Robustness check: repeat the 10c
# regressions using control units drawn from successive 10-pp slices of the
# gap distribution (gap = threshold - imp_itemizable). If the event study
# pattern is stable across bands, misclassification near the cutoff is not
# driving the result.

run_matched_es_band <- function(treat_label, control_label, gap_col, threshold,
                                fs, data, lo_pct = 0, hi_pct = 10) {
    control_all <- data |>
        filter(group == control_label, fstatus == fs) |>
        mutate(gap = threshold - .data[[gap_col]])

    q_lo <- if (lo_pct == 0) -Inf else quantile(control_all$gap, lo_pct / 100, na.rm = TRUE)
    q_hi <- quantile(control_all$gap, hi_pct / 100, na.rm = TRUE)

    control_band <- control_all |> filter(gap > q_lo, gap <= q_hi)

    dat <- bind_rows(control_band, data |> filter(group == treat_label, fstatus == fs)) |>
        mutate(treated = as.integer(group == treat_label))

    feols(
        mover ~ treated + i(year, treated, ref = 2017) + age + sex + race +
            hispan + educ + metro + multgen | year + statefip,
        data = dat, vcov = ~statefip
    )
}

bands_10g <- list(
    "0-10% (baseline)" = c(0,  10),
    "10-20%"           = c(10, 20),
    "20-30%"           = c(20, 30),
    "30-40%"           = c(30, 40)
)

comparisons_10g <- list(
    "Stopped itemizing (TCJA)" = list(
        treat   = "Stopped itemizing (TCJA)",
        control = "Never itemized",
        gap_col = "imp_itemizable_2017",
        thresh  = std_ded_2017_map
    ),
    "Always itemized" = list(
        treat   = "Always itemized",
        control = "Stopped itemizing (TCJA)",
        gap_col = "imp_itemizable_2018",
        thresh  = tcja_std_ded_map
    )
)

robust_results <- bind_rows(lapply(c("MFJ", "Single"), function(fs) {
    bind_rows(lapply(names(comparisons_10g), function(comp_name) {
        comp <- comparisons_10g[[comp_name]]
        bind_rows(lapply(names(bands_10g), function(band_name) {
            b <- bands_10g[[band_name]]
            tidy(run_matched_es_band(
                treat_label   = comp$treat,
                control_label = comp$control,
                gap_col       = comp$gap_col,
                threshold     = comp$thresh[[fs]],
                fs            = fs,
                data          = item_data,
                lo_pct        = b[1],
                hi_pct        = b[2]
            )) |>
                filter(str_detect(term, "year::")) |>
                mutate(
                    year      = as.integer(str_extract(term, "\\d{4}")),
                    fstatus   = fs,
                    group     = comp_name,
                    band      = band_name
                )
        }))
    }))
})) |>
    bind_rows(expand.grid(
        year      = 2017L,
        estimate  = 0,
        std.error = 0,
        fstatus   = c("MFJ", "Single"),
        group     = names(comparisons_10g),
        band      = names(bands_10g),
        stringsAsFactors = FALSE
    )) |>
    mutate(
        fstatus = factor(fstatus, levels = c("MFJ", "Single")),
        group   = factor(group,   levels = names(comparisons_10g)),
        band    = factor(band,    levels = names(bands_10g)),
        ci_lo   = estimate - 1.96 * std.error,
        ci_hi   = estimate + 1.96 * std.error
    ) |>
    arrange(fstatus, group, band, year)

band_colors <- c(
    "0-10% (baseline)" = "#23373b",
    "10-20%"           = "#4a7c82",
    "20-30%"           = "#c06028",
    "30-40%"           = "#eb811b"
)

ggplot(robust_results,
       aes(year, estimate, color = band, fill = band, group = band)) +
    geom_hline(yintercept = 0, color = "grey70") +
    geom_vline(xintercept = 2017.5, linetype = "dashed", color = "grey50") +
    geom_ribbon(aes(ymin = ci_lo, ymax = ci_hi), alpha = 0.10, color = NA) +
    geom_line(linewidth = 0.9) +
    geom_point(size = 2) +
    facet_grid(fstatus ~ group) +
    scale_x_continuous(breaks = seq(2012, 2023, by = 2)) +
    scale_y_continuous(labels = scales::percent_format(accuracy = 0.01)) +
    scale_color_manual(values = band_colors, name = "Control band") +
    scale_fill_manual(values  = band_colors, name = "Control band") +
    labs(
        x        = NULL,
        y        = "Additional probability of moving (pp)",
        title    = "Robustness: event study by control group distance band",
        subtitle = "Each line = different 10-pp slice of gap distribution; ref = 2017; clustered by state"
    ) +
    theme(
        panel.background  = element_rect(fill = "#fafafa", color = NA),
        plot.background   = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major  = element_line(color = "grey90"),
        panel.grid.minor  = element_blank(),
        strip.background  = element_rect(fill = "grey92", color = NA),
        strip.text        = element_text(color = "grey20"),
        axis.line.x       = element_line(color = "grey30"),
        axis.line.y       = element_line(color = "grey30"),
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.key        = element_rect(fill = "#fafafa", color = NA),
        axis.text.x       = element_text(color = "grey20", angle = 45, hjust = 1),
        axis.text.y       = element_text(color = "grey20"),
        text              = element_text(color = "grey20"),
        legend.position   = "bottom"
    )
ggsave("../../../output/Cormac/10g_robustness_bands.png", last_plot(), width = 10, height = 7, dpi = 150)

########################################################
# 10h. Robustness: quantile-anchored itemization groups
########################################################

# Concern: imputed itemization decisions are based on survey data from
# future/past years, not 2017 itself. Nominal wage and property tax growth
# over time pushes more households above the fixed standard-deduction
# threshold in later years, causing spurious drift in group membership.
#
# Fix: replace fixed dollar thresholds with year × fstatus quantile thresholds
# chosen so that the share of "itemizers" in each year (under 2017 and TCJA
# rules respectively) equals the observed share in the reference year.
# Group sizes are held constant; only the composition of who falls in each
# group changes. Then repeat the 10c DiD.

# --- Step 1: Reference itemizer shares ---

ref_p_2017 <- item_data |>
    filter(year == 2017L) |>
    group_by(fstatus) |>
    summarise(p_2017 = mean(imp_itemizes_2017 == 1L, na.rm = TRUE), .groups = "drop")

ref_p_2018 <- item_data |>
    filter(year == 2018L) |>
    group_by(fstatus) |>
    summarise(p_2018 = mean(tcja_itemizes == 1L, na.rm = TRUE), .groups = "drop")

# --- Step 2: Year × fstatus quantile thresholds ---

q_thresholds <- item_data |>
    left_join(ref_p_2017, by = "fstatus") |>
    left_join(ref_p_2018, by = "fstatus") |>
    group_by(year, fstatus) |>
    summarise(
        q_thresh_2017 = quantile(imp_itemizable_2017, 1 - first(p_2017), na.rm = TRUE),
        q_thresh_2018 = quantile(imp_itemizable_2018, 1 - first(p_2018), na.rm = TRUE),
        .groups = "drop"
    )

# --- Step 3: Quantile-based groups ---

item_data_q <- item_data |>
    left_join(q_thresholds, by = c("year", "fstatus")) |>
    mutate(
        q_itemizes_2017 = as.integer(!is.na(imp_itemizable_2017) & imp_itemizable_2017 > q_thresh_2017),
        q_itemizes_2018 = as.integer(!is.na(imp_itemizable_2018) & imp_itemizable_2018 > q_thresh_2018),
        q_group = case_when(
            q_itemizes_2017 == 1L & q_itemizes_2018 == 1L ~ "Always itemized",
            q_itemizes_2017 == 1L & q_itemizes_2018 == 0L ~ "Stopped itemizing (TCJA)",
            q_itemizes_2017 == 0L & q_itemizes_2018 == 0L ~ "Never itemized",
            TRUE ~ NA_character_
        )
    )

# Sanity check: group shares should be roughly constant across years
cat("\nQuantile-group shares by year (MFJ and Single):\n")
item_data_q |>
    filter(!is.na(q_group), fstatus %in% c("MFJ", "Single")) |>
    count(year, fstatus, q_group) |>
    group_by(year, fstatus) |>
    mutate(share = round(n / sum(n), 3)) |>
    print(n = 60)

# --- Step 4: Matched TWFE with row-varying threshold column ---

run_matched_es_q <- function(treat_label, control_label, gap_col, thresh_col, fs, data) {
    control_close <- data |>
        filter(q_group == control_label, fstatus == fs) |>
        mutate(gap = .data[[thresh_col]] - .data[[gap_col]]) |>
        filter(gap <= quantile(gap, 0.10, na.rm = TRUE))

    dat <- bind_rows(control_close, data |> filter(q_group == treat_label, fstatus == fs)) |>
        mutate(treated = as.integer(q_group == treat_label))

    feols(
        mover ~ treated + i(year, treated, ref = 2017) + age + sex + race +
            hispan + educ + metro + multgen | year + statefip,
        data = dat, vcov = ~statefip
    )
}

# --- Step 5: Run regressions ---

es_10h_results <- bind_rows(lapply(c("MFJ", "Single"), function(fs) {
    bind_rows(
        tidy(run_matched_es_q("Stopped itemizing (TCJA)", "Never itemized",
                              "imp_itemizable_2017", "q_thresh_2017", fs, item_data_q)) |>
            filter(str_detect(term, "year::")) |>
            mutate(year  = as.integer(str_extract(term, "\\d{4}")),
                   group = "Stopped itemizing (TCJA)", fstatus = fs),
        tidy(run_matched_es_q("Always itemized", "Stopped itemizing (TCJA)",
                              "imp_itemizable_2018", "q_thresh_2018", fs, item_data_q)) |>
            filter(str_detect(term, "year::")) |>
            mutate(year  = as.integer(str_extract(term, "\\d{4}")),
                   group = "Always itemized", fstatus = fs)
    )
})) |>
    bind_rows(expand.grid(
        year      = 2017L,
        estimate  = 0,
        std.error = 0,
        group     = treat_labels,
        fstatus   = c("MFJ", "Single"),
        stringsAsFactors = FALSE
    )) |>
    mutate(
        group   = factor(group,   levels = c("Stopped itemizing (TCJA)", "Always itemized")),
        fstatus = factor(fstatus, levels = c("MFJ", "Single")),
        ci_lo   = estimate - 1.96 * std.error,
        ci_hi   = estimate + 1.96 * std.error
    ) |>
    arrange(fstatus, group, year)

# --- Step 6: Plot ---

ggplot(es_10h_results,
       aes(year, estimate, color = group, fill = group, group = group)) +
    geom_hline(yintercept = 0, color = "grey70") +
    geom_vline(xintercept = 2017.5, linetype = "dashed", color = "grey50") +
    geom_ribbon(aes(ymin = ci_lo, ymax = ci_hi), alpha = 0.10, color = NA) +
    geom_line(linewidth = 0.9) +
    geom_point(size = 2) +
    facet_wrap(~ fstatus, ncol = 2) +
    scale_x_continuous(breaks = seq(2012, 2023, by = 2)) +
    scale_y_continuous(labels = scales::percent_format(accuracy = 0.01)) +
    scale_color_manual(values = es_group_colors, name = NULL) +
    scale_fill_manual(values  = es_group_colors, name = NULL) +
    labs(
        x        = NULL,
        y        = "Additional probability of moving (pp)",
        title    = "Robustness: quantile-anchored itemization groups",
        subtitle = "Year-specific thresholds hold itemizer share constant at 2017/2018 levels; matched controls as in 10c"
    ) +
    theme(
        panel.background  = element_rect(fill = "#fafafa", color = NA),
        plot.background   = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major  = element_line(color = "grey90"),
        panel.grid.minor  = element_blank(),
        strip.background  = element_rect(fill = "grey92", color = NA),
        strip.text        = element_text(color = "grey20"),
        axis.line.x       = element_line(color = "grey30"),
        axis.line.y       = element_line(color = "grey30"),
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.key        = element_rect(fill = "#fafafa", color = NA),
        axis.text.x       = element_text(color = "grey20", angle = 45, hjust = 1),
        axis.text.y       = element_text(color = "grey20"),
        text              = element_text(color = "grey20"),
        legend.position   = "bottom"
    )
ggsave("../../../output/Cormac/10h_quantile_robustness.png", last_plot(), width = 9, height = 5, dpi = 150)

########################################################
# 10i. First stage: mean SALT deduction by group over time
########################################################

# Shows that TCJA directly reduced the SALT deduction for treated groups,
# validating the "first stage." Uses the full-sample exposure_groups data
# already built in 10e — no additional DB query needed.
#
# Layout: facet_grid(comparison ~ fstatus), two rows (one per comparison),
# three columns (MFJ / HoH / Single). Within each panel, solid = treatment
# group, dashed = matched control.

fs_data <- exposure_groups |>
    mutate(
        comparison = case_when(
            plot_group %in% c("Stopped itemizing (TCJA)", "Near-2017-threshold\nnever-itemizers") ~
                "Stopped itemizing (TCJA)\nvs. near-2017-threshold never-itemizers",
            TRUE ~
                "Always itemized\nvs. near-TCJA-threshold compliers"
        ),
        comparison = factor(comparison, levels = c(
            "Stopped itemizing (TCJA)\nvs. near-2017-threshold never-itemizers",
            "Always itemized\nvs. near-TCJA-threshold compliers"
        ))
    )

fs_summary <- fs_data |>
    group_by(year, fstatus, plot_group, ltype, comparison) |>
    summarise(mean_salt_refund = mean(salt_refund, na.rm = TRUE), .groups = "drop") |>
    mutate(
        plot_group = factor(plot_group, levels = c(
            "Always itemized", "Near-TCJA-threshold\ncompliers",
            "Stopped itemizing (TCJA)", "Near-2017-threshold\nnever-itemizers"
        ))
    )

ggplot(fs_summary,
       aes(year, mean_salt_refund, color = plot_group, linetype = plot_group, group = plot_group)) +
    geom_vline(xintercept = 2017.5, linetype = "dashed", color = "grey50") +
    geom_line(linewidth = 0.9) +
    geom_point(size = 2) +
    facet_grid(comparison ~ fstatus) +
    scale_x_continuous(breaks = seq(2013, 2023, by = 2)) +
    scale_y_continuous(labels = scales::dollar_format()) +
    scale_color_manual(values = exposure_colors, name = NULL) +
    scale_linetype_manual(values = exposure_ltypes, name = NULL) +
    labs(
        x        = NULL,
        y        = "Mean SALT refund (deduction × marginal federal rate, $)",
        title    = "First stage: SALT refund by group over time",
        subtitle = "Solid = treatment; dashed = matched control (10th-pct gap) | dashed line = TCJA"
    ) +
    theme(
        panel.background  = element_rect(fill = "#fafafa", color = NA),
        plot.background   = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major  = element_line(color = "grey90"),
        panel.grid.minor  = element_blank(),
        strip.background  = element_rect(fill = "grey92", color = NA),
        strip.text        = element_text(color = "grey20"),
        axis.line.x       = element_line(color = "grey30"),
        axis.line.y       = element_line(color = "grey30"),
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.key        = element_rect(fill = "#fafafa", color = NA),
        axis.text.x       = element_text(color = "grey20", angle = 45, hjust = 1),
        axis.text.y       = element_text(color = "grey20"),
        text              = element_text(color = "grey20"),
        legend.position   = "bottom"
    )
ggsave("../../../output/Cormac/10i_first_stage.png", last_plot(), width = 12, height = 7, dpi = 150)

########################################################
# 10j. SALT refund by regime and itemizable total (MFJ filers)
########################################################

# Two lines per panel: refund under 2017 rules and under TCJA rules.
#   refund_2017 = imp_salt_ded_2017 × mtr
#   refund_2018 = imp_salt_ded_2018 × mtr
# Panel (a): x = imp_itemizable_2017
# Panel (b): x = imp_itemizable_2018
# Both panels restricted to x ∈ [0, 30 000]; stacked vertically.

refund_change_raw <- dbGetQuery(con, "
    SELECT
        CAST(imp_salt_ded_2017       AS DOUBLE) AS salt_ded_2017,
        CAST(imp_salt_ded_2018       AS DOUBLE) AS salt_ded_2018,
        CAST(imp_itemizable_2017     AS DOUBLE) AS imp_itemizable_2017,
        CAST(imp_itemizable_2018     AS DOUBLE) AS imp_itemizable_2018,
        CAST(imp_marginal_fed_rate   AS DOUBLE) AS mtr
    FROM acs_microdata
    WHERE pernum = 1 AND gq IN (1, 2, 5)
      AND marst = 1
      AND imp_salt_ded_2017      IS NOT NULL
      AND imp_salt_ded_2018      IS NOT NULL
      AND imp_marginal_fed_rate  IS NOT NULL
      AND year BETWEEN 2013 AND 2023
") |>
    as_tibble() |>
    mutate(
        refund_2017 = salt_ded_2017 * mtr,
        refund_2018 = salt_ded_2018 * mtr
    )

bin_size  <- 500L
cut_2017  <- 12700
cut_tcja  <- 24000

diff_summary <- refund_change_raw |>
    filter(imp_itemizable_2017 >= 0, imp_itemizable_2017 <= 30000) |>
    mutate(
        refund_diff = refund_2017 - refund_2018,
        x_bin       = floor(imp_itemizable_2017 / bin_size) * bin_size + bin_size / 2L + 250L
    ) |>
    group_by(x_bin) |>
    summarise(mean_refund_diff = mean(refund_diff, na.rm = TRUE), .groups = "drop")

ggplot(diff_summary, aes(x_bin, mean_refund_diff)) +
    annotate("rect",
             xmin = cut_2017 * 0.9, xmax = cut_2017 * 1.1, ymin = -Inf, ymax = Inf,
             fill = "#4a7c82", alpha = 0.12) +
    annotate("rect",
             xmin = cut_tcja * 0.9,  xmax = cut_tcja * 1.1,  ymin = -Inf, ymax = Inf,
             fill = "#c06028", alpha = 0.12) +
    geom_hline(yintercept = 0, color = "grey70") +
    geom_vline(xintercept = cut_2017, linetype = "dotted", color = "#4a7c82", linewidth = 0.7) +
    geom_vline(xintercept = cut_tcja,  linetype = "dashed", color = "#c06028", linewidth = 0.7) +
    geom_line(linewidth = 0.9, color = "#23373b") +
    geom_point(size = 1.5, color = "#23373b") +
    annotate("text", x = cut_2017, y = Inf, label = "2017 std ded\n($12.7k)",
             hjust = -0.07, vjust = 1.3, size = 2.8, color = "#4a7c82") +
    annotate("text", x = cut_tcja,  y = Inf, label = "TCJA std ded\n($24k)",
             hjust = -0.07, vjust = 1.3, size = 2.8, color = "#c06028") +
    scale_x_continuous(
        labels = scales::dollar_format(scale = 1e-3, suffix = "k"),
        breaks = seq(0, 30000, by = 5000),
        limits = c(0, 30000)
    ) +
    scale_y_continuous(labels = scales::dollar_format()) +
    labs(
        x        = "2017 itemizable total ($)",
        y        = "Mean SALT refund lost: 2017 − TCJA rules ($)",
        title    = "SALT refund loss under TCJA by 2017 itemizable total (MFJ filers)",
        subtitle = "$500 bins | 2013–2023 pooled | shaded bands = ±10% of each std ded cutoff (DiD windows)"
    ) +
    theme(
        panel.background = element_rect(fill = "#fafafa", color = NA),
        plot.background  = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major = element_line(color = "grey90"),
        panel.grid.minor = element_blank(),
        axis.line.x      = element_line(color = "grey30"),
        axis.line.y      = element_line(color = "grey30"),
        axis.text.x      = element_text(color = "grey20"),
        axis.text.y      = element_text(color = "grey20"),
        text             = element_text(color = "grey20")
    )
ggsave("../../../output/Cormac/10j_salt_refund_diff.png", last_plot(), width = 8, height = 5, dpi = 150)

########################################################
# 10k. Balance table
########################################################

# Variables: age, household income, children, homeownership, home value,
#   total SALT, marginal federal tax rate, metro share, pre-TCJA move rate.
#
# Columns:
#   (1) Mean for "Stopped itemizing (TCJA)" — reference group
#   (2) Δ Never itemized − Stopped, full sample          (comp 1 control)
#   (3) Δ Always itemized − Stopped, full sample         (comp 2 control)
#   (4) Δ Never − Stopped, near 2017 threshold (±10%)   (comp 1, near)
#   (5) Δ Always − Stopped, near TCJA threshold (±10%)  (comp 2, near)
# t-statistics in parentheses below each difference.

bal_raw <- dbGetQuery(con, "
    SELECT
        year,
        CASE WHEN migrate1 IN (2,3,4) THEN 1 ELSE 0 END        AS mover,
        imp_itemizes_2017,
        CAST(imp_itemizable_2017   AS DOUBLE)                   AS imp_itemizable_2017,
        CAST(imp_itemizable_2018   AS DOUBLE)                   AS imp_itemizable_2018,
        CAST(imp_salt              AS DOUBLE)                   AS imp_salt,
        CAST(imp_marginal_fed_rate AS DOUBLE)                   AS mtr,
        CAST(age                   AS DOUBLE)                   AS age,
        CASE WHEN hhincome >= 9999999 THEN NULL
             ELSE CAST(hhincome AS DOUBLE) END                  AS hhincome,
        CAST(nchild                AS DOUBLE)                   AS nchild,
        CASE WHEN ownershp = 1 THEN 1.0 ELSE 0.0 END           AS owner,
        CASE WHEN valueh >= 9999999 OR ownershp != 1 THEN NULL
             ELSE CAST(valueh AS DOUBLE) END                    AS home_value,
        CASE WHEN TRY_CAST(metro AS INT) IN (2,3,4) THEN 1.0
             ELSE 0.0 END                                       AS metropolitan,
        CASE WHEN marst = 1                 THEN 24000
             WHEN marst != 1 AND nchild > 0 THEN 18000
             ELSE                                12000
        END                                                     AS tcja_std_ded
    FROM acs_microdata
    WHERE pernum = 1 AND gq IN (1, 2, 5)
      AND marst = 1
      AND migrate1 != 0
      AND imp_itemizes_2017   IS NOT NULL
      AND imp_itemizable_2018 IS NOT NULL
      AND year BETWEEN 2012 AND 2023
") |>
    as_tibble() |>
    mutate(
        tcja_itemizes = as.integer(imp_itemizable_2018 > tcja_std_ded),
        group = case_when(
            imp_itemizes_2017 == 1L & tcja_itemizes == 1L ~ "Always itemized",
            imp_itemizes_2017 == 1L & tcja_itemizes == 0L ~ "Stopped itemizing",
            imp_itemizes_2017 == 0L                        ~ "Never itemized"
        )
    )

bal_vars <- c("age", "hhincome", "nchild", "owner", "home_value",
              "imp_salt", "mtr", "metropolitan")

var_labels <- c(
    age           = "Age",
    hhincome      = "Household income ($)",
    nchild        = "Number of children",
    owner         = "Homeowner (share)",
    home_value    = "Home value, owners ($)",
    imp_salt      = "Total SALT ($)",
    mtr           = "Marginal federal tax rate",
    metropolitan  = "Metropolitan area (share)"
)

fmt_digits <- c(age = 1, hhincome = 0, nchild = 2, owner = 3,
                home_value = 0, imp_salt = 0, mtr = 3, metropolitan = 3)

stopped_full  <- bal_raw |> filter(group == "Stopped itemizing")
never_full    <- bal_raw |> filter(group == "Never itemized")
always_full   <- bal_raw |> filter(group == "Always itemized")

stopped_near1 <- bal_raw |> filter(group == "Stopped itemizing",
                                    imp_itemizable_2017 >= cut_2017,
                                    imp_itemizable_2017 <= cut_2017 * 1.1)
never_near    <- bal_raw |> filter(group == "Never itemized",
                                    imp_itemizable_2017 >= cut_2017 * 0.9)
stopped_near2 <- bal_raw |> filter(group == "Stopped itemizing",
                                    imp_itemizable_2018 >= cut_tcja * 0.9,
                                    imp_itemizable_2018 <  cut_tcja)
always_near   <- bal_raw |> filter(group == "Always itemized",
                                    imp_itemizable_2018 >= cut_tcja,
                                    imp_itemizable_2018 <= cut_tcja * 1.1)

# Regression-based balance: regress each variable on group dummies,
# reference = "Stopped itemizing". HC1 robust SEs.
# Returns tidy tibble with estimate, conf.low, conf.high per group level.
run_bal_reg <- function(data, vars) {
    nref_levels <- c("Never itemized", "Always itemized")
    data <- data |>
        mutate(grp = factor(group, levels = c("Stopped itemizing",
                                               "Never itemized",
                                               "Always itemized")))
    map_dfr(vars, function(v) {
        dat <- data |> filter(!is.na(.data[[v]])) |> mutate(depvar = .data[[v]])
        if (length(unique(dat$depvar)) <= 1L) {
            return(tibble(variable = v, group_label = nref_levels,
                          estimate = NA_real_, std.error = NA_real_))
        }
        mod <- feols(depvar ~ grp, data = dat, vcov = "HC1")
        tidy(mod) |>
            filter(term != "(Intercept)") |>
            mutate(variable    = v,
                   group_label = str_remove(term, "^grp")) |>
            select(variable, group_label, estimate, std.error)
    })
}

# Full-sample: both Never and Always vs. Stopped
full_reg <- run_bal_reg(
    bind_rows(stopped_full, never_full, always_full), bal_vars
)

# Near-threshold comp 1: Never vs. Stopped (near $12,700)
near1_reg <- run_bal_reg(bind_rows(stopped_near1, never_near), bal_vars)

# Near-threshold comp 2: Always vs. Stopped (near $24,000)
near2_reg <- run_bal_reg(bind_rows(stopped_near2, always_near), bal_vars)

# Pre-TCJA move rate (same regression approach, years 2012–2017 only)
bal_raw_pre <- bal_raw |>
    filter(year < 2018L) |>
    mutate(depvar = mover,
           grp    = factor(group, levels = c("Stopped itemizing",
                                              "Never itemized",
                                              "Always itemized")))

move_full_mod  <- feols(depvar ~ grp, data = bal_raw_pre |>
                            filter(group %in% c("Stopped itemizing", "Never itemized",
                                                "Always itemized")), vcov = "HC1")
move_near1_mod <- feols(depvar ~ grp, data = bal_raw_pre |>
                            filter(group == "Stopped itemizing",
                                   imp_itemizable_2017 >= cut_2017,
                                   imp_itemizable_2017 <= cut_2017 * 1.1) |>
                            bind_rows(bal_raw_pre |>
                                          filter(group == "Never itemized",
                                                 imp_itemizable_2017 >= cut_2017 * 0.9)),
                        vcov = "HC1")
move_near2_mod <- feols(depvar ~ grp, data = bal_raw_pre |>
                            filter(group == "Stopped itemizing",
                                   imp_itemizable_2018 >= cut_tcja * 0.9,
                                   imp_itemizable_2018 <  cut_tcja) |>
                            bind_rows(bal_raw_pre |>
                                          filter(group == "Always itemized",
                                                 imp_itemizable_2018 >= cut_tcja,
                                                 imp_itemizable_2018 <= cut_tcja * 1.1)),
                        vcov = "HC1")

move_full  <- tidy(move_full_mod) |>
    filter(term != "(Intercept)") |>
    mutate(variable = "mover", group_label = str_remove(term, "^grp")) |>
    select(variable, group_label, estimate, std.error)
move_near1 <- tidy(move_near1_mod) |>
    filter(term != "(Intercept)") |>
    mutate(variable = "mover", group_label = str_remove(term, "^grp")) |>
    select(variable, group_label, estimate, std.error)
move_near2 <- tidy(move_near2_mod) |>
    filter(term != "(Intercept)") |>
    mutate(variable = "mover", group_label = str_remove(term, "^grp")) |>
    select(variable, group_label, estimate, std.error)

all_vars  <- c(bal_vars, "mover")
all_labels <- c(var_labels, mover = "Pre-TCJA move rate")
all_digits <- c(fmt_digits, mover = 3)

full_reg_all  <- bind_rows(full_reg,  move_full)
near1_reg_all <- bind_rows(near1_reg, move_near1)
near2_reg_all <- bind_rows(near2_reg, move_near2)

# Stopped-group means
stopped_means <- bind_rows(stopped_full, bal_raw_pre |> filter(group == "Stopped itemizing")) |>
    summarise(across(all_of(c(bal_vars, "mover")), ~ mean(.x, na.rm = TRUE))) |>
    pivot_longer(everything(), names_to = "variable", values_to = "mean_stopped")

# Assemble two-row-per-variable table: estimate row, then SE row in parentheses
bal_table <- map_dfr(all_vars, function(v) {
    digs   <- all_digits[[v]]
    mean_s <- stopped_means |> filter(variable == v) |> pull(mean_stopped)
    fe  <- function(x) formatC(round(x, digs),     format = "f", digits = digs,     big.mark = ",")
    fse <- function(x) sprintf("(%s)", formatC(round(x, max(digs, 1)), format = "f",
                                               digits = max(digs, 1), big.mark = ","))

    get_est <- function(reg, label) {
        row <- reg |> filter(variable == v, group_label == label)
        if (nrow(row) == 0) return(NA_character_)
        fe(row$estimate)
    }
    get_se <- function(reg, label) {
        row <- reg |> filter(variable == v, group_label == label)
        if (nrow(row) == 0) return(NA_character_)
        fse(row$std.error)
    }

    bind_rows(
        tibble(
            Variable                                   = all_labels[[v]],
            `Stopped (mean)`                           = fe(mean_s),
            `Never itemized − Stopped (full sample)`   = get_est(full_reg_all,  "Never itemized"),
            `Always itemized − Stopped (full sample)`  = get_est(full_reg_all,  "Always itemized"),
            `Never itemized − Stopped (near $12.7k)`   = get_est(near1_reg_all, "Never itemized"),
            `Always itemized − Stopped (near $24k)`    = get_est(near2_reg_all, "Always itemized")
        ),
        tibble(
            Variable                                   = "",
            `Stopped (mean)`                           = "",
            `Never itemized − Stopped (full sample)`   = get_se(full_reg_all,  "Never itemized"),
            `Always itemized − Stopped (full sample)`  = get_se(full_reg_all,  "Always itemized"),
            `Never itemized − Stopped (near $12.7k)`   = get_se(near1_reg_all, "Never itemized"),
            `Always itemized − Stopped (near $24k)`    = get_se(near2_reg_all, "Always itemized")
        )
    )
})

cat("\nBalance table — MFJ filers\n")
cat("Reference group: 'Stopped itemizing (TCJA)' (itemized under 2017 rules, not under TCJA)\n")
cat("Estimates from OLS(y ~ group dummy); reference = Stopped itemizing; HC1 robust SEs\n")
cat("Each cell: OLS coefficient, with SE in parentheses on the row below.\n\n")
print(as.data.frame(bal_table), row.names = FALSE)

########################################################
# 10l. Move rate: itemizers vs. non-itemizers at equal SALT level
########################################################

# At a given SALT level, itemizers face a lower effective SALT cost than
# non-itemizers because they can deduct it against federal income tax.
# Variation in itemization status within a SALT bin comes mainly from
# differences in mortgage interest (itemizers have enough total itemizable
# to clear the standard deduction; non-itemizers don't). We ask whether
# this effective-cost difference predicts lower mobility among itemizers.

SALT_BIN_W <- 1000

salt_item_data <- dbGetQuery(con, "
    SELECT
        CAST(imp_salt AS DOUBLE)                         AS imp_salt,
        imp_itemizes_2017,
        CASE WHEN migrate1 IN (2,3,4) THEN 1 ELSE 0 END AS mover,
        year
    FROM acs_microdata
    WHERE pernum = 1 AND gq IN (1, 2, 5) AND migrate1 != 0
      AND imp_salt          IS NOT NULL
      AND imp_itemizes_2017 IS NOT NULL
      AND year BETWEEN 2013 AND 2023
") |>
    as_tibble() |>
    mutate(
        bin_start = floor(imp_salt / SALT_BIN_W) * SALT_BIN_W,
        bin_mid   = bin_start + SALT_BIN_W / 2
    )

SALT_MAX <- round(quantile(salt_item_data$imp_salt, 0.95), -3)

# --- (a) Itemization rate by SALT bin ---
item_rate_by_salt <- salt_item_data |>
    filter(bin_start >= 0, bin_start <= SALT_MAX) |>
    group_by(bin_start, bin_mid) |>
    summarise(pct_itemize = mean(imp_itemizes_2017), n = n(), .groups = "drop")

threshold_10pct <- item_rate_by_salt |>
    filter(pct_itemize >= 0.10) |>
    slice_min(bin_start, n = 1, with_ties = FALSE) |>
    pull(bin_mid)

cat(sprintf("\n10%% itemization threshold: SALT bin mid = $%.0f\n", threshold_10pct))

ggplot(item_rate_by_salt, aes(bin_mid, pct_itemize)) +
    geom_hline(yintercept = 0.10, linetype = "dashed", color = "grey50") +
    geom_vline(xintercept = threshold_10pct, linetype = "dotted", color = "#c06028") +
    geom_line(linewidth = 0.8, color = "#23373b") +
    geom_point(size = 1.5, color = "#23373b") +
    annotate("text", x = threshold_10pct + SALT_BIN_W * 0.4, y = 0.10,
             label = sprintf("$%.0fk", threshold_10pct / 1000),
             vjust = -0.5, hjust = 0, size = 3, color = "#c06028") +
    scale_x_continuous(labels = scales::dollar_format(scale = 1e-3, suffix = "k")) +
    scale_y_continuous(labels = scales::percent_format(1)) +
    labs(
        x        = "SALT (property tax + state income tax, $1k bins)",
        y        = "Share itemizing (2017 rules)",
        title    = "Itemization rate by SALT level",
        subtitle = "Dashed: 10% threshold  •  Dotted: first bin to cross it"
    ) +
    theme(
        panel.background  = element_rect(fill = "#fafafa", color = NA),
        plot.background   = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major  = element_line(color = "grey90"),
        panel.grid.minor  = element_blank(),
        axis.line.x       = element_line(color = "grey30"),
        axis.line.y       = element_line(color = "grey30"),
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.key        = element_rect(fill = "#fafafa", color = NA),
        axis.text.x       = element_text(color = "grey20"),
        axis.text.y       = element_text(color = "grey20"),
        text              = element_text(color = "grey20")
    )
ggsave("../../../output/Cormac/10l_itemization_rate_by_salt.png", last_plot(), width = 8, height = 5, dpi = 150)

# --- (b) Move rate by year × itemization status, faceted by $2k SALT bin ---
# $2k bins keep the number of facets manageable (~8-10 panels).
# Each facet shows whether the itemizer/non-itemizer gap changes around TCJA.

item_colors  <- c("Does not itemize" = "#23373b", "Itemizes" = "#c06028")
SALT_BIN_W2  <- 2000

mover_salt_year <- salt_item_data |>
    mutate(
        bin2_start = floor(imp_salt / SALT_BIN_W2) * SALT_BIN_W2,
        bin2_label = sprintf("$%dk–%dk", bin2_start / 1000,
                             (bin2_start + SALT_BIN_W2) / 1000),
        itemizes   = factor(imp_itemizes_2017, 0:1,
                            labels = c("Does not itemize", "Itemizes"))
    ) |>
    filter(bin2_start >= 0, bin2_start <= SALT_MAX) |>
    group_by(bin2_start, bin2_label, year, itemizes) |>
    summarise(
        share_movers = mean(mover),
        se           = sqrt(share_movers * (1 - share_movers) / n()),
        n            = n(),
        .groups      = "drop"
    ) |>
    filter(n >= 15) |>
    mutate(bin2_label = factor(bin2_label,
                               levels = unique(bin2_label[order(bin2_start)])))

ggplot(mover_salt_year,
       aes(year, share_movers, color = itemizes, fill = itemizes, group = itemizes)) +
    geom_vline(xintercept = 2017.5, linetype = "dashed", color = "grey60") +
    geom_ribbon(aes(ymin = share_movers - 1.96 * se,
                    ymax = share_movers + 1.96 * se),
                alpha = 0.12, color = NA) +
    geom_line(linewidth = 0.7) +
    geom_point(size = 1.3) +
    facet_wrap(~ bin2_label, ncol = 4, scales = "free_y") +
    scale_x_continuous(breaks = seq(2013, 2023, by = 2)) +
    scale_y_continuous(labels = scales::percent_format(1)) +
    scale_color_manual(values = item_colors, name = NULL) +
    scale_fill_manual(values  = item_colors, name = NULL) +
    labs(
        x        = NULL,
        y        = "Share of household heads who moved",
        title    = "Move rate by year: itemizers vs. non-itemizers, by SALT bin",
        subtitle = "Each facet = $2k SALT bin; dashed = TCJA (2018)"
    ) +
    theme(
        panel.background  = element_rect(fill = "#fafafa", color = NA),
        plot.background   = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major  = element_line(color = "grey90"),
        panel.grid.minor  = element_blank(),
        strip.background  = element_rect(fill = "grey92", color = NA),
        strip.text        = element_text(color = "grey20", size = 8),
        axis.line.x       = element_line(color = "grey30"),
        axis.line.y       = element_line(color = "grey30"),
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.key        = element_rect(fill = "#fafafa", color = NA),
        axis.text.x       = element_text(color = "grey20", angle = 45, hjust = 1, size = 7),
        axis.text.y       = element_text(color = "grey20", size = 7),
        text              = element_text(color = "grey20"),
        legend.position   = "bottom"
    )
ggsave("../../../output/Cormac/10l_move_rate_by_salt_bin.png", last_plot(), width = 12, height = 8, dpi = 150)

########################################################
# 11. Move rate by additional federal tax from SALT cap, by period
########################################################

# Households are sorted into four groups by their additional federal tax from
# the SALT cap (imp_additional_fed_tax_salt), with thresholds fixed on the
# full pooled sample. The "no/minimal cost" group (< $500) serves as a
# reference — these households are largely unaffected by the SALT cap.
# The remaining three groups are equal-count terciles among affected households.

tax_cost_data <- dbGetQuery(con, "
    SELECT
        year,
        imp_additional_fed_tax_salt,
        CASE WHEN migrate1 IN (2,3,4) THEN 1 ELSE 0 END AS mover
    FROM acs_microdata
    WHERE pernum = 1 AND gq IN (1, 2, 5) AND migrate1 != 0
      AND imp_additional_fed_tax_salt IS NOT NULL
      AND year BETWEEN 2012 AND 2023
") |> as_tibble()

tax_bin_breaks <- c(500, 2000, 4000, 6000, 8000)
tax_bin_labels <- c("<$500", "$500–2k", "$2k–4k", "$4k–6k", "$6k–8k", "$8k–10k")
tax_bin_colors <- setNames(
    c("#cccccc", RColorBrewer::brewer.pal(5, "YlOrRd")),
    tax_bin_labels
)

tax_cost_data <- tax_cost_data |>
    filter(imp_additional_fed_tax_salt < 10000) |>
    mutate(tax_bin = cut(
        imp_additional_fed_tax_salt,
        breaks = c(-Inf, tax_bin_breaks),
        labels = tax_bin_labels,
        right  = FALSE
    ))

mover_by_tax_bin <- tax_cost_data |>
    group_by(tax_bin, year) |>
    summarise(
        share_movers = mean(mover),
        se           = sqrt(share_movers * (1 - share_movers) / n()),
        n            = n(),
        .groups      = "drop"
    )
 
ggplot(mover_by_tax_bin,
       aes(year, share_movers, color = tax_bin, fill = tax_bin, group = tax_bin)) +
    geom_vline(xintercept = 2017.5, linetype = "dashed", color = "grey60") +
    geom_ribbon(aes(ymin = share_movers - 1.96 * se,
                    ymax = share_movers + 1.96 * se),
                alpha = 0.12, color = NA) +
    geom_line(linewidth = 0.9) +
    geom_point(size = 2) +
    annotate("text", x = 2017.7, y = Inf, label = "TCJA",
             vjust = 1.5, hjust = 0, color = "grey40", size = 3.5) +
    scale_x_continuous(breaks = 2012:2023) +
    scale_y_continuous(labels = scales::percent_format(1)) +
    scale_color_manual(values = tax_bin_colors, name = NULL) +
    scale_fill_manual(values  = tax_bin_colors, name = NULL) +
    labs(
        x        = NULL,
        y        = "Share of household heads who moved",
        title    = "Move rate over time by SALT reform tax cost",
        subtitle = "$2k fixed bins; >$10k grouped together; <$500 = reference (unaffected)"
    ) +
    theme(
        panel.background  = element_rect(fill = "#fafafa", color = NA),
        plot.background   = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major  = element_line(color = "grey90"),
        panel.grid.minor  = element_blank(),
        axis.line.x       = element_line(color = "grey30"),
        axis.line.y       = element_line(color = "grey30"),
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.key        = element_rect(fill = "#fafafa", color = NA),
        axis.text.x       = element_text(color = "grey20", angle = 45, hjust = 1),
        axis.text.y       = element_text(color = "grey20"),
        text              = element_text(color = "grey20"),
        legend.position   = "bottom"
    )
ggsave("../../../output/Cormac/11_move_rate_by_tax_bin.png", last_plot(), width = 9, height = 5, dpi = 150)

########################################################
# 12. Binned DiD event studies: each cost bin vs. <$500 control group
########################################################

# For each of the five cost bins, estimate a separate event study using
# households in that bin and in the <$500 reference group (unaffected by
# the SALT cap). The five event studies are overlaid in a single plot.
# Reference year = 2017. Controls + year FE + state FE; cluster by state.

did_data_binned <- dbGetQuery(con, "
    SELECT
        CAST(imp_additional_fed_tax_salt AS DOUBLE)      AS salt_cost,
        CASE WHEN migrate1 IN (2,3,4) THEN 1 ELSE 0 END AS mover,
        year,
        age, sex, marst, race, hispan, educ,
        metro, multgen, statefip
    FROM acs_microdata
    WHERE pernum = 1 AND gq IN (1, 2, 5) AND migrate1 != 0
      AND imp_additional_fed_tax_salt IS NOT NULL
      AND imp_additional_fed_tax_salt <  10000
      AND year BETWEEN 2013 AND 2023
      AND random() < 0.25
") |>
    as_tibble() |>
    mutate(
        bin = cut(salt_cost,
                  breaks = c(-Inf, 2000, 4000, 6000, 8000, Inf),
                  labels = c("<$2k", "$2k–4k", "$4k–6k", "$6k–8k", "$8k–10k"),
                  right  = FALSE),
        across(c(sex, marst, race, hispan, educ, metro, multgen, statefip), factor)
    )

treatment_bins <- c("$2k–4k", "$4k–6k", "$6k–8k", "$8k–10k")
bin_es_colors  <- setNames(RColorBrewer::brewer.pal(4, "YlOrRd"), treatment_bins)

es_results <- bind_rows(lapply(treatment_bins, function(b) {
    dat <- did_data_binned |>
        filter(bin %in% c("<$2k", b)) |>
        mutate(treated = as.integer(bin == b))

    fit <- feols(
        mover ~ i(year, treated, ref = 2016) + age + sex + marst + race +
            hispan + educ + metro + multgen | year + statefip,
        data = dat, vcov = ~statefip
    )

    tidy(fit) |>
        filter(str_detect(term, "year::")) |>
        mutate(
            year = as.integer(str_extract(term, "\\d{4}")),
            bin  = b
        )
})) |>
    mutate(bin = factor(bin, levels = treatment_bins))

ggplot(es_results,
       aes(year, estimate, color = bin, fill = bin, group = bin)) +
    geom_hline(yintercept = 0, color = "grey70") +
    geom_vline(xintercept = 2017.5, linetype = "dashed", color = "grey50") +
    geom_ribbon(aes(ymin = estimate - 1.96 * std.error,
                    ymax = estimate + 1.96 * std.error),
                alpha = 0.10, color = NA) +
    geom_line(linewidth = 0.9) +
    geom_point(size = 2) +
    annotate("text", x = 2017.7, y = Inf, label = "TCJA",
             vjust = 1.5, hjust = 0, color = "grey40", size = 3.5) +
    scale_x_continuous(breaks = 2013:2023) +
    scale_y_continuous(labels = scales::percent_format(accuracy = 0.01)) +
    scale_color_manual(values = bin_es_colors, name = "SALT reform cost") +
    scale_fill_manual(values  = bin_es_colors, name = "SALT reform cost") +
    labs(
        x        = NULL,
        y        = "Additional probability of moving (pp)",
        title    = "Event study: move probability by SALT reform cost bin",
        subtitle = "Each bin vs. <$500 control group; ref year = 2017; controls + year + state FE"
    ) +
    theme(
        panel.background  = element_rect(fill = "#fafafa", color = NA),
        plot.background   = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major  = element_line(color = "grey90"),
        panel.grid.minor  = element_blank(),
        axis.line.x       = element_line(color = "grey30"),
        axis.line.y       = element_line(color = "grey30"),
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.key        = element_rect(fill = "#fafafa", color = NA),
        axis.text.x       = element_text(color = "grey20", angle = 45, hjust = 1),
        axis.text.y       = element_text(color = "grey20"),
        text              = element_text(color = "grey20"),
        legend.position   = "bottom"
    )
ggsave("../../../output/Cormac/12_event_study_by_cost_bin.png", last_plot(), width = 9, height = 5, dpi = 150)

########################################################
# 13. Move rate by itemizable income (2017 rules), by year
########################################################

ITEM_BIN_W <- 2000

move_by_item_raw <- dbGetQuery(con, "
    SELECT
        CAST(imp_itemizable_2017 AS DOUBLE) AS imp_itemizable_2017,
        CASE WHEN migrate1 IN (2,3,4) THEN 1 ELSE 0 END AS mover,
        year
    FROM acs_microdata
    WHERE pernum = 1 AND gq IN (1, 2, 5) AND migrate1 != 0
      AND imp_itemizable_2017 IS NOT NULL
      AND year BETWEEN 2013 AND 2023
") |>
    as_tibble() |>
    mutate(
        bin_start = floor(imp_itemizable_2017 / ITEM_BIN_W) * ITEM_BIN_W,
        bin_mid   = bin_start + ITEM_BIN_W / 2
    )

ITEM_MAX <- round(quantile(move_by_item_raw$imp_itemizable_2017, 0.95, na.rm = TRUE), -3)

move_by_item_yr <- move_by_item_raw |>
    filter(bin_start >= 0, bin_start <= ITEM_MAX) |>
    group_by(year, bin_start, bin_mid) |>
    summarise(
        share_movers = mean(mover),
        n            = n(),
        .groups      = "drop"
    ) |>
    filter(n >= 30) |>
    mutate(year = factor(year))

year_colors_13 <- setNames(
    colorRampPalette(c("#2166ac", "#74add1", "#abd9e9", "#e0f3f8",
                       "#ffffbf", "#fee090", "#fdae61", "#f46d43",
                       "#d73027", "#a50026", "#67001f"))(11),
    as.character(2013:2023)
)

ggplot(move_by_item_yr,
       aes(bin_mid, share_movers, color = year, group = year)) +
    geom_vline(xintercept = 6350,  linetype = "dotted", color = "grey50", linewidth = 0.5) +
    geom_vline(xintercept = 12700, linetype = "dashed", color = "grey40", linewidth = 0.6) +
    geom_line(linewidth = 0.7, alpha = 0.85) +
    geom_point(size = 1.2) +
    annotate("text", x = 6350  + 400, y = Inf, label = "Single std ded\n($6,350)",
             vjust = 1.4, hjust = 0, size = 2.8, color = "grey40") +
    annotate("text", x = 12700 + 400, y = Inf, label = "MFJ std ded\n($12,700)",
             vjust = 1.4, hjust = 0, size = 2.8, color = "grey40") +
    scale_x_continuous(labels = scales::dollar_format(scale = 1e-3, suffix = "k")) +
    scale_y_continuous(labels = scales::percent_format(1)) +
    scale_color_manual(values = year_colors_13, name = "Year") +
    labs(
        x        = paste0("Itemizable income, 2017 rules ($",
                          ITEM_BIN_W / 1000, "k bins)"),
        y        = "Share of household heads who moved",
        title    = "Move rate by itemizable income (2017 rules), by year",
        subtitle = "Dotted = Single std ded ($6,350)  •  Dashed = MFJ std ded ($12,700)"
    ) +
    theme(
        panel.background  = element_rect(fill = "#fafafa", color = NA),
        plot.background   = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major  = element_line(color = "grey90"),
        panel.grid.minor  = element_blank(),
        axis.line.x       = element_line(color = "grey30"),
        axis.line.y       = element_line(color = "grey30"),
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.key        = element_rect(fill = "#fafafa", color = NA),
        axis.text.x       = element_text(color = "grey20"),
        axis.text.y       = element_text(color = "grey20"),
        text              = element_text(color = "grey20"),
        legend.position   = "right"
    )
ggsave("../../../output/Cormac/13_move_rate_by_itemizable_income.png", last_plot(), width = 10, height = 6, dpi = 150)

########################################################
# 14. Pre-post TCJA first differences by demographic cell
########################################################

# In a repeated cross-section, first differences can only be estimated at
# the group level. Strategy: compute cell × period mean move rates in SQL
# (averaging 2013-2017 and 2018-2023 separately), first-difference post−pre
# within each cell, then WLS on demographic characteristics weighted by
# harmonic mean of pre/post cell sizes. Averaging over 5+ years per period
# greatly reduces sampling noise vs. year-over-year FDs.

cell_pp_14 <- dbGetQuery(con, "
    SELECT
        CASE WHEN year <= 2017 THEN 'pre' ELSE 'post' END AS period,
        CASE
            WHEN age BETWEEN 18 AND 29 THEN '18-29'
            WHEN age BETWEEN 30 AND 39 THEN '30-39'
            WHEN age BETWEEN 40 AND 49 THEN '40-49'
            WHEN age BETWEEN 50 AND 64 THEN '50-64'
            ELSE '65+'
        END AS age_grp,
        CASE WHEN sex      = 2       THEN 1 ELSE 0 END AS female,
        CASE WHEN marst    = 1       THEN 1 ELSE 0 END AS married,
        CASE WHEN educ     >= 10     THEN 1 ELSE 0 END AS college,
        CASE WHEN nchild   > 0       THEN 1 ELSE 0 END AS has_child,
        CASE WHEN ownershp = 1       THEN 1 ELSE 0 END AS owns_home,
        CASE WHEN metro    IN (1, 2) THEN 1 ELSE 0 END AS in_metro,
        CASE
            WHEN race = 1        THEN 'White'
            WHEN race = 2        THEN 'Black'
            WHEN race IN (4,5,6) THEN 'Asian'
            ELSE 'Other'
        END AS race_grp,
        CASE
            WHEN CAST(hhincome AS DOUBLE) <  25000 THEN '<$25k'
            WHEN CAST(hhincome AS DOUBLE) <  50000 THEN '$25-50k'
            WHEN CAST(hhincome AS DOUBLE) < 100000 THEN '$50-100k'
            WHEN CAST(hhincome AS DOUBLE) < 200000 THEN '$100-200k'
            ELSE '$200k+'
        END AS inc_grp,
        AVG(CASE WHEN migrate1 IN (2,3,4) THEN 1.0 ELSE 0.0 END) AS move_rate,
        COUNT(*) AS n
    FROM acs_microdata
    WHERE pernum = 1 AND gq IN (1, 2, 5) AND migrate1 != 0
      AND hhincome != 9999999
      AND age BETWEEN 18 AND 100
      AND year BETWEEN 2013 AND 2023
    GROUP BY period, age_grp, female, married, college, has_child,
             owns_home, in_metro, race_grp, inc_grp
") |> as_tibble()

cell_fd_14 <- cell_pp_14 |>
    pivot_wider(names_from = period, values_from = c(move_rate, n),
                names_sep = "_") |>
    filter(!is.na(move_rate_pre), !is.na(move_rate_post)) |>
    mutate(
        delta  = move_rate_post - move_rate_pre,
        w_harm = 2 * n_pre * n_post / (n_pre + n_post),
        age_grp  = factor(age_grp, c("18-29","30-39","40-49","50-64","65+")),
        race_grp = relevel(factor(race_grp), ref = "White"),
        inc_grp  = factor(inc_grp,
                          c("<$25k","$25-50k","$50-100k","$100-200k","$200k+"))
    )

cat(sprintf("\nPre-post FD dataset: %d cells\n", nrow(cell_fd_14)))
cat(sprintf("  Mean pre move rate:   %.3f\n", weighted.mean(cell_fd_14$move_rate_pre,  cell_fd_14$n_pre)))
cat(sprintf("  Mean post move rate:  %.3f\n", weighted.mean(cell_fd_14$move_rate_post, cell_fd_14$n_post)))
cat(sprintf("  Mean delta:           %.4f\n", weighted.mean(cell_fd_14$delta, cell_fd_14$w_harm)))

# WLS: outcome = post - pre move rate, predictors = cell demographics, HC1 SEs
fit_14 <- feols(
    delta ~ age_grp + female + married + college + has_child +
            owns_home + in_metro + race_grp + inc_grp,
    data    = cell_fd_14,
    weights = ~w_harm,
    vcov    = "HC1"
)

cat("\nSection 14 WLS (pre-post FD ~ demographics):\n")
print(summary(fit_14))

label_map_14 <- c(
    "age_grp30-39"    = "Age: 30-39",
    "age_grp40-49"    = "Age: 40-49",
    "age_grp50-64"    = "Age: 50-64",
    "age_grp65+"      = "Age: 65+",
    "female"          = "Female",
    "married"         = "Married",
    "college"         = "College graduate",
    "has_child"       = "Has children",
    "owns_home"       = "Owns home",
    "in_metro"        = "Metro area",
    "race_grpBlack"   = "Race: Black",
    "race_grpAsian"   = "Race: Asian",
    "race_grpOther"   = "Race: Other",
    "inc_grp$25-50k"  = "Income: $25-50k",
    "inc_grp$50-100k" = "Income: $50-100k",
    "inc_grp$100-200k"= "Income: $100-200k",
    "inc_grp$200k+"   = "Income: $200k+"
)

coef_df_14 <- tidy(fit_14) |>
    filter(term != "(Intercept)") |>
    mutate(
        conf.low  = estimate - 1.96 * std.error,
        conf.high = estimate + 1.96 * std.error,
        label     = coalesce(label_map_14[term], term),
        sig       = if_else(abs(estimate) > 1.96 * std.error, "p < 0.05", "p ≥ 0.05")
    )

ggplot(coef_df_14,
       aes(x = estimate, y = fct_reorder(label, estimate), color = sig)) +
    geom_vline(xintercept = 0, color = "grey40", linewidth = 0.4) +
    geom_errorbarh(aes(xmin = conf.low, xmax = conf.high),
                   height = 0.25, linewidth = 0.6) +
    geom_point(size = 2.5) +
    scale_color_manual(
        values = c("p < 0.05" = "#23373b", "p ≥ 0.05" = "grey65"),
        name = NULL
    ) +
    scale_x_continuous(labels = scales::percent_format(accuracy = 0.1)) +
    labs(
        x        = "Post-TCJA minus pre-TCJA move rate (pp)",
        y        = NULL,
        title    = "Pre-post TCJA change in move rate by demographic cell",
        subtitle = paste0(
            "WLS, weighted by harmonic mean of pre/post cell size | HC1 SEs\n",
            "Ref: Age 18-29, Male, Not married, No college, No children, ",
            "Rents, Non-metro, White, Income <$25k"
        )
    ) +
    theme(
        panel.background  = element_rect(fill = "#fafafa", color = NA),
        plot.background   = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major  = element_line(color = "grey90"),
        panel.grid.minor  = element_blank(),
        axis.line.x       = element_line(color = "grey30"),
        axis.line.y       = element_line(color = "grey30"),
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.key        = element_rect(fill = "#fafafa", color = NA),
        axis.text.x       = element_text(color = "grey20"),
        axis.text.y       = element_text(color = "grey20"),
        text              = element_text(color = "grey20"),
        legend.position   = "bottom"
    )
ggsave("../../../output/Cormac/14_prepost_fd_demographics.png", last_plot(), width = 9, height = 7, dpi = 150)

########################################################
# 15. Move rate vs. itemizable income: most vs. least mobile demographic
########################################################

# For each of several single-variable demographic cuts, compute move rate by
# $2k itemizable-income bin in one SQL pass (CTE + UNION ALL). Identify the
# group with the highest and lowest overall move rate across all cuts, then
# plot those two lines plus the pooled average.

item_by_demo_15 <- dbGetQuery(con, "
    WITH binned AS (
        SELECT
            CASE WHEN migrate1 IN (2,3,4) THEN 1.0 ELSE 0.0 END AS mover,
            FLOOR(CAST(imp_itemizable_2017 AS DOUBLE) / 2000) * 2000 AS bin_start,
            age, marst, ownershp, educ, sex, nchild, metro
        FROM acs_microdata
        WHERE pernum = 1 AND gq IN (1, 2, 5) AND migrate1 != 0
          AND imp_itemizable_2017 IS NOT NULL
          AND CAST(imp_itemizable_2017 AS DOUBLE) >= 0
          AND year BETWEEN 2013 AND 2023
    )
    SELECT grp, bin_start, AVG(mover) AS move_rate, COUNT(*) AS n
    FROM (
        SELECT 'Overall' AS grp, bin_start, mover FROM binned
        UNION ALL
        SELECT CASE WHEN age BETWEEN 18 AND 29 THEN 'Age: 18-29'
                    WHEN age BETWEEN 30 AND 39 THEN 'Age: 30-39'
                    WHEN age BETWEEN 40 AND 49 THEN 'Age: 40-49'
                    WHEN age BETWEEN 50 AND 64 THEN 'Age: 50-64'
                    ELSE 'Age: 65+' END,                                      bin_start, mover FROM binned
        UNION ALL
        SELECT CASE WHEN marst    = 1       THEN 'Married'      ELSE 'Not married'  END, bin_start, mover FROM binned
        UNION ALL
        SELECT CASE WHEN ownershp = 1       THEN 'Owns home'    ELSE 'Rents'        END, bin_start, mover FROM binned
        UNION ALL
        SELECT CASE WHEN educ     >= 10     THEN 'College+'     ELSE 'No college'   END, bin_start, mover FROM binned
        UNION ALL
        SELECT CASE WHEN sex      = 2       THEN 'Female'       ELSE 'Male'         END, bin_start, mover FROM binned
        UNION ALL
        SELECT CASE WHEN nchild   > 0       THEN 'Has children' ELSE 'No children'  END, bin_start, mover FROM binned
        UNION ALL
        SELECT CASE WHEN metro    IN (1, 2) THEN 'Metro'        ELSE 'Non-metro'    END, bin_start, mover FROM binned
    ) t
    GROUP BY grp, bin_start
") |> as_tibble()

# Overall move rate per group — used to rank mobility
group_ranks_15 <- item_by_demo_15 |>
    filter(grp != "Overall") |>
    group_by(grp) |>
    summarise(overall_rate = weighted.mean(move_rate, n), .groups = "drop") |>
    arrange(desc(overall_rate))

cat("\nOverall move rates by demographic group (2013-2023):\n")
print(as.data.frame(group_ranks_15 |> mutate(overall_rate = round(100 * overall_rate, 2))))

most_mobile_15  <- group_ranks_15$grp[1]
least_mobile_15 <- group_ranks_15$grp[nrow(group_ranks_15)]
cat(sprintf("\nMost mobile:  %s (%.1f%%)\n", most_mobile_15,
            100 * group_ranks_15$overall_rate[1]))
cat(sprintf("Least mobile: %s (%.1f%%)\n\n", least_mobile_15,
            100 * tail(group_ranks_15$overall_rate, 1)))

# Approximate 95th-percentile of itemizable income from the Overall bin distribution
ITEM_MAX_15 <- item_by_demo_15 |>
    filter(grp == "Overall") |>
    arrange(bin_start) |>
    mutate(cum_pct = cumsum(n) / sum(n)) |>
    filter(cum_pct >= 0.95) |>
    slice_head(n = 1) |>
    pull(bin_start)

plot_data_15 <- item_by_demo_15 |>
    filter(grp %in% c(most_mobile_15, "Overall", least_mobile_15),
           bin_start <= ITEM_MAX_15,
           n >= 30) |>
    mutate(
        bin_mid = bin_start + 1000,
        group   = factor(grp,
                         levels = c(most_mobile_15, "Overall", least_mobile_15))
    )

grp_colors_15 <- setNames(c("#c06028", "grey40", "#2c5f8a"),
                           c(most_mobile_15, "Overall", least_mobile_15))
grp_ltypes_15 <- setNames(c("solid", "dashed", "solid"),
                           c(most_mobile_15, "Overall", least_mobile_15))

ggplot(plot_data_15,
       aes(bin_mid, move_rate, color = group, linetype = group, group = group)) +
    geom_vline(xintercept = 6350,  linetype = "dotted", color = "grey55",
               linewidth = 0.5) +
    geom_vline(xintercept = 12700, linetype = "dotted", color = "grey55",
               linewidth = 0.5) +
    geom_line(linewidth = 0.85) +
    geom_point(size = 1.8) +
    annotate("text", x = 6350  + 400, y = Inf, label = "Single\nstd ded",
             vjust = 1.3, hjust = 0, size = 2.7, color = "grey45") +
    annotate("text", x = 12700 + 400, y = Inf, label = "MFJ\nstd ded",
             vjust = 1.3, hjust = 0, size = 2.7, color = "grey45") +
    scale_x_continuous(labels = scales::dollar_format(scale = 1e-3, suffix = "k")) +
    scale_y_continuous(labels = scales::percent_format(1)) +
    scale_color_manual(values = grp_colors_15, name = NULL) +
    scale_linetype_manual(values = grp_ltypes_15, name = NULL) +
    labs(
        x        = "Itemizable income, 2017 rules ($2k bins)",
        y        = "Share of household heads who moved",
        title    = "Move rate by itemizable income: most vs. least mobile demographic",
        subtitle = sprintf(
            "Most mobile: %s  |  Least mobile: %s  |  Overall avg dashed  |  pooled 2013–2023",
            most_mobile_15, least_mobile_15
        )
    ) +
    theme(
        panel.background  = element_rect(fill = "#fafafa", color = NA),
        plot.background   = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major  = element_line(color = "grey90"),
        panel.grid.minor  = element_blank(),
        axis.line.x       = element_line(color = "grey30"),
        axis.line.y       = element_line(color = "grey30"),
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.key        = element_rect(fill = "#fafafa", color = NA),
        axis.text.x       = element_text(color = "grey20"),
        axis.text.y       = element_text(color = "grey20"),
        text              = element_text(color = "grey20"),
        legend.position   = "bottom"
    )
ggsave("../../../output/Cormac/15_move_rate_item_income_het.png",
       last_plot(), width = 9, height = 5, dpi = 150)

dbDisconnect(con, shutdown = TRUE)




