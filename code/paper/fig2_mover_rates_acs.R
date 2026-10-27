# Formerly code/paper/fig2_acs.R (moved in the 2026-10-27 restructuring).
#!/usr/bin/env Rscript

# ACS Figure 2: mover rates by itemizable-expense quantile bands (MFJ + singles).
# Run from the repo root: Rscript code/paper/fig2_acs.R

gc()
rm(list = ls())

library(duckdb)
library(DBI)
library(dplyr)
library(ggplot2)
library(scales)
library(glue)
library(tibble)
library(fixest)
library(broom)
library(cli)

source("code/R/paths.R")
DB_PATH <- file.path(DATA, "database.db")
OUTPUT_DIR <- file.path(ROOT, "output/paper/fig2")
BAND_W <- 0.03
PRE_YEARS <- 2011:2016

CONTROL_VARS <- c("age", "bedrooms", "nchild", "rooms", "yrmarr", "nfams")
MISSING_NUMERIC <- c(9999999L, 9999998L, 9999997L, 999999L, 99999L, 9999L)

clean_control <- function(x, var_name) {
  x <- if (is.factor(x)) as.vector(x) else x
  num <- suppressWarnings(as.numeric(x))
  if (!all(is.na(num))) {
    x <- num
    x[x %in% MISSING_NUMERIC] <- NA_real_
    if (var_name %in% c("hhincome", "valueh", "inctot", "ftotinc", "incwage")) {
      x[x >= 9999990] <- NA_real_
    }
  }
  x
}

prepare_controls <- function(dat, control_vars) {
  out <- dat
  for (v in control_vars) {
    x <- as.numeric(clean_control(out[[v]], v))
    med <- stats::median(x, na.rm = TRUE)
    if (!is.finite(med)) {
      med <- 0
    }
    x[is.na(x)] <- med
    out[[v]] <- x
  }
  out
}

build_twfe_formula <- function(control_vars) {
  rhs_controls <- paste(control_vars, collapse = " + ")
  stats::as.formula(glue::glue(
    "mover ~ i(year, treated, ref = 2017) + {rhs_controls} | statefip^year + treated"
  ))
}

TWFE_FML <- build_twfe_formula(CONTROL_VARS)

band_levels <- function(n_bands) {
  c(
    paste0("q-", rev(seq_len(n_bands))),
    paste0("q+", seq_len(n_bands))
  )
}

assign_quantile_groups <- function(pctl, p_anchor, n_bands, band_w) {
  out <- rep(NA_character_, length(pctl))
  for (k in seq_len(n_bands)) {
    lo <- p_anchor - k * band_w
    hi <- p_anchor - (k - 1L) * band_w
    out[pctl >= lo & pctl < hi] <- paste0("q-", k)
  }
  for (k in seq_len(n_bands)) {
    lo <- p_anchor + (k - 1L) * band_w
    hi <- p_anchor + k * band_w
    out[pctl >= lo & pctl < hi] <- paste0("q+", k)
  }
  factor(out, levels = band_levels(n_bands))
}

compute_p_anchor_2017 <- function(dat, std_ded_2017) {
  s2017 <- dat |>
    dplyr::filter(year == 2017L)
  mean(s2017$imp_itemizable_2017 <= std_ded_2017, na.rm = TRUE)
}

pull_acs_sample <- function(con, sql_where) {
  control_cols <- paste(CONTROL_VARS, collapse = ", ")
  q <- glue::glue("
    SELECT
      year,
      statefip,
      migrate1,
      CAST(imp_itemizable_2017 AS DOUBLE) AS imp_itemizable_2017,
      {control_cols}
    FROM acs_microdata
    WHERE pernum = 1
      AND gq IN (1, 2, 5)
      AND {sql_where}
      AND migrate1 != 0
      AND imp_itemizable_2017 IS NOT NULL
  ")
  DBI::dbGetQuery(con, q) |>
    tibble::as_tibble() |>
    prepare_controls(CONTROL_VARS)
}

summarize_bins_by_year <- function(dat) {
  dat |>
    dplyr::group_by(year, group) |>
    dplyr::summarise(
      mover_rate = mean(mover, na.rm = TRUE),
      n = dplyr::n(),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      se = sqrt(mover_rate * (1 - mover_rate) / n),
      ci_lo = pmax(0, mover_rate - 1.96 * se),
      ci_hi = pmin(1, mover_rate + 1.96 * se)
    )
}

summarize_tc_by_year <- function(dat, control_bands, treat_bands) {
  dat |>
    dplyr::filter(group %in% c(control_bands, treat_bands)) |>
    dplyr::mutate(
      tc_group = dplyr::if_else(
        group %in% treat_bands,
        "Treated",
        "Control"
      )
    ) |>
    dplyr::group_by(year, tc_group) |>
    dplyr::summarise(
      mover_rate = mean(mover, na.rm = TRUE),
      n = dplyr::n(),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      se = sqrt(mover_rate * (1 - mover_rate) / n),
      ci_lo = pmax(0, mover_rate - 1.96 * se),
      ci_hi = pmin(1, mover_rate + 1.96 * se)
    )
}

plot_all_bands <- function(
    summary_df,
    title,
    subtitle,
    png_path,
    csv_path
) {
  p <- ggplot2::ggplot(
    summary_df,
    ggplot2::aes(
      x = year,
      y = mover_rate,
      color = group,
      fill = group,
      group = group
    )
  ) +
    ggplot2::geom_ribbon(
      ggplot2::aes(ymin = ci_lo, ymax = ci_hi),
      alpha = 0.15,
      color = NA
    ) +
    ggplot2::geom_line(linewidth = 0.9) +
    ggplot2::geom_point(size = 1.7) +
    ggplot2::geom_vline(
      xintercept = 2017.5,
      linetype = "dashed",
      color = "grey30"
    ) +
    ggplot2::scale_color_viridis_d(
      option = "C",
      direction = 1,
      name = "Quantile band"
    ) +
    ggplot2::scale_fill_viridis_d(
      option = "C",
      direction = 1,
      guide = "none"
    ) +
    ggplot2::scale_x_continuous(breaks = scales::pretty_breaks()) +
    ggplot2::scale_y_continuous(
      labels = scales::percent_format(accuracy = 0.1)
    ) +
    ggplot2::labs(
      x = NULL,
      y = "Mover rate",
      title = title,
      subtitle = subtitle
    ) +
    ggplot2::theme_minimal(base_size = 11) +
    ggplot2::theme(
      panel.grid.minor = ggplot2::element_blank(),
      legend.position = "right"
    )

  ggplot2::ggsave(png_path, p, width = 8.5, height = 5.5, dpi = 150)
  utils::write.csv(summary_df, csv_path, row.names = FALSE)
}

plot_tc_summary <- function(
    summary_df,
    title,
    subtitle,
    png_path,
    csv_path
) {
  p <- ggplot2::ggplot(
    summary_df,
    ggplot2::aes(
      x = year,
      y = mover_rate,
      color = tc_group,
      fill = tc_group,
      group = tc_group
    )
  ) +
    ggplot2::geom_ribbon(
      ggplot2::aes(ymin = ci_lo, ymax = ci_hi),
      alpha = 0.15,
      color = NA
    ) +
    ggplot2::geom_line(linewidth = 0.9) +
    ggplot2::geom_point(size = 1.7) +
    ggplot2::geom_vline(
      xintercept = 2017.5,
      linetype = "dashed",
      color = "grey30"
    ) +
    ggplot2::scale_color_manual(
      name = NULL,
      values = c("Control" = "#23373b", "Treated" = "#eb811b")
    ) +
    ggplot2::scale_fill_manual(
      guide = "none",
      values = c("Control" = "#23373b", "Treated" = "#eb811b")
    ) +
    ggplot2::scale_x_continuous(breaks = scales::pretty_breaks()) +
    ggplot2::scale_y_continuous(
      labels = scales::percent_format(accuracy = 0.1)
    ) +
    ggplot2::labs(
      x = NULL,
      y = "Mover rate",
      title = title,
      subtitle = subtitle
    ) +
    ggplot2::theme_minimal(base_size = 11) +
    ggplot2::theme(
      panel.grid.minor = ggplot2::element_blank(),
      legend.position = "right"
    )

  ggplot2::ggsave(png_path, p, width = 8.5, height = 5.5, dpi = 150)
  utils::write.csv(summary_df, csv_path, row.names = FALSE)
}

extract_wald_stats <- function(w) {
  as_scalar_num <- function(x) {
    if (is.null(x)) {
      return(NA_real_)
    }
    if (is.list(x) && length(x) >= 1) {
      x <- x[[1]]
    }
    suppressWarnings(as.numeric(x))[1]
  }
  if (!is.list(w)) {
    return(c(stat = NA_real_, pval = NA_real_))
  }
  stat <- as_scalar_num(w$stat)
  if (!is.finite(stat)) {
    stat <- as_scalar_num(w$statistic)
  }
  pval <- as_scalar_num(w$`p-value`)
  if (!is.finite(pval)) {
    pval <- as_scalar_num(w$p)
  }
  c(stat = stat, pval = pval)
}

run_f_band_grid <- function(acs, max_band, twfe_fml, pre_years) {
  Fmat <- matrix(
    NA_real_,
    nrow = max_band,
    ncol = max_band,
    dimnames = list(paste0("L", seq_len(max_band)), paste0("U", seq_len(max_band)))
  )
  Pmat <- Fmat
  hyp <- paste0("year::", pre_years, ":treated")

  for (L in seq_len(max_band)) {
    control <- paste0("q-", L:1)
    for (U in seq_len(max_band)) {
      treat <- paste0("q+", seq_len(U))
      dat_lu <- acs |>
        dplyr::filter(group %in% c(control, treat)) |>
        dplyr::mutate(treated = as.integer(group %in% treat))

      mod_lu <- fixest::feols(twfe_fml, data = dat_lu)
      w <- tryCatch(
        suppressWarnings(suppressMessages(fixest::wald(mod_lu, hyp))),
        error = function(e) NULL
      )
      stats <- extract_wald_stats(w)
      if (is.finite(stats["stat"])) {
        Fmat[L, U] <- stats["stat"]
      }
      if (is.finite(stats["pval"])) {
        Pmat[L, U] <- stats["pval"]
      }
    }
  }
  list(Fmat = Fmat, Pmat = Pmat)
}

run_filing_group <- function(spec, con) {
  prefix <- glue::glue("fig2_{spec$id}")
  cli::cli_h2("Figure 2 ACS: {spec$label}")

  acs <- pull_acs_sample(con, spec$sql_where)

  if (!is.null(spec$p_anchor_fixed)) {
    p_anchor <- spec$p_anchor_fixed
  } else {
    p_anchor <- compute_p_anchor_2017(acs, spec$std_ded_2017)
    s2017 <- acs |> dplyr::filter(year == 2017L)
    anchor_dollar <- stats::quantile(
      s2017$imp_itemizable_2017,
      probs = p_anchor,
      na.rm = TRUE,
      type = 1
    )
    cli::cli_alert_info(
      paste0(
        "2017 anchor: $", spec$std_ded_2017, " std ded = ",
        round(100 * p_anchor, 2), "th pct (dollar pctl = $",
        round(anchor_dollar), ")"
      )
    )
    utils::write.csv(
      tibble::tibble(
        filing_status = spec$label,
        std_deduction_2017 = spec$std_ded_2017,
        p_anchor = p_anchor,
        itemizable_at_anchor_q = as.numeric(anchor_dollar),
        n_2017 = nrow(s2017)
      ),
      file.path(OUTPUT_DIR, glue::glue("{prefix}_anchor.csv")),
      row.names = FALSE
    )
  }

  acs <- acs |>
    dplyr::group_by(year) |>
    dplyr::mutate(pctl = dplyr::percent_rank(imp_itemizable_2017)) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      year = as.integer(year),
      statefip = as.integer(statefip),
      mover = as.integer(migrate1 %in% c(2L, 3L, 4L)),
      group = assign_quantile_groups(
        pctl,
        p_anchor,
        spec$n_bands,
        spec$band_w
      )
    ) |>
    dplyr::filter(!is.na(group))

  # 1. All-band line plot
  bin_summary <- summarize_bins_by_year(acs)
  plot_all_bands(
    bin_summary,
    title = glue::glue(
      "Mover rate by year and itemizable-expense quantile band ({spec$label})"
    ),
    subtitle = glue::glue(
      "Bands are {scales::percent(spec$band_w, accuracy = 0.1)} wide, ",
      "centered at the {spec$anchor_subtitle}; ",
      "within-year percentiles; 95% CIs (normal approx.)"
    ),
    png_path = file.path(OUTPUT_DIR, glue::glue("{prefix}_all_bands.png")),
    csv_path = file.path(OUTPUT_DIR, glue::glue("{prefix}_bin_summary.csv"))
  )

  # 2. Treatment vs control (pre-TWFE descriptive)
  tc_bands <- c(spec$twfe_control_bands, spec$twfe_treat_bands)
  tc_summary <- summarize_tc_by_year(
    acs,
    control_bands = spec$twfe_control_bands,
    treat_bands = spec$twfe_treat_bands
  )
  plot_tc_summary(
    tc_summary,
    title = glue::glue("{spec$label} mover rate: treatment vs control (TWFE sample)"),
    subtitle = glue::glue(
      "Control: {paste(spec$twfe_control_bands, collapse = ', ')}; ",
      "treated: {paste(spec$twfe_treat_bands, collapse = ', ')}; ",
      "{spec$anchor_subtitle}; 95% CIs"
    ),
    png_path = file.path(OUTPUT_DIR, glue::glue("{prefix}_treatment_control.png")),
    csv_path = file.path(
      OUTPUT_DIR,
      glue::glue("{prefix}_treatment_control_summary.csv")
    )
  )

  # 3. Main TWFE
  twfe_data <- acs |>
    dplyr::filter(group %in% tc_bands) |>
    dplyr::mutate(
      treated = as.integer(group %in% spec$twfe_treat_bands)
    )

  twfe_fit <- fixest::feols(TWFE_FML, data = twfe_data)
  twfe_table <- broom::tidy(twfe_fit, conf.int = TRUE)
  utils::write.csv(
    twfe_table,
    file.path(OUTPUT_DIR, glue::glue("twfe_{spec$id}_coefs.csv")),
    row.names = FALSE
  )

  # 4. F-stat matrix (lower q-L..q-1 vs upper q+1..q+U)
  grid <- run_f_band_grid(
    acs,
    max_band = spec$max_band_grid,
    twfe_fml = TWFE_FML,
    pre_years = PRE_YEARS
  )
  utils::write.csv(
    grid$Fmat,
    file.path(OUTPUT_DIR, glue::glue("twfe_{spec$id}_Fmat_2012_2016.csv")),
    row.names = TRUE
  )
  utils::write.csv(
    grid$Pmat,
    file.path(OUTPUT_DIR, glue::glue("twfe_{spec$id}_Pmat_2012_2016.csv")),
    row.names = TRUE
  )

  invisible(list(
    id = spec$id,
    p_anchor = p_anchor,
    n_obs = nrow(acs),
    outputs = c(
      glue::glue("{prefix}_all_bands.png"),
      glue::glue("{prefix}_bin_summary.csv"),
      glue::glue("{prefix}_treatment_control.png"),
      glue::glue("{prefix}_treatment_control_summary.csv"),
      glue::glue("twfe_{spec$id}_coefs.csv"),
      glue::glue("twfe_{spec$id}_Fmat_2012_2016.csv"),
      glue::glue("twfe_{spec$id}_Pmat_2012_2016.csv")
    )
  ))
}

MFJ_SPEC <- list(
  id = "mfj",
  label = "MFJ",
  sql_where = "marst = 1",
  p_anchor_fixed = 0.707482,
  std_ded_2017 = 12700L,
  n_bands = 5L,
  band_w = BAND_W,
  max_band_grid = 6L,
  twfe_control_bands = c("q-2", "q-1"),
  twfe_treat_bands = c("q+1", "q+2"),
  anchor_subtitle = "70.7th percentile anchor (2017 MFJ $12,700 std ded)"
)

SINGLES_SPEC <- list(
  id = "singles",
  label = "single filers",
  sql_where = "marst != 1 AND COALESCE(nchild, 0) = 0",
  p_anchor_fixed = NULL,
  std_ded_2017 = 6350L,
  n_bands = 4L,
  band_w = BAND_W,
  max_band_grid = 4L,
  twfe_control_bands = c("q-2", "q-1"),
  twfe_treat_bands = c("q+1", "q+2", "q+3", "q+4"),
  anchor_subtitle = "2017 single std-ded anchor percentile"
)

if (!dir.exists(OUTPUT_DIR)) {
  dir.create(OUTPUT_DIR, recursive = TRUE)
}

con <- DBI::dbConnect(
  duckdb::duckdb(),
  dbdir = DB_PATH,
  read_only = TRUE
)

results <- list(
  mfj = run_filing_group(MFJ_SPEC, con),
  singles = run_filing_group(SINGLES_SPEC, con)
)

DBI::dbDisconnect(con, shutdown = TRUE)

cli::cli_alert_success("Wrote outputs to {OUTPUT_DIR}")
for (res in results) {
  cli::cli_alert_info("{res$id}: p_anchor = {round(res$p_anchor, 6)}, n = {res$n_obs}")
}
