# code/R/theme.R -- ggplot defaults (loaded by code/R/paths.R). Scripts so far call theme_minimal(base_size = 8..11)
# inline; new figures use theme_ptax() and the side/period colours below so they look alike.
theme_ptax <- function(base_size = 10) {
  ggplot2::theme_minimal(base_size = base_size) +
    ggplot2::theme(panel.grid.minor = ggplot2::element_blank(), legend.position = "bottom", plot.title.position = "plot")
}
COL_SIDE <- c(high = "#b2182b", low = "#2166ac")          # higher- vs lower-tax side of a border
COL_PERIOD <- c(pre = "grey55", post = "#b2182b")           # before / after the 2017 cap
