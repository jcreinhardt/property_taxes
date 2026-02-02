calibrate_lognormal_from_percentiles <- function(p5, p95, q5 = 0.05, q95 = 0.95) {
  if (!is.numeric(p5) || !is.numeric(p95) || p5 <= 0 || p95 <= 0 || p95 <= p5) {
    stop("p5 and p95 must be positive with p95 > p5.")
  }
  z5 <- qnorm(q5)
  z95 <- qnorm(q95)
  sigma <- (log(p95) - log(p5)) / (z95 - z5)
  mu <- log(p5) - sigma * z5
  list(mu = mu, sigma = sigma)
}

draw_wages <- function(n, mu, sigma, seed = NULL) {
  if (!is.null(seed)) {
    set.seed(seed)
  }
  rlnorm(n, meanlog = mu, sdlog = sigma)
}

labor_supply_isoelastic <- function(w, tau, epsilon) {
  if (any(tau < 0 | tau >= 1)) {
    stop("tau must be in [0, 1).")
  }
  if (epsilon <= 0) {
    stop("epsilon must be positive.")
  }
  ((1 - tau) * w) ^ epsilon
}

labor_disutility_isoelastic <- function(l, epsilon) {
  l ^ (1 + 1 / epsilon) / (1 + 1 / epsilon)
}

simulate_model <- function(n,
                           tau,
                           T,
                           acs_p5,
                           acs_p95,
                           epsilon = 0.5,
                           seed = 123) {
  if (!is.numeric(n) || n <= 0) {
    stop("n must be a positive integer.")
  }
  params <- calibrate_lognormal_from_percentiles(acs_p5, acs_p95)
  w <- draw_wages(n, params$mu, params$sigma, seed = seed)

  l <- labor_supply_isoelastic(w, tau, epsilon)
  y <- w * l
  c <- T + (1 - tau) * y
  u <- c - labor_disutility_isoelastic(l, epsilon)

  data.frame(
    w = w,
    l = l,
    y = y,
    c = c,
    u = u
  )
}

wage_percentiles <- function(w, probs = c(0.05, 0.5, 0.95)) {
  quantile(w, probs = probs, names = TRUE)
}
