# Setup
getwd()

# Libraries
library(dplyr)
library(ggplot2)
library(languageserver)

########################################
##### - Single crossing condition -#####
########################################

# Utility
u <- function(c, y, omega, epsilon) {
    c - 1 / (1 + epsilon^-1) * (y / omega)^(1 + epsilon^-1)
}

# Indirect utility
# v = max c - 1 / (1 + epsilon^-1) * (y / w)^(1 + epsilon^-1) s.t. c = (1 - tau) * y
# FOC: (1 - tau)  = (y / w)^(epsilon^-1)
v <- function(tau, alpha, omega, epsilon) {
    y_star <- (1 - tau)^epsilon * omega
    c_star <- alpha + (1 - tau) * y_star
    v_star <- c_star - (1 / (1 + 1 / epsilon)) * (y_star / omega)^(1 + 1 / epsilon)
    return(c(v_star, y_star, c_star))
}

# Parameters
epsilon <- .5

# Indifference curves in the (y,c) space
grid <- expand.grid(
    y = seq(0, 10, by = .01),
    c = seq(0, 10, by = .01)
)
grid <- grid |> 
    mutate(u = u(c, y, omega = 1, epsilon))
ggplot(grid, aes(x = y, y = c, z = u)) +
    geom_contour_filled(breaks = seq(-100, 100, by = 5)) +
    labs(
        title = "Indifference Curves in (y,c) space",
        x = "y",
        y = "c",
        fill = "Utility"
    ) +
    theme_minimal()

# Indifference curves in the (alpha, tau) space
alphas <- seq(0, 10, by = .1)
taus <- seq(0, 1, by = .01)
omega1 <- 1
omega2 <- 2
z1 <- outer(alphas, taus, function(a, t) v(t, a, omega = 1, epsilon)[1])
z2 <- outer(alphas, taus, function(a, t) v(t, a, omega = 2, epsilon)[1])
V_target <- v(tau = 0.3, alpha = 5, omega = 1, epsilon)[1]
plot(0, 0, xlim = range(tau_vals), ylim = range(alphas),
     xlab = "tau (tax rate)", ylab = "alpha (non-labor income)", 
     type = "n",
     main = paste("Indifference Curves at V =", round(V_target, 2)))

contour(taus, alphas, z1, levels = V_target, 
        col = "blue", lwd = 2, add = TRUE, labcex = 0.8)

contour(taus, alphas, z2, levels = V_target, 
        col = "red", lwd = 2, add = TRUE, labcex = 0.8)

legend("topright", 
       legend = c(paste("omega =", omega1), 
                  paste("omega =", omega2)),
       col = c("blue", "red"), lwd = 2)

grid()
