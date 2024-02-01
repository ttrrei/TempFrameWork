
import numpy as np
from scipy.optimize import curve_fit

class CurvFit:
    def __init__(self):
        pass

    def linear_fit(self, x, y):
        fit_params, covariance = curve_fit(self.linear_function, x, y)
        return fit_params

    def quadratic_fit(self, x, y):

        initial_guess = [1, 1, 1]
        fit_params, covariance = curve_fit(self.quadratic_function, x, y, p0=initial_guess)
        return fit_params

    def sin_fit(self, x, y):
        initial_guess = [1, 0.005, 0, 0]
        bounds = ([-np.inf, -2, -np.pi, -np.inf], [np.inf, 2, np.pi, np.inf])
        try:
            fit_params, covariance = curve_fit(self.sine_function, x, y, p0=initial_guess, bounds=bounds)
            return fit_params
        except RuntimeError as e:
            return np.array(['', '', '', ''])



    def linear_function(self, x, slope, intec):
        return slope * x + intec

    def quadratic_function(self, x, a, b, c):
        return a * x ** 2 + b * x + c

    def sine_function(self, t, A, f, phi, offset):
        return A * np.sin(2 * np.pi * f * t + phi) + offset