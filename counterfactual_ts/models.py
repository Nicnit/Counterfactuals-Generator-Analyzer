"""AR model for time series forecasting."""

import numpy as np
from typing import Dict, Optional


class ARModel:
    """AR(p) model for time series forecasting."""
    
    def __init__(self, order: int = 1):
        """Initialize AR model."""
        if order < 1:
            raise ValueError("AR order must be >= 1")
        self.order = order
        self.coefficients = None
        self.residuals = None
        self.residual_std = None
    
    def fit(self, y: np.ndarray) -> Dict:
        """Fit AR(p) model using OLS."""
        if len(y) < self.order + 1:
            raise ValueError(
                f"Need at least {self.order + 1} data points for AR({self.order}) model, "
                f"got {len(y)}"
            )
        
        if np.std(y[:-self.order]) < 1e-10:
            return {
                'phi': np.zeros(self.order),
                'c': np.mean(y[self.order:]) if len(y) > self.order else y[-1],
                'residual_std': 0.0,
                'residuals': np.zeros(len(y) - self.order)
            }
        
        X = self._build_design_matrix(y, self.order)
        y_target = y[self.order:]
        
        try:
            coeffs = np.linalg.lstsq(X, y_target, rcond=None)[0]
        except np.linalg.LinAlgError:
            return {
                'phi': np.zeros(self.order),
                'c': np.mean(y_target),
                'residual_std': 0.0,
                'residuals': np.zeros(len(y_target))
            }
        
        if len(coeffs) > 1:
            phi = coeffs[1:]
            c = coeffs[0]
        else:
            phi = np.array([coeffs[0]])
            c = 0.0
        
        if not (np.isfinite(phi).all() and np.isfinite(c)):
            phi = np.zeros(self.order)
            c = np.mean(y_target) if len(y_target) > 0 else y[-1]
        
        fitted = X @ coeffs
        residuals = y_target - fitted
        residual_std = np.std(residuals) if len(residuals) > 0 else 0.0
        
        self.coefficients = {'phi': phi, 'c': c}
        self.residuals = residuals
        self.residual_std = residual_std
        
        return {
            'phi': phi,
            'c': c,
            'residual_std': residual_std,
            'residuals': residuals
        }
    
    def _build_design_matrix(self, y: np.ndarray, order: int) -> np.ndarray:
        """
        Build design matrix for AR(p) model.
        
        Args:
            y: Time series values
            order: AR order
        
        Returns:
            Design matrix X with shape (n, order+1)
            First column is ones (intercept), remaining columns are lags
        """
        n = len(y) - order
        X = np.ones((n, order + 1))
        
        for i in range(order):
            X[:, i + 1] = y[i:n + i]
        
        return X
    
    def forecast(
        self,
        last_values: np.ndarray,
        horizon: int,
        c: float,
        phi: np.ndarray,
        add_noise: bool = False,
        noise_std: Optional[float] = None,
        random_seed: Optional[int] = None
    ) -> np.ndarray:
        """Generate forecast using AR model."""
        if len(last_values) < len(phi):
            raise ValueError(
                f"Need at least {len(phi)} last values, got {len(last_values)}"
            )
        
        forecast = np.zeros(horizon)
        state = last_values.copy()
        
        if random_seed is not None:
            rng = np.random.RandomState(random_seed)
        else:
            rng = np.random
        
        for i in range(horizon):
            forecast[i] = c + np.dot(phi, state[-len(phi):])
            
            if add_noise:
                noise_std_to_use = noise_std if noise_std is not None else self.residual_std
                if noise_std_to_use > 0:
                    forecast[i] += rng.normal(0, noise_std_to_use)
            
            state = np.append(state[1:], forecast[i])
        
        return forecast

