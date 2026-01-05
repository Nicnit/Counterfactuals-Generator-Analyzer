"""Cyclical pattern extraction."""

import pandas as pd
import numpy as np
from typing import Optional, Callable, Dict

class CyclicalPatternExtractor:
    """Extract cyclical patterns."""
    
    def __init__(self, period: str = 'hour'):
        """Initialize extractor."""
        self.period = period
        self.period_map: Dict[str, Callable] = {
            'hour': lambda ts: ts.hour,
            'day': lambda ts: ts.dayofweek,
            'week': lambda ts: ts.isocalendar().week,
            'month': lambda ts: ts.month,
            'day_of_year': lambda ts: ts.dayofyear,
            'quarter': lambda ts: ts.quarter,
        }
    
    def extract(self, df: pd.DataFrame, target_col: str) -> pd.Series:
        """Extract cyclical pattern."""
        if target_col not in df.columns:
            raise ValueError(f"Target column '{target_col}' not found in DataFrame")
        
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("DataFrame must have datetime index")
        
        if self.period not in self.period_map:
            raise ValueError(
                f"Unsupported period: {self.period}. "
                f"Supported: {list(self.period_map.keys())}"
            )
        
        period_func = self.period_map[self.period]
        df = df.copy()
        df['_period'] = df.index.map(period_func)
        
        period_avg = df.groupby('_period')[target_col].mean()
        overall_mean = df[target_col].mean()
        
        pattern = period_avg - overall_mean
        pattern = self._fill_missing_periods(pattern)
        
        return pattern.sort_index()
    
    def _fill_missing_periods(self, pattern: pd.Series) -> pd.Series:
        """Fill missing periods with default values."""
        if self.period == 'hour':
            all_periods = set(range(24))
        elif self.period == 'day':
            all_periods = set(range(7))  # Monday=0 to Sunday=6
        elif self.period == 'week':
            all_periods = set(range(1, 54))  # ISO weeks 1-53
        elif self.period == 'month':
            all_periods = set(range(1, 13))  # Months 1-12
        elif self.period == 'day_of_year':
            all_periods = set(range(1, 367))  # Days 1-366 (leap year)
        elif self.period == 'quarter':
            all_periods = set(range(1, 5))
        else:
            return pattern
        
        missing = all_periods - set(pattern.index)
        for p in missing:
            pattern[p] = 0.0  
        
        return pattern
    
    def apply(
        self,
        pattern: pd.Series,
        timestamps: pd.DatetimeIndex
    ) -> np.ndarray:
        """Apply cyclical pattern to forecast timestamps."""
        if self.period not in self.period_map:
            raise ValueError(f"Unsupported period: {self.period}")
        
        period_func = self.period_map[self.period]
        adjustments = np.zeros(len(timestamps))
        
        for i, ts in enumerate(timestamps):
            period_val = period_func(ts)
            if period_val in pattern.index:
                adjustments[i] = pattern[period_val]
            else:
                adjustments[i] = 0.0  
        
        return adjustments
    
    def get_period_value(self, timestamp: pd.Timestamp) -> int:
        """Get period value for a given timestamp."""
        if self.period not in self.period_map:
            raise ValueError(f"Unsupported period: {self.period}")
        
        return self.period_map[self.period](timestamp)

