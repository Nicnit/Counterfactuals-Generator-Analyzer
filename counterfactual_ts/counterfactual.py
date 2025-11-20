"""Counterfactual generator."""

import pandas as pd
import numpy as np
from typing import Optional, Dict, List, Union
from .models import ARModel
from .patterns import CyclicalPatternExtractor
from .utils import (
    normalize_timezone,
    infer_frequency,
    create_forecast_index,
    validate_event_dates,
    auto_detect_frequency,
    auto_detect_cycle_period
)
from .events import Event


class TimeSeriesCounterfactualGenerator:
    """Generate counterfactual forecasts using AR(p) models and cyclical patterns."""
    
    def __init__(
        self,
        time_col: Optional[str] = None,
        target_col: Optional[str] = None,
        ar_order: int = 1,
        cycle_period: Optional[str] = None,  # Auto-detect if None
        forecast_days: int = 5,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        noise_factor: float = 0.5,
        output_prefix: str = 'counterfactual',
        auto_detect: bool = True
    ):
        """
        Args:
            time_col: Time column name (auto-detected if None)
            target_col: Target column name (auto-detected if None)
            ar_order: AR model order
            cycle_period: 'hour', 'day', 'week', 'month' (auto-detected if None)
            forecast_days: Days to forecast after event
            min_value: Min value constraint
            max_value: Max value constraint
            noise_factor: Noise injection factor (0-1)
            output_prefix: Output column prefix
            auto_detect: Enable auto-detection
        """
        self.time_col = time_col
        self.target_col = target_col
        self.ar_order = ar_order
        self.cycle_period = cycle_period
        self.forecast_days = forecast_days
        self.min_value = min_value
        self.max_value = max_value
        self.noise_factor = noise_factor
        self.output_prefix = output_prefix
        self.auto_detect = auto_detect
    
    def generate(
        self,
        df: pd.DataFrame,
        event_start: Union[pd.Timestamp, str],
        event_end: Union[pd.Timestamp, str],
        event_name: str,
        time_col: Optional[str] = None,
        target_col: Optional[str] = None
    ) -> pd.DataFrame:
        """Generate counterfactual forecast for a single event."""
        event_start = normalize_timezone(pd.Timestamp(event_start))
        event_end = normalize_timezone(pd.Timestamp(event_end))
        validate_event_dates(event_start, event_end, event_name)
        
        time_col = time_col or self.time_col
        target_col = target_col or self.target_col
        
        # Validate DataFrame structure
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("DataFrame must have datetime index")
        
        if target_col not in df.columns:
            raise ValueError(f"Target column '{target_col}' not found")
        
        pre_event_df = df[df.index < event_start].copy()
        
        if len(pre_event_df) < self.ar_order + 1:
            raise ValueError(f"Need at least {self.ar_order + 1} data points before {event_name}")
        
        if pre_event_df[target_col].isna().all():
            raise ValueError(f"All {target_col} values are NaN before {event_name}")
        
        y = pre_event_df[target_col].values
        
        cycle_period = self.cycle_period
        if cycle_period is None and self.auto_detect:
            temp_df = pre_event_df.reset_index()
            time_col_for_detection = temp_df.columns[0]
            cycle_period = auto_detect_cycle_period(temp_df, time_col_for_detection)
            if cycle_period is None:
                cycle_period = 'hour'
        elif cycle_period is None:
            cycle_period = 'hour'
        
        ar_model = ARModel(order=self.ar_order)
        model_params = ar_model.fit(y)
        
        pattern_extractor = CyclicalPatternExtractor(period=cycle_period)
        pattern = pattern_extractor.extract(pre_event_df, target_col)
        
        freq = infer_frequency(pre_event_df)
        if freq is None:
            freq = auto_detect_frequency(pre_event_df.reset_index(), time_col or 'index')
        
        forecast_end = event_end + pd.Timedelta(days=self.forecast_days)
        
        if event_start >= forecast_end:
            raise ValueError(
                f"Invalid event dates for {event_name}: "
                f"start {event_start} >= forecast_end {forecast_end}"
            )
        
        forecast_index = create_forecast_index(event_start, forecast_end, freq)
        
        if len(forecast_index) == 0:
            raise ValueError(f"Empty forecast period for {event_name}")
        
        forecast = self._generate_forecast(
            forecast_index=forecast_index,
            model_params=model_params,
            pattern=pattern,
            pattern_extractor=pattern_extractor,
            last_values=y[-self.ar_order:],
            event_name=event_name
        )
        
        if self.min_value is not None:
            forecast = np.maximum(forecast, self.min_value)
        if self.max_value is not None:
            forecast = np.minimum(forecast, self.max_value)
        
        time_col_name = time_col or df.index.name or 'datetime'
        forecast_df = pd.DataFrame({
            time_col_name: forecast_index,
            f"{self.output_prefix}_{event_name}": forecast
        })
        
        return forecast_df
    
    def _generate_forecast(
        self,
        forecast_index: pd.DatetimeIndex,
        model_params: Dict,
        pattern: pd.Series,
        pattern_extractor: CyclicalPatternExtractor,
        last_values: np.ndarray,
        event_name: str
    ) -> np.ndarray:
        """Generate forecast using AR model and cyclical patterns."""
        phi = model_params['phi']
        c = model_params['c']
        residual_std = model_params['residual_std']
        
        pattern_adjustments = pattern_extractor.apply(pattern, forecast_index)
        
        horizon = len(forecast_index)
        forecast = np.zeros(horizon)
        state = last_values.copy()
        
        # Deterministic seed for reproducibility
        seed = hash(event_name) % (2**31) if event_name else None
        
        for i in range(horizon):
            # AR(p) base forecast
            base_forecast = c + np.dot(phi, state[-len(phi):])
            
            # Add cyclical pattern adjustment
            forecast[i] = base_forecast + pattern_adjustments[i]
            
            # Update state
            state = np.append(state[1:], forecast[i])
        
        # Add noise if requested
        if self.noise_factor > 0 and residual_std > 0:
            rng = np.random.RandomState(seed)
            noise = rng.normal(0, residual_std * self.noise_factor, horizon)
            forecast = forecast + noise
        
        return forecast
    
    def generate_multiple(
        self,
        df: pd.DataFrame,
        events: List[Event],
        time_col: Optional[str] = None,
        target_col: Optional[str] = None
    ) -> pd.DataFrame:
        """Generate counterfactuals for multiple events."""
        event_forecasts = []
        
        for event in events:
            try:
                event_forecast = self.generate(
                    df=df,
                    event_start=event.start,
                    event_end=event.end,
                    event_name=event.name,
                    time_col=time_col,
                    target_col=target_col
                )
                event_forecasts.append(event_forecast)
            except ValueError as e:
                print(f"Warning: Skipping event {event.name}: {e}")
                continue
        
        if len(event_forecasts) == 0:
            raise ValueError("No events could be processed")
        
        all_dates = set()
        for forecast_df in event_forecasts:
            time_col_name = forecast_df.columns[0]
            dates = forecast_df[time_col_name].values
            normalized_dates = [normalize_timezone(pd.Timestamp(dt)) for dt in dates]
            all_dates.update(normalized_dates)
        
        all_dates = sorted(all_dates)
        time_col_name = event_forecasts[0].columns[0]
        combined_df = pd.DataFrame({time_col_name: all_dates})
        combined_df = combined_df.set_index(time_col_name)
        
        for forecast_df in event_forecasts:
            forecast_df = forecast_df.set_index(forecast_df.columns[0])
            if forecast_df.index.tz is not None:
                forecast_df.index = forecast_df.index.tz_localize(None)
            
            for col in forecast_df.columns:
                combined_df[col] = forecast_df[col]
        
        combined_df = combined_df.reset_index()
        
        return combined_df

