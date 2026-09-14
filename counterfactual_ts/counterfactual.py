"""Counterfactual generator."""

import hashlib
import warnings
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
    auto_detect_cycle_period,
    auto_detect_target_column
)
from .events import Event


# A unit root fits as 0.999999999999999 rather than exactly 1, so compare with
# a little slack. Genuinely stationary series sit well below this.
UNIT_ROOT_TOLERANCE = 1e-8


def stable_seed(name: str) -> int:
    """Reproducible seed for an event name.

    hash() is salted per interpreter run, so it cannot be used here.
    """
    digest = hashlib.blake2b(name.encode('utf-8'), digest_size=4).digest()
    return int.from_bytes(digest, 'big')


class TimeSeriesCounterfactualGenerator:
    """Generate counterfactual forecasts using AR models."""

    def __init__(
        self,
        time_col: Optional[str] = None,
        target_col: Optional[str] = None,
        ar_order: int = 1,
        cycle_period: Optional[str] = None,
        forecast_days: int = 5,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        noise_factor: float = 0.0,
        output_prefix: str = 'counterfactual',
        auto_detect: bool = True,
        random_seed: Optional[int] = None,
        interval: Optional[float] = None,
        n_paths: int = 500
    ):
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
        self.random_seed = random_seed
        self.interval = interval
        self.n_paths = n_paths
        self.model_ = None

    def generate(
        self,
        df: pd.DataFrame,
        event_start: Union[pd.Timestamp, str],
        event_end: Union[pd.Timestamp, str],
        event_name: str,
        time_col: Optional[str] = None,
        target_col: Optional[str] = None
    ) -> pd.DataFrame:
        event_start = normalize_timezone(pd.Timestamp(event_start))
        event_end = normalize_timezone(pd.Timestamp(event_end))
        validate_event_dates(event_start, event_end, event_name)

        time_col = time_col or self.time_col
        target_col = target_col or self.target_col

        # Validate DataFrame structure
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("DataFrame must have datetime index")

        if target_col is None and self.auto_detect:
            target_col = auto_detect_target_column(df)

        if target_col is None:
            raise ValueError(
                "Target column not found. Pass target_col or enable auto_detect."
            )

        if target_col not in df.columns:
            raise ValueError(f"Target column '{target_col}' not found")

        # Keep the full slice for frequency inference; gaps left by dropped
        # NaNs would otherwise make the spacing look irregular.
        pre_event_all = df[df.index < event_start]
        pre_event_df = pre_event_all[pre_event_all[target_col].notna()].copy()

        if pre_event_df.empty:
            raise ValueError(f"All {target_col} values are NaN before {event_name}")

        if len(pre_event_df) < self.ar_order + 1:
            raise ValueError(f"Need at least {self.ar_order + 1} data points before {event_name}")

        y = pre_event_df[target_col].to_numpy(dtype=float)

        cycle_period = self.cycle_period
        if cycle_period is None and self.auto_detect:
            temp_df = pre_event_df.reset_index()
            time_col_for_detection = temp_df.columns[0]
            cycle_period = auto_detect_cycle_period(temp_df, time_col_for_detection)
            if cycle_period is None:
                cycle_period = 'hour'
        elif cycle_period is None:
            cycle_period = 'hour'

        pattern_extractor = CyclicalPatternExtractor(period=cycle_period)
        pattern = pattern_extractor.extract(pre_event_df, target_col)

        # Fit the AR model on the deseasonalised level. Fitting on the raw
        # series leaves the cycle in the residuals and inflates residual_std.
        seasonal = pattern_extractor.apply(pattern, pre_event_df.index)
        y_level = y - seasonal

        ar_model = ARModel(order=self.ar_order)
        model_params = ar_model.fit(y_level)

        # Kept for inspection: phi and residual_std say how much of the series
        # the model actually explained.
        self.model_ = ar_model

        root = model_params.get('max_root_modulus', 0.0)
        if root >= 1.0 - UNIT_ROOT_TOLERANCE:
            warnings.warn(
                f"{event_name}: fitted AR model is non-stationary (largest root "
                f"{root:.3f}). The counterfactual will run away from the data "
                f"instead of settling, and the band with it. This usually means "
                f"the series trends and the model has no term for it.",
                stacklevel=2
            )

        freq = infer_frequency(pre_event_all)
        if freq is None:
            index_name = pre_event_all.index.name or 'index'
            freq = auto_detect_frequency(pre_event_all.reset_index(), index_name)

        forecast_end = event_end + pd.Timedelta(days=self.forecast_days)

        if event_start >= forecast_end:
            raise ValueError(
                f"Invalid event dates for {event_name}: "
                f"start {event_start} >= forecast_end {forecast_end}"
            )

        forecast_index = create_forecast_index(event_start, forecast_end, freq)

        if len(forecast_index) == 0:
            raise ValueError(f"Empty forecast period for {event_name}")

        self._warn_on_thin_history(
            pre_event_df, event_start, forecast_end, event_name,
            pattern_extractor, pattern
        )

        pattern_adjustments = pattern_extractor.apply(pattern, forecast_index)
        last_values = y_level[-self.ar_order:]

        forecast = self._generate_forecast(
            model_params=model_params,
            pattern_adjustments=pattern_adjustments,
            last_values=last_values,
            event_name=event_name
        )

        time_col_name = time_col or df.index.name or 'datetime'
        column = f"{self.output_prefix}_{event_name}"
        columns = {time_col_name: forecast_index, column: self._clamp(forecast)}

        if self.interval is not None:
            lower, upper = self._forecast_interval(
                model_params=model_params,
                pattern_adjustments=pattern_adjustments,
                last_values=last_values,
                event_name=event_name,
                point_forecast=forecast
            )
            columns[f"{column}_lower"] = self._clamp(lower)
            columns[f"{column}_upper"] = self._clamp(upper)

        return pd.DataFrame(columns)

    def _clamp(self, values: np.ndarray) -> np.ndarray:
        if self.min_value is not None:
            values = np.maximum(values, self.min_value)
        if self.max_value is not None:
            values = np.minimum(values, self.max_value)
        return values

    def _warn_on_thin_history(
        self,
        pre_event_df: pd.DataFrame,
        event_start: pd.Timestamp,
        forecast_end: pd.Timestamp,
        event_name: str,
        pattern_extractor: CyclicalPatternExtractor,
        pattern: pd.Series
    ) -> None:
        """Flag histories too short to support the forecast being asked for."""
        span = pre_event_df.index.max() - pre_event_df.index.min()
        window = forecast_end - event_start

        if span < window * 2:
            warnings.warn(
                f"{event_name}: {span} of pre-event data for a {window} forecast "
                f"window. Two to three times the window is recommended.",
                stacklevel=3
            )

        period_func = pattern_extractor.period_map[pattern_extractor.period]
        observed = pre_event_df.index.map(period_func).nunique()
        if observed * 2 < len(pattern):
            warnings.warn(
                f"{event_name}: pre-event data covers {observed} of "
                f"{len(pattern)} {pattern_extractor.period} values. The rest of "
                f"the cycle is treated as flat.",
                stacklevel=3
            )

    def _forecast_interval(
        self,
        model_params: Dict,
        pattern_adjustments: np.ndarray,
        last_values: np.ndarray,
        event_name: str,
        point_forecast: np.ndarray
    ):
        """Bootstrap a prediction band by resampling the fitted residuals.

        Shocks are fed through the same recursion as the forecast, so the band
        widens with the horizon and settles at the model's stationary spread.
        """
        if not 0.0 < self.interval < 1.0:
            raise ValueError("interval must be between 0 and 1")

        residuals = np.asarray(model_params['residuals'], dtype=float)
        residuals = residuals[np.isfinite(residuals)]

        if len(residuals) == 0 or model_params['residual_std'] == 0:
            return point_forecast.copy(), point_forecast.copy()

        phi = np.asarray(model_params['phi'], dtype=float)
        c = model_params['c']
        horizon = len(pattern_adjustments)

        seed = self.random_seed
        if seed is None:
            seed = stable_seed(f"{event_name}:interval")
        rng = np.random.RandomState(seed)

        draws = rng.choice(residuals, size=(self.n_paths, horizon), replace=True)
        state = np.tile(np.asarray(last_values, dtype=float), (self.n_paths, 1))
        paths = np.empty((self.n_paths, horizon))

        for i in range(horizon):
            level = c + state @ phi + draws[:, i]
            if state.shape[1] > 1:
                state[:, :-1] = state[:, 1:]
            state[:, -1] = level
            paths[:, i] = level

        paths = paths + pattern_adjustments

        tail = (1.0 - self.interval) / 2.0 * 100.0
        return (
            np.percentile(paths, tail, axis=0),
            np.percentile(paths, 100.0 - tail, axis=0),
        )

    def _generate_forecast(
        self,
        model_params: Dict,
        pattern_adjustments: np.ndarray,
        last_values: np.ndarray,
        event_name: str
    ) -> np.ndarray:
        phi = model_params['phi']
        c = model_params['c']
        residual_std = model_params['residual_std']

        horizon = len(pattern_adjustments)
        level = np.zeros(horizon)
        state = np.asarray(last_values, dtype=float).copy()

        for i in range(horizon):
            level[i] = c + np.dot(phi, state[-len(phi):])
            # Only the level is carried forward. Feeding the cyclical term back
            # into the state would push it through the AR filter, which scales
            # it by 1/|1 - phi| and shifts its phase.
            state = np.append(state[1:], level[i])

        forecast = level + pattern_adjustments

        if self.noise_factor > 0 and residual_std > 0:
            seed = self.random_seed
            if seed is None:
                seed = stable_seed(event_name) if event_name else None
            rng = np.random.RandomState(seed)
            forecast = forecast + rng.normal(0, residual_std * self.noise_factor, horizon)

        return forecast

    def generate_multiple(
        self,
        df: pd.DataFrame,
        events: List[Event],
        time_col: Optional[str] = None,
        target_col: Optional[str] = None
    ) -> pd.DataFrame:
        event_forecasts = []
        seen_names = set()

        for event in events:
            if event.name in seen_names:
                print(f"Warning: Skipping event {event.name}: duplicate event name")
                continue

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
                seen_names.add(event.name)
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
