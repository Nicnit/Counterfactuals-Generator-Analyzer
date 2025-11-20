"""Time series utilities."""

import pandas as pd
import numpy as np
from typing import Optional, Union, List
import re

def normalize_timezone(ts: Union[pd.Timestamp, str]) -> pd.Timestamp:
    """Convert timestamp to timezone-naive."""
    if isinstance(ts, str):
        ts = pd.Timestamp(ts)
    
    if isinstance(ts, pd.Timestamp) and ts.tz is not None:
        return ts.tz_localize(None)
    
    return pd.Timestamp(ts) if not isinstance(ts, pd.Timestamp) else ts

def infer_frequency(
    df: pd.DataFrame,
    time_col: Optional[str] = None,
    default: str = 'h'
) -> str:
    """Infer time series frequency."""
    if time_col is not None:
        if time_col in df.columns:
            inferred_freq = pd.infer_freq(df[time_col])
        else:
            return default
    else:
        if isinstance(df.index, pd.DatetimeIndex):
            inferred_freq = pd.infer_freq(df.index)
        else:
            return default
    
    return inferred_freq if inferred_freq is not None else default

def auto_detect_frequency(df: pd.DataFrame, time_col: str) -> str:
    """Detect frequency from data."""
    freq = infer_frequency(df, time_col)
    
    if freq is not None:
        return freq
    
    if time_col in df.columns:
        df_sorted = df.sort_values(time_col)
        time_diffs = df_sorted[time_col].diff().dropna()
        
        if len(time_diffs) > 0:
            median_diff = time_diffs.median()
            
            if median_diff <= pd.Timedelta(hours=1):
                return 'h'
            elif median_diff <= pd.Timedelta(days=1):
                return 'D'
            elif median_diff <= pd.Timedelta(weeks=1):
                return 'W'
            else:
                return 'M'
    
    return 'h'

def create_forecast_index(
    start: pd.Timestamp,
    end: pd.Timestamp,
    freq: str
) -> pd.DatetimeIndex:
    """
    Create datetime index for forecast period.
    
    Args:
        start: Start timestamp
        end: End timestamp
        freq: Frequency string
    
    Returns:
        DatetimeIndex for forecast period
    """
    # Normalize timezones
    start = normalize_timezone(start)
    end = normalize_timezone(end)
    
    forecast_index = pd.date_range(start=start, end=end, freq=freq)
    
    if len(forecast_index) > 0 and forecast_index[0] != start:
        forecast_index = pd.date_range(
            start=start,
            periods=len(forecast_index),
            freq=freq
        )
    
    if len(forecast_index) == 0:
        raise ValueError(f"Empty forecast period (start: {start}, end: {end})")
    
    return forecast_index

def validate_event_dates(
    event_start: pd.Timestamp,
    event_end: pd.Timestamp,
    event_name: str
) -> None:
    """Validate event date ranges."""
    event_start = normalize_timezone(event_start)
    event_end = normalize_timezone(event_end)
    
    if event_start >= event_end:
        raise ValueError(f"Invalid event dates for {event_name}: start >= end")

def auto_detect_time_column(df: pd.DataFrame) -> Optional[str]:
    """Detect time/datetime column in DataFrame."""
    time_patterns = [
        r'^datetime',
        r'^date',
        r'^time',
        r'^timestamp',
        r'^dt',
        r'time',
        r'date',
    ]
    
    for col in df.columns:
        col_lower = col.lower()
        for pattern in time_patterns:
            if re.search(pattern, col_lower):
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    return col
                try:
                    pd.to_datetime(df[col].iloc[0])
                    return col
                except:
                    continue
    
    if isinstance(df.index, pd.DatetimeIndex):
        return df.index.name if df.index.name else None
    
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            return col
    
    return None

def auto_detect_target_column(
    df: pd.DataFrame, 
    exclude_cols: Optional[List[str]] = None,
    exclude_patterns: Optional[List[str]] = None,
    target_patterns: Optional[List[str]] = None
) -> Optional[str]:
    """Detect target/value column in DataFrame."""
    exclude_cols = exclude_cols or []
    
    time_col = auto_detect_time_column(df)
    if time_col:
        exclude_cols.append(time_col)
    
    default_exclude_patterns = [
        r'^id$',
        r'^name$',
        r'^lat',
        r'^lon',
        r'latitude',
        r'longitude',
    ]
    metadata_patterns = exclude_patterns if exclude_patterns is not None else default_exclude_patterns
    
    # Find numeric columns (likely targets)
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    
    # Filter out explicitly excluded columns
    candidate_cols = [
        col for col in numeric_cols
        if col not in exclude_cols
    ]
    
    # Filter out metadata columns matching patterns
    for col in candidate_cols[:]:  # Copy list for iteration
        col_lower = col.lower()
        for pattern in metadata_patterns:
            if re.search(pattern, col_lower):
                candidate_cols.remove(col)
                break
    
    # If target_patterns provided, prefer matching columns
    
    if target_patterns:
        for pattern in target_patterns:
            for col in candidate_cols:
                if re.search(pattern, col.lower()):
                    return col
    
    # Return first numeric column (most generalizable approach)
    
    if candidate_cols:
        return candidate_cols[0]
    
    return None

def auto_detect_cycle_period(df: pd.DataFrame, time_col: str) -> str:
    """
    Automatically detect appropriate cycle period from data.
    
    Args:
        df: DataFrame with time series data
        time_col: Name of time column
    
    Returns:
        Cycle period string: 'hour', 'day', 'week', or 'month'
    """
    if time_col not in df.columns:
        return 'hour'  # Default
    
    df_sorted = df.sort_values(time_col)
    time_diffs = df_sorted[time_col].diff().dropna()
    
    if len(time_diffs) == 0:
        return 'hour'
    
    median_diff = time_diffs.median()
    
    
    if median_diff <= pd.Timedelta(hours=6):
        return 'hour'
    elif median_diff <= pd.Timedelta(days=3):
        return 'day'
    elif median_diff <= pd.Timedelta(weeks=2):
        return 'week'
    else:
        return 'month'

