"""Time series utilities."""

import pandas as pd
import numpy as np
from pandas.tseries.frequencies import to_offset
from typing import Optional, Union, List
import re

# Spacings that have a readable, non-anchored alias. Anything else falls back
# to whatever to_offset() derives, which keeps the observed cadence but reads
# as a multiple of hours or minutes.
_TICK_ALIASES = [
    (pd.Timedelta(minutes=1), 'min'),
    (pd.Timedelta(minutes=5), '5min'),
    (pd.Timedelta(minutes=15), '15min'),
    (pd.Timedelta(minutes=30), '30min'),
    (pd.Timedelta(hours=1), 'h'),
    (pd.Timedelta(days=1), 'D'),
    (pd.Timedelta(days=7), '7D'),
]

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
    default: Optional[str] = None
) -> Optional[str]:
    """Infer time series frequency, or return default if pandas cannot."""
    if time_col is not None:
        if time_col not in df.columns:
            return default
        values = df[time_col]
    elif isinstance(df.index, pd.DatetimeIndex):
        values = df.index
    else:
        return default

    # pd.infer_freq raises below three observations rather than returning None.
    if len(values) < 3:
        return default

    try:
        inferred = pd.infer_freq(values)
    except (ValueError, TypeError):
        return default

    return inferred if inferred is not None else default

def freq_from_timedelta(delta: pd.Timedelta) -> str:
    """Turn an observed spacing into a pandas frequency alias."""
    delta = delta.round('s')

    for span, alias in _TICK_ALIASES:
        if delta == span:
            return alias

    return to_offset(delta).freqstr

def auto_detect_frequency(df: pd.DataFrame, time_col: str) -> str:
    """Detect frequency from data, falling back to the median spacing."""
    freq = infer_frequency(df, time_col)

    if freq is not None:
        return freq

    if time_col in df.columns:
        times = pd.to_datetime(df[time_col], errors='coerce').dropna()
    elif isinstance(df.index, pd.DatetimeIndex):
        times = pd.Series(df.index)
    else:
        return 'h'

    time_diffs = times.sort_values().diff().dropna()
    time_diffs = time_diffs[time_diffs > pd.Timedelta(0)]

    if len(time_diffs) == 0:
        return 'h'

    return freq_from_timedelta(time_diffs.median())

def create_forecast_index(
    start: pd.Timestamp,
    end: pd.Timestamp,
    freq: str
) -> pd.DatetimeIndex:
    start = normalize_timezone(start)
    end = normalize_timezone(end)

    forecast_index = pd.date_range(start=start, end=end, freq=freq)

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

    # start == end is a legitimate single-point event; date-only JSON entries
    # land here routinely.
    if event_start > event_end:
        raise ValueError(f"Invalid event dates for {event_name}: start > end")

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
                if _parses_as_datetime(df[col]):
                    return col
                break

    if isinstance(df.index, pd.DatetimeIndex):
        return df.index.name if df.index.name else None

    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            return col

    return None

def _parses_as_datetime(series: pd.Series) -> bool:
    """Whether a column can be read as timestamps."""
    if pd.api.types.is_datetime64_any_dtype(series):
        return True

    # Numeric columns convert happily as epoch offsets, so a column called
    # 'date_id' full of integers would otherwise pass.
    if pd.api.types.is_numeric_dtype(series):
        return False

    sample = series.dropna().head(5)
    if len(sample) == 0:
        return False

    try:
        pd.to_datetime(sample)
    except (ValueError, TypeError, OverflowError):
        return False

    return True

def auto_detect_target_column(
    df: pd.DataFrame,
    exclude_cols: Optional[List[str]] = None,
    exclude_patterns: Optional[List[str]] = None,
    target_patterns: Optional[List[str]] = None
) -> Optional[str]:
    # Copied rather than aliased: this list gets appended to below.
    exclude_cols = list(exclude_cols) if exclude_cols else []

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
    if time_col not in df.columns:
        return 'hour'  # Default

    times = pd.to_datetime(df[time_col], errors='coerce').dropna()
    time_diffs = times.sort_values().diff().dropna()
    time_diffs = time_diffs[time_diffs > pd.Timedelta(0)]

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
