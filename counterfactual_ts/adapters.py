"""
Adapters for backward compatibility with existing code.
"""

import pandas as pd
from typing import Optional
from .counterfactual import TimeSeriesCounterfactualGenerator
from .preprocessing import clean_time_series
from .events import Event


def azureml_main(
    dataframe1: Optional[pd.DataFrame] = None,
    dataframe2: Optional[pd.DataFrame] = None,
    forecast_days: int = 5,
    events: Optional[list] = None,
    time_col: Optional[str] = None,
    target_col: Optional[str] = None,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
    output_prefix: str = 'counterfactual'
) -> tuple:
    """
    Azure ML compatible interface for backward compatibility.
    
    This function provides the same interface as the original azureml_main
    but uses the generalized counterfactual_ts library.
    
    Args:
        dataframe1: Input dataframe with time series data
        dataframe2: Not used (for compatibility)
        forecast_days: Number of days to forecast after event end
        events: List of event tuples (start, end, name). If None, raises error.
        time_col: Name of time column (auto-detected if None)
        target_col: Name of target column (auto-detected if None)
        min_value: Minimum value constraint (None = no constraint)
        max_value: Maximum value constraint (None = no constraint)
        output_prefix: Prefix for output column names
    
    Returns:
        Tuple of (counterfactual_df, None) for compatibility
    """
    if dataframe1 is None or dataframe1.empty:
        raise ValueError("Input dataframe1 is empty")
    
    if events is None or len(events) == 0:
        raise ValueError("events parameter is required. Provide a list of event tuples (start, end, name).")
    
    # Clean data with auto-detection
    df, detected_cols = clean_time_series(
        dataframe1,
        time_col=time_col,
        target_col=target_col,
        auto_detect=True
    )
    
    # Create generator with auto-detected or provided parameters
    generator = TimeSeriesCounterfactualGenerator(
        time_col=detected_cols.get('time_col') or time_col,
        target_col=detected_cols.get('target_col') or target_col,
        forecast_days=forecast_days,
        min_value=min_value,
        max_value=max_value,
        output_prefix=output_prefix,
        auto_detect=True
    )
    
    # Convert events to Event objects
    event_objects = []
    for event_start, event_end, event_name in events:
        event_objects.append(Event(
            start=pd.Timestamp(event_start),
            end=pd.Timestamp(event_end),
            name=event_name
        ))
    
    # Generate counterfactuals
    result_df = generator.generate_multiple(
        df=df,
        events=event_objects
    )
    
    # Copy metadata if it exists
    for col in ["City", "Name", "longitude", "latitude"]:
        if col in dataframe1.columns and len(dataframe1) > 0:
            result_df[col] = dataframe1[col].iloc[-1]
    
    return result_df, None


def generate_event_counterfactual(
    df: pd.DataFrame,
    event_start: pd.Timestamp,
    event_end: pd.Timestamp,
    event_name: str,
    time_col: Optional[str] = None,
    target_col: Optional[str] = None,
    forecast_days: int = 5,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
    output_prefix: str = 'counterfactual'
) -> pd.DataFrame:
    """
    Generate counterfactual for a single event (backward compatible).
    
    This function provides the same interface as the original
    generate_event_counterfactual but uses the generalized library.
    
    Args:
        df: Full dataframe with datetime index
        event_start: Start date of event
        event_end: End date of event
        event_name: Name of event (for column naming)
        time_col: Name of time column (auto-detected if None)
        target_col: Name of target column (auto-detected if None)
        forecast_days: Number of days to forecast after event end
        min_value: Minimum value constraint (None = no constraint)
        max_value: Maximum value constraint (None = no constraint)
        output_prefix: Prefix for output column names
    
    Returns:
        DataFrame with datetime index and counterfactual column
    """
    # Auto-detect columns if not provided
    if time_col is None or target_col is None:
        from .preprocessing import auto_detect_columns
        detected = auto_detect_columns(df)
        time_col = time_col or detected.get('time_col')
        target_col = target_col or detected.get('target_col')
        
        if time_col is None:
            raise ValueError("Time column not found. Provide time_col or ensure data has datetime index.")
        if target_col is None:
            raise ValueError("Target column not found. Provide target_col or enable auto-detection.")
    
    # Ensure df has datetime index
    if not isinstance(df.index, pd.DatetimeIndex):
        if time_col in df.columns:
            df = df.set_index(time_col)
        else:
            raise ValueError(f"Time column '{time_col}' not found and no datetime index")
    
    # Create generator
    generator = TimeSeriesCounterfactualGenerator(
        time_col=time_col,
        target_col=target_col,
        forecast_days=forecast_days,
        min_value=min_value,
        max_value=max_value,
        output_prefix=output_prefix,
        auto_detect=False
    )
    
    # Generate counterfactual
    result = generator.generate(
        df=df,
        event_start=event_start,
        event_end=event_end,
        event_name=event_name,
        time_col=time_col,
        target_col=target_col
    )
    
    return result

