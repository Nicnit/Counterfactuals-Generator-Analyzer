"""Data preprocessing utilities."""

import pandas as pd
import numpy as np
import re
from typing import Optional, Dict, Tuple, List
from .utils import (
    normalize_timezone,
    auto_detect_time_column,
    auto_detect_target_column
)


def auto_detect_columns(
    df: pd.DataFrame,
    time_col: Optional[str] = None,
    target_col: Optional[str] = None,
    entity_col: Optional[str] = None,
    exclude_patterns: Optional[List[str]] = None,
    target_patterns: Optional[List[str]] = None
) -> Dict[str, Optional[str]]:
    """Detect column names in DataFrame."""
    detected = {}
    
    if time_col is None:
        detected['time_col'] = auto_detect_time_column(df)
    else:
        detected['time_col'] = time_col if time_col in df.columns else None
    
    if target_col is None:
        exclude_cols = [detected['time_col']] if detected['time_col'] else []
        detected['target_col'] = auto_detect_target_column(
            df, 
            exclude_cols=exclude_cols,
            exclude_patterns=exclude_patterns,
            target_patterns=target_patterns
        )
    else:
        detected['target_col'] = target_col if target_col in df.columns else None
    
    if entity_col is None:
        detected['entity_col'] = _detect_entity_column(
            df, detected['time_col'], detected['target_col']
        )
    else:
        detected['entity_col'] = entity_col if entity_col in df.columns else None

    return detected


# Matched on word boundaries. Plain substrings pick up measurement columns:
# 'id' is inside 'humidity', 'name' is inside 'filename'.
_ENTITY_NAME_RE = re.compile(
    r'(^|[^a-z])(name|id|sensor|station|location|entity|store|site|region|group)([^a-z]|$)'
)


def _detect_entity_column(
    df: pd.DataFrame,
    time_col: Optional[str],
    target_col: Optional[str]
) -> Optional[str]:
    """Find the column that splits the frame into separate series."""
    candidates = []

    for col in df.columns:
        if col in (time_col, target_col):
            continue
        if pd.api.types.is_numeric_dtype(df[col]):
            continue
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            continue

        # An entity repeats across timestamps, so it has far fewer distinct
        # values than rows. Free-text columns fail this.
        distinct = df[col].nunique(dropna=True)
        if distinct == 0 or distinct > max(1, len(df) // 2):
            continue

        candidates.append(col)

    if not candidates:
        return None

    for col in candidates:
        if _ENTITY_NAME_RE.search(col.lower()):
            return col

    return candidates[0]


def clean_time_series(
    df: pd.DataFrame,
    time_col: Optional[str] = None,
    target_col: Optional[str] = None,
    entity_col: Optional[str] = None,
    drop_na: bool = True,
    normalize_timezone: bool = True,
    sort: bool = True,
    set_index: bool = True,
    auto_detect: bool = True,
    exclude_patterns: Optional[List[str]] = None,
    target_patterns: Optional[List[str]] = None
) -> Tuple[pd.DataFrame, Dict[str, Optional[str]]]:
    """
    Clean and prepare time series data.
    
    Args:
        df: Input dataframe
        time_col: Name of time column (auto-detected if None and auto_detect=True)
        target_col: Name of target column (auto-detected if None and auto_detect=True)
        entity_col: Optional entity identifier column
        drop_na: Whether to drop rows with missing values
        normalize_timezone: Whether to convert to timezone-naive
        sort: Whether to sort by time
        set_index: Whether to set time column as index
        auto_detect: Whether to auto-detect columns if not provided
    
    Returns:
        Tuple of (cleaned dataframe, detected columns dictionary)
    """
    df = df.copy()
    detected_cols = {}
    
    if auto_detect:
        detected = auto_detect_columns(
            df, time_col, target_col, entity_col,
            exclude_patterns=exclude_patterns,
            target_patterns=target_patterns
        )
        time_col = detected['time_col'] or time_col
        target_col = detected['target_col'] or target_col
        entity_col = detected.get('entity_col') or entity_col
        detected_cols = detected
    else:
        detected_cols = {
            'time_col': time_col,
            'target_col': target_col,
            'entity_col': entity_col
        }
    
    if time_col is None:
        raise ValueError("Time column not found")
    if time_col not in df.columns and not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError(f"Time column '{time_col}' not found")
    
    if target_col is None:
        raise ValueError("Target column not found")
    if target_col not in df.columns:
        raise ValueError(f"Target column '{target_col}' not found")
    
    if time_col in df.columns:
        df[time_col] = pd.to_datetime(df[time_col], errors='coerce')
    
    if drop_na:
        if time_col in df.columns:
            df = df.dropna(subset=[time_col, target_col])
        else:
            df = df.dropna(subset=[target_col])
    
    if sort:
        if time_col in df.columns:
            df = df.sort_values(time_col)
        elif isinstance(df.index, pd.DatetimeIndex):
            df = df.sort_index()
    
    if set_index:
        if time_col in df.columns:
            df = df.set_index(time_col)
        
        if normalize_timezone and isinstance(df.index, pd.DatetimeIndex):
            if df.index.tz is not None:
                df.index = df.index.tz_localize(None)
    
    if entity_col and entity_col in df.columns:
        df = _deduplicate_by_entity(df, entity_col, target_col)
    
    return df, detected_cols


def _deduplicate_by_entity(
    df: pd.DataFrame,
    entity_col: str,
    target_col: str
) -> pd.DataFrame:
    """
    Deduplicate by taking mean of target_col for same entity+time.
    
    Args:
        df: DataFrame (should have datetime index)
        entity_col: Entity identifier column
        target_col: Target column to aggregate
    
    Returns:
        Deduplicated DataFrame
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("DataFrame must have datetime index for deduplication")

    # Reset index to allow grouping
    index_name = df.index.name
    df_reset = df.reset_index()
    time_col_name = index_name if index_name else 'index'
    duplicate_cols = [time_col_name, entity_col]

    if df_reset.duplicated(subset=duplicate_cols).any():
        # Group by entity and time, take mean of target
        agg_dict = {target_col: 'mean'}

        # Preserve other columns (take first)
        for col in df_reset.columns:
            if col not in duplicate_cols + [target_col]:
                agg_dict[col] = 'first'

        df_reset = df_reset.groupby(duplicate_cols, as_index=False).agg(agg_dict)

    # Restore the index on both paths. Returning a RangeIndex when there was
    # nothing to deduplicate breaks every caller that asked for set_index.
    result = df_reset.set_index(time_col_name).sort_index()
    result.index.name = index_name

    return result

