"""Statistical analysis utilities."""

import pandas as pd
import numpy as np
from typing import Optional, Dict, List


def calculate_differences(
    actual: pd.DataFrame,
    counterfactual: pd.DataFrame,
    time_col: str,
    actual_col: str,
    counterfactual_col: str,
    entity_col: Optional[str] = None,
    difference_col: str = 'difference'
) -> pd.DataFrame:
    """Calculate differences between actual and counterfactual values."""
    merge_cols = [time_col]
    if entity_col:
        merge_cols.append(entity_col)
    
    cf_cols = [time_col, counterfactual_col]
    if entity_col and entity_col in counterfactual.columns:
        cf_cols.append(entity_col)
    
    merged = actual.merge(
        counterfactual[cf_cols],
        on=merge_cols,
        how='inner',
        suffixes=('_actual', '_cf')
    )
    
    merged[difference_col] = merged[actual_col] - merged[counterfactual_col]
    
    return merged


def aggregate_statistics(
    df: pd.DataFrame,
    time_col: str,
    value_col: str,
    entity_col: Optional[str] = None
) -> pd.DataFrame:
    """Aggregate statistics across entities for each time point."""
    group_cols = [time_col]
    
    def q25(x):
        return x.quantile(0.25)
    
    def q75(x):
        return x.quantile(0.75)
    
    agg_dict = {
        value_col: ['mean', 'median', 'std', 'min', 'max', 'count', q25, q75]
    }
    
    summary = df.groupby(group_cols).agg(agg_dict)
    
    if isinstance(summary.columns, pd.MultiIndex):
        summary.columns = [
            f"{col[0]}_{col[1]}" if col[1] != col[0] else col[0]
            for col in summary.columns
        ]
    else:
        summary.columns = [f"{value_col}_{col}" if col != value_col else col for col in summary.columns]
    
    summary = summary.reset_index()
    
    if entity_col and entity_col in df.columns:
        entity_agg = df.groupby(group_cols).agg({
            value_col: [
                lambda x: (x > 0).sum(),
                lambda x: (x < 0).sum(),
                lambda x: (x == 0).sum()
            ]
        })
        if isinstance(entity_agg.columns, pd.MultiIndex):
            entity_agg.columns = ['num_positive', 'num_negative', 'num_zero']
        entity_agg = entity_agg.reset_index()
        summary = summary.merge(entity_agg, on=group_cols, how='left')
    
    return summary


def compute_summary_statistics(
    df: pd.DataFrame,
    value_col: str
) -> Optional[Dict]:
    """Compute summary statistics."""
    if value_col not in df.columns:
        return None
    
    valid_values = df[value_col].dropna()
    
    if len(valid_values) == 0:
        return None
    
    stats = {
        'count': len(valid_values),
        'mean': float(valid_values.mean()),
        'median': float(valid_values.median()),
        'std': float(valid_values.std()),
        'min': float(valid_values.min()),
        'max': float(valid_values.max()),
        'q25': float(valid_values.quantile(0.25)),
        'q75': float(valid_values.quantile(0.75)),
        'num_positive': int((valid_values > 0).sum()),
        'num_negative': int((valid_values < 0).sum()),
        'num_zero': int((valid_values == 0).sum()),
    }
    
    if stats['count'] > 0:
        stats['pct_positive'] = stats['num_positive'] / stats['count'] * 100
        stats['pct_negative'] = stats['num_negative'] / stats['count'] * 100
        stats['pct_zero'] = stats['num_zero'] / stats['count'] * 100
    
    return stats


def compare_actual_vs_counterfactual(
    actual: pd.DataFrame,
    counterfactual: pd.DataFrame,
    time_col: str,
    actual_col: str,
    counterfactual_col: str,
    entity_col: Optional[str] = None,
    aggregate: bool = True
) -> Dict:
    """Compare actual vs counterfactual."""
    differences = calculate_differences(
        actual=actual,
        counterfactual=counterfactual,
        time_col=time_col,
        actual_col=actual_col,
        counterfactual_col=counterfactual_col,
        entity_col=entity_col
    )
    
    summary = compute_summary_statistics(differences, 'difference')
    
    result = {
        'differences': differences,
        'summary': summary
    }
    
    if aggregate:
        time_aggregated = aggregate_statistics(
            differences,
            time_col=time_col,
            value_col='difference',
            entity_col=entity_col
        )
        result['time_aggregated'] = time_aggregated
    
    return result

