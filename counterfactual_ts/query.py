"""
Query interface for time series data.
"""

import pandas as pd
from typing import Optional, List, Dict, Any
from .utils import normalize_timezone


class TimeSeriesQuery:
    """
    Query interface for time series data.
    """
    
    def __init__(self, df: pd.DataFrame, time_col: str):
        """
        Initialize query interface.
        
        Args:
            df: DataFrame with time series data
            time_col: Name of time column
        """
        self.df = df.copy()
        self.time_col = time_col
        
        # Ensure time column is datetime
        if time_col in self.df.columns:
            self.df[time_col] = pd.to_datetime(self.df[time_col])
        elif isinstance(self.df.index, pd.DatetimeIndex):
            # Use index as time column
            self.df = self.df.reset_index()
            if self.df.index.name:
                self.time_col = self.df.index.name
            else:
                self.time_col = 'index'
                self.df['index'] = pd.to_datetime(self.df['index'])
    
    def filter_date_range(
        self,
        start: Optional[pd.Timestamp] = None,
        end: Optional[pd.Timestamp] = None,
        inclusive: str = 'both'
    ) -> pd.DataFrame:
        """
        Filter by date range.
        
        Args:
            start: Start timestamp (None = no lower bound)
            end: End timestamp (None = no upper bound)
            inclusive: 'both', 'left', 'right', or 'neither'
        
        Returns:
            Filtered DataFrame
        """
        result = self.df.copy()
        
        if start is not None:
            start = normalize_timezone(pd.Timestamp(start))
            if inclusive in ['both', 'left']:
                result = result[result[self.time_col] >= start]
            else:
                result = result[result[self.time_col] > start]
        
        if end is not None:
            end = normalize_timezone(pd.Timestamp(end))
            if inclusive in ['both', 'right']:
                result = result[result[self.time_col] <= end]
            else:
                result = result[result[self.time_col] < end]
        
        return result
    
    def filter_entity(
        self,
        entity_col: str,
        entity_value: str,
        exact_match: bool = False
    ) -> pd.DataFrame:
        """
        Filter by entity (sensor, location, etc.).
        
        Args:
            entity_col: Name of entity column
            entity_value: Entity value to match
            exact_match: If True, exact match; if False, case-insensitive partial match
        
        Returns:
            Filtered DataFrame
        """
        if entity_col not in self.df.columns:
            raise ValueError(f"Entity column '{entity_col}' not found")
        
        if exact_match:
            return self.df[self.df[entity_col] == entity_value].copy()
        else:
            # Case-insensitive partial match
            mask = self.df[entity_col].astype(str).str.contains(
                str(entity_value),
                case=False,
                na=False,
                regex=False
            )
            return self.df[mask].copy()
    
    def filter(
        self,
        start: Optional[pd.Timestamp] = None,
        end: Optional[pd.Timestamp] = None,
        entity_col: Optional[str] = None,
        entity_value: Optional[str] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Apply multiple filters.
        
        Args:
            start: Start timestamp
            end: End timestamp
            entity_col: Entity column name
            entity_value: Entity value to filter
            **kwargs: Additional column=value filters
        
        Returns:
            Filtered DataFrame
        """
        result = self.df.copy()
        
        # Date range filter
        if start is not None or end is not None:
            result = self.filter_date_range(start, end)
        
        # Entity filter
        if entity_col and entity_value:
            result = self.filter_entity(entity_col, entity_value)
        
        # Additional filters from kwargs
        for col, value in kwargs.items():
            if col in result.columns:
                result = result[result[col] == value]
        
        return result
    
    def get_available_entities(self, entity_col: str) -> List[str]:
        """
        Get list of available entity values.
        
        Args:
            entity_col: Name of entity column
        
        Returns:
            List of unique entity values
        """
        if entity_col not in self.df.columns:
            raise ValueError(f"Entity column '{entity_col}' not found")
        
        return sorted(self.df[entity_col].unique().tolist())
    
    def get_date_range(self) -> Dict[str, pd.Timestamp]:
        """
        Get date range of data.
        
        Returns:
            Dictionary with 'start' and 'end' timestamps
        """
        return {
            'start': self.df[self.time_col].min(),
            'end': self.df[self.time_col].max()
        }

