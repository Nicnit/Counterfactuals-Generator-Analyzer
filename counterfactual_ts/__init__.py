"""
Counterfactual Time Series Analysis Library

A general-purpose library for generating counterfactual forecasts
for time series data given events.
"""

from .counterfactual import TimeSeriesCounterfactualGenerator
from .models import ARModel
from .patterns import CyclicalPatternExtractor
from .events import Event, EventManager
from .utils import (
    normalize_timezone,
    infer_frequency,
    create_forecast_index,
    validate_event_dates,
    auto_detect_time_column,
    auto_detect_target_column,
    auto_detect_frequency
)
from .preprocessing import clean_time_series, auto_detect_columns
from .setup import (
    run_setup_script,
    find_setup_script,
    install_dependencies,
    setup_environment
)
from .analysis import (
    calculate_differences,
    aggregate_statistics,
    compute_summary_statistics
)
from .query import TimeSeriesQuery

__version__ = "0.1.0"

__all__ = [
    # Main classes
    "TimeSeriesCounterfactualGenerator",
    "ARModel",
    "CyclicalPatternExtractor",
    "Event",
    "EventManager",
    "TimeSeriesQuery",
    # Utility functions
    "normalize_timezone",
    "infer_frequency",
    "create_forecast_index",
    "validate_event_dates",
    "auto_detect_time_column",
    "auto_detect_target_column",
    "auto_detect_frequency",
    # Preprocessing
    "clean_time_series",
    "auto_detect_columns",
    # Setup
    "run_setup_script",
    "find_setup_script",
    "install_dependencies",
    "setup_environment",
    # Analysis
    "calculate_differences",
    "aggregate_statistics",
    "compute_summary_statistics",
]

