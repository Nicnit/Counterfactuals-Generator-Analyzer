"""
Example usage of the counterfactual_ts library.
"""

import pandas as pd
import numpy as np
from .counterfactual import TimeSeriesCounterfactualGenerator
from .events import Event, EventManager
from .preprocessing import clean_time_series, auto_detect_columns
from .analysis import compare_actual_vs_counterfactual


def example_basic_usage():
    """
    Basic example: Generate counterfactual for a single event.
    """
    # Create sample data
    dates = pd.date_range('2024-01-01', periods=1000, freq='h')
    values = 75 + 20 * np.sin(2 * np.pi * np.arange(1000) / 24) + np.random.normal(0, 5, 1000)
    values = np.maximum(values, 10)
    
    df = pd.DataFrame({
        'timestamp': dates,
        'value': values
    })
    
    # Clean data (auto-detects columns)
    df_clean, detected = clean_time_series(df, auto_detect=True)
    print(f"Detected columns: {detected}")
    
    # Create generator (auto-detects cycle period)
    generator = TimeSeriesCounterfactualGenerator(
        ar_order=1,
        forecast_days=5,
        auto_detect=True
    )
    
    # Generate counterfactual
    event = Event(
        start=pd.Timestamp('2024-07-15'),
        end=pd.Timestamp('2024-07-17'),
        name='event1'
    )
    
    counterfactual = generator.generate(
        df=df_clean,
        event_start=event.start,
        event_end=event.end,
        event_name=event.name
    )
    
    print(f"Generated {len(counterfactual)} counterfactual points")
    return counterfactual


def example_multiple_events():
    """
    Example: Generate counterfactuals for multiple events.
    """
    # Load or create data
    dates = pd.date_range('2024-01-01', periods=2000, freq='h')
    values = 75 + 20 * np.sin(2 * np.pi * np.arange(2000) / 24) + np.random.normal(0, 5, 2000)
    values = np.maximum(values, 10)
    
    df = pd.DataFrame({
        'timestamp': dates,
        'value': values
    })
    
    df_clean, _ = clean_time_series(df, auto_detect=True)
    
    # Create generator
    generator = TimeSeriesCounterfactualGenerator(
        forecast_days=7,
        auto_detect=True
    )
    
    # Define multiple events
    events = [
        Event(start=pd.Timestamp('2024-07-15'), end=pd.Timestamp('2024-07-17'), name='event1'),
        Event(start=pd.Timestamp('2024-11-19'), end=pd.Timestamp('2024-11-22'), name='event2')
    ]
    
    # Generate counterfactuals for all events
    counterfactuals = generator.generate_multiple(df_clean, events)
    
    print(f"Generated counterfactuals for {len(events)} events")
    return counterfactuals


def example_with_actual_data():
    """
    Example: Compare actual vs counterfactual.
    """
    # Create sample data with event impact
    dates = pd.date_range('2024-01-01', periods=1000, freq='h')
    base_values = 75 + 20 * np.sin(2 * np.pi * np.arange(1000) / 24)
    
    # Add event impact (increase during event)
    event_start_idx = 500
    event_end_idx = 550
    values = base_values.copy()
    values[event_start_idx:event_end_idx] += 30  # Event impact
    
    values += np.random.normal(0, 5, 1000)
    values = np.maximum(values, 10)
    
    df = pd.DataFrame({
        'timestamp': dates,
        'value': values
    })
    
    df_clean, _ = clean_time_series(df, auto_detect=True)
    
    # Generate counterfactual
    generator = TimeSeriesCounterfactualGenerator(auto_detect=True)
    event = Event(
        start=dates[event_start_idx],
        end=dates[event_end_idx],
        name='event'
    )
    
    counterfactual = generator.generate(
        df=df_clean,
        event_start=event.start,
        event_end=event.end,
        event_name=event.name
    )
    
    # Compare actual vs counterfactual
    # Filter actual data to event period
    actual_event = df_clean[
        (df_clean.index >= event.start) & 
        (df_clean.index <= event.end + pd.Timedelta(days=5))
    ].copy()
    
    # Merge for comparison
    comparison = compare_actual_vs_counterfactual(
        actual=actual_event.reset_index(),
        counterfactual=counterfactual,
        time_col='timestamp',
        actual_col='value',
        counterfactual_col='counterfactual_event',
        aggregate=True
    )
    
    print(f"Mean difference: {comparison['summary']['mean']:.2f}")
    return comparison


def example_custom_configuration():
    """
    Example: Custom configuration for different use cases.
    """
    # Sales data example
    dates = pd.date_range('2024-01-01', periods=365, freq='D')
    sales = 1000 + 200 * np.sin(2 * np.pi * np.arange(365) / 7) + np.random.normal(0, 50, 365)
    sales = np.maximum(sales, 0)
    
    df = pd.DataFrame({
        'date': dates,
        'sales': sales
    })
    
    df_clean, _ = clean_time_series(
        df,
        time_col='date',
        target_col='sales',
        auto_detect=False
    )
    
    # Custom generator for sales forecasting
    generator = TimeSeriesCounterfactualGenerator(
        time_col='date',
        target_col='sales',
        ar_order=2,  # AR(2) model
        cycle_period='day',  # Weekly cycle (day of week)
        forecast_days=14,  # 2 weeks forecast
        min_value=0,  # Sales cannot be negative
        output_prefix='sales_counterfactual',
        auto_detect=False
    )
    
    event = Event(
        start=pd.Timestamp('2024-07-15'),
        end=pd.Timestamp('2024-07-17'),
        name='promotion'
    )
    
    counterfactual = generator.generate(
        df=df_clean,
        event_start=event.start,
        event_end=event.end,
        event_name=event.name
    )
    
    print(f"Generated sales counterfactual with AR(2) and weekly cycle")
    return counterfactual


if __name__ == '__main__':
    print("Running examples...")
    print("\n1. Basic usage:")
    example_basic_usage()
    
    print("\n2. Multiple events:")
    example_multiple_events()
    
    print("\n3. Actual vs counterfactual:")
    example_with_actual_data()
    
    print("\n4. Custom configuration:")
    example_custom_configuration()
    
    print("\nAll examples completed!")

