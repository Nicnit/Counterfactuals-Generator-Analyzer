import pandas as pd
import numpy as np

# days to forecast ahead, past end of event
FORECAST_DAYS = 5

# event definitions: (start_date, end_date, name)
# ensure timezone-naive timestamps
def ensure_naive(ts):
    """Ensure timestamp is timezone-naive"""
    if isinstance(ts, pd.Timestamp) and ts.tz is not None:
        return ts.tz_localize(None)
    return pd.Timestamp(ts) if not isinstance(ts, pd.Timestamp) else ts

EVENTS = [
    (ensure_naive(pd.Timestamp('2024-07-15')), ensure_naive(pd.Timestamp('2024-07-17')), 'muharran'),
    (ensure_naive(pd.Timestamp('2024-11-19')), ensure_naive(pd.Timestamp('2024-11-22')), 'expo')
]

# validate event dates
for event_start, event_end, event_name in EVENTS:
    if event_start >= event_end:
        raise ValueError(f"Invalid event dates for {event_name}: start {event_start} >= end {event_end}")

# check for overlapping events
for i, (start1, end1, name1) in enumerate(EVENTS):
    for start2, end2, name2 in EVENTS[i+1:]:
        if not (end1 < start2 or end2 < start1):
            # events overlap
            pass  # allow overlaps, they're handled independently

def generate_event_counterfactual(df, event_start, event_end, event_name, time_col, target_col):
    """
    Generate counterfactual for a single event.
    
    Args:
        df: Full dataframe with datetime index
        event_start: Start date of event
        event_end: End date of event
        event_name: Name of event (for column naming)
        time_col: Name of time column
        target_col: Name of target column
    
    Returns:
        DataFrame with datetime index and counterfactual column
    """
    # filter to pre-event data only
    pre_event_df = df[df.index < event_start].copy()
    
    if len(pre_event_df) < 2:
        raise ValueError(f"Need at least 2 data points before {event_name} event (got {len(pre_event_df)})")
    
    # check for all-nan values
    if pre_event_df[target_col].isna().all():
        raise ValueError(f"All PM2.5 values are NaN before {event_name} event")
    
    y = pre_event_df[target_col].values
    
    # warn if very little pre-event data
    if len(pre_event_df) < 24:
        pass  # could add warning here
    
    # fit ar(1) model
    y_lag = y[:-1]
    y_current = y[1:]
    
    # check for constant time series (singular matrix)
    if np.std(y_lag) < 1e-10:
        # constant series, use mean
        phi = 0.0
        c = np.mean(y_current) if len(y_current) > 0 else y[-1]
    else:
        # ols fit
        X = np.column_stack([np.ones(len(y_lag)), y_lag])
        coeffs = np.linalg.lstsq(X, y_current, rcond=None)[0]
        phi = coeffs[1]
        c = coeffs[0]
        
        # check for invalid coefficients
        if not np.isfinite(phi) or not np.isfinite(c):
            # fallback to mean if ols fails
            phi = 0.0
            c = np.mean(y_current) if len(y_current) > 0 else y[-1]
    
    # get residuals for noise later
    fitted = c + phi * y_lag
    residuals = y_current - fitted
    residual_std = np.std(residuals) if len(residuals) > 0 else 0
    
    # get daily cycle pattern from pre-event data
    pre_event_df['hour'] = pre_event_df.index.hour
    hourly_avg = pre_event_df.groupby('hour')[target_col].mean()
    overall_mean = pre_event_df[target_col].mean()
    hourly_cycle = hourly_avg - overall_mean
    
    # fill missing hours with nearest hour's value or 0
    all_hours = set(range(24))
    missing_hours = all_hours - set(hourly_cycle.index)
    if missing_hours:
        # use 0 for missing hours (no cycle adjustment)
        for h in missing_hours:
            hourly_cycle[h] = 0.0
        hourly_cycle = hourly_cycle.sort_index()
    
    # figure out frequency
    inferred_freq = pd.infer_freq(pre_event_df.index)
    if inferred_freq is None:
        inferred_freq = "h"  # hourly
    
    # forecast period: event_start to event_end + FORECAST_DAYS
    forecast_end = event_end + pd.Timedelta(days=FORECAST_DAYS)
    
    # validate dates
    if event_start >= forecast_end:
        raise ValueError(f"Invalid event dates for {event_name}: start {event_start} >= forecast_end {forecast_end}")
    
    # create date range from event_start to forecast_end (inclusive)
    forecast_index = pd.date_range(
        start=event_start,
        end=forecast_end,
        freq=inferred_freq
    )
    
    # ensure forecast starts at event_start
    if len(forecast_index) > 0 and forecast_index[0] != event_start:
        # adjust to start exactly at event_start
        forecast_index = pd.date_range(
            start=event_start,
            periods=len(forecast_index),
            freq=inferred_freq
        )
    
    if len(forecast_index) == 0:
        raise ValueError(f"Empty forecast period for {event_name} (start: {event_start}, end: {forecast_end})")
    
    forecast_horizon = len(forecast_index)
    
    # forecast with ar(1) + daily cycle
    forecast_mean = np.zeros(forecast_horizon)
    last_value = y[-1]  # start from last pre-event value
    
    for i in range(forecast_horizon):
        # ar(1) base forecast
        base_forecast = c + phi * last_value
        
        # add daily cycle if we have the pattern
        hour_of_day = forecast_index[i].hour
        if hour_of_day in hourly_cycle.index:
            cycle_adj = hourly_cycle[hour_of_day]
        else:
            cycle_adj = 0
        
        forecast_mean[i] = base_forecast + cycle_adj
        last_value = forecast_mean[i]
    
    # add some noise based on historical residuals
    # use seed for reproducibility (based on event name hash)
    if residual_std > 0:
        seed = hash(event_name) % (2**31)  # deterministic seed per event
        rng = np.random.RandomState(seed)
        noise = rng.normal(0, residual_std * 0.5, forecast_horizon)
        forecast_mean = forecast_mean + noise
    
    # ensure pm2.5 values are non-negative
    forecast_mean = np.maximum(forecast_mean, 0)
    
    # build output
    forecast_df = pd.DataFrame({
        "Datetime (UTC+5)": forecast_index,
        f"PM25_counterfactual_{event_name}": forecast_mean
    })
    
    return forecast_df

# azureml_main required by designer
def azureml_main(dataframe1=None, dataframe2=None):
    """
    dataframe1: pm2.5 data
    dataframe2: not used
    """

    if dataframe1 is None or dataframe1.empty:
        raise ValueError("Input dataframe1 is empty")

    df = dataframe1.copy()

    time_col = "Datetime (UTC+5)"
    target_col = "PM2.5 (μg/m3)"

    # clean data
    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    df = df.dropna(subset=[time_col, target_col])
    df = df.sort_values(time_col)
    df = df.set_index(time_col)
    
    # ensure timezone-naive index (Azure ML may provide timezone-aware)
    if df.index.tz is not None:
        df.index = df.index.tz_localize(None)

    # generate counterfactual for each event
    event_forecasts = []
    
    for event_start, event_end, event_name in EVENTS:
        try:
            event_forecast = generate_event_counterfactual(
                df, event_start, event_end, event_name, time_col, target_col
            )
            event_forecasts.append(event_forecast)
        except ValueError as e:
            # skip events that don't have enough pre-event data
            continue
    
    if len(event_forecasts) == 0:
        raise ValueError("No events could be processed (insufficient pre-event data)")
    
    # combine all event forecasts
    # create datetime index covering all forecast periods
    all_dates = set()
    for forecast_df in event_forecasts:
        dates = forecast_df["Datetime (UTC+5)"].values
        # ensure timezone-naive timestamps
        normalized_dates = []
        for dt in dates:
            ts = pd.Timestamp(dt)
            if ts.tz is not None:
                ts = ts.tz_localize(None)
            normalized_dates.append(ts)
        all_dates.update(normalized_dates)
    
    all_dates = sorted(all_dates)
    combined_df = pd.DataFrame({"Datetime (UTC+5)": all_dates})
    combined_df = combined_df.set_index("Datetime (UTC+5)")
    
    # merge counterfactual columns
    for forecast_df in event_forecasts:
        forecast_df = forecast_df.set_index("Datetime (UTC+5)")
        # ensure timezone-naive index
        if forecast_df.index.tz is not None:
            forecast_df.index = forecast_df.index.tz_localize(None)
        
        for col in forecast_df.columns:
            # check for overlapping dates (shouldn't happen but safety check)
            overlap = combined_df.index.intersection(forecast_df.index)
            if len(overlap) > 0 and col in combined_df.columns:
                # dates overlap and column exists - use forecast_df values (last one wins)
                pass
            combined_df[col] = forecast_df[col]
    
    # reset index to column for output
    combined_df = combined_df.reset_index()
    
    # copy metadata if it exists
    for col in ["City", "Name", "longitude", "latitude"]:
        if col in dataframe1.columns and len(dataframe1) > 0:
            combined_df[col] = dataframe1[col].iloc[-1]

    return combined_df, None