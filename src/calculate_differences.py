#!/usr/bin/env python3
"""
Calculate differences and statistics between actual and counterfactual values.
For each time point in each event period, calculates statistics across all entities.
Note: This is a reference implementation. For general use, see generate_counterfactuals.py
"""

import pandas as pd
import numpy as np
import sys
import os

# Add examples directory to path to import gen_counterfactuals
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, script_dir)
from gen_counterfactuals import EVENTS, FORECAST_DAYS

def main():
    print("=" * 60)
    print("Calculating Differences Between Actual and Counterfactual")
    print("=" * 60)
    
    # Read input data (example: replace with your data file)
    actual_file = os.path.join(script_dir, 'data', 'your_data.csv')
    counterfactual_file = os.path.join(script_dir, 'Output', 'counterfactuals_output.csv')
    
    print(f"\nReading actual data from: {actual_file}")
    actual_df = pd.read_csv(actual_file)
    
    # Normalize column names
    column_mapping = {
        'Latitude': 'latitude',
        'Longitude': 'longitude',
        'PM2.5': 'PM2.5 (μg/m3)'
    }
    actual_df = actual_df.rename(columns=column_mapping)
    
    # Add City if missing (optional metadata)
    if 'City' not in actual_df.columns:
        actual_df['City'] = ''
    
    # Parse datetime
    actual_df['Datetime (UTC+5)'] = pd.to_datetime(actual_df['Datetime (UTC+5)'], errors='coerce')
    actual_df = actual_df.dropna(subset=['Datetime (UTC+5)', 'PM2.5 (μg/m3)'])
    
    # Handle duplicates
    duplicate_cols = ['Datetime (UTC+5)', 'Name']
    if actual_df.duplicated(subset=duplicate_cols).any():
        actual_df = actual_df.groupby(duplicate_cols, as_index=False).agg({
            'PM2.5 (μg/m3)': 'mean',
            'latitude': 'first',
            'longitude': 'first',
            'City': 'first'
        })
    
    print(f"  Loaded {len(actual_df)} rows")
    
    print(f"\nReading counterfactual data from: {counterfactual_file}")
    counterfactual_df = pd.read_csv(counterfactual_file)
    counterfactual_df['Datetime (UTC+5)'] = pd.to_datetime(counterfactual_df['Datetime (UTC+5)'], errors='coerce')
    print(f"  Loaded {len(counterfactual_df)} rows")
    
    # Process each event
    for event_start, event_end, event_name in EVENTS:
        print(f"\n{'=' * 60}")
        print(f"Processing event: {event_name}")
        print(f"  Event period: {event_start} to {event_end}")
        print(f"  Forecast period: {event_start} to {event_end + pd.Timedelta(days=FORECAST_DAYS)}")
        print(f"{'=' * 60}")
        
        # Calculate event period
        forecast_end = event_end + pd.Timedelta(days=FORECAST_DAYS)
        event_period_start = event_start
        event_period_end = forecast_end
        
        # Filter actual data to event period
        actual_event = actual_df[
            (actual_df['Datetime (UTC+5)'] >= event_period_start) &
            (actual_df['Datetime (UTC+5)'] <= event_period_end)
        ].copy()
        print(f"\nActual data in event period: {len(actual_event)} rows")
        
        # Filter counterfactual data to event period
        counterfactual_col = f'PM25_counterfactual_{event_name}'
        if counterfactual_col not in counterfactual_df.columns:
            print(f"  Warning: Counterfactual column {counterfactual_col} not found, skipping event")
            continue
        
        counterfactual_event = counterfactual_df[
            (counterfactual_df['Datetime (UTC+5)'] >= event_period_start) &
            (counterfactual_df['Datetime (UTC+5)'] <= event_period_end)
        ].copy()
        print(f"Counterfactual data in event period: {len(counterfactual_event)} rows")
        
        # Merge on Datetime + sensor identifier
        merge_cols = ['Datetime (UTC+5)', 'Name']
        # Get all columns we need from counterfactual
        counterfactual_cols = merge_cols + [counterfactual_col]
        # Add location columns if they exist
        for col in ['latitude', 'longitude', 'City']:
            if col in counterfactual_event.columns:
                counterfactual_cols.append(col)
        
        merged = actual_event.merge(
            counterfactual_event[counterfactual_cols],
            on=merge_cols,
            how='inner',  # Only keep sensors present in both
            suffixes=('', '_cf')
        )
        print(f"After merge: {len(merged)} rows")
        
        if len(merged) == 0:
            print(f"  Warning: No matching data after merge, skipping event")
            continue
        
        # Calculate difference
        merged['difference'] = merged['PM2.5 (μg/m3)'] - merged[counterfactual_col]
        
        # Calculate statistics for each time point
        summary_rows = []
        detailed_rows = []
        
        for time_point in sorted(merged['Datetime (UTC+5)'].unique()):
            time_data = merged[merged['Datetime (UTC+5)'] == time_point].copy()
            
            # Filter out NaN differences
            valid_diffs = time_data['difference'].dropna()
            
            if len(valid_diffs) > 0:
                summary_row = {
                    'Datetime (UTC+5)': time_point,
                    'mean_diff': valid_diffs.mean(),
                    'median_diff': valid_diffs.median(),
                    'std_diff': valid_diffs.std(),
                    'min_diff': valid_diffs.min(),
                    'max_diff': valid_diffs.max(),
                    'num_sensors': len(valid_diffs),
                    'num_positive': (valid_diffs > 0).sum(),
                    'num_negative': (valid_diffs < 0).sum()
                }
                summary_rows.append(summary_row)
            
            # Detailed data for this time point
            for _, row in time_data.iterrows():
                # Get coordinates (handle both lowercase and capitalized)
                lon = row.get('longitude', row.get('Longitude', np.nan))
                lat = row.get('latitude', row.get('Latitude', np.nan))
                
                detailed_row = {
                    'Datetime (UTC+5)': time_point,
                    'Name': row['Name'],
                    'City': row.get('City', ''),
                    'longitude': lon,
                    'latitude': lat,
                    'actual_PM25': row['PM2.5 (μg/m3)'],
                    'counterfactual_PM25': row[counterfactual_col],
                    'difference': row['difference']
                }
                detailed_rows.append(detailed_row)
        
        # Create summary dataframe
        summary_df = pd.DataFrame(summary_rows)
        detailed_df = pd.DataFrame(detailed_rows)
        
        # Save outputs
        output_dir = os.path.join(script_dir, 'Output')
        os.makedirs(output_dir, exist_ok=True)
        
        summary_file = os.path.join(output_dir, f'differences_summary_{event_name}.csv')
        summary_df.to_csv(summary_file, index=False)
        print(f"\nSaved summary to: {summary_file}")
        print(f"  Rows: {len(summary_df)}")
        
        detailed_file = os.path.join(output_dir, f'differences_detailed_{event_name}.csv')
        detailed_df.to_csv(detailed_file, index=False)
        print(f"Saved detailed differences to: {detailed_file}")
        print(f"  Rows: {len(detailed_df)}")
        
        # Print some statistics
        if len(summary_df) > 0:
            print(f"\nSummary Statistics for {event_name}:")
            print(f"  Time points analyzed: {len(summary_df)}")
            print(f"  Average mean difference: {summary_df['mean_diff'].mean():.2f} μg/m3")
            print(f"  Average number of sensors: {summary_df['num_sensors'].mean():.1f}")
            print(f"  Total sensor-time observations: {len(detailed_df)}")
    
    print("\n" + "=" * 60)
    print("Difference calculation complete")
    print("=" * 60)

if __name__ == '__main__':
    main()

