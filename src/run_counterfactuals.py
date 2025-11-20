#!/usr/bin/env python3
"""
Generate entity-specific counterfactuals from local CSV data.
Example script demonstrating multi-entity processing (e.g., sensors, locations, stores).
Note: This is a reference implementation. For general use, see generate_counterfactuals.py
"""

import pandas as pd
import numpy as np
import sys
import os

# Add examples directory to path to import gen_counterfactuals
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, script_dir)
from gen_counterfactuals import generate_event_counterfactual, EVENTS, FORECAST_DAYS

def get_sensor_identifier(row):
    """Get unique sensor identifier from row"""
    if pd.notna(row.get('Name')):
        return str(row['Name'])
    else:
        # Fallback: use coordinates
        lat = row.get('Latitude', row.get('latitude', ''))
        lon = row.get('Longitude', row.get('longitude', ''))
        return f"{lat}_{lon}"

def main():
    print("=" * 60)
    print("Generating Sensor-Specific Counterfactuals")
    print("=" * 60)
    
    # Read input data (example: replace with your data file)
    input_file = os.path.join(script_dir, 'data', 'your_data.csv')
    print(f"\nReading data from: {input_file}")
    
    df = pd.read_csv(input_file)
    print(f"Loaded {len(df)} rows")
    print(f"Columns: {df.columns.tolist()}")
    
    # Normalize column names (handle case differences)
    column_mapping = {
        'Latitude': 'latitude',
        'Longitude': 'longitude',
        'PM2.5': 'PM2.5 (μg/m3)'
    }
    df = df.rename(columns=column_mapping)
    
    # Ensure required columns exist
    required_cols = ['Datetime (UTC+5)', 'PM2.5 (μg/m3)', 'Name']
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Add City column if missing (optional metadata)
    if 'City' not in df.columns:
        df['City'] = ''  # Default empty
    
    # Parse datetime
    print("\nParsing datetime...")
    df['Datetime (UTC+5)'] = pd.to_datetime(df['Datetime (UTC+5)'], errors='coerce')
    df = df.dropna(subset=['Datetime (UTC+5)', 'PM2.5 (μg/m3)'])
    print(f"After parsing: {len(df)} rows")
    
    # Handle duplicates: same sensor, same time - take mean PM2.5
    print("\nHandling duplicate entries...")
    duplicate_cols = ['Datetime (UTC+5)', 'Name']
    if df.duplicated(subset=duplicate_cols).any():
        print(f"Found {df.duplicated(subset=duplicate_cols).sum()} duplicate rows")
        df = df.groupby(duplicate_cols, as_index=False).agg({
            'PM2.5 (μg/m3)': 'mean',
            'latitude': 'first',
            'longitude': 'first',
            'City': 'first'
        })
        print(f"After deduplication: {len(df)} rows")
    
    # Identify unique sensors
    print("\nIdentifying unique sensors...")
    sensors = df.groupby('Name').first().reset_index()
    print(f"Found {len(sensors)} unique sensors")
    
    # Process each sensor
    all_counterfactuals = []
    sensors_processed = 0
    sensors_successful = {'muharran': 0, 'expo': 0}
    sensors_skipped = {'muharran': 0, 'expo': 0}
    
    time_col = "Datetime (UTC+5)"
    target_col = "PM2.5 (μg/m3)"
    
    for sensor_name in sensors['Name'].unique():
        print(f"\nProcessing sensor: {sensor_name}")
        sensors_processed += 1
        
        # Filter to this sensor's data
        sensor_df = df[df['Name'] == sensor_name].copy()
        sensor_df = sensor_df.sort_values(time_col)
        
        # Get sensor metadata
        sensor_meta = sensor_df.iloc[0][['Name', 'City', 'latitude', 'longitude']].to_dict()
        
        # Set datetime index
        sensor_df = sensor_df.set_index(time_col)
        
        # Normalize timezone
        if sensor_df.index.tz is not None:
            sensor_df.index = sensor_df.index.tz_localize(None)
        
        # Generate counterfactuals for each event
        event_counterfactuals = []
        
        for event_start, event_end, event_name in EVENTS:
            try:
                print(f"  Generating counterfactual for {event_name}...")
                event_forecast = generate_event_counterfactual(
                    sensor_df, event_start, event_end, event_name, time_col, target_col
                )
                event_counterfactuals.append((event_name, event_forecast))
                sensors_successful[event_name] += 1
                print(f"    Success: {len(event_forecast)} time points")
            except ValueError as e:
                print(f"    Skipped: {e}")
                sensors_skipped[event_name] += 1
                continue
        
        # Merge counterfactuals from all events for this sensor
        if len(event_counterfactuals) > 0:
            # Start with first event's dataframe
            combined = event_counterfactuals[0][1].copy()
            
            # Merge other events using outer join to preserve all dates
            for event_name, event_df in event_counterfactuals[1:]:
                combined = combined.merge(
                    event_df,
                    on="Datetime (UTC+5)",
                    how='outer'
                )
            
            # Add sensor metadata to each row
            for col in ['Name', 'City', 'latitude', 'longitude']:
                combined[col] = sensor_meta.get(col, None)
            
            all_counterfactuals.append(combined)
    
    # Combine all sensors
    if len(all_counterfactuals) == 0:
        raise ValueError("No counterfactuals generated for any sensor")
    
    print(f"\nCombining counterfactuals from {len(all_counterfactuals)} sensors...")
    final_df = pd.concat(all_counterfactuals, ignore_index=True)
    final_df = final_df.sort_values(['Name', 'Datetime (UTC+5)'])
    
    # Reorder columns
    col_order = ['Datetime (UTC+5)', 'Name', 'City', 'latitude', 'longitude']
    counterfactual_cols = [c for c in final_df.columns if 'counterfactual' in c]
    col_order.extend(counterfactual_cols)
    final_df = final_df[[c for c in col_order if c in final_df.columns]]
    
    # Save output
    output_file = os.path.join(script_dir, 'Output', 'counterfactuals_output.csv')
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    final_df.to_csv(output_file, index=False)
    print(f"\nSaved counterfactuals to: {output_file}")
    print(f"  Total rows: {len(final_df)}")
    print(f"  Columns: {final_df.columns.tolist()}")
    
    # Print summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Sensors processed: {sensors_processed}")
    for event_name in ['muharran', 'expo']:
        print(f"  {event_name}: {sensors_successful[event_name]} successful, {sensors_skipped[event_name]} skipped")
    
    return final_df

if __name__ == '__main__':
    main()

