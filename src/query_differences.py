#!/usr/bin/env python3
"""
Query differences statistics for a specific date range, sensor name, and event.
Usage:
    python3 query_differences.py --start "2024-11-19" --end "2024-11-22" --name "Kerry Freight Pakistan Pvt Ltd" --event expo
    python3 query_differences.py --start "2024-07-15" --end "2024-07-17" --name "Aisha Manzil" --event muharran
"""

import pandas as pd
import numpy as np
import argparse
import sys
import os
from datetime import datetime

def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Query differences statistics between actual and counterfactual values',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 query_differences.py --start "2024-11-19" --end "2024-11-22" --name "Kerry Freight Pakistan Pvt Ltd" --event expo
  python3 query_differences.py --start "2024-07-15" --end "2024-07-20" --name "Aisha Manzil" --event muharran
        """
    )
    
    parser.add_argument(
        '--start',
        type=str,
        required=True,
        help='Start date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)'
    )
    
    parser.add_argument(
        '--end',
        type=str,
        required=True,
        help='End date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)'
    )
    
    parser.add_argument(
        '--name',
        type=str,
        required=True,
        nargs='+',  # Accept multiple words
        help='Entity name (e.g., sensor, location, store). Can be multiple words.'
    )
    
    parser.add_argument(
        '--event',
        type=str,
        required=True,
        help='Event name (e.g., event1, event2)'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        default=None,
        help='Optional: Output CSV file path to save results'
    )
    
    args = parser.parse_args()
    
    # Join name list into a single string if it's a list
    if isinstance(args.name, list):
        args.name = ' '.join(args.name)
    
    return args

def load_differences_data(event_name, script_dir):
    """Load the differences detailed file for the specified event."""
    differences_file = os.path.join(
        script_dir,
        'Output',
        f'differences_detailed_{event_name}.csv'
    )
    
    if not os.path.exists(differences_file):
        raise FileNotFoundError(
            f"Differences file not found: {differences_file}\n"
            f"Please run calculate_differences.py first to generate the differences files."
        )
    
    df = pd.read_csv(differences_file)
    df['Datetime (UTC+5)'] = pd.to_datetime(df['Datetime (UTC+5)'])
    
    return df

def filter_data(df, start_date, end_date, entity_name):
    """Filter dataframe by date range and entity name.
    
    Note: This function expects specific column names from the original example data.
    For general use, adapt column names to match your data structure.
    """
    # Filter by date range
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    
    # Include the end date (inclusive)
    if end.hour == 0 and end.minute == 0 and end.second == 0:
        # If end is at midnight, include the entire day
        end = end + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    
    filtered = df[
        (df['Datetime (UTC+5)'] >= start) & 
        (df['Datetime (UTC+5)'] <= end)
    ].copy()
    
    if len(filtered) == 0:
        return None
    
    # Filter by entity name (case-insensitive partial match)
    # First try exact match, then case-insensitive, then partial match
    name_filtered = filtered[
        filtered['Name'].str.contains(entity_name, case=False, na=False, regex=False)
    ].copy()
    
    if len(name_filtered) == 0:
        # Try to find similar names
        all_names = filtered['Name'].unique()
        print(f"\nWarning: No exact match found for '{entity_name}'")
        print(f"Available entity names in date range:")
        for name in sorted(all_names):
            print(f"  - {name}")
        return None
    
    return name_filtered

def calculate_statistics(df):
    """Calculate comprehensive statistics on the filtered data."""
    if df is None or len(df) == 0:
        return None
    
    # Filter out NaN differences
    valid_diffs = df['difference'].dropna()
    valid_actual = df['actual_PM25'].dropna()
    valid_counterfactual = df['counterfactual_PM25'].dropna()
    
    if len(valid_diffs) == 0:
        return None
    
    stats = {
        # Basic counts
        'total_time_points': len(df['Datetime (UTC+5)'].unique()),
        'total_observations': len(df),
        'valid_observations': len(valid_diffs),
        
        # Difference statistics
        'mean_difference': valid_diffs.mean(),
        'median_difference': valid_diffs.median(),
        'std_difference': valid_diffs.std(),
        'min_difference': valid_diffs.min(),
        'max_difference': valid_diffs.max(),
        'q25_difference': valid_diffs.quantile(0.25),
        'q75_difference': valid_diffs.quantile(0.75),
        
        # Counts by sign
        'num_positive': (valid_diffs > 0).sum(),
        'num_negative': (valid_diffs < 0).sum(),
        'num_zero': (valid_diffs == 0).sum(),
        
        # Percentage statistics
        'pct_positive': (valid_diffs > 0).sum() / len(valid_diffs) * 100,
        'pct_negative': (valid_diffs < 0).sum() / len(valid_diffs) * 100,
        
        # Actual PM2.5 statistics
        'mean_actual_PM25': valid_actual.mean() if len(valid_actual) > 0 else np.nan,
        'median_actual_PM25': valid_actual.median() if len(valid_actual) > 0 else np.nan,
        'min_actual_PM25': valid_actual.min() if len(valid_actual) > 0 else np.nan,
        'max_actual_PM25': valid_actual.max() if len(valid_actual) > 0 else np.nan,
        
        # Counterfactual PM2.5 statistics
        'mean_counterfactual_PM25': valid_counterfactual.mean() if len(valid_counterfactual) > 0 else np.nan,
        'median_counterfactual_PM25': valid_counterfactual.median() if len(valid_counterfactual) > 0 else np.nan,
        'min_counterfactual_PM25': valid_counterfactual.min() if len(valid_counterfactual) > 0 else np.nan,
        'max_counterfactual_PM25': valid_counterfactual.max() if len(valid_counterfactual) > 0 else np.nan,
        
        # Entity information
        'entity_name': df['Name'].iloc[0],
        'city': df['City'].iloc[0] if 'City' in df.columns else 'N/A',
        'latitude': df['latitude'].iloc[0] if 'latitude' in df.columns else np.nan,
        'longitude': df['longitude'].iloc[0] if 'longitude' in df.columns else np.nan,
    }
    
    return stats

def print_statistics(stats, start_date, end_date, event_name):
    """Print statistics in a formatted way."""
    if stats is None:
        print("\nNo statistics available (no data found)")
        return
    
    print("\n" + "=" * 70)
    print(f"DIFFERENCES STATISTICS")
    print("=" * 70)
    print(f"Event: {event_name.upper()}")
    print(f"Date Range: {start_date} to {end_date}")
    print(f"Entity: {stats['entity_name']}")
    print(f"Location: {stats['city']} ({stats['latitude']:.4f}, {stats['longitude']:.4f})")
    print("-" * 70)
    
    print(f"\n📊 DATA SUMMARY")
    print(f"  Total time points: {stats['total_time_points']}")
    print(f"  Total observations: {stats['total_observations']}")
    print(f"  Valid observations: {stats['valid_observations']}")
    
    print(f"\n📈 DIFFERENCE STATISTICS (Actual - Counterfactual)")
    print(f"  Mean difference:     {stats['mean_difference']:>10.2f} μg/m³")
    print(f"  Median difference:   {stats['median_difference']:>10.2f} μg/m³")
    print(f"  Std deviation:       {stats['std_difference']:>10.2f} μg/m³")
    print(f"  Min difference:      {stats['min_difference']:>10.2f} μg/m³")
    print(f"  Max difference:      {stats['max_difference']:>10.2f} μg/m³")
    print(f"  25th percentile:     {stats['q25_difference']:>10.2f} μg/m³")
    print(f"  75th percentile:     {stats['q75_difference']:>10.2f} μg/m³")
    
    print(f"\n🔢 DIFFERENCE COUNTS")
    print(f"  Positive (actual > counterfactual): {stats['num_positive']:>4} ({stats['pct_positive']:>5.1f}%)")
    print(f"  Negative (actual < counterfactual): {stats['num_negative']:>4} ({stats['pct_negative']:>5.1f}%)")
    print(f"  Zero:                                {stats['num_zero']:>4}")
    
    print(f"\n🌬️  ACTUAL PM2.5 STATISTICS")
    if not np.isnan(stats['mean_actual_PM25']):
        print(f"  Mean:    {stats['mean_actual_PM25']:>10.2f} μg/m³")
        print(f"  Median:  {stats['median_actual_PM25']:>10.2f} μg/m³")
        print(f"  Range:   {stats['min_actual_PM25']:>10.2f} - {stats['max_actual_PM25']:>10.2f} μg/m³")
    else:
        print(f"  No data available")
    
    print(f"\n📉 COUNTERFACTUAL PM2.5 STATISTICS")
    if not np.isnan(stats['mean_counterfactual_PM25']):
        print(f"  Mean:    {stats['mean_counterfactual_PM25']:>10.2f} μg/m³")
        print(f"  Median:  {stats['median_counterfactual_PM25']:>10.2f} μg/m³")
        print(f"  Range:   {stats['min_counterfactual_PM25']:>10.2f} - {stats['max_counterfactual_PM25']:>10.2f} μg/m³")
    else:
        print(f"  No data available")
    
    print("\n" + "=" * 70)
    
    # Interpretation
    mean_diff = stats['mean_difference']
    if mean_diff > 10:
        print(f"\n💡 Interpretation: Actual PM2.5 was on average {mean_diff:.1f} μg/m³ HIGHER than counterfactual")
        print(f"   This suggests the event may have INCREASED air pollution.")
    elif mean_diff < -10:
        print(f"\n💡 Interpretation: Actual PM2.5 was on average {abs(mean_diff):.1f} μg/m³ LOWER than counterfactual")
        print(f"   This suggests the event may have DECREASED air pollution.")
    else:
        print(f"\n💡 Interpretation: Actual PM2.5 was on average {abs(mean_diff):.1f} μg/m³ different from counterfactual")
        print(f"   The difference is relatively small, suggesting limited impact.")

def save_results(df, stats, output_file, start_date, end_date, event_name, entity_name):
    """Save results to CSV file."""
    if df is None or len(df) == 0:
        print(f"\nWarning: No data to save")
        return
    
    # Create output directory if needed
    output_dir = os.path.dirname(output_file) if os.path.dirname(output_file) else '.'
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Save detailed data
    df.to_csv(output_file, index=False)
    print(f"\nSaved detailed results to: {output_file}")
    
    # Save summary statistics if stats available
    if stats is not None:
        summary_file = output_file.replace('.csv', '_summary.csv')
        summary_df = pd.DataFrame([stats])
        summary_df.to_csv(summary_file, index=False)
        print(f"Saved summary statistics to: {summary_file}")

def main():
    """Main function."""
    args = parse_arguments()
    
    # Get script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    print("=" * 70)
    print("QUERY DIFFERENCES STATISTICS")
    print("=" * 70)
    print(f"Event: {args.event}")
    print(f"Date Range: {args.start} to {args.end}")
    print(f"Entity Name: {args.name}")
    print("-" * 70)
    
    try:
        # Load differences data
        print(f"\nLoading differences data for event: {args.event}...")
        df = load_differences_data(args.event, script_dir)
        print(f"  Loaded {len(df)} total rows")
        
        # Filter data
        print(f"\nFiltering data...")
        filtered_df = filter_data(df, args.start, args.end, args.name)
        
        if filtered_df is None or len(filtered_df) == 0:
            print(f"\nNo data found matching the criteria")
            print(f"   Date range: {args.start} to {args.end}")
            print(f"   Entity name: {args.name}")
            print(f"   Event: {args.event}")
            sys.exit(1)
        
        print(f"  Found {len(filtered_df)} matching rows")
        print(f"  Time points: {len(filtered_df['Datetime (UTC+5)'].unique())}")
        
        # Calculate statistics
        print(f"\nCalculating statistics...")
        stats = calculate_statistics(filtered_df)
        
        # Print statistics
        print_statistics(stats, args.start, args.end, args.event)
        
        # Save results if output file specified
        if args.output:
            save_results(filtered_df, stats, args.output, args.start, args.end, args.event, args.name)
        
    except FileNotFoundError as e:
        print(f"\nError: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()

