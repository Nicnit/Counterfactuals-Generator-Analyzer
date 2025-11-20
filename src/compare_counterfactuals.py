#!/usr/bin/env python3
"""Compare actual vs counterfactual data."""

import pandas as pd
import numpy as np
import sys
import os
import argparse
import json
from pathlib import Path
from typing import Optional, List, Dict

# Add project root to path
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.insert(0, project_root)

from counterfactual_ts import (
    Event,
    clean_time_series
)
from counterfactual_ts.analysis import (
    compare_actual_vs_counterfactual,
    compute_summary_statistics
)
from counterfactual_ts.preprocessing import auto_detect_columns


def parse_arguments():
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description='Compare actual vs counterfactual data for events',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fully automatic (auto-detects everything)
  python3 compare_counterfactuals.py \\
      --actual data.csv \\
      --counterfactual counterfactuals.csv \\
      --events events.json
  
  # Auto-detect events from counterfactual column names
  python3 compare_counterfactuals.py \\
      --actual data.csv \\
      --counterfactual counterfactuals.csv
        """
    )
    
    parser.add_argument(
        '--actual', '-a',
        required=True,
        help='CSV file with actual data'
    )
    
    parser.add_argument(
        '--counterfactual', '-c',
        required=True,
        help='CSV file with counterfactual data (output from generate_counterfactuals.py)'
    )
    
    parser.add_argument(
        '--events', '-e',
        help='Events JSON file. If not provided, will auto-detect from counterfactual column names.'
    )
    
    parser.add_argument(
        '--forecast-days',
        type=int,
        default=5,
        help='Number of forecast days after event end (default: 5)'
    )
    
    parser.add_argument(
        '--output', '-o',
        help='Output directory for results (default: same as counterfactual file directory)'
    )
    
    parser.add_argument(
        '--time-col',
        help='Name of time column (auto-detected if not provided)'
    )
    
    parser.add_argument(
        '--target-col',
        help='Name of target column (auto-detected if not provided)'
    )
    
    parser.add_argument(
        '--entity-col',
        help='Name of entity column (e.g., sensor, location). Auto-detected if not provided.'
    )
    
    return parser.parse_args()


def load_events_from_json(events_file: str) -> List[Event]:
    """Load events from JSON."""
    with open(events_file, 'r') as f:
        data = json.load(f)
        if not isinstance(data, list):
            raise ValueError("JSON file must contain a list of events")
        
        events = []
        for event_data in data:
            events.append(Event(
                start=pd.Timestamp(event_data['start']),
                end=pd.Timestamp(event_data['end']),
                name=event_data['name'],
                metadata=event_data.get('metadata', {})
            ))
        return events


def detect_events_from_columns(counterfactual_df: pd.DataFrame) -> List[Event]:
    """Detect events from counterfactual column names."""
    events = []
    counterfactual_cols = [col for col in counterfactual_df.columns 
                          if 'counterfactual' in col.lower() and col != 'counterfactual']
    
    if not counterfactual_cols:
        raise ValueError("No counterfactual columns found")
    
    for col in counterfactual_cols:
        if 'counterfactual_' in col.lower():
            event_name = col.split('counterfactual_')[-1]
        else:
            parts = col.split('_')
            event_name = parts[-1] if len(parts) >= 2 else col
        
        event_data = counterfactual_df[counterfactual_df[col].notna()]
        if len(event_data) > 0:
            time_col = counterfactual_df.columns[0]
            start = pd.Timestamp(event_data[time_col].min())
            end = start + pd.Timedelta(days=2)
            
            events.append(Event(
                start=start,
                end=end,
                name=event_name
            ))
    
    if not events:
        raise ValueError("Could not detect events from counterfactual columns")
    
    return events


def main():
    args = parse_arguments()
    
    print("=" * 70)
    print("Compare Actual vs Counterfactual")
    print("=" * 70)
    
    print(f"\nLoading actual data: {args.actual}")
    if not os.path.exists(args.actual):
        raise FileNotFoundError(f"Actual data file not found: {args.actual}")
    
    actual_df = pd.read_csv(args.actual)
    print(f"   Loaded {len(actual_df)} rows")
    print(f"   Columns: {actual_df.columns.tolist()}")
    
    print(f"\nLoading counterfactual data: {args.counterfactual}")
    if not os.path.exists(args.counterfactual):
        raise FileNotFoundError(f"Counterfactual file not found: {args.counterfactual}")
    
    counterfactual_df = pd.read_csv(args.counterfactual)
    print(f"   Loaded {len(counterfactual_df)} rows")
    print(f"   Columns: {counterfactual_df.columns.tolist()}")
    
    print(f"\nDetecting columns...")
    actual_detected = auto_detect_columns(
        actual_df,
        time_col=args.time_col,
        target_col=args.target_col,
        entity_col=args.entity_col
    )
    
    time_col = actual_detected.get('time_col') or args.time_col
    target_col = actual_detected.get('target_col') or args.target_col
    entity_col = actual_detected.get('entity_col') or args.entity_col
    
    if not time_col:
        raise ValueError("Time column not found")
    if not target_col:
        raise ValueError("Target column not found")
    
    print(f"   Detected columns:")
    print(f"     - Time: {time_col}")
    print(f"     - Target: {target_col}")
    if entity_col:
        print(f"     - Entity: {entity_col}")
    
    print(f"\nCleaning data...")
    actual_clean, _ = clean_time_series(
        actual_df,
        time_col=time_col,
        target_col=target_col,
        entity_col=entity_col,
        auto_detect=False
    )
    print(f"   Cleaned: {len(actual_clean)} rows")
    
    print(f"\nLoading events...")
    if args.events and os.path.exists(args.events):
        events = load_events_from_json(args.events)
        print(f"   Loaded {len(events)} events")
    else:
        events = detect_events_from_columns(counterfactual_df)
        print(f"   Detected {len(events)} events")
        if args.events:
            print(f"   Warning: Events file not found")
    
    for event in events:
        print(f"     - {event.name}: {event.start.date()} to {event.end.date()}")
    
    print(f"\nPreparing counterfactual data...")
    cf_time_col = counterfactual_df.columns[0]
    counterfactual_df[cf_time_col] = pd.to_datetime(counterfactual_df[cf_time_col], errors='coerce')
    counterfactual_df = counterfactual_df.dropna(subset=[cf_time_col])
    counterfactual_df = counterfactual_df.sort_values(cf_time_col)
    print(f"   Prepared: {len(counterfactual_df)} rows")
    
    print(f"\nComparing events...")
    all_results = []
    
    for event in events:
        print(f"\n   Event: {event.name} ({event.start.date()} to {event.end.date()})")
        
        forecast_end = event.end + pd.Timedelta(days=args.forecast_days)
        event_period_start = event.start
        event_period_end = forecast_end
        
        cf_col = None
        for col in counterfactual_df.columns:
            if 'counterfactual' in col.lower() and event.name in col.lower():
                cf_col = col
                break
        
        if not cf_col:
            cf_col = f'counterfactual_{event.name}'
            if cf_col not in counterfactual_df.columns:
                print(f"     Warning: Column not found, skipping")
                continue
        
        # Filter to event period
        if isinstance(actual_clean.index, pd.DatetimeIndex):
            actual_event = actual_clean[
                (actual_clean.index >= event_period_start) &
                (actual_clean.index <= event_period_end)
            ].copy()
            actual_event = actual_event.reset_index()
            actual_time_col = actual_event.columns[0]
        else:
            actual_event = actual_clean[
                (actual_clean[time_col] >= event_period_start) &
                (actual_clean[time_col] <= event_period_end)
            ].copy()
            actual_time_col = time_col
        
        counterfactual_event = counterfactual_df[
            (counterfactual_df[cf_time_col] >= event_period_start) &
            (counterfactual_df[cf_time_col] <= event_period_end) &
            (counterfactual_df[cf_col].notna())
        ].copy()
        
        if len(actual_event) == 0:
            print(f"     Warning: No actual data in event period, skipping")
            continue
        if len(counterfactual_event) == 0:
            print(f"     Warning: No counterfactual data in event period, skipping")
            continue
        
        print(f"     Data points: {len(actual_event)} actual, {len(counterfactual_event)} counterfactual")
        
        cf_comparison = counterfactual_event[[cf_time_col, cf_col]].copy()
        if entity_col and entity_col in counterfactual_event.columns:
            cf_comparison[entity_col] = counterfactual_event[entity_col]
        cf_comparison = cf_comparison.rename(columns={cf_time_col: actual_time_col})
        
        try:
            comparison = compare_actual_vs_counterfactual(
                actual=actual_event,
                counterfactual=cf_comparison,
                time_col=actual_time_col,
                actual_col=target_col,
                counterfactual_col=cf_col,
                entity_col=entity_col,
                aggregate=True
            )
            
            differences_df = comparison['differences']
            summary = comparison['summary']
            time_aggregated = comparison.get('time_aggregated')
            
            differences_df['event_name'] = event.name
            if time_aggregated is not None:
                time_aggregated['event_name'] = event.name
            
            all_results.append({
                'event': event.name,
                'differences': differences_df,
                'summary': summary,
                'time_aggregated': time_aggregated
            })
            
            print(f"     Mean: {summary.get('mean', np.nan):.2f}, Median: {summary.get('median', np.nan):.2f}, Std: {summary.get('std', np.nan):.2f}")
            
        except Exception as e:
            print(f"     Error comparing: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    if len(all_results) == 0:
        raise ValueError("No events could be compared")
    
    print(f"\nSaving results...")
    if args.output:
        output_dir = args.output
    else:
        output_dir = os.path.dirname(os.path.abspath(args.counterfactual))
    
    os.makedirs(output_dir, exist_ok=True)
    
    all_differences = pd.concat([r['differences'] for r in all_results], ignore_index=True)
    differences_file = os.path.join(output_dir, 'comparison_differences.csv')
    all_differences.to_csv(differences_file, index=False)
    print(f"   Saved: {differences_file} ({len(all_differences)} rows)")
    
    if all_results[0]['time_aggregated'] is not None:
        all_time_agg = pd.concat([r['time_aggregated'] for r in all_results], ignore_index=True)
        time_agg_file = os.path.join(output_dir, 'comparison_time_aggregated.csv')
        all_time_agg.to_csv(time_agg_file, index=False)
        print(f"   Saved: {time_agg_file}")
    
    summary_rows = []
    for result in all_results:
        summary = result['summary'].copy()
        summary['event_name'] = result['event']
        summary_rows.append(summary)
    
    summary_df = pd.DataFrame(summary_rows)
    summary_file = os.path.join(output_dir, 'comparison_summary.csv')
    summary_df.to_csv(summary_file, index=False)
    print(f"   Saved: {summary_file}")
    
    print("\n" + "=" * 70)
    print("Summary")
    print("=" * 70)
    for result in all_results:
        event_name = result['event']
        summary = result['summary']
        print(f"\n{event_name}:")
        print(f"  Mean difference: {summary.get('mean', np.nan):.2f}")
        print(f"  Median difference: {summary.get('median', np.nan):.2f}")
        print(f"  Std difference: {summary.get('std', np.nan):.2f}")
        print(f"  Min difference: {summary.get('min', np.nan):.2f}")
        print(f"  Max difference: {summary.get('max', np.nan):.2f}")
        print(f"  Data points: {summary.get('count', 0)}")
    print("=" * 70)
    
    return all_results


if __name__ == '__main__':
    try:
        results = main()
        print("\nDone")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

