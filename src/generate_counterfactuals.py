#!/usr/bin/env python3
"""Generate counterfactuals for time series data."""

import pandas as pd
import sys
import os
import argparse
import json
from pathlib import Path
from typing import Optional, List, Dict

# Add project root to path to import counterfactual_ts
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.insert(0, project_root)

from counterfactual_ts import (
    TimeSeriesCounterfactualGenerator,
    Event,
    clean_time_series
)


def parse_arguments():
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description='Generate counterfactuals for time series data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 generate_counterfactuals.py --input data.csv --events events.json
  python3 generate_counterfactuals.py --input data.csv --time-col timestamp --target-col value
  python3 generate_counterfactuals.py --input data.csv --ar-order 2 --forecast-days 7 --cycle-period day
        """
    )
    
    # Arguments
    parser.add_argument(
        '--input', '-i',
        required=True,
        help='Input CSV file path'
    )
    
    parser.add_argument(
        '--events', '-e',
        required=True,
        help='Events JSON file or comma-separated event definitions (format: "name:start:end,name2:start2:end2")'
    )
    
    parser.add_argument(
        '--output', '-o',
        help='Output CSV file path (default: <input>_counterfactuals.csv)'
    )
    
    # Column detection - auto-detected if not provided)
    parser.add_argument(
        '--time-col',
        help='Name of time/datetime column (auto-detected if not provided)'
    )
    
    parser.add_argument(
        '--target-col',
        help='Name of target/value column (auto-detected if not provided)'
    )
    
    parser.add_argument(
        '--entity-col',
        help='Name of entity identifier column (e.g., sensor, location). If provided, processes each entity separately.'
    )
    
    # Parameters
    parser.add_argument(
        '--ar-order',
        type=int,
        default=1,
        help='AR model order (number of lags). Default: 1'
    )
    
    parser.add_argument(
        '--cycle-period',
        choices=['hour', 'day', 'week', 'month'],
        help='Cycle period for pattern extraction. Auto-detected if not provided.'
    )
    
    parser.add_argument(
        '--forecast-days',
        type=int,
        default=5,
        help='Number of days to forecast after event end. Default: 5'
    )
    
    parser.add_argument(
        '--min-value',
        type=float,
        help='Minimum value constraint (e.g., 0 for non-negative values)'
    )
    
    parser.add_argument(
        '--max-value',
        type=float,
        help='Maximum value constraint'
    )
    
    parser.add_argument(
        '--no-auto-detect',
        action='store_true',
        help='Disable auto-detection (requires --time-col and --target-col)'
    )
    
    return parser.parse_args()


def load_events(events_arg: str) -> List[Event]:
    """Load events from JSON file or parse from string."""
    events = []
    
    if os.path.exists(events_arg):
        with open(events_arg, 'r') as f:
            if events_arg.endswith('.json'):
                data = json.load(f)
                if isinstance(data, list):
                    for event_data in data:
                        events.append(Event(
                            start=pd.Timestamp(event_data['start']),
                            end=pd.Timestamp(event_data['end']),
                            name=event_data['name'],
                            metadata=event_data.get('metadata', {})
                        ))
                else:
                    raise ValueError("JSON file must contain a list of events")
            else:
                data = json.load(f)
                if isinstance(data, list):
                    for event_data in data:
                        events.append(Event(
                            start=pd.Timestamp(event_data['start']),
                            end=pd.Timestamp(event_data['end']),
                            name=event_data['name'],
                            metadata=event_data.get('metadata', {})
                        ))
    else:
        for event_str in events_arg.split(','):
            parts = event_str.strip().split(':')
            if len(parts) != 3:
                raise ValueError(f"Invalid event format: {event_str}. Expected 'name:start:end'")
            name, start, end = parts
            events.append(Event(
                start=pd.Timestamp(start.strip()),
                end=pd.Timestamp(end.strip()),
                name=name.strip()
            ))
    
    return events


def process_single_entity(
    df: pd.DataFrame,
    generator: TimeSeriesCounterfactualGenerator,
    events: List[Event],
    entity_name: Optional[str] = None
) -> Optional[pd.DataFrame]:
    """Generate counterfactuals for a single entity."""
    try:
        result = generator.generate_multiple(df, events)
        
        if result is not None and len(result) > 0:
            if entity_name is not None:
                time_col = result.columns[0]
                result['entity'] = entity_name
                cols = [time_col, 'entity'] + [c for c in result.columns if c != time_col and c != 'entity']
                result = result[cols]
            
            return result
    except Exception as e:
        if entity_name:
            print(f"      Error processing {entity_name}: {e}")
        else:
            print(f"      Error: {e}")
        return None


def main():
    args = parse_arguments()
    
    print("=" * 70)
    print("Counterfactual Generator")
    print("=" * 70)
    
    print(f"\nLoading data from: {args.input}")
    if not os.path.exists(args.input):
        raise FileNotFoundError(f"Input file not found: {args.input}")
    
    df = pd.read_csv(args.input)
    print(f"   Loaded {len(df)} rows")
    print(f"   Columns: {df.columns.tolist()}")
    
    print(f"\nLoading events from: {args.events}")
    events = load_events(args.events)
    print(f"   Loaded {len(events)} events:")
    for event in events:
        print(f"     - {event.name}: {event.start.date()} to {event.end.date()}")
    
    print(f"\nCleaning data...")
    auto_detect = not args.no_auto_detect
    
    df_clean, detected = clean_time_series(
        df,
        time_col=args.time_col,
        target_col=args.target_col,
        entity_col=args.entity_col,
        auto_detect=auto_detect
    )
    
    print(f"   Cleaned: {len(df_clean)} rows")
    print(f"   Detected columns:")
    print(f"     - Time: {detected.get('time_col')}")
    print(f"     - Target: {detected.get('target_col')}")
    if detected.get('entity_col'):
        print(f"     - Entity: {detected.get('entity_col')}")
    
    print(f"\nCreating generator...")
    generator = TimeSeriesCounterfactualGenerator(
        time_col=detected.get('time_col'),
        target_col=detected.get('target_col'),
        ar_order=args.ar_order,
        cycle_period=args.cycle_period,  # None = auto-detect
        forecast_days=args.forecast_days,
        min_value=args.min_value,
        max_value=args.max_value,
        auto_detect=auto_detect
    )
    print(f"   Generator created")
    print(f"     AR order: {generator.ar_order}")
    print(f"     Forecast days: {generator.forecast_days}")
    print(f"     Cycle period: {args.cycle_period or 'auto-detect'}")
    
    entity_col = detected.get('entity_col') or args.entity_col
    all_results = []
    
    if entity_col and entity_col in df.columns:
        print(f"\nProcessing entities (column: {entity_col})...")
        entities = df[entity_col].unique()
        print(f"   Found {len(entities)} entities")
        
        for entity_name in entities:
            print(f"\n   Processing: {entity_name}")
            entity_df = df[df[entity_col] == entity_name].copy()
            
            entity_clean, _ = clean_time_series(
                entity_df,
                time_col=detected.get('time_col'),
                target_col=detected.get('target_col'),
                entity_col=None,
                auto_detect=False
            )
            
            result = process_single_entity(entity_clean, generator, events, str(entity_name))
            if result is not None:
                all_results.append(result)
    else:
        print(f"\nProcessing time series...")
        result = process_single_entity(df_clean, generator, events)
        if result is not None:
            all_results.append(result)
    
    if len(all_results) == 0:
        raise ValueError("No counterfactuals generated")
    
    print(f"\nCombining results...")
    final_df = pd.concat(all_results, ignore_index=True)
    
    sort_cols = ['entity'] if 'entity' in final_df.columns else []
    time_col = final_df.columns[0]
    sort_cols.insert(0, time_col)
    final_df = final_df.sort_values(sort_cols)
    
    if args.output:
        output_file = args.output
    else:
        input_path = Path(args.input)
        output_file = input_path.parent / f"{input_path.stem}_counterfactuals.csv"
    
    output_dir = os.path.dirname(output_file)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    
    final_df.to_csv(output_file, index=False)
    
    print(f"\nSaved to: {output_file}")
    print(f"Rows: {len(final_df)}")
    
    print("\n" + "=" * 70)
    print("Summary")
    print("=" * 70)
    print(f"Input rows: {len(df)}")
    print(f"Output rows: {len(final_df)}")
    print(f"Events processed: {len(events)}")
    if entity_col:
        print(f"Entities processed: {len(all_results)}")
    print("=" * 70)
    
    return final_df


if __name__ == '__main__':
    try:
        result = main()
        print("\nDone")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
