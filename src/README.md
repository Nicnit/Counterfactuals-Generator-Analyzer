# Examples Directory

Example scripts for using the counterfactual time series library.

## Quick Start

### Generate Counterfactuals

The `generate_counterfactuals.py` script works with any time series data:

```bash
python3 src/generate_counterfactuals.py \
  --input src/Data/your_data.csv \
  --events src/events_example.json
```

Or use inline event definitions:
```bash
python3 src/generate_counterfactuals.py \
  --input src/Data/your_data.csv \
  --events "event1:2024-07-15:2024-07-17,event2:2024-11-19:2024-11-22"
```

### Auto-Detection

The script detects:
- Time column: any datetime/timestamp column
- Target column: any numeric value column
- Entity column: if multiple entities (sensors, locations, etc.)
- Data frequency: hourly, daily, etc.
- Cycle period: hour, day, week, or month patterns

### Manual Override

Override any auto-detected parameter:

```bash
python3 src/generate_counterfactuals.py \
  --input data.csv \
  --events events.json \
  --time-col timestamp \
  --target-col sales \
  --entity-col location
```

```bash
python3 src/generate_counterfactuals.py \
  --input data.csv \
  --events events.json \
  --ar-order 2 \
  --forecast-days 7 \
  --cycle-period day \
  --min-value 0
```

### Events File Format

Create a JSON file (e.g., `events.json`):

```json
[
  {
    "name": "event1",
    "start": "2024-07-15",
    "end": "2024-07-17",
    "metadata": {"type": "holiday"}
  },
  {
    "name": "event2",
    "start": "2024-11-19",
    "end": "2024-11-22"
  }
]
```

Or use inline format: `"name1:start1:end1,name2:start2:end2"`

### Output

Output CSV contains:
- Time column (auto-detected name)
- Entity column (if multiple entities)
- Counterfactual columns: `counterfactual_<event_name>` for each event

## Compare Actual vs Counterfactual

After generating counterfactuals, compare them against actual data:

```bash
python3 src/compare_counterfactuals.py \
  --actual src/Data/traffic_dataset_with_trend.csv \
  --counterfactual src/Data/traffic_dataset_with_trend_counterfactuals.csv
```

With events JSON file:
```bash
python3 src/compare_counterfactuals.py \
  --actual data.csv \
  --counterfactual counterfactuals.csv \
  --events events.json
```

Generates:
- `comparison_differences.csv`: detailed differences for each time point
- `comparison_time_aggregated.csv`: statistics aggregated by time
- `comparison_summary.csv`: overall summary statistics per event

## Other Scripts

- `gen_counterfactuals.py`: original implementation (for reference)
- `run_counterfactuals.py`: original script using `gen_counterfactuals.py`
- `calculate_differences.py`: calculate differences between actual and counterfactual values (reference)
- `query_differences.py`: query and filter difference results (reference)

## Requirements

- Python 3.8+
- pandas >= 2.0.0
- numpy >= 1.24.0

Install dependencies:
```bash
pip install -r ../requirements.txt
```
