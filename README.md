# Counterfactuals Generator and Analyzer

Python library and CLI for generating counterfactual forecasts for time series data. Detects parameters from your data automatically. Works with sales, traffic, temperature, air quality, and other time series.

## Description

Project that uses Datetime .csv data to generate counterfactuals given events with a date range.
Can them compare given data against the generated counterfactual data to see if the event was statistically significant.

Generates counterfactual forecasts by:
1. Learning patterns from pre-event data
2. Forecasting what would have happened during and after the event
3. Comparing actual vs counterfactual to measure event impact

Use cases:
- Measure holiday impact on sales
- Evaluate policy changes on economic indicators
- Assess marketing campaign effects

## Quick Start

### Installation

```bash
pip install -r requirements.txt
```

### Generate Counterfactuals

```bash
python3 src/generate_counterfactuals.py \
  --input your_data.csv \
  --events events.json
```

Example output:
Summary
Input rows: 36144
Output rows: 4732
Events processed: 1
Entities processed: 28

### Compare Actual vs Counterfactual

```bash
python3 src/compare_counterfactuals.py \
  --actual your_data.csv \
  --counterfactual your_data_counterfactuals.csv
```

Example output:
  Mean difference: 2.90
  Median difference: -4.05
  Std difference: 72.81
  Min difference: -125.87
  Max difference: 263.08
  Data points: 56

Generates comparison statistics showing differences between actual and counterfactual values.

### Autoregressive (AR) Model

Fits an AR(p) model to pre-event data: `y_t = c + φ₁y_{t-1} + ... + φₚy_{t-p} + ε_t`

### Cyclical Pattern Extraction

Extracts repeating patterns (hourly, daily, weekly, monthly):
- Hourly: daily cycles
- Daily: weekly patterns
- Weekly: monthly patterns
- Monthly: yearly patterns

### Forecast Generation

Combines AR model predictions with cyclical adjustments. Adds controlled noise based on historical residuals. Applies value constraints. Generates forecasts for event period plus configurable forecast horizon.

### Features

- Auto-detection: detects columns, frequency, and cycle patterns
- Manual override: all parameters can be specified explicitly
- Domain agnostic: works with any time series
- Multiple events: process multiple events in one run
- Entity support: handles multiple entities
- Statistical analysis: compares actual vs counterfactual with detailed statistics

## Installation

### Requirements

- Python 3.8+
- pandas >= 2.0.0
- numpy >= 1.24.0

### Setup

install requirements.txt ^

A bash setup available
```bash
./setup.sh
```

### Data Requirements

CSV file needs:
A datetime column (any name: `timestamp`, `date`, `time`, etc.)
A numeric value column (any name: `sales`, `traffic`, `temperature`, etc.)
can have an entity column

Example data structure:
```csv
Timestamp,Sales,Store
2024-01-01 00:00:00,1500,Store_A
2024-01-01 01:00:00,1200,Store_A
2024-01-01 02:00:00,800,Store_A
...
```

### Prepare Data

Data needs to
- have a datetime column
- have a numeric value column
- be sorted by time
- have enough pre event data

### Define Events

Create an events JSON file (`events.json`):

```json
[
  {
    "name": "holiday",
    "start": "2024-12-25",
    "end": "2024-12-26",
    "metadata": {
      "type": "holiday",
      "description": "Christmas holiday"
    }
  }
]
```

Or use inline format:
```bash
--events "holiday:2024-12-25:2024-12-26"
```

### Generate Counterfactuals

```bash
python3 src/generate_counterfactuals.py \
  --input your_data.csv \
  --events events.json
```

Output: `your_data_counterfactuals.csv`

### Compare Results

```bash
python3 src/compare_counterfactuals.py \
  --actual your_data.csv \
  --counterfactual your_data_counterfactuals.csv \
  --events events.json
```

Generates:
- `comparison_differences.csv`: detailed differences for each time point
- `comparison_time_aggregated.csv`: statistics aggregated by time
- `comparison_summary.csv`: overall summary statistics per event

## Example Commands

### Examples

```bash
python3 src/generate_counterfactuals.py \
  --input sales_data.csv \
  --events events.json
```

### Manual Column Specification

```bash
python3 src/generate_counterfactuals.py \
  --input data.csv \
  --events events.json \
  --time-col timestamp \
  --target-col revenue \
  --entity-col store_id
```

### Custom Model Parameters

```bash
python3 src/generate_counterfactuals.py \
  --input data.csv \
  --events events.json \
  --ar-order 2 \
  --forecast-days 7 \
  --cycle-period day \
  --min-value 0
```

### Inline Events

```bash
python3 src/generate_counterfactuals.py \
  --input data.csv \
  --events "holiday:2024-12-25:2024-12-26,event2:2024-11-20:2024-11-22"
```

## Using the Library

```python
from counterfactual_ts import (
    TimeSeriesCounterfactualGenerator,
    Event,
    clean_time_series
)
import pandas as pd

df = pd.read_csv('your_data.csv')
df_clean, detected = clean_time_series(df, auto_detect=True)

generator = TimeSeriesCounterfactualGenerator(auto_detect=True)

events = [
    Event(start='2024-07-15', end='2024-07-17', name='holiday')
]

result = generator.generate_multiple(df_clean, events)
```

## Auto Detection

The library detects:

Time column looks for datetime columns (`timestamp`, `date`, `time`, `datetime`, etc.)
Target column identifies first numeric column (excludes `id`, `name`, `latitude`, `longitude`)
Entity column detects if multiple entities exist
Frequency infers from time differences
Cycle period determines appropriate cycle based on frequency:
- Sub-daily data -> `hour`
- Daily data -> `day`
- Weekly data -> `week`
- Monthly data -> `month`

All parameters can be manually overridden. Auto-detection is a convenience, not a requirement.

### CLI Args

```bash
--input, -i              Input CSV file (required)
--events, -e             Events JSON file or inline string (required)
--output, -o             Output CSV file (default: <input>_counterfactuals.csv)
--time-col               Time column name (auto-detected if none)
--target-col             Target column name (auto-detected if none)
--entity-col             Entity column name (auto-detected if none)
--ar-order               AR model order (default: 1)
--cycle-period           Cycle period: hour/day/week/month (auto-detected if not provided)
--forecast-days          Days to forecast after event (default: 5)
--min-value              Minimum value constraint
--max-value              Maximum value constraint
--no-auto-detect         Disable auto-detection (requires --time-col and --target-col)
```

### Library Configuration

```python
generator = TimeSeriesCounterfactualGenerator(
    time_col='timestamp',          # Override auto-detection (None = auto-detect)
    target_col='value',             # Override auto-detection (None = auto-detect)
    ar_order=1,                     # AR model order (default: 1)
    cycle_period='hour',            # 'hour', 'day', 'week', 'month' (None = auto-detect)
    forecast_days=5,                # Days to forecast after event
    min_value=0,                    # Minimum value constraint (None = no constraint)
    max_value=None,                 # Maximum value constraint (None = no constraint)
    noise_factor=0.5,               # Noise injection factor (0 = no noise, 1 = full std)
    output_prefix='counterfactual', # Output column prefix
    auto_detect=True                # Enable auto-detection for unspecified parameters
)
```

## Troubleshooting

### Column Not Detected

```bash
python3 src/generate_counterfactuals.py \
  --input data.csv \
  --events events.json \
  --time-col my_time_column \
  --target-col my_value_column
```

### Wrong Cycle Period

```bash
python3 src/generate_counterfactuals.py \
  --input data.csv \
  --events events.json \
  --cycle-period day
```

### Insufficient Pre-Event Data

Need at least 2-3x the event duration in pre-event data.

### Multiple Entities

With multiple entities, the script processes each entity separately, generates counterfactuals for each, and combines results in the output file.

## License

This is free and unencumbered software released into the public domain.

Anyone is free to copy, modify, publish, use, compile, sell, or
distribute this software, either in source code form or as a compiled
binary, for any purpose, commercial or non-commercial, and by any
means.

In jurisdictions that recognize copyright laws, the author or authors
of this software dedicate any and all copyright interest in the
software to the public domain. We make this dedication for the benefit
of the public at large and to the detriment of our heirs and
successors. We intend this dedication to be an overt act of
relinquishment in perpetuity of all present and future rights to this
software under copyright law.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.

For more information, please refer to <https://unlicense.org>
