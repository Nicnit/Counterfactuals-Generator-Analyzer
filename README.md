# Counterfactuals Generator

Tool for generating counterfactual forecasts from time series data. Built for my stats project.

## Quick Start

```bash
pip install -r requirements.txt
python3 src/generate_counterfactuals.py --input data.csv --events events.json
```

## Data Format

CSV needs:
- Datetime column 
- Numeric value column
- Optional: entity column

Example:
```csv
Timestamp,Sales,Store
2024-01-01 00:00:00,1500,Store_A
```

## Events

Create `events.json`:
```json
[{"name": "holiday", "start": "2024-12-25", "end": "2024-12-26"}]
```

Or inline: `--events "holiday:2024-12-25:2024-12-26"`

## Usage

Generate counterfactuals:
```bash
python3 src/generate_counterfactuals.py --input sales.csv --events events.json
```

Compare results:
```bash
python3 src/compare_counterfactuals.py --actual sales.csv --counterfactual sales_counterfactuals.csv
```

## How it Works

1. **AR Model**: Fits autoregressive model to pre-event data
2. **Cyclical Patterns**: Extracts hourly/daily/weekly patterns  
3. **Forecast**: Combines AR predictions with patterns + noise
4. **Comparison**: Stats on actual vs counterfactual differences

## Options

```bash
--time-col timestamp      # Time column
--target-col sales        # Value column
--ar-order 2              # AR model order
--cycle-period day        # Pattern cycle
--forecast-days 7         # Days to forecast
--min-value 0             # Min constraint
```

## Library Use

```python
from counterfactual_ts import TimeSeriesCounterfactualGenerator, Event

generator = TimeSeriesCounterfactualGenerator()
events = [Event(start='2024-07-15', end='2024-07-17', name='holiday')]
result = generator.generate_multiple(df, events)
```

## Requirements

- Python 3.8+
- pandas >= 2.0.0
- numpy >= 1.24.0

## Notes

- Need 2-3x event duration in pre-event data
- Works with multiple entities
- Auto-detects columns and frequency