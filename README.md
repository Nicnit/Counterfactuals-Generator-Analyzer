# Counterfactuals Generator

Tool for generating counterfactual forecasts from time series data. Built for my stats project.

```bash
pip install -r requirements.txt
python3 src/generate_counterfactuals.py --input data.csv --events events.json
```

## Data format

CSV needs:
- Datetime column 
- Numeric value column
- Optional: entity column

Example:
```csv
Timestamp,Sales,Store
2024-01-01 00:00:00,1500,Store_A
```

## Time events

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

## How it works

**AR Model**: Fits autoregressive model to pre-event data
**Cyclical Patterns**: Extracts hourly/daily/weekly patterns  
**Forecast**: Combines AR predictions with patterns + noise
**Comparison**: Stats on actual vs counterfactual differences

## Options

```bash
--time-col timestamp    # Time column
--target-col sales  # Value column
--ar-order 2    # AR model order
--cycle-period day  # Pattern cycle
--forecast-days 7   # Days to forecast
--min-value 0   # Min constraint
```

## Using library

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

## License

This project is released under the Unlicense - see [LICENSE](LICENSE) file for details.

## Notes

- Need two or three times the event duration in pre-event data
- Works with multiple entities
- Usually auto detects columns and frequency