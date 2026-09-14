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

With an entity column each entity is modelled on its own, so `Store_A` and
`Store_B` never share an AR fit.

## Time events

Create `events.json`:
```json
[{"name": "holiday", "start": "2024-12-25", "end": "2024-12-26"}]
```

Or inline: `--events "holiday:2024-12-25:2024-12-26"`

`start` and `end` may be equal for a single-day event. The forecast window
always runs from `start` to `end` plus `--forecast-days`.

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

**Cyclical Patterns**: Extracts the average hourly/daily/weekly offset from pre-event data
**AR Model**: Fits an autoregressive model to the pre-event data *after* that pattern is subtracted
**Forecast**: Runs the AR model forward on the level, then adds the pattern back
**Comparison**: Stats on actual vs counterfactual differences

The cycle is deliberately kept out of the AR recursion. Feeding it back in
would push it through the AR filter, scaling it by `1 / |1 - phi|` and shifting
its phase, which inflates the swing in the counterfactual.

By default the output is the conditional expectation with no random component.
`--noise-factor` adds residual-scaled noise if you want a sampled path instead;
the draw is seeded from the event name, so repeated runs agree.

## Saying whether an effect is real

A point counterfactual on its own cannot tell you whether a difference is
bigger than noise. Pass `--interval` to get a prediction band alongside it:

```bash
python3 src/generate_counterfactuals.py --input sales.csv --events events.json --interval 0.9
python3 src/compare_counterfactuals.py --actual sales.csv --counterfactual sales_counterfactuals.csv --interval 0.9
```

The band comes from resampling the fitted AR residuals and running them through
the same recursion as the forecast, so it widens with the horizon and settles at
the model's stationary spread. The comparison then reports how many points fell
outside it against how many you would expect by chance:

```
Mean difference: 2.60
Outside prediction band: 9.8% of points (10.0% expected by chance)
  -> consistent with no effect
```

On the same data with a 200-unit effect injected over the event window, the
same command recovers it and flags it:

```
Mean difference: 200.87
Outside prediction band: 80.2% of points (10.0% expected by chance)
  -> more than chance would give
```

The band covers innovation uncertainty, not uncertainty in the fitted
coefficients, so treat it as a guide rather than a formal test.

## When there isn't enough history

The generator warns if the pre-event window is shorter than twice the forecast
window, or if the data covers less than half the chosen cycle. Both mean the
counterfactual is extrapolating further than the history supports.

## What this will not do

**Trending series.** The model is AR plus a repeating cycle. It has no trend
term, so a series that drifts steadily fits an AR root at or above 1 and the
forecast runs away instead of settling. The generator warns when that happens:

```
fitted AR model is non-stationary (largest root 1.158)
```

Take the warning seriously. Run against California's cigarette sales from the
Proposition 99 study — a series falling steadily from 1970 — it extrapolates the
decline to *negative* 97 packs per capita by 2000. Difference the series or
detrend it first, or use a method built for this.

**Borrowing from control units.** This builds a counterfactual from one series'
own past. It is not a synthetic control and cannot use unaffected units to
predict what the treated one would have done. For studies of that shape, use
synthetic control or CausalImpact instead.

**Formal inference.** The band covers innovation uncertainty only. It is not a
hypothesis test and does not account for uncertainty in the fitted
coefficients.

## Options

```bash
--time-col timestamp    # Time column
--target-col sales  # Value column
--entity-col store  # Entity column
--ar-order 2    # AR model order
--cycle-period day  # hour, day, week, month, day_of_year, quarter
--forecast-days 7   # Days to forecast past the event end
--min-value 0   # Min constraint
--max-value 100 # Max constraint
--noise-factor 0    # Residual noise scale, 0 = expectation only
--random-seed 42    # Overrides the event-name seed
--interval 0.9  # Emit a prediction band at this level
--n-paths 500   # Bootstrap paths behind the band
```

## Using library

```python
from counterfactual_ts import TimeSeriesCounterfactualGenerator, Event

generator = TimeSeriesCounterfactualGenerator(target_col='Sales', interval=0.9)
events = [Event(start='2024-07-15', end='2024-07-17', name='holiday')]
result = generator.generate_multiple(df, events)
```

`df` needs a `DatetimeIndex`; `clean_time_series` will build one for you.
After a run, `generator.model_` holds the fitted AR model, so you can check
`phi` and `residual_std`.

## Requirements

- Python 3.8+
- pandas >= 2.0.0
- numpy >= 1.24.0

## Tests

```bash
pip install -r requirements-dev.txt
python3 -m pytest tests/
```

The core test builds a series with a known cycle and known noise but no event
effect, then asserts the counterfactual lands back on the observed data.

## License

Unlicense

## Notes

- Need two or three times the event duration in pre-event data
- Works with multiple entities
- Usually auto detects columns and frequency
- Rows with a missing target are dropped before fitting; the frequency is read
  from the full pre-event slice so those gaps don't distort it
- `src/gen_counterfactuals.py`, `src/run_counterfactuals.py` and
  `src/calculate_differences.py` are the older PM2.5-specific scripts and do
  not share the library's model code
