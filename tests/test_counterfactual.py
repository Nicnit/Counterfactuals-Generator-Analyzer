"""Regression tests for the counterfactual generator.

The important ones build a series with a known seasonal swing and a known
noise level but no event effect at all, so the counterfactual should land back
on the observed data.
"""

import json
import os
import subprocess
import sys
import textwrap
import warnings

import numpy as np
import pandas as pd
import pytest

from counterfactual_ts import (
    TimeSeriesCounterfactualGenerator,
    Event,
    clean_time_series,
)
from counterfactual_ts.analysis import calculate_differences
from counterfactual_ts.models import ARModel
from counterfactual_ts.patterns import CyclicalPatternExtractor
from counterfactual_ts.preprocessing import auto_detect_columns
from counterfactual_ts.query import TimeSeriesQuery
from counterfactual_ts.utils import infer_frequency, auto_detect_frequency

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

AMPLITUDE = 300.0
NOISE = 50.0


def hourly_series(days=365, amplitude=AMPLITUDE, noise=NOISE, seed=0):
    """Hourly series with a daily cycle and no event effect."""
    rng = np.random.default_rng(seed)
    index = pd.date_range('2024-01-01', periods=days * 24, freq='h', name='Timestamp')
    values = 1500 + amplitude * np.sin(2 * np.pi * np.arange(len(index)) / 24)
    values = values + rng.normal(0, noise, len(index))
    return pd.DataFrame({'Sales': values}, index=index)


def persistent_hourly_series(days=365, amplitude=AMPLITUDE, phi=0.9, noise=NOISE, seed=0):
    """Same shape, but the level itself is autocorrelated.

    A white-noise level fits phi near zero, which hides any mistake in how the
    cyclical term is carried through the AR recursion. Persistence is what
    makes that mistake visible.
    """
    rng = np.random.default_rng(seed)
    index = pd.date_range('2024-01-01', periods=days * 24, freq='h', name='Timestamp')

    level = np.empty(len(index))
    level[0] = 1500.0
    shocks = rng.normal(0, noise, len(index))
    for i in range(1, len(index)):
        level[i] = 1500.0 + phi * (level[i - 1] - 1500.0) + shocks[i]

    seasonal = amplitude * np.sin(2 * np.pi * np.arange(len(index)) / 24)
    return pd.DataFrame({'Sales': level + seasonal}, index=index)


def counterfactual_for(df, start='2024-07-15', end='2024-07-17', **kwargs):
    kwargs.setdefault('target_col', 'Sales')
    kwargs.setdefault('auto_detect', False)
    generator = TimeSeriesCounterfactualGenerator(**kwargs)
    result = generator.generate(df, start, end, 'holiday')
    return result.set_index(result.columns[0])['counterfactual_holiday']


def test_generate_methods_are_bound_to_the_class():
    # These were once defined at module level by a stray de-indent, which left
    # the class with only __init__ and broke every entry point.
    for name in ('generate', '_generate_forecast', 'generate_multiple'):
        assert callable(getattr(TimeSeriesCounterfactualGenerator, name, None))


def test_no_method_has_escaped_its_class():
    """A def taking 'self' at module level means the indentation slipped.

    This parses cleanly, so nothing else catches it.
    """
    import ast
    import pathlib

    escaped = []
    for path in sorted(pathlib.Path(PROJECT_ROOT).glob('*/*.py')):
        if '__pycache__' in path.parts:
            continue
        tree = ast.parse(path.read_text())
        for node in tree.body:
            if isinstance(node, ast.FunctionDef) and node.args.args:
                if node.args.args[0].arg == 'self':
                    escaped.append(f"{path.name}:{node.lineno} {node.name}")

    assert escaped == []


def test_baseline_recovers_a_series_with_no_event_effect():
    df = hourly_series()
    cf = counterfactual_for(df)

    diff = df.loc[cf.index, 'Sales'] - cf
    rmse = float(np.sqrt((diff ** 2).mean()))

    assert abs(diff.mean()) < NOISE / 2
    assert rmse < NOISE * 1.5


def test_cycle_is_not_amplified_by_the_ar_recursion():
    # Feeding the cyclical term back into the AR state runs it through the
    # filter, which scales it by 1 / |1 - phi * exp(-i * w)| and shifts its
    # phase. At phi = 0.9 over a 24 point cycle that is close to 3.7x.
    df = persistent_hourly_series(phi=0.9)
    cf = counterfactual_for(df, forecast_days=10)

    settled = cf.iloc[-72:]
    half_amplitude = (settled.max() - settled.min()) / 2

    assert AMPLITUDE * 0.6 < half_amplitude < AMPLITUDE * 1.4


def test_persistent_series_does_not_blow_up_the_difference():
    df = persistent_hourly_series(phi=0.9)
    cf = counterfactual_for(df)

    diff = df.loc[cf.index, 'Sales'] - cf
    rmse = float(np.sqrt((diff ** 2).mean()))

    # The level wanders on shocks the model cannot see, so the floor here is
    # the stationary spread of the level, not the observation noise.
    stationary_std = NOISE / np.sqrt(1 - 0.9 ** 2)
    assert rmse < stationary_std * 2.5


def test_target_column_is_found_when_auto_detect_is_on():
    df = hourly_series(days=60)
    generator = TimeSeriesCounterfactualGenerator(auto_detect=True)

    result = generator.generate(df, '2024-02-10', '2024-02-12', 'holiday')

    assert 'counterfactual_holiday' in result.columns


def test_missing_target_column_says_so():
    df = pd.DataFrame(
        {'label': ['a', 'b', 'c']},
        index=pd.date_range('2024-01-01', periods=3, name='Timestamp'),
    )
    generator = TimeSeriesCounterfactualGenerator(auto_detect=True)

    with pytest.raises(ValueError, match="Target column not found"):
        generator.generate(df, '2024-02-10', '2024-02-12', 'holiday')


def test_residual_scale_is_not_inflated_by_the_cycle():
    # Fitting on the raw series leaves the daily swing in the residuals, so
    # residual_std reports the cycle rather than the innovation scale.
    df = persistent_hourly_series(phi=0.9)
    generator = TimeSeriesCounterfactualGenerator(target_col='Sales', auto_detect=False)
    generator.generate(df, '2024-07-15', '2024-07-17', 'holiday')

    assert generator.model_.residual_std == pytest.approx(NOISE, rel=0.25)


def test_ar_fit_rejects_non_finite_input():
    # Without this, the NaNs reach LAPACK and it prints a DLASCL warning to
    # stderr while returning garbage coefficients.
    with pytest.raises(ValueError, match="NaN"):
        ARModel(order=1).fit(np.array([1.0, 2.0, np.nan, 4.0]))


def test_pattern_survives_a_period_with_no_usable_values():
    index = pd.date_range('2024-01-01', periods=48, freq='h')
    values = np.arange(48, dtype=float)
    values[index.hour == 3] = np.nan

    pattern = CyclicalPatternExtractor('hour').extract(
        pd.DataFrame({'x': values}, index=index), 'x'
    )

    assert pattern.notna().all()
    assert pattern[3] == 0.0


def test_default_output_is_the_expectation_with_no_noise_added():
    df = hourly_series(days=60)
    default = counterfactual_for(df, start='2024-02-10', end='2024-02-12')
    explicit = counterfactual_for(df, start='2024-02-10', end='2024-02-12', noise_factor=0.0)

    pd.testing.assert_series_equal(default, explicit)


def test_noise_when_enabled_is_repeatable():
    df = hourly_series(days=60)
    first = counterfactual_for(df, start='2024-02-10', end='2024-02-12', noise_factor=0.5)
    second = counterfactual_for(df, start='2024-02-10', end='2024-02-12', noise_factor=0.5)

    pd.testing.assert_series_equal(first, second)
    assert not np.allclose(
        first, counterfactual_for(df, start='2024-02-10', end='2024-02-12')
    )


def test_a_single_varying_point_still_gets_a_real_fit():
    # The constant-series shortcut used to test y[:-order], so a series that
    # only moves at the very end was treated as flat and reported zero spread.
    y = np.array([5.0] * 10 + [9.0])

    assert ARModel(order=1).fit(y)['residual_std'] > 0


def test_seed_does_not_move_between_processes():
    script = textwrap.dedent(
        """
        import sys
        sys.path.insert(0, %r)
        sys.path.insert(0, %r)
        from test_counterfactual import hourly_series, counterfactual_for
        cf = counterfactual_for(hourly_series(days=60), '2024-02-10', '2024-02-12',
                                noise_factor=0.5)
        print(list(cf.round(6))[:5])
        """
        % (PROJECT_ROOT, os.path.dirname(os.path.abspath(__file__)))
    )

    outputs = set()
    for hash_seed in ('0', '1', '2'):
        env = dict(os.environ, PYTHONHASHSEED=hash_seed)
        run = subprocess.run(
            [sys.executable, '-c', script],
            env=env, capture_output=True, text=True,
        )
        assert run.returncode == 0, run.stderr
        outputs.add(run.stdout.strip())

    assert len(outputs) == 1, f"seed drifted between runs: {outputs}"


def band_coverage(df, start='2024-03-01', end='2024-03-03', shift=0.0, **kwargs):
    """Share of observed points falling inside the prediction band."""
    kwargs.setdefault('target_col', 'Sales')
    kwargs.setdefault('auto_detect', False)
    kwargs.setdefault('interval', 0.9)
    generator = TimeSeriesCounterfactualGenerator(**kwargs)

    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        result = generator.generate(df, start, end, 'ev')

    result = result.set_index(result.columns[0])
    actual = df.loc[result.index, 'Sales'] + shift
    lower, upper = result.iloc[:, 1], result.iloc[:, 2]
    return float(((actual >= lower) & (actual <= upper)).mean())


def test_interval_columns_are_opt_in():
    df = hourly_series(days=120)

    plain = counterfactual_for(df, start='2024-03-01', end='2024-03-03')
    generator = TimeSeriesCounterfactualGenerator(
        target_col='Sales', auto_detect=False, interval=0.9
    )
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        banded = generator.generate(df, '2024-03-01', '2024-03-03', 'ev')

    assert plain.name == 'counterfactual_holiday'
    assert list(banded.columns[1:]) == [
        'counterfactual_ev', 'counterfactual_ev_lower', 'counterfactual_ev_upper'
    ]


@pytest.mark.parametrize('maker', [hourly_series, persistent_hourly_series])
def test_prediction_band_is_roughly_calibrated(maker):
    # A 90% band should hold about 90% of points when nothing happened. Any one
    # realisation is noisy because the points are autocorrelated, so average.
    covered = [band_coverage(maker(days=120, seed=s)) for s in range(6)]

    assert 0.80 < np.mean(covered) < 0.97


def test_prediction_band_widens_when_the_level_is_persistent():
    generator = TimeSeriesCounterfactualGenerator(
        target_col='Sales', auto_detect=False, interval=0.9, forecast_days=10
    )
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        result = generator.generate(
            persistent_hourly_series(days=120, phi=0.9), '2024-03-01', '2024-03-03', 'ev'
        )

    width = result['counterfactual_ev_upper'] - result['counterfactual_ev_lower']

    assert width.iloc[-1] > width.iloc[0] * 1.5


def test_prediction_band_flags_a_real_effect():
    df = hourly_series(days=120)

    assert band_coverage(df, shift=0.0) > 0.80
    assert band_coverage(df, shift=200.0) < 0.20


def test_interval_outside_the_unit_range_is_rejected():
    generator = TimeSeriesCounterfactualGenerator(
        target_col='Sales', auto_detect=False, interval=90
    )
    with pytest.raises(ValueError, match="interval must be between 0 and 1"):
        generator.generate(hourly_series(days=120), '2024-03-01', '2024-03-03', 'ev')


def test_short_history_warns():
    index = pd.date_range('2024-01-01', periods=30, freq='h', name='Timestamp')
    df = pd.DataFrame({'Sales': np.arange(30, dtype=float)}, index=index)
    generator = TimeSeriesCounterfactualGenerator(target_col='Sales', auto_detect=False)

    with pytest.warns(UserWarning, match="pre-event data"):
        generator.generate(df, '2024-01-02', '2024-01-03', 'ev')


def test_partial_cycle_coverage_warns():
    # Eight hours of history is both too short and only an eighth of the daily
    # cycle, so both warnings are expected.
    index = pd.date_range('2024-01-01', periods=8, freq='h', name='Timestamp')
    df = pd.DataFrame({'Sales': np.arange(8, dtype=float)}, index=index)
    generator = TimeSeriesCounterfactualGenerator(target_col='Sales', auto_detect=False)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        generator.generate(df, '2024-01-02', '2024-01-03', 'ev')

    messages = [str(w.message) for w in caught]

    assert any('8 of 24 hour values' in m for m in messages)
    assert any('pre-event data' in m for m in messages)


@pytest.mark.parametrize(
    'phi, expected',
    [
        (np.array([0.5]), 0.5),
        (np.array([1.2]), 1.2),
        # phi runs oldest lag first. For [0.2, 0.5] the polynomial is
        # z^2 - 0.5z - 0.2; reading the lags the other way round gives 0.8145,
        # so the tolerance here has to be tight enough to tell them apart.
        (np.array([0.2, 0.5]), 0.762348),
        (np.array([0.5, 0.2]), 0.814143),
    ],
)
def test_max_root_modulus(phi, expected):
    assert ARModel.max_root_modulus(phi) == pytest.approx(expected, abs=1e-5)


def test_trending_series_warns_that_the_forecast_will_diverge():
    # A steady trend fits an AR root above 1, and the counterfactual then runs
    # off instead of settling. Found on the Proposition 99 tobacco data.
    index = pd.date_range('1970-01-01', periods=19, freq='YS', name='year')
    df = pd.DataFrame({'Sales': np.linspace(120.0, 90.0, 19)}, index=index)
    generator = TimeSeriesCounterfactualGenerator(target_col='Sales', auto_detect=False)

    with pytest.warns(UserWarning, match="non-stationary"):
        generator.generate(df, '1989-01-01', '1989-01-01', 'prop99')

    # A straight line fits a unit root, which lands just under 1 in floating
    # point rather than on it.
    root = generator.model_.max_root_modulus(generator.model_.coefficients['phi'])
    assert root == pytest.approx(1.0, abs=1e-6)


def test_accelerating_trend_warns():
    index = pd.date_range('1970-01-01', periods=19, freq='YS', name='year')
    values = 120.0 - 0.09 * np.arange(19) ** 2
    df = pd.DataFrame({'Sales': values}, index=index)
    generator = TimeSeriesCounterfactualGenerator(target_col='Sales', auto_detect=False)

    with pytest.warns(UserWarning, match="non-stationary"):
        generator.generate(df, '1989-01-01', '1989-01-01', 'trend')


def test_stationary_series_does_not_warn_about_divergence():
    df = hourly_series(days=120)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        counterfactual_for(df, start='2024-03-01', end='2024-03-03')

    assert [w for w in caught if 'non-stationary' in str(w.message)] == []


def test_ample_history_is_quiet():
    df = hourly_series(days=120)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        counterfactual_for(df, start='2024-03-01', end='2024-03-03')

    assert [w for w in caught if 'pre-event' in str(w.message)] == []


def test_daily_input_stays_daily():
    index = pd.date_range('2024-01-01', periods=400, freq='D', name='date').delete([10, 50, 90])
    rng = np.random.default_rng(1)
    values = 1000 + 200 * np.sin(2 * np.pi * np.arange(len(index)) / 7)
    df = pd.DataFrame({'Sales': values + rng.normal(0, 40, len(index))}, index=index)

    cf = counterfactual_for(df, auto_detect=True)
    spacing = cf.index.to_series().diff().dropna().unique()

    assert list(spacing) == [pd.Timedelta(days=1)]


def test_a_missing_period_does_not_nan_the_whole_forecast():
    df = hourly_series(days=60)
    df.loc[df.index.hour == 3, 'Sales'] = np.nan

    cf = counterfactual_for(df, start='2024-02-10', end='2024-02-12')

    assert cf.notna().all()


def test_infer_frequency_reports_failure_instead_of_guessing_hourly():
    index = pd.date_range('2024-01-01', periods=200, freq='D').delete([10, 50, 90])
    df = pd.DataFrame({'v': np.arange(len(index))}, index=index)

    assert infer_frequency(df) is None
    assert auto_detect_frequency(df.reset_index(), 'index') == 'D'


def test_infer_frequency_tolerates_short_series():
    df = pd.DataFrame({'v': [1, 2]}, index=pd.date_range('2024-01-01', periods=2))

    assert infer_frequency(df) is None


@pytest.mark.parametrize(
    'columns, expected',
    [
        (['Timestamp', 'Sales', 'Store'], 'Store'),
        (['Timestamp', 'Humidity', 'Temp'], None),
        (['Timestamp', 'Width', 'Sales'], None),
        (['date', 'PM25', 'Name'], 'Name'),
        (['ts', 'value', 'sensor_id'], 'sensor_id'),
        # 'is_valid' carries 'id' inside 'valid'; only the real entity column
        # should win.
        (['Timestamp', 'Sales', 'is_valid', 'Store'], 'Store'),
    ],
)
def test_entity_detection(columns, expected):
    data = {}
    for col in columns:
        if col in ('Timestamp', 'date', 'ts'):
            data[col] = pd.date_range('2024-01-01', periods=4, freq='h')
        elif col in ('Store', 'Name', 'sensor_id', 'is_valid'):
            data[col] = ['a', 'b', 'a', 'b']
        else:
            # Deliberately low cardinality: a measurement column has to be
            # rejected on dtype, not on how many distinct values it happens
            # to hold.
            data[col] = [1.0, 1.0, 2.0, 2.0]

    assert auto_detect_columns(pd.DataFrame(data))['entity_col'] == expected


@pytest.mark.parametrize('with_duplicates', [False, True])
def test_cleaning_keeps_a_datetime_index_for_entity_data(with_duplicates):
    frame = pd.DataFrame({
        'Timestamp': list(pd.date_range('2024-01-01', periods=4, freq='h')) * 2,
        'Sales': np.arange(8, dtype=float),
        'Store': ['a'] * 4 + ['b'] * 4,
    })
    if with_duplicates:
        frame = pd.concat([frame, frame.iloc[[0]]], ignore_index=True)

    cleaned, _ = clean_time_series(
        frame, time_col='Timestamp', target_col='Sales',
        entity_col='Store', auto_detect=False,
    )

    assert isinstance(cleaned.index, pd.DatetimeIndex)
    assert cleaned.index.name == 'Timestamp'


def test_single_day_event_is_allowed():
    event = Event(start=pd.Timestamp('2024-12-25'), end=pd.Timestamp('2024-12-25'), name='xmas')

    assert event.duration() == pd.Timedelta(0)


def test_reversed_event_is_rejected():
    with pytest.raises(ValueError):
        Event(start=pd.Timestamp('2024-12-26'), end=pd.Timestamp('2024-12-25'), name='xmas')


def test_query_accepts_a_frame_indexed_by_time():
    df = pd.DataFrame(
        {'v': [1, 2, 3]},
        index=pd.DatetimeIndex(pd.date_range('2024-01-01', periods=3), name='Timestamp'),
    )

    span = TimeSeriesQuery(df, 'Timestamp').get_date_range()

    assert span['start'] == pd.Timestamp('2024-01-01')
    assert span['end'] == pd.Timestamp('2024-01-03')


def test_differences_fall_back_to_a_time_join_when_the_entity_is_missing():
    actual = pd.DataFrame({
        't': pd.date_range('2024-01-01', periods=3),
        'y': [1.0, 2.0, 3.0],
        'Store': ['a'] * 3,
    })
    counterfactual = pd.DataFrame({
        't': pd.date_range('2024-01-01', periods=3),
        'cf': [1.0, 1.0, 1.0],
    })

    merged = calculate_differences(actual, counterfactual, 't', 'y', 'cf', entity_col='Store')

    assert merged['difference'].tolist() == [0.0, 1.0, 2.0]


def test_differences_reject_an_unknown_column():
    frame = pd.DataFrame({'t': pd.date_range('2024-01-01', periods=2), 'y': [1.0, 2.0]})

    with pytest.raises(ValueError, match="not found"):
        calculate_differences(frame, frame, 't', 'y', 'missing')


def run_cli(script, *args):
    return subprocess.run(
        [sys.executable, os.path.join(PROJECT_ROOT, 'src', script), *args],
        capture_output=True, text=True, cwd=PROJECT_ROOT,
    )


@pytest.fixture
def cli_inputs(tmp_path):
    frame = hourly_series(days=250).reset_index()
    frame['Store'] = 'Store_A'
    csv = tmp_path / 'sales.csv'
    frame.to_csv(csv, index=False)

    events = tmp_path / 'events.json'
    events.write_text(json.dumps(
        [{'name': 'holiday', 'start': '2024-03-01', 'end': '2024-03-03'}]
    ))
    return csv, events


def test_cli_round_trip_reports_no_effect_when_there_is_none(tmp_path, cli_inputs):
    sales, events = cli_inputs
    counterfactuals = tmp_path / 'cf.csv'

    generate = run_cli('generate_counterfactuals.py', '-i', str(sales),
                       '-e', str(events), '--interval', '0.9', '-o', str(counterfactuals))
    assert generate.returncode == 0, generate.stdout + generate.stderr

    produced = pd.read_csv(counterfactuals)
    assert 'counterfactual_holiday' in produced.columns
    assert 'counterfactual_holiday_lower' in produced.columns

    out_dir = tmp_path / 'cmp'
    compare = run_cli('compare_counterfactuals.py', '-a', str(sales),
                      '-c', str(counterfactuals), '-e', str(events),
                      '--interval', '0.9', '-o', str(out_dir))
    assert compare.returncode == 0, compare.stdout + compare.stderr

    summary = pd.read_csv(out_dir / 'comparison_summary.csv')
    assert abs(summary['mean'].iloc[0]) < NOISE
    assert summary['pct_outside_band'].iloc[0] < 30
    assert 'consistent with no effect' in compare.stdout


def test_cli_round_trip_finds_an_injected_effect(tmp_path, cli_inputs):
    sales, events = cli_inputs
    frame = pd.read_csv(sales, parse_dates=['Timestamp'])
    window = frame['Timestamp'].between('2024-03-01', '2024-03-08')
    frame.loc[window, 'Sales'] += 200
    frame.to_csv(sales, index=False)

    counterfactuals = tmp_path / 'cf.csv'
    generate = run_cli('generate_counterfactuals.py', '-i', str(sales),
                       '-e', str(events), '--interval', '0.9', '-o', str(counterfactuals))
    assert generate.returncode == 0, generate.stdout + generate.stderr

    out_dir = tmp_path / 'cmp'
    compare = run_cli('compare_counterfactuals.py', '-a', str(sales),
                      '-c', str(counterfactuals), '-e', str(events),
                      '--interval', '0.9', '-o', str(out_dir))
    assert compare.returncode == 0, compare.stdout + compare.stderr

    summary = pd.read_csv(out_dir / 'comparison_summary.csv')
    assert summary['mean'].iloc[0] == pytest.approx(200, abs=NOISE)
    assert summary['pct_outside_band'].iloc[0] > 50
    assert 'more than chance would give' in compare.stdout


def test_multi_entity_run_keeps_series_separate():
    frames = []
    for offset, store in ((0.0, 'Store_A'), (500.0, 'Store_B')):
        frame = hourly_series(days=120).reset_index()
        frame['Sales'] += offset
        frame['Store'] = store
        frames.append(frame)
    raw = pd.concat(frames, ignore_index=True)

    generator = TimeSeriesCounterfactualGenerator(target_col='Sales', auto_detect=False)
    events = [Event(start=pd.Timestamp('2024-03-01'), end=pd.Timestamp('2024-03-03'), name='holiday')]

    means = {}
    for store in ('Store_A', 'Store_B'):
        cleaned, _ = clean_time_series(
            raw[raw['Store'] == store], time_col='Timestamp', target_col='Sales',
            entity_col=None, auto_detect=False,
        )
        result = generator.generate_multiple(cleaned, events)
        means[store] = result['counterfactual_holiday'].mean()

    assert means['Store_B'] - means['Store_A'] == pytest.approx(500.0, abs=NOISE)
