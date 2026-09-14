#!/bin/bash
# Legacy PM2.5 pipeline: generate, diff, then query the diffs.
set -e

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "run_counterfactuals.py"
python3 "$script_dir/run_counterfactuals.py"

# query_differences.py reads the files this step writes.
echo "calculate_differences.py"
python3 "$script_dir/calculate_differences.py"

echo "query with parameters"
python3 "$script_dir/query_differences.py" "$@"
