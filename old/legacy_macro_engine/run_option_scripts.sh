#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if [ -d ".venv" ]; then
  source .venv/bin/activate
fi

python3 fetch_option_data.py
python3 create_option_percentile.py
python3 option_strategies.py
