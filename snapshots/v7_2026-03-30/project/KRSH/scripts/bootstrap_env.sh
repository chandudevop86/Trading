#!/usr/bin/env bash
set -euo pipefail
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r app/requirements.txt
streamlit run app/src/Trading.py --server.address 0.0.0.0 --server.port 8501
