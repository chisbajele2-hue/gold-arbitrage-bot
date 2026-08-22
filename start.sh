#!/bin/bash
set -e

echo "=== Gold Arbitrage Bot ==="
echo "Installing dependencies..."
pip install -q -r requirements.txt

echo "Starting master bot..."
python master_bot.py