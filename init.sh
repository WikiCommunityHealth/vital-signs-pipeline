#!/bin/bash

set -e

mkdir -p ./databases
sudo chown -R 50000:0 ./databases
sudo chmod -R 777 ./databases

mkdir -p ./logs
sudo chown -R 50000:0 ./logs
sudo chmod -R 777 ./logs

if [ ! -d ".venv" ]; then
    python3 -m venv .venv
fi
source .venv/bin/activate

pip install --upgrade pip
pip install -r requirements.download_dumps.txt


python download_dumps.py

exit 0