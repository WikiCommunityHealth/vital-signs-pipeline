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

if [ ! -f "requirements_download.txt" ]; then
cat <<EOL > requirements_download.txt
requests
beautifulsoup4
python-dateutil
EOL
fi

pip install --upgrade pip
pip install -r requirements_download.txt


python download_dumps.py

