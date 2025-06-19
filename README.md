
# Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/WikiCommunityHealth/vital-signs-pipeline
cd vital-signs-pipeline
```
### 2. Build and Start All Services
``` bash
./init.sh && ./start.sh
```
init.sh
```bash
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
```

start.sh
```bash
#!/bin/bash

docker build -t custom-airflow .

docker compose up --build 
```
This will:

Build the custom Airflow image

Start Airflow webserver and scheduler

Start PostgreSQL as Airflow backend

Start statsd exporter to get all the airflow metrics

Start Prometheus to scrape metrics

Start Grafana for dashboard visualization


## 🔍 Services Overview

| Service   | URL   | Notes |
|---------    |-----  |-------|
|   | Backend | |
| Airflow    | http://localhost:8080 |	Username: admin, Password: admin |
| Prometheus     |	http://localhost:9090   | |
| Grafana	| http://localhost:3000 | Username: admin, Password: admin |
| Metrics	| http://localhost:9102/metrics	| |
|   | Frontend | |
| Dashboards | http://localhost:8050 | |
## Airflow DAG
<img alt="DAG" src="./vital_signs.png" />