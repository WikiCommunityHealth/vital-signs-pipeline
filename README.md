# 🌳 Project Structure
````
.
├── dags
│   └── vital_signs_dag.py
├── dashboards
│   ├── app.py
│   ├── apps
│   │   ├── activity.py
│   │   ├── admin.py
│   │   ├── balance.py
│   │   ├── globall.py
│   │   ├── main_app.py
│   │   ├── retention.py
│   │   ├── special.py
│   │   └── stability.py
│   ├── assets
│   │   ├── logo.png
│   │   └── wikimedia-logo.png
│   ├── config.py
│   ├── Dockerfile
│   └── requirements.txt
├── databases
│   ├── vital_signs_editors.db
│   └── vital_signs_web.db
├── docker-compose.yml
├── Dockerfile
├── logs
├── mediawiki_history_dumps
│   ├── lijwiki
│   │   └── 2025-04.lijwiki.all-time.tsv.bz2
│   ├── lmowiki
│   │   └── 2025-04.lmowiki.all-time.tsv.bz2
│   ├── napwiki
│   │   └── 2025-04.napwiki.all-time.tsv.bz2
│   ├── pmswiki
│   │   └── 2025-04.pmswiki.all-time.tsv.bz2
│   ├── scnwiki
│   │   └── 2025-04.scnwiki.all-time.tsv.bz2
│   ├── scwiki
│   │   └── 2025-04.scwiki.all-time.tsv.bz2
│   └── vecwiki
│       └── 2025-04.vecwiki.all-time.tsv.bz2
├── monitoring
│   ├── grafana
│   │   ├── dashboards.yaml
│   │   └── vital_signs.json
│   ├── prometheus.yml
│   └── statsd.yaml
├── README.md
├── requirements.txt
├── scripts
│   ├── config.py
│   ├── create_db.py
│   ├── fill_editors_db.py
│   ├── fill_web_db.py
│   ├── primary_language.py
│   ├── test_db.py
│   └── utils.py
├── start.sh
└── vital_signs.png

18 directories, 41 files
````

# Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/WikiCommunityHealth/vital-signs-pipeline
cd vital-signs-pipeline
```
### 2. Build and Start All Services
``` bash
chmod +x start.sh
./start.sh
```
start.sh
```bash
#!/bin/bash

set -e

mkdir -p ./databases
sudo chown -R 50000:0 ./databases
sudo chmod -R 777 ./databases

mkdir -p ./logs
sudo chown -R 50000:0 ./logs
sudo chmod -R 777 ./logs

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