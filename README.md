# 🌳 Project Structure
````
.
├── dags
│   └── vital_signs_dag.py
├── dashboards
│   ├── app.py
│   ├── apps
│   │   ├── activity.py
│   │   ├── admin.py
│   │   ├── balance.py
│   │   ├── globall.py
│   │   ├── main_app.py
│   │   ├── retention.py
│   │   ├── special.py
│   │   └── stability.py
│   ├── assets
│   │   ├── logo.png
│   │   └── wikimedia-logo.png
│   ├── config.py
│   └── Dockerfile
├── docker-compose.yml
├── Dockerfile
├── monitoring
│   ├── grafana
│   │   ├── dashboards.yaml
│   │   └── vital_signs.json
│   ├── prometheus.yml
│   └── statsd.yaml
├── README.md
├── requirements.txt
├── scripts
│   ├── config.py
│   ├── create_db.py
│   ├── fill_editors_db.py
│   ├── fill_web_db.py
│   ├── primary_language.py
│   └── utils.py
└── vital_signs.png

8 directories, 29 files
````

# Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/WikiCommunityHealth/vital-signs-pipeline
cd vital-signs-pipeline
```
### 2. Build and Start All Services
```bash
docker build -t custom-airflow .
docker-compose up --build
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
| Airflow    | http://localhost:8080 |	Username: admin, Password: admin |
| Prometheus     |	http://localhost:9090   | |
| Grafana	| http://localhost:3000 | Username: admin, Password: admin |
| Metrics	| http://localhost:9102/metrics	| |
## Airflow DAG
<img alt="DAG" src="./vital_signs.png" />

## 📝 TODO

* testare l'integrazione con le vecchie dashboard
