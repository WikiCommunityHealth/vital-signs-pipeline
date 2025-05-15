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

* migliorare il codice in vital_signs_dag.py
* testare l'integrazione con le vecchie dashboard
* refactoring pre-deployement (rimuovere dowload_dumps_task)
