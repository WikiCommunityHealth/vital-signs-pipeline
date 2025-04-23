from opentelemetry import metrics
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.exporter.prometheus import PrometheusMetricsExporter
from prometheus_client import start_http_server

# avvia il server prometeus
start_http_server(port=8000)

# exporter
exporter = PrometheusMetricsExporter()

# meter 
resource = Resource(attributes={
    SERVICE_NAME: "airflow_pipeline"
})

metrics.set_meter_provider(MeterProvider(resource=resource))
meter = metrics.get_meter(__name__)
metrics.get_meter_provider().start_pipeline(meter, exporter, 5)

# istogramma
task_duration = meter.create_histogram(
    name="task_duration_seconds",
    unit="s",
    description="Duration in seconds of each task"
)
