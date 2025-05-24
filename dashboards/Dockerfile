FROM python:3.12

WORKDIR /app

COPY . .

COPY /home/pata/Scrivania/tirocinio/airflow/requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt

EXPOSE 8050

CMD ["python", "app.py"]
