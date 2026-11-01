# Extends the existing Airflow image with the Kafka provider; nothing is installed on the host.
ARG AIRFLOW_VERSION=3.0.0
FROM apache/airflow:${AIRFLOW_VERSION}
RUN pip install --no-cache-dir apache-airflow-providers-apache-kafka
