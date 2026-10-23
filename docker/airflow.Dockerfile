# Airflow with the pipeline's own tools installed in a separate virtualenv,
# so dbt's and Airflow's dependency pins never fight each other.
FROM apache/airflow:2.10.3-python3.11
COPY requirements.txt /tmp/lakehouse-requirements.txt
RUN python -m venv /opt/airflow/lakehouse-venv \
 && /opt/airflow/lakehouse-venv/bin/pip install --no-cache-dir -r /tmp/lakehouse-requirements.txt
