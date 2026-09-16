FROM apache/airflow:2.10.5-python3.12

ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.12

# Installed against Airflow's own constraints, and with apache-airflow pinned in
# the same command. A bare `pip install -r requirements.txt` here will happily
# upgrade the SQLAlchemy that Airflow ships with, and the scheduler then dies on
# import with MappedAnnotationError before it ever reads a DAG.
#
# Only boto3 is genuinely missing from the base image; SQLAlchemy, requests and
# psycopg2 already come with it.
RUN pip install --no-cache-dir \
        "apache-airflow==${AIRFLOW_VERSION}" \
        boto3 \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
