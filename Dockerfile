FROM apache/airflow:2.8.1-python3.10

USER airflow

RUN pip install --no-cache-dir \
    pandas \
    pyarrow \
    fastparquet \
    requests \
    psycopg2-binary