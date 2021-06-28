FROM apache/airflow:2.1.0
USER airflow
RUN pip install --no-cache-dir --user SQLAlchemy
RUN pip install --no-cache-dir --user mysql-connector-python
RUN pip install --no-cache-dir --user pandas