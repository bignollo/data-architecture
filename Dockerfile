FROM apache/airflow:3.1.6
COPY requirements-airflow.txt .
RUN pip install -r requirements-airflow.txt
