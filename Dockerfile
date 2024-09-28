FROM apache/airflow:2.10.0
COPY requirements.txt .
RUN python -m pip install -r requirements.txt