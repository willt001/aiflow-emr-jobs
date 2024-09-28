from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import json
import requests

@dag(start_date=datetime(2024, 9, 1), 
     schedule_interval='@daily',
     catchup=False, 
     description='Running a single spark job on EMR.')
def v0_spark_pipeline():

    @task.bash
    def make_output_directory(ti=None):
        return f'mkdir -p /opt/airflow/output/{ti.dag_id}'

    @task
    def get_endpoint():
        return 'https://dummyjson.com/products'

    @task(retries=3, retry_exponential_backoff=True)
    def download_json_data(endpoint, ti=None):
        response = requests.get(endpoint)
        if response.status_code != 200:
            raise AirflowException(f'Bad Status Code: {response.status_code}')

        data = response.json()
        with open(f'/opt/airflow/output/{ti.dag_id}/{ti.execution_date.date()}.json', 'w') as f:
            json.dump(data, f)
    
    local_to_s3 = LocalFilesystemToS3Operator(task_id='local_to_s3',
                                              filename='/opt/airflow/output/{{ dag.dag_id }}/{{ ds }}.json',
                                              dest_key='{{ ds }}.json',
                                              dest_bucket='airflow-product-data',
                                              aws_conn_id='s3_access',
                                              replace=True)
    
    create_emr_cluster = EmptyOperator(task_id='create_emr_cluster')

    run_spark_job = EmptyOperator(task_id='run_spark_job')

    stop_emr_cluster = EmptyOperator(task_id='stop_emr_cluster')   

    @task.bash
    def delete_local_file(ti=None):
        return f'rm /opt/airflow/output/{ti.dag_id}/{ti.execution_date.date()}.json'

    make_output_directory() >> download_json_data(get_endpoint()) >> local_to_s3 >> create_emr_cluster >> run_spark_job >> stop_emr_cluster

    local_to_s3 >> delete_local_file()

v0_spark_pipeline()