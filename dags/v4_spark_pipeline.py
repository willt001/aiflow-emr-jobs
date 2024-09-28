from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import json
import requests


def create_task_group(job):

    @task_group(group_id=f'get_data_{job['emr_job_id']}')
    def get_data():

        @task.python
        def download_json(job, ti=None):

            response = requests.get(job['endpoint'])
            if response.status_code != 200:
                raise AirflowException(f'Bad Status Code: {response.status_code}')
            data = response.json()

            filename = f'/opt/airflow/output/{ti.dag_id}_{ti.task_id.split('.')[0]}_{ti.execution_date.date()}.json'
            with open(filename, 'w') as f:
                json.dump(data, f)


        local_to_s3 = LocalFilesystemToS3Operator(task_id='local_to_s3',
                                                filename="/opt/airflow/output/{{ dag.dag_id }}_{{ ti.task_id.split('.')[0] }}_{{ ds }}.json",
                                                dest_key="{{ dag.dag_id }}/{{ ti.task_id.split('.')[0] }}/{{ ds }}.json",
                                                dest_bucket='airflow-product-data',
                                                aws_conn_id='s3_access',
                                                replace=True)
        
        @task.bash(trigger_rule='all_done')
        def remove_local_file(ti=None):
            return f'rm -rf /opt/airflow/output/{ti.dag_id}_{ti.task_id.split('.')[0]}_{ti.execution_date.date()}.json'
        
        download_json(job) >> local_to_s3 >> remove_local_file()

    return get_data

@dag(start_date=datetime(2024, 9, 1), 
     schedule_interval='@daily', 
     catchup=False)
def v4_spark_pipeline():

    @task_group(group_id=f'get_data')
    def get_data(job):

        @task.python
        def download_json(job, ti=None):

            response = requests.get(job['endpoint'])
            if response.status_code != 200:
                raise AirflowException(f'Bad Status Code: {response.status_code}')
            data = response.json()

            filename = f'/opt/airflow/output/{ti.dag_id}_{job['emr_job_id']}_{ti.execution_date.date()}.json'
            with open(filename, 'w') as f:
                json.dump(data, f)
        
        @task
        def run_local_to_s3(job, ti, context={}):
            local_to_s3 = LocalFilesystemToS3Operator(task_id='local_to_s3',
                                                    filename=f"/opt/airflow/output/{ti.dag_id}_{job['emr_job_id']}_{ti.execution_date.date()}.json",
                                                    dest_key=f"{ti.dag_id}/{job['emr_job_id']}/{ti.execution_date.date()}.json",
                                                    dest_bucket='airflow-product-data',
                                                    aws_conn_id='s3_access',
                                                    replace=True)
            local_to_s3.execute(context)

        @task.bash(trigger_rule='all_done')
        def remove_local_file(ti=None):
            return f'rm -rf /opt/airflow/output/{ti.dag_id}_{ti.task_id.split('.')[0]}_{ti.execution_date.date()}.json'
        
        run_local_to_s3_ = run_local_to_s3(job)
        
        download_json(job) >> run_local_to_s3_ >> remove_local_file()

        run_local_to_s3_ >> create_emr_cluster
    
    create_emr_cluster = EmptyOperator(task_id='create_emr_cluster', trigger_rule='one_success')

    from config import JOBS
    JOBS = json.loads(JOBS)
    for job in JOBS:
        get_data(job)
  
v4_spark_pipeline()