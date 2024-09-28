from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from datetime import datetime
import json
import requests
from config import JOBS

@dag(start_date=datetime(2024, 9, 1), 
     schedule_interval='@daily', 
     catchup=False, 
     description='''
     Running multiple spark jobs using dynamic task mapping. 
     I cant create sequential dependencies between dynamic tasks with this approach.
     ''')
def v1_spark_pipeline():

    @task.python
    def get_jobs():
        return json.loads(JOBS)
        
    @task.bash
    def make_output_directory(job, ti=None):
        return f'mkdir -p /opt/airflow/output/{ti.dag_id}/{job.get('emr_job_id')}'

    @task.python
    def download_json(job, ti=None):
        endpoint = job['endpoint']
        job_name = job['emr_job_id']

        response = requests.get(endpoint)
        if response.status_code != 200:
            raise AirflowException(f'Bad Status Code: {response.status_code}')
        data = response.json()

        dest_key = f'{job_name}/{ti.execution_date.date()}.json'
        filename = f'/opt/airflow/output/{ti.dag_id}/{job_name}/{ti.execution_date.date()}.json'
        with open(filename, 'w') as f:
            json.dump(data, f)
    
        return {"filename": filename, 
                "dest_key": dest_key,}
    
    @task
    def start_cluster():
        print('Cluster Started')

    @task 
    def run_job(job):
        job_name = job['emr_job_id']
        print(f'Running Job: {job_name}')
    
    @task
    def stop_cluster():
        print('Cluster Started')

    @task.bash(trigger_rule='none_skipped')
    def delete_output_directory(job, ti=None):
        return f'rm -rf /opt/airflow/output/{ti.dag_id}/{job.get('emr_job_id')}'
        
    get_jobs_ = get_jobs()
    make_output_directory_ = make_output_directory.expand(job=get_jobs_) 
    download_json_ = download_json.expand(job=get_jobs_)
    local_to_s3 = LocalFilesystemToS3Operator.partial(
                                                task_id='local_to_s3',
                                                dest_bucket='airflow-product-data',
                                                aws_conn_id='s3_access',
                                                replace=True)\
                                             .expand_kwargs(download_json_)

    get_jobs_ >> [download_json_, make_output_directory_] >> local_to_s3 >> start_cluster() >> run_job.expand(job=get_jobs_) >> stop_cluster()

    local_to_s3 >> delete_output_directory.expand(job=get_jobs_)

v1_spark_pipeline()