from airflow.decorators import dag, task, task_group
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import json
import requests

@dag(start_date=datetime(2024, 9, 1), 
     schedule_interval='@daily', 
     catchup=False,
     description='''
     Running multiple spark jobs using dynamic task groups. 
     Using dynamic task groups allows creating sequential dependencies between dynamically generated tasks.
     I cant pass these dependencies to the run jobs task group though.''')
def v2_spark_pipeline():

    @task_group(group_id='get_data')
    def get_data(job): 
        @task.bash
        def make_output_directory(ti=None):
            return f'mkdir -p /opt/airflow/output/{ti.dag_id}/{ti.map_index}'

        @task.python
        def download_json(job, ti=None):
            response = requests.get(job['endpoint'])
            if response.status_code != 200:
                raise AirflowException(f'Bad Status Code: {response.status_code}')
            data = response.json()

            filename = f'/opt/airflow/output/{ti.dag_id}/{ti.map_index}/{ti.execution_date.date()}.json'
            with open(filename, 'w') as f:
                json.dump(data, f)
    

        local_to_s3 = LocalFilesystemToS3Operator(  
                                                task_id='local_to_s3',
                                                filename='/opt/airflow/output/{{ dag.dag_id }}/{{ ti.map_index }}/{{ ds }}.json',
                                                dest_key='{{ dag.dag_id }}/{{ ti.map_index }}/{{ ds }}.json',
                                                dest_bucket='airflow-product-data',
                                                aws_conn_id='s3_access',
                                                replace=True)
          
        @task.bash
        def remove_output_directory(ti=None):
            return f'rm -rf /opt/airflow/output/{ti.dag_id}/{ti.map_index}'

        make_output_directory() >> download_json(job) >> local_to_s3 >> remove_output_directory()

    
    @task(trigger_rule='one_success')
    def start_emr_cluster():
        pass

    @task_group(group_id='run_spark_jobs')
    def run_spark_jobs(job):
        
        @task
        def run_job(job):
            pass
    
        run_job(job)

    @task
    def stop_emr_cluster():
        pass
    
    jobs = json.loads(Variable.get('spark_jobs'))

    get_data.expand(job=jobs) >> start_emr_cluster() >> run_spark_jobs.expand(job=jobs) >> stop_emr_cluster()

v2_spark_pipeline()