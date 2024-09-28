from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import json
import requests
from config import JOBS

@dag(start_date=datetime(2024, 9, 1), 
     schedule_interval='@daily', 
     catchup=False,
     description='''
     Running multiple spark jobs using dynamic task groups. 
     Using dynamic task groups allows creating sequential dependencies between dynamically generated tasks.
     ''')
def v3_spark_pipeline():

    @task_group(group_id='get_data_run_job')
    def get_data_run_job(job):

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

        local_to_s3 = LocalFilesystemToS3Operator(task_id='local_to_s3',
                                                filename='/opt/airflow/output/{{ dag.dag_id }}/{{ ti.map_index }}/{{ ds }}.json',
                                                dest_key='{{ dag.dag_id }}/{{ ti.map_index }}/{{ ds }}.json',
                                                dest_bucket='airflow-product-data',
                                                aws_conn_id='s3_access',
                                                replace=True)
          
        @task.bash(trigger_rule='all_done')
        def remove_output_directory(ti=None):
            return f'rm -rf /opt/airflow/output/{ti.dag_id}/{ti.map_index}'
        
        @task
        def run_spark_job(job):
            print(f'Running job_id: {job['emr_job_id']}')

        run_spark_job_ = run_spark_job(job)

        start >> make_output_directory() >> download_json(job) >> local_to_s3 >> create_emr_cluster >> run_spark_job_ >> terminate_emr_cluster >> end

        local_to_s3 >> [remove_output_directory(), run_spark_job_]

    start = EmptyOperator(task_id='start')

    create_emr_cluster = EmptyOperator(task_id='create_emr_cluster', trigger_rule='one_success')

    terminate_emr_cluster = EmptyOperator(task_id='terminate_emr_cluster', trigger_rule='all_done')

    end = EmptyOperator(task_id='end')
        
    jobs = json.loads(JOBS)    

    get_data_run_job.expand(job=jobs) 

v3_spark_pipeline()