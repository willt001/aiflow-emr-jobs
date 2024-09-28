# Objective
Authoring Airflow DAGs to run multiple spark jobs on a transient EMR cluster, utilising dynamic task mapping and dynamic task groups.

![spark-jobs-on-emr](https://github.com/user-attachments/assets/5235ef6f-467d-488b-bd0d-19df7257c1e4)

The goal is to write code for the above DAG where the number of parallel running spark jobs can be increased or decreased dynamically depending on the jobs defined in the config.py file.
I have authored four DAGs which build up to solving this problem.

## v0_spark_pipeline
This DAG demonstrates represents running the pipeline with one job and is not dynamic.

![v0-pipeline](https://github.com/user-attachments/assets/e61c23ff-f43c-4c62-a0bc-b272e5fa2bb8)

## v1_spark_pipeline
In this iteration I introduce dynamic task mapping to run all 3 jobs defined in the config file. The issue with this approach is that for each dynamic task, all of the mapped tasks must finish before moving to the next task and if one mapped task fails the entire task will fail. 

![v1-pipeline](https://github.com/user-attachments/assets/2e2dc6cf-fb2f-4a92-b3ab-b84a907e00a8)

## v2_spark_pipeline
To address the issue of no sequential dependencies between individual mapped tasks of dynamically mapped tasks, I used dynamic task groups.

![v2-pipeline](https://github.com/user-attachments/assets/8171a87c-22ec-4f25-93ee-effc6bd5b566)

This resolves the issue for all the tasks inside the 'get_data' task group. The issue is that this doesn't work for the run_spark_jobs task group. For example, if only the 3rd local to s3 task fails, the 3rd run_job task will still rather than stay on 'upstream_failed'.

## v3_spark_pipeline
This DAG achieves the desired behaviour by putting all of the tasks inside a single task group, except for 'start/stop_emr_cluster', at the cost of a messier DAG though.
Note: 
- create_emr_cluster is still an upstream task of run_spark_job.
- create_emr_cluster uses the 'one_success' trigger rule.

![v3-pipeline](https://github.com/user-attachments/assets/fe3533d0-7414-4030-aa93-2c7d93e78e3a)
