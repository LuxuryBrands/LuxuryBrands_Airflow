from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

from plugins.s3 import check_and_copy_files, upload_to_s3
from plugins.spark import get_etl_step, get_emr_cluster_id
from plugins import slack

table = "brand"
schema = "RAW_DATA"
redshift_method = "REPLACE"
has_log_table = True

default_args = {
    'start_date': days_ago(1),
    'retries': 0,
    'retry_delay': timedelta(seconds=30),
    'on_failure_callback': slack.send_failure_alert,
    'on_success_callback': slack.send_success_alert
}
with DAG(f"etl_{table}",
         description="brand data process DAG",
         schedule='@hourly',
         catchup=False,
         default_args=default_args) as dag:
    start_task = EmptyOperator(task_id='start_task')
    end_task = EmptyOperator(task_id='end_task')

    # task 에 전달할 환경 변수 생성
    bucket_name = Variable.get("aws_bucket")
    source_bucket = Variable.get("aws_raw_bucket")
    dest_bucket = Variable.get("aws_stage_bucket")
    date_pattern = Variable.get("etl_date_pattern")
    file_prefix = f"{table}_{{{{ ds }}}}/{table}_{{{{ dag_run.logical_date.strftime('{date_pattern}') }}}}"

    # S3 복사 Task
    s3_copy_task = PythonOperator(
        task_id='s3_copy_task',
        python_callable=check_and_copy_files,
        op_kwargs={
            'bucket_name': bucket_name,
            'source_bucket': source_bucket,
            'dest_bucket': dest_bucket,
            'prefix': file_prefix
        }
    )

    # Spark 작업을 위한 Task
    with TaskGroup("spark_task_group") as spark_task_group:
        script_key = f"emr/scripts/spark_{table}.py"
        dest_key = f"{dest_bucket}/{file_prefix}"
        spark_steps = get_etl_step(bucket_name, script_key, dest_key)
        job_flow_name = 'de-2-1-emr'

        get_cluster_id = PythonOperator(
            task_id="get_cluster_id",
            python_callable=get_emr_cluster_id,
            op_kwargs={"job_flow_name": job_flow_name, "cluster_states": ["RUNNING", "WAITING"]},
        )

        script_to_s3 = PythonOperator(
            task_id="script_to_s3",
            python_callable=upload_to_s3,
            op_kwargs={"filename": f"./dags/plugins/spark_{table}.py", "key": script_key, "bucket_name": bucket_name},
        )

        add_steps = EmrAddStepsOperator(
            task_id="add_steps",
            job_flow_id="{{ task_instance.xcom_pull(task_ids='spark_task_group.get_cluster_id', key='return_value') }}",
            steps=spark_steps
        )

        step_checker = EmrStepSensor(
            task_id="step_checker",
            job_flow_id="{{ task_instance.xcom_pull(task_ids='spark_task_group.get_cluster_id', key='return_value') }}",
            step_id="{{ task_instance.xcom_pull(task_ids='spark_task_group.add_steps', key='return_value')["
                    + str(len(spark_steps) - 1)
                    + "] }}",
        )

        get_cluster_id >> script_to_s3 >> add_steps >> step_checker

    # Redshift 작업을 위한 Task
    with TaskGroup("redshift_task_group") as redshift_task_group:
        redshift_task = S3ToRedshiftOperator(
            task_id=f"redshift_{table}_task",
            s3_bucket=bucket_name,
            s3_key=f"{dest_bucket}/{file_prefix}.avro",
            schema=schema,
            table=table,
            copy_options=["format as avro 'auto'"],
            method=redshift_method
        )

        redshift_log_task = S3ToRedshiftOperator(
            task_id=f"redshift_{table}_log_task",
            s3_bucket=bucket_name,
            s3_key=f"{dest_bucket}/{file_prefix}.avro",
            schema=schema,
            table=f"{table}_log",
            copy_options=["format as avro 'auto'"],
            method="APPEND",
        )

        redshift_task >> redshift_log_task

    start_task >> s3_copy_task >> spark_task_group >> redshift_task_group >> end_task
