from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago

from plugins import slack
from plugins.s3 import check_and_copy_files

default_args = {
    'start_date': days_ago(1),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': slack.send_failure_alert,
    'on_success_callback': slack.send_success_alert
}
with DAG("etl_hashtag",
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

    table = "brand"
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
    spark_task = SparkSubmitOperator(
        task_id='spark_task',
        application=f"./dags/plugins/spark_{table}.py",
        application_args=[
            f"s3a://{bucket_name}/{dest_bucket}/{file_prefix}",
            date_pattern,
        ],
        conf={
            "spark.hadoop.fs.s3a.access.key": Variable.get("aws_access_key_id"),
            "spark.hadoop.fs.s3a.secret.key": Variable.get("aws_secret_access_key"),
        },
        packages="org.apache.hadoop:hadoop-aws:3.3.2"
    )

    # Snowflake 작업을 위한 Task
    snowflake_task = SQLExecuteQueryOperator(
        task_id="snowflake_task",
        conn_id="snowflake_default",
        sql=f"""
            COPY INTO RAW_DATA.{table}
            FROM @s3_stage/{dest_bucket}/{file_prefix}.parquet
            PATTERN='.*.parquet'
            FORCE = TRUE
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;            
            """,
        autocommit=False,
        split_statements=True,
    )

    start_task >> s3_copy_task >> spark_task >> snowflake_task >> end_task
