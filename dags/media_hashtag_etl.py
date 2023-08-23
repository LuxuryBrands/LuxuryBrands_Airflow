from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

from plugins import slack
from plugins.s3 import check_and_copy_files

table = "media_hashtag"

default_args = {
    'start_date': days_ago(1),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
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
    spark_task = SparkSubmitOperator(
        task_id='spark_task',
        application=f"./dags/plugins/spark_{table}.py",
        application_args=[
            f"s3a://{bucket_name}/{dest_bucket}/{file_prefix}",
            "{{ ts }}",
        ],
        conf={
            "spark.hadoop.fs.s3a.access.key": Variable.get("aws_access_key_id"),
            "spark.hadoop.fs.s3a.secret.key": Variable.get("aws_secret_access_key"),
        },
        packages="org.apache.hadoop:hadoop-aws:3.3.2,org.apache.spark:spark-avro_2.12:3.4.1"
    )

    schema = "RAW_DATA"
    redshift_method = "APPEND"

    # Redshift 작업을 위한 Task
    redshift_task = S3ToRedshiftOperator(
        task_id="redshift_task",
        aws_conn_id="aws_default",
        redshift_conn_id="redshift_default",
        s3_bucket=bucket_name,
        s3_key=f"{dest_bucket}/{file_prefix}.avro",
        schema=schema,
        table=table,
        copy_options=["format as avro 'auto'"],
        method=redshift_method
    )

    start_task >> s3_copy_task >> spark_task >> redshift_task >> end_task
