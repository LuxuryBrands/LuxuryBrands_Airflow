from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

# DAG 설정
default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('test_dag', default_args=default_args, catchup=False, schedule_interval=timedelta(days=1))


# 작업 정의
def task_function(**kwargs):
    print("Task is running!")


start_task = EmptyOperator(task_id='start_task', dag=dag)
end_task = EmptyOperator(task_id='end_task', dag=dag)

python_task = PythonOperator(
    task_id='python_task',
    python_callable=task_function,
    dag=dag,
)

test_replace_task = S3ToRedshiftOperator(
    task_id="task_transfer_s3_to_redshift",
    aws_conn_id="aws_default",
    redshift_conn_id="redshift_conn",
    s3_bucket="de-2-1-s3",
    s3_key="stage/brand_2023-08-21/brand_2023-08-21_15_temp.avro",
    schema="RAW_DATA",
    table="brand",
    copy_options=["format as avro 'auto'"],
    method="REPLACE"
)

test_upsert_task = S3ToRedshiftOperator(
    task_id="test_upsert_task",
    aws_conn_id="aws_default",
    redshift_conn_id="redshift_conn",
    s3_bucket="de-2-1-s3",
    s3_key="stage/brand_2023-08-22/brand_2023-08-22_05_temp.avro",
    schema="RAW_DATA",
    table="brand",
    copy_options=["format as avro 'auto'"],
    method="UPSERT",
    upsert_keys=["user_id"]
)

# 작업 간의 의존성 설정
start_task >> python_task >> end_task
