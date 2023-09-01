from datetime import timedelta, datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from plugins import slack




default_args = {
    'start_date': days_ago(1),
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
    'on_failure_callback': slack.send_failure_alert,
    'on_success_callback': slack.send_success_alert
}

dag = DAG(
    'pyspark_to_snowflake',
    default_args=default_args,
    schedule_interval='30 * * * *',  # Run the DAG every 30 minutes past the hour 
    catchup=False,  # Do not catch up on historical runs
)


# Define the SparkSubmitOperator
spark_task = SparkSubmitOperator(
    task_id='submit_spark_task',
    dag=dag,
    application=f"./dags/plugins/pyspark_elt_snowflake.py",
    packages="net.snowflake:snowflake-jdbc:3.13.22,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4"


)

