from datetime import timedelta, datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from plugins import slack
from plugins.spark import get_etl_step, get_emr_cluster_id, get_elt_step
from plugins.s3 import check_and_copy_files, upload_to_s3
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator



default_args = {
    'start_date': days_ago(1),
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
    'on_failure_callback': slack.send_failure_alert,
    'on_success_callback': slack.send_success_alert
}

dag = DAG(
    'emr_to_redshift',
    default_args=default_args,
    schedule_interval='30 * * * *',  # Run the DAG every 30 minutes past the hour
    catchup=False,  # Do not catch up on historical runs
)

script_key=f"emr/scripts/emr_elt_redshift.py"
bucket_name = "de-2-1-s3"
spark_steps = get_elt_step(bucket_name, script_key)
job_flow_name = 'de-2-1-emr'


default_args = {
    'start_date': days_ago(1),
    'retries': 0,
    'retry_delay': timedelta(seconds=30),
    'on_failure_callback': slack.send_failure_alert,
    'on_success_callback': slack.send_success_alert
}



redshift_truncate_table_task1 = PostgresOperator(
    task_id='setup_create_table1',
    sql='TRUNCATE TABLE analytics.brand_basic_info;',
    postgres_conn_id="REDSHIFT_DEFAULT",
    dag = dag
)
redshift_truncate_table_task2 = PostgresOperator(
    task_id='setup_create_table2',
    sql='TRUNCATE TABLE analytics.brand_information;',
    postgres_conn_id="REDSHIFT_DEFAULT",
    dag = dag
)
redshift_truncate_table_task3 = PostgresOperator(
    task_id='setup_create_table3',
    sql='TRUNCATE TABLE analytics.aggregated_brand_information; ',
    postgres_conn_id="REDSHIFT_DEFAULT",
    dag = dag
)
redshift_truncate_table_task4 = PostgresOperator(
    task_id='setup_create_table4',
    sql='TRUNCATE TABLE analytics.brand_media_post_time;',
    postgres_conn_id="REDSHIFT_DEFAULT",
    dag = dag
)
redshift_truncate_table_task5 = PostgresOperator(
    task_id='setup_create_table5',
    sql='TRUNCATE TABLE analytics.followers_growth; ',
    postgres_conn_id="REDSHIFT_DEFAULT",
    dag = dag
)
redshift_truncate_table_task6 = PostgresOperator(
    task_id='setup_create_table6',
    sql='TRUNCATE TABLE analytics.aggregated_hashtag_search;',
    postgres_conn_id="REDSHIFT_DEFAULT",
    dag = dag
)
redshift_truncate_table_task7 = PostgresOperator(
    task_id='setup_create_table7',
    sql='TRUNCATE TABLE analytics.popularity_factor; ',
    postgres_conn_id="REDSHIFT_DEFAULT",
    dag = dag
)
redshift_truncate_table_task8 = PostgresOperator(
    task_id='setup_create_table8',
    sql='TRUNCATE TABLE analytics.popularity_calculation;',
    postgres_conn_id="REDSHIFT_DEFAULT",
    dag = dag
)
redshift_truncate_table_task9 = PostgresOperator(
    task_id='setup_create_table9',
    sql='TRUNCATE TABLE analytics.hashtag_count;',
    postgres_conn_id="REDSHIFT_DEFAULT",
    dag = dag
)


# spark_job
get_cluster_id = PythonOperator(
    task_id="get_cluster_id",
    python_callable=get_emr_cluster_id,
    op_kwargs={"job_flow_name": job_flow_name, "cluster_states": ["RUNNING", "WAITING"]},
    dag=dag
)

script_to_s3 = PythonOperator(
    task_id="script_to_s3",
    python_callable=upload_to_s3,
    op_kwargs={"filename": f"./dags/plugins/emr_elt_redshift.py", "key": script_key, "bucket_name": bucket_name},
    dag=dag
)

add_steps = EmrAddStepsOperator(
    task_id="add_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='get_cluster_id', key='return_value') }}",
    steps=spark_steps,
    dag=dag
)

step_checker = EmrStepSensor(
    task_id="step_checker",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='get_cluster_id', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
            + str(len(spark_steps) - 1)
            + "] }}",
    dag=dag
)
redshift_truncate_table_task1 >> redshift_truncate_table_task2 >> redshift_truncate_table_task3 >> redshift_truncate_table_task4 >>  redshift_truncate_table_task5 >> redshift_truncate_table_task6 >> redshift_truncate_table_task7 >> redshift_truncate_table_task8 >> redshift_truncate_table_task9 >> script_to_s3 >> add_steps >> step_checker
