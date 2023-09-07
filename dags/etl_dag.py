from datetime import timedelta

from airflow.models.dag import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

from dags.scripts.comm import slack

configs = {
    "brand": {"redshift_method": "REPLACE", "has_log_table": True},
    "media": {"redshift_method": "UPSERT", "upsert_keys": "media_id", "has_log_table": True},
    "media_hashtag": {"redshift_method": "APPEND", "has_log_table": False}
}

for config_name, config in configs.items():
    table = config_name
    redshift_method = config.get('redshift_method')
    upsert_keys = config.get('upsert_keys')
    has_log_table = config.get('has_log_table')

    default_args = {
        'start_date': days_ago(1),
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
        'on_failure_callback': slack.send_failure_alert,
        'on_success_callback': slack.send_success_alert
    }


    @dag(f"etl_{table}",
         description=f"{table} data process DAG",
         schedule='@hourly',
         catchup=False,
         default_args=default_args)
    def dynamic_generated_dag():
        start_task = EmptyOperator(task_id='start_task')
        end_task = EmptyOperator(task_id='end_task')

        # task 에 전달할 환경 변수 생성
        file_prefix = f"{table}_{{{{ ds }}}}/{table}_{{{{ dag_run.logical_date.strftime(var.value.get('etl_date_pattern')) }}}}"

        # Spark 작업을 위한 Task
        with TaskGroup("spark_task_group") as spark_task_group:
            from dags.scripts.comm.s3 import upload_to_s3, check_and_copy_files
            from dags.scripts.comm.spark import get_emr_cluster_id, get_etl_step

            file_name = ["/etl_job.py", f"/spark_{table}.py"]
            files = ["./dags/scripts/etl" + f for f in file_name]
            emr_keys = ["{{ var.value.get('aws_emr_key') }}" + f for f in file_name]

            # FIXME custom EMROperator 생성
            get_cluster_id = PythonOperator(
                task_id="get_cluster_id",
                python_callable=get_emr_cluster_id,
                op_kwargs={
                    'cluster_states': ["RUNNING", "WAITING"]
                }
            )

            # FIXME custom S3Operator 생성
            raw_to_s3 = PythonOperator(
                task_id='raw_to_s3',
                python_callable=check_and_copy_files,
                op_kwargs={
                    'prefix': file_prefix
                }
            )

            # FIXME custom S3Operator 생성
            script_to_s3 = PythonOperator(
                task_id="script_to_s3",
                python_callable=upload_to_s3,
                op_kwargs={
                    'files': files,
                    'keys': emr_keys,
                },
            )

            spark_steps = get_etl_step(table, emr_keys, f"{{{{ var.value.get('aws_stage_bucket') }}}}/{file_prefix}")

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

            get_cluster_id >> [raw_to_s3, script_to_s3] >> add_steps >> step_checker

        # Redshift 작업을 위한 Task
        with TaskGroup("redshift_task_group") as redshift_task_group:
            redshift_task = S3ToRedshiftOperator(
                task_id=f"redshift_{table}",
                s3_bucket="{{ var.value.get('aws_bucket') }}",
                s3_key="{{ var.value.get('aws_stage_bucket') }}" + f"/{file_prefix}.avro",
                schema="{{ var.value.get('aws_raw_schema') }}",
                table=table,
                copy_options=["format as avro 'auto'"],
                method=redshift_method,
                upsert_keys=[upsert_keys]
            )

            if has_log_table:
                redshift_log_task = S3ToRedshiftOperator(
                    task_id=f"redshift_{table}_log",
                    s3_bucket="{{ var.value.get('aws_bucket') }}",
                    s3_key="{{ var.value.get('aws_stage_bucket') }}" + f"/{file_prefix}.avro",
                    schema="{{ var.value.get('aws_raw_schema') }}",
                    table=f"{table}_log",
                    copy_options=["format as avro 'auto'"],
                    method="APPEND",
                )

                redshift_task >> redshift_log_task

        start_task >> spark_task_group >> redshift_task_group >> end_task


    dynamic_generated_dag()
