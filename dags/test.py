from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator

# DAG 설정
default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchUp': False,
}

dag = DAG('test_dag', default_args=default_args, schedule_interval=timedelta(days=1))


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

# 작업 간의 의존성 설정
start_task >> python_task >> end_task
