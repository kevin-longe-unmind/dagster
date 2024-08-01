from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from dagster_airlift import mark_as_dagster_migrating


def print_hello():
    print("Hello")  # noqa: T201


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

dag = DAG(
    "print_dag", default_args=default_args, schedule_interval=None, is_paused_upon_creation=False
)
print_op = PythonOperator(task_id="print_task", python_callable=print_hello, dag=dag)
downstream_print_op = PythonOperator(
    task_id="downstream_print_task", python_callable=print_hello, dag=dag
)

mark_as_dagster_migrating(
    {
        "print_dag": {
            "print_task": True,
            "downstream_print_task": False,
        }
    }
)
