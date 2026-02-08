
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


default_args = {
    "owner": "data_team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="ecommerce_full_load",
    default_args=default_args,
    description="FULL LOAD: Truncate và load tất cả Excel files",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # Manual trigger
    catchup=False,
    tags=["ecommerce", "full-load", "dbt"],
) as dag:

    # Task 1: Start
    start = EmptyOperator(task_id="start")

    # Task 2: Full Load - Load tất cả Excel files
    load_raw_orders = BashOperator(
        task_id="load_raw_orders_full",
        bash_command="""
        cd /opt/airflow
        python scripts/load_raw_orders.py
        echo " Full load complete"
        """,
    )

    # Task 3: dbt deps - Install packages
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="""
        cd /opt/airflow/dbt/ecommerce_dbt
        dbt deps --profiles-dir /opt/airflow/.dbt --target dev
        """,
    )

    # Task 4: dbt run - Transform Bronze → Silver → Gold
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
        cd /opt/airflow/dbt/ecommerce_dbt
        dbt run --profiles-dir /opt/airflow/.dbt --target dev --full-refresh
        echo " dbt run complete"
        """,
    )

    # Task 6: End
    end = EmptyOperator(task_id="end")

    # Dependencies
    start >> load_raw_orders >> dbt_deps >> dbt_run >> end
