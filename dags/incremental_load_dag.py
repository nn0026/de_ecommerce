
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
    dag_id="ecommerce_incremental_load",
    default_args=default_args,
    description="INCREMENTAL: Load 1 file Excel mới ",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # Manual trigger với config
    catchup=False,
    tags=["ecommerce", "incremental", "dbt"],
    params={
        "file": "",  
    },
) as dag:

    # Task 1: Start
    start = EmptyOperator(task_id="start")


    # Task 2: Incremental Load - Load file mới 
    load_raw_orders = BashOperator(
        task_id="load_raw_orders_incremental",
        bash_command="""
        cd /usr/local/airflow
        FILE="{{ dag_run.conf.get('file', '') or params.file }}"
        
        echo " Loading file: $FILE"
        python scripts/load_raw_orders.py "$FILE"
        echo " Incremental load complete"
        """,
    )

    # Task 4: dbt run - Transform Bronze → Silver → Gold
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
        cd /usr/local/airflow/dbt/ecommerce_dbt
        dbt run --profiles-dir /usr/local/airflow/dbt --target dev
        echo " dbt run complete"
        """,
    )

    # Task 5: dbt test - Chạy tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
        cd /usr/local/airflow/dbt/ecommerce_dbt
        dbt test --profiles-dir /usr/local/airflow/dbt --target dev
        echo " dbt test complete"
        """,
    )

    # Task 6: End
    end = EmptyOperator(task_id="end")

    # Dependencies
    start >> load_raw_orders >> dbt_run >> dbt_test >> end
