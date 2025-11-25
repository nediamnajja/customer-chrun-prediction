from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import json
import pandas as pd

def test_imports():
    print("Testing imports...")
    print(f"os module: {os}")
    print(f"json module: {json}")
    print(f"pandas version: {pd.__version__}")
    print(f"Current working directory: {os.getcwd()}")
    print("All imports successful!")

test_dag = DAG(
    dag_id='test_imports',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
)

test_task = PythonOperator(
    task_id='test_imports_task',
    python_callable=test_imports,
    dag=test_dag
)
