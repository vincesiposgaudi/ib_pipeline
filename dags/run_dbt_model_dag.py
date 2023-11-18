import os
import sys
from airflow.models import DAG
from datetime import datetime, timedelta 
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

airflow_home = os.environ.get('AIRFLOW_HOME')
if airflow_home:
    sys.path.append(airflow_home)
    from src.python import dag_functions as f

dbt_dir = f"{airflow_home}/ib_dbt"

default_args = {
    'owner': 'Vince',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 2),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG(dag_id='run_dbt_model_dag',
         default_args = default_args,
         schedule_interval = None,
         catchup = False,
         tags = ['my_dags']
) as dag:
    
    start_task = EmptyOperator(
        task_id='start_task'
    )

    run_dbt_model = BashOperator(
        task_id='run_dbt_model',
        bash_command=f"cd {dbt_dir} && dbt run --models test",
        dag=dag
    )

    end_task = EmptyOperator(
        task_id='end_task'
    )

    start_task >> run_dbt_model >> end_task
    
if __name__ == "__main__":
    dag.cli()