import os
import sys
from airflow import DAG
from datetime import datetime, timedelta 
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

airflow_home = os.environ.get('AIRFLOW_HOME')
if airflow_home:
    sys.path.append(airflow_home)
    from src.python import dag_functions as f

tickers = [
    'BAC',    # Bank of America Merrill Lynch
    'BX',     # Blackstone
    'C',      # Citi
    'DB',     # Deutsche Bank
    'GS',     # Goldman Sachs
    'HSBC',   # HSBC
    'JPM',    # J.P. Morgan Chase
    'MS',     # Morgan Stanley
    'UBS'     # UBS
]

default_args = {
    'owner': 'Vince',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 2),
    'retries': 2,
    'retry_delay': timedelta(seconds=5)
}

with DAG(dag_id='weekly_financials',
         default_args = default_args,
         schedule_interval = '0 12 * * 1',
         catchup = False,
         tags=['my_dags']
) as dag:
    
    start_task = EmptyOperator(
        task_id='start_task'
    )

    get_weekly_data = PythonOperator(
        task_id = 'get_weekly_data',
        python_callable = f.get_financials,
        op_kwargs = {'tickers': tickers, 'function': 'TIME_SERIES_WEEKLY_ADJUSTED'},
        provide_context = True,
        dag = dag
    )

    validate_weekly_data = PythonOperator(
        task_id='validate_weekly_data',
        python_callable=f.weekly_metrics_are_consistent,
        op_kwargs = {'pulled_task_id': 'get_weekly_data', 'pulled_key': 'raw_file_path'},
        provide_context=True,
        dag=dag
    )

    transform_weekly_data = PythonOperator(
        task_id = 'transform_weekly_data',
        python_callable = f.get_weekly_financials,
        op_kwargs = {'pulled_task_id': 'get_weekly_data', 'pulled_key': 'raw_file_path', 'pushed_key': 'csv_file_path'},
        provide_context = True,
        dag = dag
    )

    weekly_upload_to_s3 = PythonOperator(
        task_id = 'weekly_upload_to_s3',
        python_callable = f.load_to_s3,
        op_kwargs = {'pulled_task_id': 'transform_weekly_data', 'pulled_key': 'csv_file_path'},
        provide_context = True,
        dag = dag
    )

    delete_weekly_local_files = PythonOperator(
        task_id = 'delete_weekly_local_files',
        python_callable = f.delete_local_file,
        op_kwargs = {'files_to_delete': [{'pulled_task_id': 'transform_weekly_data', 'pulled_key': 'csv_file_path'}, 
                                        {'pulled_task_id': 'get_weekly_data', 'pulled_key': 'raw_file_path'}]},
        provide_context = True,
        dag = dag
    )
    
    trigger_second_dag = TriggerDagRunOperator(
    task_id='trigger_second_dag',
    trigger_dag_id="load_weekly_to_dwh",
    dag=dag
    )

    end_task = EmptyOperator(
        task_id='end_task'
    )

    start_task >> get_weekly_data >> validate_weekly_data >> transform_weekly_data >> weekly_upload_to_s3 >> delete_weekly_local_files >> trigger_second_dag >> end_task

if __name__ == "__main__":
    dag.cli()
