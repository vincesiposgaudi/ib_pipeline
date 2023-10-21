import os
import sys
from airflow import DAG
from datetime import datetime, timedelta 
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

airflow_home = os.environ.get('AIRFLOW_HOME')
if airflow_home:
    sys.path.append(airflow_home)
    from src import dag_functions as f

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
    'retries': 1,
    'retry_delay': timedelta(seconds=60)
}

with DAG(dag_id='quarterly_financials',
         default_args = default_args,
         schedule_interval = '0 12 5 1,4,7,10 *',
         catchup = False,
         tags=['my_dags']
) as dag:
    
    start_task = EmptyOperator(
        task_id='start_task'
    )

    get_quarterly_data = PythonOperator(
        task_id = 'get_quarterly_data',
        python_callable = f.get_financials,
        op_kwargs = {'tickers': tickers, 'function': 'INCOME_STATEMENT'},
        provide_context = True,
        dag = dag
    )

    validate_quarterly_data = PythonOperator(
        task_id='validate_quarterly_data',
        python_callable=f.quarterly_metrics_are_consistent,
        op_kwargs = {'pulled_task_id': 'get_quarterly_data', 'pulled_key': 'raw_file_path'},
        provide_context=True,
        dag=dag
    )

    transform_quarterly_data = PythonOperator(
        task_id = 'transform_quarterly_data',
        python_callable = f.get_quarterly_financials,
        op_kwargs = {'pulled_task_id': 'get_quarterly_data', 'pulled_key': 'raw_file_path', 'pushed_key': 'csv_file_path'},
        provide_context = True,
        dag = dag
    )

    weekly_upload_to_s3 = PythonOperator(
        task_id = 'weekly_upload_to_s3',
        python_callable = f.load_to_s3,
        op_kwargs = {'pulled_task_id': 'transform_quarterly_data', 'pulled_key': 'csv_file_path'},
        provide_context = True,
        dag = dag
    )

    delete_quarterly_local_files = PythonOperator(
        task_id = 'delete_quarterly_local_files',
        python_callable = f.delete_local_file,
        op_kwargs = {'files_to_delete': [{'pulled_task_id': 'transform_quarterly_data', 'pulled_key': 'csv_file_path'}, 
                                        {'pulled_task_id': 'get_quarterly_data', 'pulled_key': 'raw_file_path'}]},
        provide_context = True,
        dag = dag
    )
    
    end_task = EmptyOperator(
        task_id='end_task'
    )

    start_task >> get_quarterly_data >> validate_quarterly_data >> transform_quarterly_data >> weekly_upload_to_s3 >> delete_quarterly_local_files >> end_task

if __name__ == "__main__":
    dag.cli()
