import os
from airflow.models import DAG
from datetime import datetime, timedelta 
from airflow.providers.postgres.operators.postgres import PostgresOperator

airflow_home = os.environ.get('AIRFLOW_HOME')

default_args = {
    'owner': 'Vince',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 2),
    'retries': 1,
    'retry_delay': timedelta(seconds=60)
}

with DAG(dag_id='load_quarterly_to_dwh',
         default_args = default_args,
         schedule_interval = None,
         catchup = False,
         tags = ['my_dags'],
         template_searchpath = [airflow_home]
) as dag:
    
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres",
        sql="src/sql/create_income_statement_table.sql",
        dag=dag
    )

    create_table 

if __name__ == "__main__":
    dag.cli()