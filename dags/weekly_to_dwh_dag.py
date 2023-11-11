import os
import sys
from airflow.models import DAG
from datetime import datetime, timedelta 
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator

cols = [
    "symbol",
    "date",
    "open",
    "high",
    "low",
    "close",
    "adjusted_close",
    "volume",
    "dividend_amount"
]

airflow_home = os.environ.get('AIRFLOW_HOME')
if airflow_home:
    sys.path.append(airflow_home)
    from src import dag_functions as f

default_args = {
    'owner': 'Vince',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 2),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG(dag_id='load_weekly_to_dwh',
         default_args = default_args,
         schedule_interval = None,
         catchup = False,
         tags = ['my_dags'],
         template_searchpath = [airflow_home]
) as dag:
    
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres",
        sql="src/sql/create_stock_data_table.sql",
        dag=dag
    )

    transfer_s3_to_postgres = S3ToSqlOperator(
        task_id="transfer_s3_to_sql",
        s3_bucket=str(os.environ.get('BUCKET')),
        s3_key='processed_data_weekly.csv',
        table='finance.stock_data',
        column_list=cols,
        parser=f.parse_csv_to_list,
        sql_conn_id="postgres",
    )   

    create_table >> transfer_s3_to_postgres

if __name__ == "__main__":
    dag.cli()