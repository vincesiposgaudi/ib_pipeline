import os
import sys
from airflow.models import DAG
from datetime import datetime, timedelta 
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator

cols = [
    "symbol",
    "fiscal_date_ending",
    "reported_currency",
    "gross_profit",
    "total_revenue",
    "cost_of_revenue",
    "cost_of_goods_and_services_sold",
    "operating_income",
    "selling_general_and_administrative",	
    "research_and_development",
    "operating_expenses",
    "investment_income_net",
    "net_interest_income",
    "interest_income",
    "interest_expense",
    "non_interest_income",
    "other_non_operating_income",	
    "depreciation",	
    "depreciation_and_amortization",
    "income_before_tax",
    "income_tax_expense",
    "interest_and_debt_expense",	
    "net_income_from_continuing_operations",
    "comprehensive_income_net_of_tax",
    "ebit",
    "ebitda",
    "net_income"
]

airflow_home = os.environ.get('AIRFLOW_HOME')
if airflow_home:
    sys.path.append(airflow_home)
    from src.python import dag_functions as f

default_args = {
    'owner': 'Vince',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 2),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
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

    transfer_s3_to_postgres = S3ToSqlOperator(
        task_id="transfer_s3_to_sql",
        s3_bucket=str(os.environ.get('BUCKET')),
        s3_key='processed_data_quarterly.csv',
        table='finance.income_statement',
        column_list=cols,
        parser=f.parse_csv_to_list,
        sql_conn_id="postgres",
    )   

    create_table >> transfer_s3_to_postgres

if __name__ == "__main__":
    dag.cli()