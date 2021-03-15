"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from os import listdir
from os.path import isfile, join
import pandas as pd 
from sqlalchemy import create_engine

default_args = {
    "owner": "Cristiano",
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "email": ["fontana.c12@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("sales_consolidated_year_month", default_args=default_args, schedule_interval=timedelta(1))

def get_conn():
    hostname="35.222.130.110"
    dbname="testeboticario"
    uname="cristiano"
    pwd="3w3w1234"

    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=hostname, db=dbname, user=uname, pw=pwd))

    return engine

def get_data(**kwargs):

    engine = get_conn()

    # Consolidado de vendas por ano e mês;
    sql = """
                select 
                YEAR(DATA_VENDA) as ANO, 
                month(DATA_VENDA) as MES,
                sum(QTD_VENDA) QTD_VENDA
                from sales 
                group by YEAR(DATA_VENDA), month(DATA_VENDA)
                order by 1,2 asc;
            """
    df = pd.read_sql_query(sql, engine)

    return df


def insert_data(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='get_data')

    engine = get_conn()

    df.to_sql('sales_consolidated_year_month', engine, index=False,if_exists='append')


with DAG('sales_consolidated_year_month',
        default_args = default_args,
        schedule_interval = '0 3 * * *',
        catchup=False) as dag:

        start_task = DummyOperator(
            task_id='start_task')
        get_data = PythonOperator(
            task_id='get_data',
            python_callable=get_data,
            provide_context=True)
        insert_data = PythonOperator(
            task_id='insert_db_gcp',
            python_callable=insert_data,
            provide_context=True)
        
        start_task >> get_data >> insert_data