"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd 
from sqlalchemy import create_engine

import jaydebeapi
import jpype
import mysql.connector
import json 
import os

default_args = {
    "owner": "Cristiano",
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "email": ["fontana.c12@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

tableName = 'fat_venda_airflow'
schema = 'cubo_vendas_2'

dag = DAG("fat_venda_netsuite", default_args=default_args, schedule_interval=timedelta(1))

def data_connection():
    USER = 'admin'
    PASSWORD = 'Tania2022++'

    sqlEngine  = create_engine(f'mysql+pymysql://admin:Tania2022++@data-analytics.cluster-cjihk8tzhwqt.us-east-1.rds.amazonaws.com/{schema}', pool_recycle=3600)

    return sqlEngine

def connection():
    
    classpath = os.path.join(os.getcwd(), "/usr/local/airflow/dags/jars/NQjc.jar")
    jpype.startJVM(jpype.getDefaultJVMPath(), f"-Djava.class.path={classpath}")

    conn = jaydebeapi.connect("com.netsuite.jdbc.openaccess.OpenAccessDriver", "jdbc:ns://4887209.connect.api.netsuite.com:1708;ServerDataSource=NetSuite.com;encrypted=1;Ciphersuites=TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384;User=cristiano.fontana@taniabulhoes.com.br;Password=Tania@2022++;CustomProperties=(AccountID=4887209;RoleID=3)")
    return conn

def setSchema(df_netSuite):   
    schema = ['PEDITO_TB','ID_CANAL','TIPO_VENDA','ID_CLIENTE','ID_VENDEDOR','DATA_CREDITO','DATA_VENDA','ID_CREDITO','ID_VENDA','VALOR_CREDITO','VALOR_VENDA']
    df_netSuite.columns = schema

    return df_netSuite

def get_data(**kwargs):
    numberofdays = 30
    
    conn_lake = data_connection()
    start = datetime.today()
    criteria = datetime.today()
    endDate = criteria.strftime("%Y-%m-%d 23:59:59")

    startDate = (criteria+timedelta(days=-numberofdays)).strftime("%Y-%m-%d 00:00:00")
    deleteStartDate = (criteria+timedelta(days=-numberofdays)).strftime("%Y-%m-%d 00:00:00")
    deleteEndDate = criteria.strftime("%Y-%m-%d 23:59:59")

    print(f'Start Tread: {start}')
    conn = connection()

    qtdDias = 800
    chunk = 100000

    sql = f"""
    select
    t2.PEDIDO_TB,
    case when t2.SALES_REP_ID in(20040,2253362) then 147 else tl2.class_id end as ID_CANAL,
    case WHEN T2.TB_WEDDING_LIST IS NOT NULL THEN 'Lista de Casamento' else 'Venda Normal' end as TipoVenda,
    t.ENTITY_ID as ID_CLIENTE,
    t2.sales_rep_id,
    t.TRANDATE as dateCredit,
    t2.TRANDATE as dateOrder,
    t.TRANSACTION_ID as creditId
    ,t2.TRANSACTION_ID as orderId
    ,max(tl.amount) as valueCredit
    ,sum(tl2.amount)*-1 as valueOrder
    from Administrator.TRANSACTIONS t
            join Administrator.TRANSACTION_LINES tl on tl.TRANSACTION_ID = t.TRANSACTION_ID
            join Administrator.CLASSES a on a.CLASS_ID = tl.CLASS_ID
            join Administrator.ACCOUNTS ac on ac.ACCOUNT_ID = tl.ACCOUNT_ID
            join Administrator.TRANSACTIONS t2 on t2.TRANSACTION_ID = t.CREATED_FROM_ID -- salesOrder
            join Administrator.transaction_lines tl2 on tl2.transaction_id = t2.TRANSACTION_ID and tl2.amount < 0 and tl2.item_id is not null
    where 1 = 1
    and COALESCE(ac.ACCOUNTNUMBER, '0') not in ('3.03.01.01.01.016', '2.02.01.01.001', '2.02.01.01.002', '3.03.02.02.03.004')
    and tl.AMOUNT > 0
    and t.trandate between '{startDate}' and '{endDate}'
        and t.TRANSACTION_TYPE = 'Customer Deposit'
    group by t2.PEDIDO_TB, 
    case when t2.SALES_REP_ID in(20040,2253362) then 147 else tl2.class_id end,
    case WHEN T2.TB_WEDDING_LIST IS NOT NULL THEN 'Lista de Casamento' else 'Venda Normal' end,
    t.ENTITY_ID,t2.sales_rep_id,t.TRANDATE,t2.TRANDATE, t.TRANSACTION_ID, t2.TRANSACTION_ID
    """
    print(sql)

    for df_netSuite in pd.read_sql(sql,conn,chunksize=chunk):
        df_netSuite = setSchema(df_netSuite)
        
        insert_data(tableName,conn_lake,df_netSuite, 'append')
        print(df_netSuite)


def insert_data(tableName, conn, df=None, exists='append'):
    print('insert data')
    
    if(df is not None):
        df.to_sql(tableName, con=conn, if_exists=exists, index=False)
        print(f'Dados inseridos na tabela: {tableName}')
    else:
        print(f'Tabela: {tableName} não tem dados à serem inseridos.')


with DAG('fat_venda_netsuite',
        default_args = default_args,
        schedule_interval = '0 3 * * *',
        catchup=False) as dag:

        start_task = DummyOperator(
            task_id='start_task')
        get_data = PythonOperator(
            task_id='get_data',
            python_callable=get_data,
            provide_context=True)

        start_task >> get_data 