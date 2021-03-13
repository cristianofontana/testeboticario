from datetime import datetime, timedelta,timezone
from airflow import DAG
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.base_hook import BaseHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from os import listdir
from os.path import isfile, join
import numpy as np
import json
from pandas.io.json import json_normalize

AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')

default_args = {
    'owner':'Cristiano',
    'depend_on_past': False,
    'start_date': datetime(2021,3,10),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

### configuração de parametros 

timeout = 1000
index = "fretzy-monitoramento"
doc_type = "type"
size = 10000

conn = json.loads(get_secret('pord/shark-tank/elasticsearch'))

# Init Elasticsearch instance
host = 'search-shark-tank-prd-wlrr7ccgr3px752c7idvv5behy.sa-east-1.es.amazonaws.com'
awsauth = AWS4Auth(conn['key'], conn['secret_key'], 'sa-east-1', 'es')

es = Elasticsearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)


def get_data(**kwargs):



    keys = json.loads(get_secret('prod/fretzy/elasticsearch'))

    cursor = connect(aws_access_key_id=keys['key'],
                 aws_secret_access_key=keys['secret_key'],
                 s3_staging_dir='s3://lake-fretzy-monitoramento/athena-results',
                 schema_name='fretzy',
                 region_name='us-east-2').cursor()

    sql = "SELECT max(data_compra) FROM fretzy_monitoramento"
    cursor.execute(sql)
    result = cursor.fetchall()
    data = datetime.strptime(result[0][0].replace('T',' ').replace('.000000-03:00',''),'%Y-%m-%d %H:%M:%S')

    return(int(data.timestamp()*1000))

def upload_file(file_name, bucket, object_name=None): # if u want to send directly to a s3 bucket just call this function
    """Upload a file to an S3 bucket
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.put_object(Body=file_name, Bucket=bucket, Key=object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def process_hits(hits):

    df = json_normalize(hits)
    
    df = df.drop(['_id', '_index','_score','_type'], axis=1)
    df = df.rename(columns={"_source.id":"id", "_source.pedido":"pedido", "_source.cep":"cep", "_source.sku":"sku", "_source.quantidade":"quantidade", "_source.grupo_id":"grupo_id", "_source.canal":"canal", "_source.id_cotacao":"id_cotacao", "_source.pedido_prazo_fornecedor":"pedido_prazo_fornecedor", "_source.pedido_prazo_transportadora":"pedido_prazo_transportadora", "_source.data_entrega_prometida":"data_entrega_prometida", "_source.data_compra":"data_compra", "_source.classificacao":"classificacao", "_source.data_entrega_calculada":"data_entrega_calculada", "_source.cotacao_encontrada":"cotacao_encontrada", "_source.prazos_corretos":"prazos_corretos", "_source.cotacao_execution_time":"cotacao_execution_time", "_source.prazo_transportadora_correto":"prazo_transportadora_correto", "_source.cotacao_prazo_transportadora":"cotacao_prazo_transportadora", "_source.prazo_fornecedor_correto":"prazo_fornecedor_correto", "_source.cotacao_prazo_fornecedor":"cotacao_prazo_fornecedor", "_source.data_entrega_correta":"data_entrega_correta"})
    df = df.astype(object).where(pd.notnull(df), None)
    df.to_parquet('fretzy.parquet', compression='gzip')

    s3 = boto3.client('s3') 
    with open('fretzy.parquet') as f:
        current_time = datetime.now()
        s3.upload_file('fretzy.parquet', 'lake-fretzy-monitoramento','fretzy-monitoramento/'+str(current_time)+'.parquet')
        print('Pronto!')

        

def start_main(**kwargs):
    # Check index exists
    if not es.indices.exists(index=index):
        print("Index " + index + " not exists")
        exit()

    criteria = get_criteria()

    body = query_fretzy_monitoramento(criteria)

    # Init scroll by search
    data = es.search(
        index=index,
        scroll='2m',
        size=size,
        body=body
    )

    # Get the scroll ID
    sid = data['_scroll_id']
    scroll_size = len(data['hits']['hits'])

    while scroll_size > 0:
        "Scrolling..."
        
        # Before scroll, process current batch of hits
        process_hits(data['hits']['hits'])
        
        data = es.scroll(scroll_id=sid, scroll='2m')

        # Update the scroll ID
        sid = data['_scroll_id']

        # Get the number of results that returned in the last scroll
        scroll_size = len(data['hits']['hits'])


with DAG('FRETZY',
        default_args = default_args,
        schedule_interval = '00 * * * * ',
        catchup=False) as dag:

        start_task = DummyOperator(
            task_id='start_task')
        start_main = PythonOperator(
            task_id='start_main',
            python_callable=start_main,
            provide_context=True)

        start_task >> start_main