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
import os
import tweepy as tw

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

dag = DAG("twitter", default_args=default_args, schedule_interval=timedelta(1))

def get_conn():
    hostname="35.222.130.110"
    dbname="testeboticario"
    uname="cristiano"
    pwd="3w3w1234"

    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=hostname, db=dbname, user=uname, pw=pwd))

    return engine

def get_linha(**kwargs):
    engine = get_conn()

    sql = """
            select *
            from sales_consolidated_linha_ano_mes 
            where ano = 2019 and mes = 12
            order by 4 desc limit 1;
            """

    df_linha = pd.read_sql_query(sql, engine)

    return df_linha['LINHA'][0]

def search_twitter(**kwargs):

    consumer_key= 'VWJVDQTFS78u6aBbscsEy3Kp7'
    consumer_secret= 'gg3LypoSGAHCVXSjTgSto1wQQGK6hhg2BzfCqQq157Y9kqmGId'
    access_token= '1327719692388143104-Al4r97i2lLaxAnYOAk9llXJrIzQzCN'
    access_token_secret= 'gOec1RvsoSoHE6g5enrshwdAOItpb6P3BCRpDGWIiuyI6'
    auth = tw.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tw.API(auth, wait_on_rate_limit=True)

    linha = get_linha()

    search_query = linha+"Boticario"

    tweets = tw.Cursor(api.search,
                q=search_query,
                lang="pt-br").items(50)

    # store the API responses in a list
    tweets_copy = []
    for tweet in tweets:
        tweets_copy.append(tweet)
        
    tweets_df = pd.DataFrame()

    for tweet in tweets_copy:
        hashtags = []
        try:
            for hashtag in tweet.entities["hashtags"]:
                hashtags.append(hashtag["text"])
            text = api.get_status(id=tweet.id, tweet_mode='extended').full_text
        except:
            pass
        tweets_df = tweets_df.append(pd.DataFrame({'user_name': [tweet.user.name], 
                                                'user_location': [tweet.user.location],
                                                'user_verified': [tweet.user.verified],
                                                'date': [tweet.created_at],
                                                'text': [text], 
                                                'source': [tweet.source]}))
        tweets_df = tweets_df.reset_index(drop=True)
    # show the dataframe
    return tweets_df


def insert_data(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='search_twitter')

    engine = get_conn()
    print(df)
    df.to_sql('twitter', engine, index=False,if_exists='append')


with DAG('twitter',
        default_args = default_args,
        schedule_interval = '0 3 * * *',
        catchup=False) as dag:

        start_task = DummyOperator(
            task_id='start_task')
        search_twitter = PythonOperator(
            task_id='search_twitter',
            python_callable=search_twitter,
            provide_context=True)
        insert_data = PythonOperator(
            task_id='insert_db_gcp',
            python_callable=insert_data,
            provide_context=True)
        
        start_task >> search_twitter >> insert_data