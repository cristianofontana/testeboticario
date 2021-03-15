from os import listdir
from os.path import isfile, join
import pandas as pd 
from sqlalchemy import create_engine 
import boto3

hostname="35.222.130.110"
dbname="testeboticario"
uname="cristiano"
pwd="3w3w1234"

engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=hostname, db=dbname, user=uname, pw=pwd))

df = pd.read_sql_query("select * from sales", engine)

print(df)