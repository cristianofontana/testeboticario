from datetime import datetime, timedelta
from os import listdir
from os.path import isfile, join
import pandas as pd 
from sqlalchemy import create_engine


path = './bases/'
files = [f for f in listdir(path) if isfile(join(path, f))]


hostname="35.222.130.110"
dbname="testeboticario"
uname="cristiano"
pwd="3w3w1234"

engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=hostname, db=dbname, user=uname, pw=pwd))

for file in files:
    df = pd.read_excel("./bases/"+file)
    df.to_sql('vendas', engine, index=False, if_exists='append')
    print("Inserido no BD")