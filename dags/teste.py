from os import listdir
from os.path import isfile, join
import pandas as pd 
from sqlalchemy import create_engine

# Credentials to database connection
hostname="35.222.130.110"
dbname="testeboticario"
uname="cristiano"
pwd="3w3w1234"

engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
				.format(host=hostname, db=dbname, user=uname, pw=pwd))


path = './'
files = [f for f in listdir(path) if isfile(join(path, f))]

for file in files:
    if('.xlsx' in file):
        df = pd.read_excel(file)
        df.to_sql('sales', engine, index=False,if_exists='append')
