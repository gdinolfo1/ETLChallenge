import pandas as pd
import io
import requests
from google.cloud import bigquery
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= 'C:/Users/User/Downloads/etlchallenge-335623-fed9a0203073.json'

df = pd.read_csv('https://raw.githubusercontent.com/spunkjockey/sample-datasets/main/Datasets/Retail/OrderDetails/00.csv', encoding= "unicode_escape")

dfnew = df['ORDER_DETAIL'].str.split('|', expand = True)

df['PRODUCT'] = dfnew[0]

df['AISLE'] = dfnew[1]

df['SEQ_PROD'] = dfnew[2]

df.drop(columns = ['ORDER_DETAIL'], inplace = True)

df3 = df['SEQ_PROD'].str.split('~', expand = True)

df.drop(columns = ['SEQ_PROD'], inplace = True)

df['SEQ_PROD'] = df3[0]

df['PRODUCT_DETAIL'] = df3[1]

table_id = 'etlchallenge-335623.etl_challenge.etl2'

df.to_gbq(table_id)




