import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time
logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)
engine=create_engine('sqlite:///inventory.db')
'''this function will ingest the database the dataframe into databasetable'''
def ingest_db(df,table_name,engine):
    df.to_sql(table_name,con=engine,if_exists='replace',index=False)
def load_raw_data():
   '''this function will load the csv as dataframe and ingest into db'''
   start=time.time()
   for file in os.listdir('d'):
      
      df=pd.read_csv('d/'+file)
      logging.info(f'Ingesting {file} in db')
      ingest_db(df,file[:-4],engine)
    
   end=time.time()
   total_time=(end-start)/60
   logging.info("ingestion complete")
   logging.info(f'\ntotal time taken: {total_time} minutes')

if __name__=='__main__':
   load_raw_data()