#import dependencies 
import pandas as pd 
import requests 
from sqlalchemy import create_engine
import psycopg2
import config 


#get data 
class getData(self):
    #api request
    def api_r():
    self.endpoint = "https://data.sfgov.org/resource/imvp-dq3v.json?$limit=1000"
    self.r = requests.get(url=endpoint)
    self.data = r.json()
    
    
    
    
    
class insertData(self,data):
    
    #connect to databse 
    def db_connect():
        conn = psycopg2.connect(database="donniedata", user=config.db_user, 
                            password=config.db_password, sslmode="disable")
    cur = conn.cursor();  
