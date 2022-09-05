#import dependencies 
import pandas as pd 
import requests 
from sqlalchemy import create_engine
import psycopg2
import config 


#get data 
class GetData(self):
    #api request
    def __init__(self):
    self.endpoint = "https://data.sfgov.org/resource/imvp-dq3v.json?$limit=1000"
    self.r = requests.get(url=endpoint)
    self.data = r.json()
    
    
    
    
    
class InsertData(self,data):
    
    #connect to databse 
    def __init__(self):
        conn = psycopg2.connect(database="donniedata", user=config.db_user, 
                                        password=config.db_password, sslmode="disable")
        cur = conn.cursor();  

        
def main():
    data = GetData()
    insert = InsertData()
    
if __name__ == "__main__":
    main()
    