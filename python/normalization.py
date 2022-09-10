#import dependencies 
import requests 
from sqlalchemy import create_engine
from datetime import datetime
import psycopg2
import config 
import queries
import pandas as pd


#get data 
class GetData():
    #api request
     def __init__(self, endpoint_url):
        #print("requesting data")
        self.endpoint = endpoint_url
        self.response = requests.get(url=self.endpoint)
        self.data = self.response.json() 
    

    
#insert data into database schema   
class InsertData():
    
    #connect to databse 
    def __init__(self, data):
        conn = psycopg2.connect(database="donniedata", user=config.db_user, 
                                    password=config.db_password, sslmode="disable")
        cur = conn.cursor();  
        conn.autocommit = True
        
        #insert data into dimension tables -- perfromed first due to FK constraints
        print("inserting dimension tables")
        for i in range(len(data)): 
            #dim_payment
            cur.execute(queries.dim_paymeny_insert, {'payment':data[i]['payment_type']})
        for i in range(len(data)):
            #dim_street
            cur.execute(queries.dim_street_insert, {'street':data[i]['street_block']})
            
        #insert data into fact table 
        print("--Fact Table--")
        counter = 0
        size = len(data)
        for i in range(len(data)):
            cur.execute(queries.fact_tran_insert, {
                     'transmission_dt' : data[i]['transmission_datetime'],
                     'payment': data[i]['payment_type'],
                     'street': data[i]['street_block'],
                     'post' : data[i]['post_id'],
                     'meter_event': data[i]['meter_event_type'],
                     'paid_amt': data[i]['gross_paid_amt'],
                     'sessionstart' : data[i]['session_start_dt'],
                     'sessionend': data[i]['session_end_dt']
                    })
            #counter += 1 
            #if counter % 100 == 0:
                #print(f"{counter} of {size} isnerted")
                
      
        
#run      
def main():
    #create date ranges to query with get request
    date_select = pd.date_range('2022-08-01','2022-08-03',freq='D')
    date_select = [str(datetime.date(i)) for i in date_select]
    
    #loop through date pairing and get data , normalize and insert by day
    for i in range(len(date_select)-1): 
        
        url_param = f"""https://data.sfgov.org/resource/imvp-dq3v.json?$limit=100000
                       &$where=session_start_dt between '{date_select[i]}' and '{date_select[i+1]}'"""
        
        response_data = GetData(url_param)
        print(f"data request for {date_select[i]} complete")
        print(f"Date: {date_select[i]}, \n Rows: {len(response_data.data)}")
        
        insert = InsertData(response_data.data)
        print(f"normalization and insertion for {date_select[i]} complete")
    
if __name__ == "__main__":
    main()
    