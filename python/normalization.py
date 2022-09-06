#import dependencies 
import requests 
from sqlalchemy import create_engine
import psycopg2
import config 
import queries


#get data 
class GetData():
    #api request
     def __init__(self):
        self.endpoint = "https://data.sfgov.org/resource/imvp-dq3v.json?$limit=1000"
        self.response = requests.get(url=self.endpoint)
        self.data = self.response.json()
    
    
    
    
#insert data into database schema   
class InsertData():
    
    #connect to databse 
    def __init__(self, data):
        conn = psycopg2.connect(database="donniedata", user=config.db_user, 
                                password=config.db_password, sslmode="disable")
        cur = conn.cursor();  
        
       #insert data into dimension tables -- perfromed first due to FK constraints
        for i in range(len(data)): 
            #dim_payment
            cur.execute(queries.dim_paymeny_insert, {'payment':data[i]['payment_type']})
            #dim_street
            cur.execute(queries.dim_street_insert, {'street':data[i]['street_block']})
            
        #insert data into fact table 
        for i in range(len(data)):
             cur.execute(queries.dim_street_insert, {
                     'transmission_dt' : data[i]['transmission_datetime'],
                     'payment': data[i]['payment_type'],
                     'street': data[i]['street_block'],
                     'post' : data[i]['post_id'],
                     'meter_event': data[i]['meter_event_type'],
                     'paid_amt': data[i]['gross_paid_amt'],
                     'sessionstart' : data[i]['session_start_dt'],
                     'sessionend': data[i]['session_end_dt']
                    })

        
        
        
#run      
def main():
    response_data = GetData()
    insert = InsertData(response_data.data)
    
if __name__ == "__main__":
    main()
    