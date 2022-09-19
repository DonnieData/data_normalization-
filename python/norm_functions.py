from datetime import datetime

#groups time, rounds minute to the nearest factor of 5 
def group_time(datetime_string):
    time_extract = datetime.strptime(time_string,'%Y-%m-%dT%H:%M:%S.%f').time()
    hour_extract = time_extract.strftime('%H')
    minute_grouped = str(round(time_extract.minute / 5) *5) 
    time_group = datetime.strptime(hour_extract + minute_grouped, '%H%M').time()
    return time_group 

#get date from datetime string 
def get_date(datetime_string):
    return datetime.strptime(datetime_string, '%Y-%m-%dT%H:%M:%S.%f').date()
    
#convert and round round gross payment     
def roound_gross(gross_string):
    return round(float(gross_string) / .50) * .50
    
    