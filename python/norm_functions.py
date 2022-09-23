from datetime import datetime

#mapping for group_time function for when minute is rouned to '60' then time should be set to beginnig of the next hour
next_hour_map = {'00': '01', '01': '02', '02': '03', '03': '04', '04': '05', '05': '06', '06': '07', '07': '08', '08': '09', '09': '10', '10': '11', '11': '12', '12': '13', '13': '14', '14': '15', '15': '16', '16': '17', '17': '18', '18': '19', '19': '20', '20': '21', '21': '22', '22': '23', '23': '00'}

#groups time, rounds minute to the nearest factor of 5 
def group_time(datetime_string):
    time_extract = datetime.strptime(datetime_string,'%Y-%m-%dT%H:%M:%S.%f').time()
    hour_extract = time_extract.strftime('%H')
    minute_grouped = str(round(time_extract.minute / 5) *5) 
    if minute_grouped == '60':
        hour_extract = next_hour_map[hour_extract]
        minute_grouped = '00'
    time_group = datetime.strptime(hour_extract + minute_grouped, '%H%M').time()
    return time_group 

#get date from datetime string 
def get_date(datetime_string):
    return datetime.strptime(datetime_string, '%Y-%m-%dT%H:%M:%S.%f').date()
    
#convert and round round gross payment     
def roound_gross(gross_string):
    return round(float(gross_string) / .50) * .50
    
    