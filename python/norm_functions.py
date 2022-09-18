#groups time, rounds minute to the nearest factor of 5 
def group_time(time_string):
    time_extract = datetime.strptime(time_string,'%Y-%m-%dT%H:%M:%S.%f').time()
    hour_extract = time_extract.strftime('%H')
    minute_grouped = str(round(time_extract.minute / 5) *5) 
    time_group = datetime.strptime(hour_extract + minute_grouped, '%H%M').time()
    return time_group