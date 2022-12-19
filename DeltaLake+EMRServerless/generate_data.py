import random
from datetime import datetime, timedelta
import pandas as pd

def generate_data(start_time, n):
	frmt = '%Y-%m-%d'
	end_time = datetime.today()
	start_time = datetime.strptime(start_time, frmt)
	td = end_time - start_time
	ts_list = []
	for i in range(n):
		timestamp = random.random() * td + start_time 
		ts = timestamp.isoformat("-","microseconds").replace("-", "").replace(":", "") + str(random.randint(100, 999))
		ts_list.append(ts)
	return ts_list

def save_to_csv():
	df = pd.DataFrame(generate_data("2022-11-01", 10), columns = ['EEE_EVENT_TS'])
	print(df)

#print(generate_data("2022-11-01", 10))
save_to_csv()