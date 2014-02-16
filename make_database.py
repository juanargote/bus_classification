#imports
import pandas as pd 
import sqlite3
import pandas.io.sql as sql
from datetime import datetime
from pytz import timezone


#connect to sqlite to make a database for the bus arrivals
connection =sqlite3.connect('predecessor.db')
#connection = sqlite3.connect(':memory:')
cursor = connection.cursor()

#import arrivalDatanew.json as a dataframe
actual_arrival_df = pd.read_json('./bus_data/arrivalDataNew.json')
#import scheduledArrivalDataNew.json as a dataframe
sched_arrival_df = pd.read_json('./bus_data/scheduledArrivalDataNew.json')

#convert unix time to datetime objects
sansebastianTimeZone = timezone('Europe/Amsterdam')
actual_arrival_df['arrival_time'] = actual_arrival_df['arrival_time'].apply(lambda x: datetime.fromtimestamp(int(x/1000), sansebastianTimeZone))
actual_arrival_df['departure_time'] = actual_arrival_df['departure_time'].apply(lambda x: datetime.fromtimestamp(int(x/1000), sansebastianTimeZone))
sched_arrival_df.rename(columns={'arrival_time': 'sched_arrival_time'}, inplace=True)
#sched_arrival_df['sched_arrival_time'] = sched_arrival_df['sched_arrival_time'].apply(lambda x: datetime.fromtimestamp(int(x/1000), sansebastianTimeZone))

#write the dataframe to a .db sql database
sql.write_frame(frame = actual_arrival_df,name='bus_data',con=connection)
sql.write_frame(frame = sched_arrival_df,name='sched_arrival',con=connection)