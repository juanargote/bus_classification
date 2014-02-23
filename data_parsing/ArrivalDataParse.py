'''
Created on Nov 7, 2013

@author: juanargote
'''

# Import libraries and classes.
import calendar
import psycopg2 as db
from pytz import timezone
from Util import *
import time
import json
from pprint import pprint
from argparse import ArgumentParser


def main(username,password):
    demandFileName = 'demand.txt'
    demandStoreName = 'demandStore.h5'
    demandStore = parseDemandFile(demandFileName,demandStoreName)
    outputArrivalFilename = 'arrivalDataNew.json'
    outputScheduledArrivalFilename = 'scheduledArrivalDataNew.json'

    # DB Parameters
    host = 'dbus.cueepsqael4s.us-west-2.rds.amazonaws.com'

    # REAL TIME DATABASE
    dbname = 'dbus'
    table = 'event'
    remote_connection1 = 'host=\'{0}\' dbname=\'{1}\' user=\'{2}\' password=\'{3}\''.format(host, dbname, user, password)

    # SCHEDULE DATABASE (perhaps we will use sqlite here)
    db_name2 = 'dbusdb'
    table2 = 'trips'
    table3 = 'stop_times'
    
    # Query parameters
    #
    route_id = ['5','25']
    mTZ = timezone('Europe/Amsterdam')
    t0 = datetime(2013,10,1,0,0,0)
    t1 = datetime(2014,2,17,0,0,0)    
    start = calendar.timegm(mTZ.localize(t0).utctimetuple())*1000
    end = calendar.timegm(mTZ.localize(t1).utctimetuple())*1000
    stop_ids = list(map(str,range(46,36,-1)))
    
    # Model parameters
    #
    tau = 1 # [seconds]
    t_b = 2 # [seconds/pax]
    
    start_time = time.time()
    # Establish a connection to the database
    #
    try:
        connection = db.connect(remote_connection_str)
        cursor = connection.cursor()
        # connection = MongoClient(mongodb_uri)
        # database = connection[db_name]
        # collection = database[collection_name]
        # connection2 = MongoClient(mongodb_uri2)
        # database2 = connection2[db_name2]
        # collection2 = database2[collection_name2]
        # collection3 = database2[collection_name3]
    except:
        print('Error: Unable to connect to database.')
        connection = None

    # Initialize the data storage files
    filepath = os.path.abspath(os.path.join(os.path.dirname(__file__ ),os.pardir,'External Files'))
    
    arrivalDataFileName = os.path.join(filepath,outputArrivalFilename)
    with open(arrivalDataFileName, mode='w+', encoding='utf-8') as f:
        json.dump([], f)    
    
    scheduledArrivalDataFileName = os.path.join(filepath,outputScheduledArrivalFilename)
    with open(scheduledArrivalDataFileName, mode='w+', encoding='utf-8') as f:
        json.dump([], f)    

    # If connection is established, query the events collection to retrieve events of a day
    if (connection is not None):
    # if (connection is not None) and (connection2 is not None):
        # Iterate through the corridor stops
        for stop_id in stop_ids:
            print('Parsing arrivals at ' + str(stop_id))
            start = time.time()
            # (i) Query all arrivals to the stop during the time period of interest 
            #
            arrivals = mongoParseArrivals(route_id,stop_id,start,end,collection)
#             arrivals = mongoParseOldArrivals(route_id,stop_id,start,end,collection)
            print('(i) took: ' + str(time.time()-start))
            start = time.time()
            # (ii) Assign a demand rate value to each one of the arrivals
#             #
#             print('Arrivals count: ' + str(len(arrivals)))
#             t0 = time.time()
#             for i,arrival in enumerate(arrivals):
#                 arrivals[i].demand = select_demand(demandStore,'demandDataFrame',arrival,mTZ)
#                 arrivals[i].stop_pm = float('%.2f' % arrivals[i].stop_pm)
           
#             # Select the unique service_ids
#             #
#             uniqueServiceIds = list(set(map(lambda x: int(x.service_id),arrivals)))
#             print('Parsing scheduledArrivals')
#             # Using the uniqueServiceIds
#             # Create a list of potential trip_ids. Note that this list does not take into 
#             # consideration stop_id, so it could have more arrivals than the final schArrivalsCursor
#             scheduledArrivals = mongoParseScheduleArrivals(route_id,stop_id,uniqueServiceIds,collection2,collection3)
#             print('Determining headways')
#             print('(ii) took: ' + str(time.time()-start))
#             start = time.time()
#             # (iii) Add the schedule information to all the arrivals (headway, sch_headway, sch_headway_own, route_id_follower)
#             arrivals = determineHeadways(arrivals,scheduledArrivals)
#             print('Assigning departure times and filtering arrivals with missing data')
#             print('(iii) took: ' + str(time.time()-start))
#             start = time.time()
#             # (iv) Parse the departure information from mongo and assign it to arrivals, then filter those arrivals
#             arrivals = mongoAssignDepartureTimes(route_id,stop_id,start,end,collection,mTZ,arrivals,tau,t_b)
# #             arrivals = mongoAssignOldDepartureTimes(route_id,stop_id,start,end,collection,mTZ,arrivals,tau,t_b)
#             print('(iv) took: ' + str(time.time()-start))
            
#             # Dump new arrivals to a json file
#             with open(arrivalDataFileName, mode='r', encoding='utf-8') as f1:
#                 try:
#                     feed = json.load(f1)
#                 except ValueError:
#                     feed = []
            
#             newFeed1 = feed + [arrival.__dict__ for arrival in arrivals]
            
#             with open(arrivalDataFileName, mode='w+', encoding='utf-8') as f1:
#                 json.dump(newFeed1, f1, indent=1, sort_keys=True)
            
#             # Dump new scheduled arrivals to another json file
#             with open(scheduledArrivalDataFileName, mode='r', encoding='utf-8') as f2:
#                 try:
#                     feed = json.load(f2)
#                 except ValueError:
#                     feed = []
            
#             newFeed2 = feed + [schArrival.__dict__ for schArrival in scheduledArrivals]
            
#             with open(scheduledArrivalDataFileName, mode='w+', encoding='utf-8') as f2:
#                 json.dump(newFeed2, f2, indent=1, sort_keys=True)
        
#     # Closing resources
#     demandStore.close()
             
    return
        

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-u","--username",dest='username',
                        metavar="DBUSERNAME",
                        required=True,
                        help="Postgres database user name.")
    parser.add_argument("-p","--password",dest='password',
                        metavar="DBPASSWORD",
                        required=True,
                        help="Postgres database password.")

    args = parser.parse_args()
    username = args.username
    password = args.password
    main(username,password)