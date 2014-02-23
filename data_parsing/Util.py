'''
Created on Nov 8, 2013

@author: juanargote
'''

from datetime import datetime
import os
from operator import attrgetter
import pandas as pd
import numpy as np


# Define the classes and functions to use through the code.
class Arrival:
    def __iter__(self):
        for attr in dir(Arrival):
            if not attr.startswith("__"):
                yield attr

class SchArrival:
    def __iter__(self):
        for attr in dir(SchArrival):
            if not attr.startswith("__"):
                yield attr

def parseServiceId(trip_id):
    """Function to parse the service_id from a trip_id string"""
    rawServiceId = trip_id[0:4]
    if (rawServiceId == '0000'):
        return '0'
    else:
        return rawServiceId[1:]

def seconds(x):
    hh = int(x[0:2])
    mm = int(x[3:5])
    ss = int(x[6:8])
    return ss + 60 * (mm + 60 * hh)

def seconds_interval(x,y):
    return seconds(x) - seconds(y)

def day_type_of(datetime):
    if (datetime.weekday() < 5):
        return 'Laborable'
    elif (datetime.weekday() < 6):
        return 'Sabado'
    else:
        return 'Dom. y Fest.'

def parseDemandFile(jsonFilename,storeFilename):
    '''Parse a demand file into an HDFStore if necessary. Returns the HDFStore handle.
    
    Keyword arguments:
    jsonFilename -- Name of the JSON file containing the demand data
    storeFilename -- Name of the HDFStore where the parsed demand data will be stored
    '''
    
    # Parse the demand file if necessary
    #
    filepath = os.path.abspath(os.path.join(os.path.dirname(__file__ ),os.pardir,'External Files'))
    demandStoreFileName = os.path.join(filepath,storeFilename)
    demandStore = pd.HDFStore(demandStoreFileName)
    if (not hasattr(demandStore,'demandDataFrame')):
        # The demandDataFrame has not been parsed yet
        print('Parsing demand data')
        jsonFilePath = os.path.join(filepath,jsonFilename)
        demandDtypeDict = {'day':'int32',\
                           'demand':'int32',\
                           'hour':'int32',\
                           'month':'int32',\
                           'route_id':'string',\
                           'stop_id':'string'}
        demandDataColumns = ['day',\
                             'hour',\
                             'month',\
                             'route_id',\
                             'stop_id']
        demandDataFrame = pd.read_json(jsonFilePath,dtype=demandDtypeDict)
        print(demandDataFrame)
        demandStore.append('demandDataFrame',demandDataFrame,data_columns = demandDataColumns);       
        print(demandStore)
    else:
        print('Store file already exists, loading it.')
    return demandStore

# TODO: Take out the if clause to limit the demand to the November month
def select_demand(demandStore,demandDataFrameName,arrival,timeZone):
    '''Select a demand rate value.
    
    Keyword arguments:
    demandStore -- pandas.HDFStore object containing a demand DataFrame 
    demandDataFrameName -- string containing the demand DataFrame name within demandStore
    arrival -- Util.Arrival object containing arrival information
    timeZone -- pytz.timezone object
    '''
    
    # Auxiliary variables to perform the query
    mDateTime = datetime.fromtimestamp(int(arrival.arrival_time/1000),timeZone)
    
    # Set up the query array
    query = ['day = ' + day_type_of(mDateTime),
             'hour = ' + str(mDateTime.hour),
             'month = ' + str(11 if (mDateTime.month > 11) else mDateTime.month),
             'route_id = ' + arrival.route_id,
             'stop_id = ' + arrival.stop_id]
    
    # Query the demandDataFrame
    demandResultSeries = demandStore.select(demandDataFrameName,query).demand
    
    if (len(demandResultSeries) > 0):
        return np.asscalar(demandResultSeries.max())
    else:
        return 0
    
def drange(start, stop, step):
    mList = []
    r = start
    mList.append(r)
    while r < stop:
        r += step
        mList.append(r)
    return mList

def same_date(x,y,tz):
    '''Logic test to see if x and y share the date
    
    x -- Unix timestamp in milliseconds
    y -- Unix timestamp in milliseconds
    tz -- Timezone object (pytz.timezone)
    '''
    # Generate the two datetime objects
    datetimeX = datetime.fromtimestamp(int(x/1000),tz)
    datetimeY = datetime.fromtimestamp(int(y/1000),tz)
    return True if (datetimeX.date() == datetimeY.date()) else False

def mongoParseArrivals(route_id,stop_id,start,end,eventsCollection):
    '''Function that parses a GTFS event collection. It returns an array of Arrival objects.
    
    route_id -- Array with route ids as strings
    stop_id -- Stop id string (single element)
    start -- Unix timestamp in milliseconds
    end -- Unix timestamp in milliseconds
    eventsCollection -- MongoClient database object
    '''
    arrivalsArray = []
    arrivalsCursor = eventsCollection.find({\
            'time':{'$gte':start,'$lt':end}, \
            'type':0, \
            'route_id':{'$in':route_id}, \
            'stop_id':stop_id})\
            .sort('time')
    
    # Iterate through the results
    #
    if arrivalsCursor.count() > 0:
        for arrival in arrivalsCursor:
            elem = Arrival()
            elem.arrival_time = arrival['time']
            elem.delay = arrival['delay']
            elem.trip_id = arrival['trip_id']
            elem.stop_id = arrival['stop_id']
            elem.stop_pm = arrival['stop_postmile']
            elem.stop_seq = arrival['stop_sequence']
            elem.service_id = parseServiceId(arrival['trip_id'])
            elem.route_id = arrival['route_id']
            elem.bus_id = arrival['driver_compliance']['bus_id']
            elem.driver_id = arrival['driver_compliance']['bus_id']
            arrivalsArray.append(elem)
    else:
        print('No arrivals this day!')
    
    return arrivalsArray

def mongoParseOldArrivals(route_id,stop_id,start,end,eventsCollection):
    '''Function that parses a GTFS event collection. It returns an array of Arrival objects.
    
    route_id -- Array with route ids as strings
    stop_id -- Stop id string (single element)
    start -- Unix timestamp in milliseconds
    end -- Unix timestamp in milliseconds
    eventsCollection -- MongoClient database object
    '''
    arrivalsArray = []
    arrivalsCursor = eventsCollection.find({\
            'entity.trip_update.stop_time_update.arrival.time':{'$gte':start,'$lt':end}, \
            'entity.trip_update.trip.route_id':{'$in':route_id}, \
            'entity.trip_update.stop_time_update.stop_id':stop_id})\
            .sort('entity.trip_update.stop_time_update.arrival.time')
    
    # Iterate through the results
    #
    if arrivalsCursor.count() > 0:
        for arrival in arrivalsCursor:
            elem = Arrival()
            elem.arrival_time = arrival['entity']['trip_update']['stop_time_update']['arrival']['time']
            elem.delay = arrival['entity']['trip_update']['stop_time_update']['arrival']['delay']
            elem.trip_id = arrival['entity']['trip_update']['trip']['trip_id']
            elem.stop_id = arrival['entity']['trip_update']['stop_time_update']['stop_id']
            elem.stop_pm = arrival['entity']['trip_update']['stop_time_update']['stop_postmile']
            elem.stop_seq = arrival['entity']['trip_update']['stop_time_update']['stop_sequence']
            elem.service_id = parseServiceId(arrival['entity']['trip_update']['trip']['trip_id'])
            elem.route_id = arrival['entity']['trip_update']['trip']['route_id']
            arrivalsArray.append(elem)
    else:
        print('No arrivals this day!')
    
    return arrivalsArray

def mongoParseScheduleArrivals(route_ids,stop_id,uniqueServiceIds,tripsCollection
,stop_timesCollection):
    '''Function that parses the trips and stop_times GTFS collections. 
    It returns an array of SchArrival objects.
    
    route_id -- Array with route ids as strings
    stop_id -- Stop id string (single element)
    uniqueServiceIds -- Unix timestamp in milliseconds
    tripsCollection -- Unix timestamp in milliseconds
    stop_timesCollection -- MongoClient database object
    '''
    # schArrivals array
    schArrivals = []
    
    # (i) Find a sequence of scheduled trips corresponding to the input routes.
    # Note that those trips may not visit the input stop.
    candidateTrips = []
    for route_id in route_ids:
        schTripsCursor = tripsCollection.find({'route_id':route_id,
                                               'service_id':{'$in':uniqueServiceIds}}) 
        if (schTripsCursor.count() > 0):
            for schTrip in schTripsCursor:
                tid =  schTrip['trip_id']
                candidateTrips.append(tid)
        else:
            print('No scheduled trip ids for route ' + str(route_id) +' and services ' + str(uniqueServiceIds))
        
    # Create an ordered list of schArrivals based on the potential trip_ids
    # Note that the method to get route_id is adapted to Dbus!!!!!
    schArrivalsCursor = stop_timesCollection.find({\
        'stop_id':stop_id,'trip_id':{'$in':candidateTrips}})
    if schArrivalsCursor.count() > 0:
        for schArrival in schArrivalsCursor:
            elem = SchArrival()
            elem.stop_id = schArrival['stop_id']
            elem.trip_id = schArrival['trip_id']
            elem.arrival_time = schArrival['arrival_time']
            elem.stop_seq = schArrival['stop_sequence']
            elem.service_id = parseServiceId(schArrival['trip_id'])
            elem.route_id = schArrival['trip_id'][6:8].replace('0','')
            schArrivals.append(elem)
    else:
        print('No scheduled arrivals matching the scheduled trip ids!')

    # Sort scheduled arrivals based on service_id first and arrival time second,
    # assign a scheduled headway (in seconds)
    #
    schArrivals = sorted(schArrivals, key = attrgetter('service_id','arrival_time'))
    for i,x in enumerate(schArrivals):
        if (i != 0 and x.service_id == schArrivals[i-1].service_id):
            schArrivals[i].sch_headway = seconds_interval(x.arrival_time,schArrivals[i-1].arrival_time)
            schArrivals[i].route_id_preceding = schArrivals[i-1].route_id
            schArrivals[i].sch_headway_own = None
            for j in range(i-1,-1,-1):
                if (x.service_id != schArrivals[j].service_id):
                    break
                else:
                    if (x.route_id == schArrivals[j].route_id):
                        schArrivals[i].sch_headway_own = seconds_interval(x.arrival_time,schArrivals[j].arrival_time)
                        break
        else:
            schArrivals[i].sch_headway = None
            schArrivals[i].sch_headway_own = None
            schArrivals[i].route_id_preceding = None
    
    return schArrivals

def determineHeadways(arrivals,schArrivals):
    '''Function that takes a list of Arrivals and a list of ScheduledArrivals 
    and combines their information to determine headways. Returns list of arrivals
    with the headways.
    
    arrivals -- List of Arrival objects.
    schArrivals -- List of ScheduledArrival objects.
    '''
    
    # Complete the headway info for each arrival: schHeadway, headway, headwayDev
    #
    for i,x in enumerate(arrivals):
        arrivals[i].sch_headway = None
        arrivals[i].route_id_preceding = None
        arrivals[i].headway = None
        arrivals[i].headway_dev = None
        arrivals[i].sch_headway_own = None
        if (i > 0):
            # Find the index of schArrival with the arrival trip_id
            index = [j for j,y in enumerate(schArrivals) if x.trip_id == y.trip_id]
            
            if (len(index) != 0):
                index = index[0]
                arrivals[i].sch_headway = schArrivals[index].sch_headway
                arrivals[i].sch_headway_own = schArrivals[index].sch_headway_own
                arrivals[i].route_id_preceding = schArrivals[index].route_id_preceding
                # Check if the previous arrival corresponds to the previous scheduled arrival
                if (arrivals[i-1].trip_id == schArrivals[index-1].trip_id \
                    and arrivals[i].service_id == arrivals[i-1].service_id):
                    arrivals[i].headway = (arrivals[i].arrival_time - arrivals[i-1].arrival_time)/1000
    
                if (arrivals[i].headway != None):
                    arrivals[i].headway_dev = arrivals[i].headway - arrivals[i].sch_headway
    return arrivals

def mongoAssignDepartureTimes(route_ids,stop_id,start,end,eventsCollection,timeZone,arrivals,tau,t_b):
    '''Function that assigns the departure times for a list of arrivals and 
    calculates the dwell time. It returns a list of filtered arrivals containing
    headways and dwell times.
    
    route_ids -- list containing route_id
    stop_id -- the id of the stop of interest
    start -- Unix time to start our query
    end -- Unix time to end the query
    arrivals -- List of Arrival objects.
    eventsCollection -- Mongodb collection that contains the departure information
    tau -- average lost time per stop (~1-2 seconds)
    t_b -- average boarding time per passenger
    '''
    
    departuresCursor = eventsCollection.find({\
                'time':{'$gte':start,'$lt':end}, \
                'type':1,\
                'route_id':{'$in':route_ids}, \
                'stop_id':stop_id})\
                .sort('time')
    print('Departures length ' + str(departuresCursor.count()))
    
    departuresCursor.batch_size(100000)
    departuresList = list(departuresCursor)
    
    if len(departuresList) > 0:
        for departure in departuresList:
            departureTime = departure['time']
            departureTripId = departure['trip_id']
            idx = next((i for i,x in enumerate(arrivals) if (x.trip_id == departureTripId\
                            and same_date(x.arrival_time,departureTime,timeZone))),None)
            if (idx != None):
                arrivals[idx].departure_time = departureTime
                try:
                    arrivals[idx].recommended_dwell = departure['driver_compliance']['dwell_time_recommended']
                    arrivals[idx].dwell_diff = departure['driver_compliance']['dwell_time_difference']
                except:
                    arrivals[idx].recommended_dwell = None
                    arrivals[idx].dwell_diff = None
    else:
        print('No departures during the time interval of interest!')

    
    # (iii) Generate all the variables of interest for the parsed data
    # filtering out those arrivals that do not have headways or departure times
    print('Original arrivals no: ' + str(len(arrivals)))
    arrivals = list(filter(lambda x: hasattr(x,'departure_time') and x.headway != None,arrivals))
    print('Filtered arrivals no: ' + str(len(arrivals)))
    
    for i,arrival in enumerate(arrivals):
        arrivals[i].dwell_time = (arrival.departure_time - arrival.arrival_time)/1000
        arrivals[i].estimated_boardings = int((arrivals[i].dwell_time - tau)/t_b)
        arrivals[i].theta = arrivals[i].demand*arrivals[i].headway/3600

    return arrivals

def mongoAssignOldDepartureTimes(route_ids,stop_id,start,end,eventsCollection,timeZone,arrivals,tau,t_b):
    '''Function that assigns the departure times for a list of arrivals and 
    calculates the dwell time. It returns a list of filtered arrivals containing
    headways and dwell times.
    
    route_ids -- list containing route_id
    stop_id -- the id of the stop of interest
    start -- Unix time to start our query
    end -- Unix time to end the query
    arrivals -- List of Arrival objects.
    eventsCollection -- Mongodb collection that contains the departure information
    tau -- average lost time per stop (~1-2 seconds)
    t_b -- average boarding time per passenger
    '''
    
    departuresCursor = eventsCollection.find({\
                'entity.trip_update.stop_time_update.departure.time':{'$gte':start,'$lt':end}, \
                'entity.trip_update.trip.route_id':{'$in':route_ids}, \
                'entity.trip_update.stop_time_update.stop_id':stop_id})\
                .sort('entity.trip_update.stop_time_update.departure.time')
    print('Departures length ' + str(departuresCursor.count()))
    
    departuresCursor.batch_size(100000)
    departuresList = list(departuresCursor)
    
    if len(departuresList) > 0:
        for departure in departuresList:
            departureTime = departure['entity']['trip_update']['stop_time_update']['departure']['time']
            departureTripId = departure['entity']['trip_update']['trip']['trip_id']
            idx = next((i for i,x in enumerate(arrivals) if (x.trip_id == departureTripId\
                            and same_date(x.arrival_time,departureTime,timeZone))),None)
            if (idx != None):
                arrivals[idx].departure_time = departureTime
    else:
        print('No departures during the time interval of interest!')

    
    # (iii) Generate all the variables of interest for the parsed data
    # filtering out those arrivals that do not have headways or departure times
    print('Original arrivals no: ' + str(len(arrivals)))
    arrivals = list(filter(lambda x: hasattr(x,'departure_time') and x.headway != None,arrivals))
    print('Filtered arrivals no: ' + str(len(arrivals)))
    
    for i,arrival in enumerate(arrivals):
        arrivals[i].dwell_time = (arrival.departure_time - arrival.arrival_time)/1000
        arrivals[i].estimated_boardings = int((arrivals[i].dwell_time - tau)/t_b)
        arrivals[i].theta = arrivals[i].demand*arrivals[i].headway/3600

    return arrivals