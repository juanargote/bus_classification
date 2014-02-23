'''
Created on Nov 6, 2013

@author: juanargote
'''

import sys
import time
import csv
import os
import json
import re


def main(args):
    
    filepath = os.path.abspath(os.path.join(os.path.dirname(__file__ ),os.pardir,'External Files'))
    
    demand = []
    lines = ['5','25']
    months = range(2,12)
    
    # Parse the demand csv files
    #
    print('Parsing the demand csv file')
    start_time = time.time()
    for line in lines:
        for month in months:
            print('Line ' + line + ', month ' + str(month))
            filename = os.path.join(filepath,"L" + line + "_" + str(month) + ".csv")
            with open(filename,'rU') as f:
                reader = csv.reader(f,delimiter=',',quoting=csv.QUOTE_NONE)
                reader.next()
                for row in reader:
                    for i in range(0,24):
                        demand.append({'route_id': line, 'month': month, 'day': row[0], 'stop_id': re.search(r'\[(\d+)\]',row[2]).group(1), 'hour': i, 'demand': int(row[i+3]) if row[i+3]!='' else 0})
            
    jsonFileName = os.path.join(filepath,"demand.txt")
    with open(jsonFileName,'w') as f:
        json.dump(demand,f,encoding='latin-1')
    
    print("Demand parsed and saved as JSON in a .txt file")

if __name__ == '__main__':
    main(sys.argv[1:])