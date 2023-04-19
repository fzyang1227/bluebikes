import requests
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os
import zipcodes
import datetime as dt
from tqdm import tqdm
import time

load_dotenv()

API_KEY = os.getenv('MAPS_API')

boston_zip = [code['zip_code'] for code in zipcodes.filter_by(city="Boston", state="MA")]
    
def trip_clean(filename):
    ''' clean trip data 
    args.
        filename: string of filename
    returns.
        trips: pandas dataframe of cleaned trip data
    '''
    df = pd.read_csv(filename)
    df = df.dropna(how='any', axis=0)    
    
    # filter out trips outside of boston
    trips = df.loc[df['postal code'].isin(boston_zip)]
    
    # filter out circular trips
    trips = trips.loc[trips['start station name'] != trips['end station name']]
    
    # add in date time attributes
    trips['starttime'] = trips['starttime'].apply(lambda x : dt.datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f'))
    trips['stoptime'] = trips['stoptime'].apply(lambda x : dt.datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f'))
    trips['day_of_week'] = trips['starttime'].dt.dayofweek
    trips['base_hour'] = trips['starttime'].apply(lambda x: x.replace(minute=0, second=0, microsecond=0))
    trips['weekday'] = trips['starttime'].dt.weekday
    trips['hour'] = trips['starttime'].dt.hour
        
    return trips

def _bike_dist(array):
    ''' calculate real distance between points
    args.
        - array: list of tuples of coordinates
                 [(lat1, lon1, lat2, lon2), ...]
    returns.
        - dists: array of distances
        - times: array of times
    '''
    assert len(array) > 0, 'array must be non-empty'
    assert len(array) <=10, 'array must be <= 10 (max request limit)'
    
    # build url
    origins = '%7C'.join([f'{x[0]}%2C{x[1]}' for x in array])
    destinations = '%7C'.join([f'{x[2]}%2C{x[3]}' for x in array])
    url = f"https://maps.googleapis.com/maps/api/distancematrix/json?origins={origins}&destinations={destinations}&mode=bicycling&key={API_KEY}"
    
    # get response
    response = requests.request("GET", url).json()
    dists = []
    times = []
    
    # parse response for distances and times
    for i in range(len(array)):
        try:
            dists.append(response['rows'][i]['elements'][i]['distance']['value'])
            times.append(response['rows'][i]['elements'][i]['duration']['value'])
        except:
            dists.append(np.nan)
            times.append(np.nan)
    return dists, times


def calc_distance_times(trips):
    ''' calculate bike distance and time between stations 
    args.
        - trips: pandas dataframe of trips
        - api_key: string of google api key
    returns
        - start_ends: pandas dataframe of trips with distance and time
    '''
    # get unique start and end stations
    start_ends = trips.groupby(['start station name', 'end station name', 'start station latitude', 'start station longitude', 'end station latitude', 'end station longitude']).size().reset_index(name='count').drop('count', axis=1)
    
    dists = []
    times = []
    
    # iterate through 10 rows at a time
    for _, rows in tqdm(start_ends.groupby(start_ends.index // 10)):
        
        # timeout to avoid api limit
        time.sleep(0.5)
        
        # get coordinates
        arr = list(rows[['start station latitude', 'start station longitude', 'end station latitude', 'end station longitude']].itertuples(index=False, name=None))
        
        # calculate distance and time
        dist, time_val = _bike_dist(arr)
        dists = dists + dist
        times = times + time_val
        
    start_ends['distance'] = dists
    start_ends['time'] = times
    
    return start_ends

if __name__ == '__main__':
    # stored in /data folder if more than one file
    filenames = ['data/202303-bluebikes-tripdata.csv']
    
    # combine files
    files = [pd.DataFrame()]
    for file in filenames:
        files.append(trip_clean(file))
    trips = pd.concat(files, ignore_index=True)
    
    # calulate distance and time
    start_ends = calc_distance_times(trips)
    
    # export to csv
    trips.to_csv('data/trips.csv', index=False)
    start_ends.to_csv('data/start_ends.csv', index=False)