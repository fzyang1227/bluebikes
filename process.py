import pandas as pd
import numpy as np
import datetime as dt
from collections import defaultdict
from neo_utils import Neo4jConnection, add_station, add_trip
from dotenv import load_dotenv
import os
load_dotenv()

URI = os.getenv('NEO4J_URI')
USER = os.getenv('NEO4J_USER')
PASS = os.getenv('NEO4J_PASS')

def station_clean():
    ''' cleaned station data
    args.
        None
    returns.
        df: pandas dataframe of cleaned station data
    '''
    # read in data
    station = 'data/boston_current_bluebike_stations.csv'
    df = pd.read_csv(station, index_col=0).reset_index(drop=True)
    
    # drop punctuation and spaces from station names
    df['abv'] = df['Name'].apply(lambda x: 'st'+''.join(e for e in x if e.isalnum()))
    return df[['Name', 'Latitude', 'Longitude', 'Total docks', 'abv']]

def build_edges(trips, routes):
    ''' build edges between stations 
    args.
        trips: pandas dataframe of trip data
        routes: pandas dataframe of station routes trip distance and time
    returns.
        edges: dictionary of edges
    '''
    # get trips that start or end at a station we have data for
    grouped = trips.groupby(['start station name', 'end station name', 'weekday', 'hour'])['tripduration'].agg(['count', 'mean'])
    
    # join with routes to get distance and time from google maps
    all_trips = grouped.join(routes.set_index(['start station name', 'end station name']), on=['start station name', 'end station name']).reset_index().rename(columns={'mean': 'avg_duration'})
    
    # build nested dictionary for edges
    edges = {k: {'total': v['count'].sum(),
                  'avg_duration': ((v['avg_duration']*v['count'])/v['count'].sum()).sum(), 
                  'distance': v.distance.iloc[0],
                  'anticipated_time': v.time.iloc[0],
                  } | {f'w{wh[0]}_h{wh[1]}_count': vals['count'].iloc[0] 
                           for wh, vals in v.groupby(['weekday', 'hour'])}
              for k, v in all_trips.groupby(['start station name', 'end station name'])}
    
    ''' dictionary structure:
        {(start station, end station):
            {
                total: total number of trips between stations
                average_duration: average duration of trips between stations
                distance: distance between stations
                anticipated_time: time between stations
                w(weekday)_h(hour)_count: number of trips between stations at this time
                ...
                }
            }
        ...
        }
    '''
        
    return edges
    
        
    
def main():
    # stored in /data folder
    stations = station_clean()
    routes = pd.read_csv('data/start_ends.csv')[['start station name', 'end station name', 'distance', 'time']]
    trips = pd.read_csv('data/trips.csv')
    
    # get trips that start or end at a station we have data for
    station_names = stations.Name.tolist()
    trips = trips[(trips['start station name'].isin(station_names)) & trips['end station name'].isin(station_names)]
    
    # get edges between stations
    edges = build_edges(trips, routes)
    
    # start connection to neo4j
    neo = Neo4jConnection(URI, USER, PASS)
    
    # add stations to neo4j
    for _, row in stations.iterrows():
        add_station(neo, row['Name'], row['Latitude'], row['Longitude'], row['Total docks'])
        
    # add edges to neo4j
    for k, v in edges.items():
        add_trip(neo, k[0], k[1], str(v).replace("'", ''))
    
    neo.close()
    
    
if __name__ == '__main__':
    main()

