import streamlit as st
import pandas as pd
import numpy as np
import datetime as dt
import time as t
from neo_utils import Neo4jConnection, get_all_stations, get_station, get_popularity, get_trip
from joblib import load
from dotenv import load_dotenv
import os
import requests

load_dotenv()

# build mappings
test = pd.read_csv('data/current_bluebikes_stations.csv', header=1)
station_map = dict(zip(test['Name'], test['Number']))
short_map = {dct['short_name']: dct['station_id'] for dct in requests.get("https://gbfs.bluebikes.com/gbfs/en/station_information.json").json()['data']['stations']}

# get the environment variables
URI = os.getenv('NEO4J_URI')
USER = os.getenv('NEO4J_USER')
PASS = os.getenv('NEO4J_PASS')

# load in the model
clf2 = load('data/clf.joblib')

# start the connection to neo4j
neo = Neo4jConnection(URI, USER, PASS)

st.title('Bluebike Dock Predictor')

data_load_state = st.text('Please wait a second')

stations = get_all_stations(neo)

# Download ml model

data_load_state.text('Lets begin!')
days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
day_option = st.selectbox("Which Day is the trip taking place?", days)
col1, col2 = st.columns(2)

with col1:

    start_station_option = st.selectbox("Which Bluebike station are you starting from?", ["None"].__add__(stations))

    end_station_option = st.selectbox("Which Bluebike station does your destination end at?", stations)

with col2:

    start_station_not_selected = (start_station_option == "None")

    start_time_option = st.time_input("Approximately what time will you start the trip?", dt.datetime.now().time(), disabled=start_station_not_selected)

    end_time_option = st.time_input("Approximately what time will you reach the destination?", dt.datetime.now().time(), disabled=(not start_station_not_selected))

if (start_station_not_selected):
    st.write('So you want to check the dock availability of ' + str(end_station_option) + ' Bluebike station on ' + str(day_option) + ' at ' + str(end_time_option.strftime('%H:%M')) + '?')
else:
    st.write('So you want to check the dock availability of ' + str(end_station_option) + ' Bluebike station on ' + str(day_option) + 
             ' when starting from ' + start_station_option + ' at ' + str(start_time_option.strftime('%H:%M')) + '?')

if st.button('YES!!'):
    data_load_state_2 = st.text('Give us a second while we compute')
    day = days.index(day_option)
    
    #build feature vector
    pop = get_popularity(neo, end_station_option)
    docks = get_station(neo, end_station_option)['docks']
    dock_id = short_map.get(station_map[end_station_option], 0)
    
    if not start_station_not_selected:
        try:
            ant_time = get_trip(neo, start_station_option, end_station_option)['anticipated_time']
        except:
            ant_time = 900
        end_time_option = dt.datetime.combine(dt.date(2023, 1, 1), start_time_option) + dt.timedelta(seconds=ant_time)
    
    if dock_id == 0:
        ans = "Station is unavailable"
    else:
        # predict the model
        cur = [sta for sta in requests.get("https://gbfs.bluebikes.com/gbfs/en/station_information.json").json()['data']['stations'] if sta['station_id'] == dock_id][0]['capacity']
        result = clf2.predict([[pop, docks, end_time_option.hour, day, cur]])[0]
    
        # compute the result
        if result == 0:
            ans = "There would be no available docks"
        elif result == 1:
            ans = "There might be some docks available"
        else:
            ans = "There will be docks available"
        ans = f'{ans} at {end_time_option.strftime("%H:%M")}'
    data_load_state_2.text(ans)