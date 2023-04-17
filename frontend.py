import streamlit as st
import pandas as pd
import numpy as np
import datetime as dt
import time as t

st.title('Bluebike Dock Predictor')

data_load_state = st.text('Please wait a second')

stations = pd.read_csv('data/boston_current_bluebike_stations.csv')

# Download ml model

data_load_state.text('Lets begin!')

station_option = st.selectbox("Which Bluebike station are you starting from?", stations['Name'])

station_option = st.selectbox("Which Bluebike station does your destination end at?", stations['Name'])

days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

day_option = st.selectbox("Which Day is the trip taking place?", days)

time_option = st.time_input("Approximately what time will you reach the destination?", dt.datetime.now().time())

st.write('So you want to check the dock availability of ' + str(station_option) + ' Bluebike station on ' + str(day_option) + ' at ' + str(time_option.strftime('%H:%M')) + '?')

if st.button('YES!!'):
    data_load_state_2 = st.text('Give us a second while we compute')
    day = days.index(day_option)
    time = time_option
    station = station_option
    #Use day, time and station option to compute the result
    t.sleep(3)
    result = 1
    if result == 0:
        ans = "There would be no available docks"
    elif result == 1:
        ans = "There might be some docks available"
    else:
        ans = "There will be docks available"
    data_load_state_2.text(ans)