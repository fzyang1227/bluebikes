import streamlit as st
import pandas as pd
import numpy as np
import datetime as dt
import time as t
st.set_page_config(layout="centered")

st.title('Bluebike Dock Predictor')

data_load_state = st.text('Please wait a second')

stations = pd.read_csv('data/boston_current_bluebike_stations.csv')

# Download ml model

data_load_state.text('Lets begin!')

days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

day_option = st.selectbox("Which Day is the trip taking place?", days)

col1, col2 = st.columns(2)

with col1:

    start_station_option = st.selectbox("Which Bluebike station are you starting from?", ["I know what time I will reach"].__add__(stations['Name'].to_list()))

    end_station_option = st.selectbox("Which Bluebike station does your destination end at?", stations['Name'])

with col2:

    start_station_not_selected = (start_station_option == "I know what time I will reach")

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
    # compute the result
    t.sleep(3)
    result = 1
    if result == 0:
        ans = "There would be no available docks"
    elif result == 1:
        ans = "There might be some docks available"
    else:
        ans = "There will be docks available"
    data_load_state_2.text(ans)