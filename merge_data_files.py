# -*- coding: utf-8 -*-
"""
Created on Wed Sep 12 08:52:59 2018

@author: Ronimo
"""

import pandas as pd
from tkinter import Tk
from tkinter.filedialog import askopenfile, asksaveasfile
from sql_utills import get_station_data

def set_time_key(df, source):   
    
    df.assign(DataSource = source)
    return df

def get_st_data_query(*args):
    
    stations = args
    if len(stations)==1:
        stations = str(stations).replace(',','')
        
    return "SELECT * FROM ObsWX WHERE staname IN {0}".format(stations)

def save_combined_station_data(df, stname):

    defaultextension='.csv'
    filetypes = [('comma to separate values', '.csv'), 
                 ('LogerNet comma to separate values', '.dat'),
                 ('all files', '.*')]
    initialfile = 'Combine_{0}_data'.format(stname) 
    params = {'defaultextension': defaultextension, 'filetypes': filetypes, 'initialfile':initialfile}
    
    combine.to_csv(asksaveasfile(mode='w', **params))
    

stations = ('CALVP',) 
quary = get_st_data_query(*stations)
server_data = get_station_data(quary)

for st in set(server_data['staname']):

    Tk().withdraw()
    try:
        loggetNet_data = pd.read_csv(askopenfile(), index_col='time')
    except ValueError as e:
        print(e)
        continue
    
    set_time_key(st, "John's Unix")
    set_time_key(loggetNet_data, "LoggerNet")
    
    combine = loggetNet_data.merge(st, how='outer', left_index=True, right_index=True)
    # If we want to have missing data prerionds in the table: 
    combine.resample(combine.date_range(combine.index[0], combine.index[1], freq='H'), inplace=True)
    
    save_combined_station_data(combine, st)
