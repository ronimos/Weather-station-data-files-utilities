# -*- coding: utf-8 -*-
"""
Created on Wed Sep 12 08:52:59 2018

@author: Avalanche
"""

import pandas as pd
from tkinter import Tk
from tkinter.filedialog import askopenfile, asksaveasfile
from sql_utills import caic_mysql_query

def set_time_key(df, source):   
    
    df.assign(DataSource = source)
    return df

def get_st_data_query(*args):
    
    return "SELECT * FROM ObsWX WHERE staname IN {0}".format(tuple(args))

def save_combined_station_data(df, stname):

    defaultextension='.csv'
    filetypes = [('comma to separate values', '.csv'), 
                 ('LogerNet comma to separate values', '.dat'),
                 ('all files', '.*')]
    initialfile = 'Combine_{0}_data'.format(stname) 
    params = {'defaultextension': defaultextension, 'filetypes': filetypes, 'initialfile':initialfile}
    
    combine.to_csv(asksaveasfile(mode='w', **params))
    

stations = ('CALVP', 'CACMP', 'Dummy Station') 
# The "Dummy Station" is to keep the SQL quarry corect 
# where there is only one station data we'd like to doemload
quary = get_st_data_query(*stations)
unix_data = caic_mysql_query(quary)

for st in set(unix_data['staname']):

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
