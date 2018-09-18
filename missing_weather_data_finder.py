# -*- coding: utf-8 -*-
"""
Created on Wed May 30 10:39:48 2018

@author: Ron Simenhois
This script check for missing data in the weather data file on Unix machine
"""

import pandas as pd
import numpy as np
import logging
import os
from sql_utills import caic_mysql_query

logging.basicConfig(level=logging.INFO, format='[%(asctime)s %(levelname)s] (%(threadName)-9s) %(message)s',)
logger = logging.getLogger(__name__)

def get_quary(*args):
    '''
    Generate SQL quary to pull weather stations data logging time 
    for a given weather stations list  
    
    arguments:
        args - a list of weather stations names
    return:
        none
    '''    
    stations = args
    if len(stations)==0:
        return ''
    
    q = '''
        SELECT time
              ,staname
        FROM ObsWX
        WHERE staname IN {}'''.format(stations)
        
    return q
    
    

def check_for_missing_data(stations):
    '''
    Load a pandas Database from caic_mysql_query fuction, clean it 
    and check for missing data. Save a CSV file with a list of missing record. 
    
    arguments:
        st - list of str Weather station key names
    return:
        none
    '''
    sql_quary = get_quary(*stations)
    
    data = caic_mysql_query(sql_quary)
    logger.info('Weather data for stations {} is loaded from the server'.format(stations))
    for st in stations:
        logger.info('Cheking for missing data in station: {}'.format(st))
        st_data = data[data['staname']==st]
        if len(st_data)==0:
            logger.info('There is no data from station {}, moving on...'.format(st))
            continue
        st_data = st_data.iloc[1:, :] # take off the 1970/1/1 row
        st_data = st_data.assign(missing = False)
        # Find station time interval (assuning it is working most of the time...)
        obs_time_d = int(((st_data['time']-st_data['time'].shift(1)).median()/np.timedelta64(1, 'm')))#.astype(int)
        resample_interval = str(obs_time_d)+'Min'
        # Fill missing records with Nan
        missing_records = st_data.resample(resample_interval, on='time').mean()
        # Add columns for missing record, start and end missing records time: 
        missing_records['missing'].fillna(True, inplace=True)     
        missing_records['st'] = ~missing_records['missing'].eq(missing_records['missing'].shift(1))
        missing_records.loc[missing_records['missing']==False, 'st'] = False 
        missing_records['end'] = ~missing_records['missing'].eq(missing_records['missing'].shift(1))
        missing_records.loc[missing_records['st']==True, 'end'] = False 
        missing_records = missing_records.iloc[1:-1, :]
        # Clean start missing time Series
        st_missing = missing_records.loc[missing_records['st']==True, 'st'].to_frame()
        st_missing.reset_index(inplace=True)
        st_missing.rename(columns={'time': 'start_time'},inplace=True)
        # Clean end missing time Series
        end_missing = missing_records.loc[missing_records['end']==True, 'end'].to_frame()
        end_missing.reset_index(inplace=True)
        end_missing.rename(columns={'time': 'end_time'},inplace=True)
        # Combine new Datafarame with start and end missing times
        missing = pd.concat([st_missing['start_time'], end_missing['end_time']], axis=1)
        missing['missing_time'] = (missing['end_time'] - missing['start_time'])
        # Save the nissing data file to the folder where the script is runing at
        missing_records_file = os.path.join(os.getcwd(), st + '_missing_data.csv')
        try:
            logger.info('Saving missing data records for station {0} to {1}'.format(st, missing_records_file))
            missing.to_csv(missing_records_file)
        except PermissionError as e:
                logger.warning('Failed to save file for station: {0}, {1}'.format(st, e))
        

if __name__ == '__main__':
    
    st_names = ['CA42R', 'CAABR', 'CABTP', 'CACMP', 'CACWP', 'CAEGE', 'CAKDL', 'CALVP', 'CAMCP', 'CAMLP', 'CAMON', 'CAVLP', 'CAWCP', 'LIZP', 'CARIC', 'CACBP']
    check_for_missing_data(st_names)