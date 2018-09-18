# -*- coding: utf-8 -*-
"""
Created on Wed Sep 12 11:19:46 2018

@author: Ron Simenhois
"""

from sshtunnel import SSHTunnelForwarder
try:
    import mysql.connector as db
except ImportError:
    import pymysql as db
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='[%(asctime)s %(levelname)s] (%(threadName)-9s) %(message)s',)
logger = logging.getLogger(__name__)


def caic_mysql_query(q):
    '''
    This fuctrion quary from an SQL database over a ssh tunnel into a pandas Dataframe
        
    arguments:
        q - SQL query
    return:
        df - Pandas Dataframe with the data
    '''


    # ssh variables
    host = '' # insert host IP
    localhost = '127.0.0.1'
    ssh_username = ''
    ssh_password = ''

    # database variables
    user=''
    password=''
    database=''


    with SSHTunnelForwarder(
        (host, 22),
        ssh_username=ssh_username,
        ssh_password=ssh_password,
        remote_bind_address=(localhost, 3306)
    ) as server:
        conn = db.connect(host=localhost,
                               port=server.local_bind_port,
                               user=user,
                               passwd=password,
                               db=database)

        df = pd.read_sql_query(q, conn)

    return df
