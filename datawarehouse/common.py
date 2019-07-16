# -*- coding: utf-8 -*-
"""
Created on Sat May 18 09:57:32 2019

@author: GAS01409
"""

import pypyodbc
import pandas as pd


class Connection:
    def __init__(self, driver, server, database):
        self.db = None
        self.credentials = {'driver':driver,
                            'server':server,
                            'database':database}
        self.connect()

    def connect(self):
        self.attempt(**self.credentials)

    def attempt(self, driver, server, database):
        self.db = pypyodbc.connect('Driver={};'
                                   'Server={};'
                                   'Database={};'.format(driver, server, database))

    def close(self):
        self.db.close()

    def query(self, sql, *args):
        with self.db.cursor() as cursor:
            cursor.execute(sql, args)
            query_results = [dict(zip([column[0] for column in cursor.description], row)) for row in cursor.fetchall()]
        df = pd.DataFrame(query_results)
        return df


'''
def check_connection(func):
    def wrapper(*args, **kwargs):
        for attempt in range(10):
            try:
                return func(*args, **kwargs)
            except pypyodbc.ProgrammingError:
                self.connect()
    return wrapper
'''