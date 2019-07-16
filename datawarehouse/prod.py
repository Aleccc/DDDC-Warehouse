# -*- coding: utf-8 -*-
"""
Created on Sat May 18 09:54:05 2019

@author: GAS01409
"""

from datawarehouse.common import Connection


DRIVER = '{SQL Server}'
SERVER = 'GSDWPROD.gassouth.com'
DATABASE = 'GSDW'

db = Connection(DRIVER, SERVER, DATABASE)

if __name__ == "__main__":
    sql = ("SELECT top 12 * "
           "FROM VW_NYMEXStrips "
           "WHERE monthorder = 1 "
           "ORDER By month DESC"
           )
    df = db.query(sql)
    db.close()
