# -*- coding: utf-8 -*-
"""
Created on Fri May 17 11:53:00 2019

@author: GAS01409
"""

from timer import timer
from datawarehouse.common import pd
from datawarehouse.stg import db as dw_stg


# =============================================================================
# select all from a specific year's cd
# =============================================================================
@timer
def get_cd(year, return_clean=True):
    dw_stg.connect()
    sql = ("select * "
           "from FM_MM_AGL_CD_{year} "
           "where sourcefilename='AGL_CD_{year}04' "
           # "and delivery_group=? "
           # "and consumption_at_meter_12_ccf > 100 "
           # "ORDER BY newid() "
           .format(year=year)
           )
    df = dw_stg.query(sql)
    dw_stg.close()
    if return_clean:
        df = clean(df)
    return df


# =============================================================================
# select a iterable of premise numbers from a cd
# =============================================================================
@timer
def get_cd_by_premise(year, premises, return_clean=True):
    if not isinstance(premises, (list, tuple,)):
        premises = [premises]

    placeholders = ','.join('?' * len(premises))
    sql = ("select * "
           "from FM_MM_AGL_CD_{year} "
           "where sourcefilename='AGL_CD_{year}04' "
           "and agl_premise_number IN ({placeholders}) "
           .format(year=year, placeholders=placeholders)
           )
    para = []
    para.extend(premises)

    dw_stg.connect()
    with dw_stg.db.cursor() as cursor:
        cursor.execute(sql, para)
        query_results = [dict(zip([column[0] for column in cursor.description], row)) for row in cursor.fetchall()]
    dw_stg.close()

    df = pd.DataFrame(query_results)
    if return_clean:
        df = clean(df)
    return df


@timer
def clean(df_query):
    # =========================================================================
    # unpivot meter reads
    # =========================================================================
    cols_to_always_keep = ['agl_account_number',
                           'agl_premise_number',
                           'customer_type_and_rate',
                           'delivery_group',
                           'design_day_usage_dathm',
                           'design_day_usage_mcf', ]

    df = pd.DataFrame()
    for read in range(12):
        read += 1
        cols = {'consumption_at_meter_{}_ccf'.format(read): 'ccf',
                'consumption_month_{}'.format(read): 'readmonth',
                'meter_read_begin_date_{}'.format(read): 'begin_date',
                'meter_read_end_date_{}'.format(read): 'end_date', }
        df_read = df_query[list(cols.keys()) + cols_to_always_keep]
        df_read = df_read.rename(columns=cols)
        df = df.append(df_read)

    # =========================================================================
    # change data type for columns; add columns; remove na rows
    # =========================================================================
    df.ccf = pd.to_numeric(df.ccf)
    df.design_day_usage_dathm = pd.to_numeric(df.design_day_usage_dathm)
    df.design_day_usage_mcf = pd.to_numeric(df.design_day_usage_mcf)
    df.begin_date = pd.to_datetime(df.begin_date, format='%Y-%m-%d')
    df.end_date = pd.to_datetime(df.end_date, format='%Y-%m-%d')
    df = df.dropna()
    df['CycleDays'] = list(map(lambda x: int(x.days), list(df.end_date - df.begin_date)))
    return df


def main():
    df = get_cd(2019)
    return df


if __name__ == '__main__':
    df = main()
