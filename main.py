# -*- coding: utf-8 -*-
"""
Created on Fri May 17 18:07:12 2019

@author: GAS01409
"""

import os
import pandas as pd
# import statsmodels.formula.api as smf

from timer import timer
from query_premises import get_cd, get_cd_by_premise
from query_weather import read_weather_csv

STATIC = 'static'
factors = pd.read_csv(os.path.join(STATIC,'true_up_factors.csv'))
YEARS_DIR = os.path.join(STATIC, 'years')
YEARS_BY_POOL_DIR = os.path.join(YEARS_DIR, 'by_pool')

def step1_sf(df, year):
    """ Summer Base Load
        returns tuple of daily summer base load and flag of how calculated"""
    this_summer = ['{}07'.format(year-1), '{}08'.format(year-1)]
    summer_months = df[df.readmonth.isin(this_summer)]
    filtered = summer_months[(summer_months.CycleDays >15)]
    if len(filtered) > 0:
        daily = round(float(filtered.ccf.sum()) / float(filtered.CycleDays.sum()),3)
        return (daily, 0)
    else:
        last_summer = ['{}07'.format(year-2), '{}08'.format(year-2)]
        summer_months = df[df.readmonth.isin(last_summer)]
        filtered = summer_months[(summer_months.CycleDays >15)]
        if len(filtered) > 0:
            daily = round(float(filtered.ccf.sum()) / float(filtered.CycleDays.sum()),3)
            return (daily, 1)
        else:
            return (None, -1)

def step2_sf(df, year, poultry=False):
    """ Monthly Base Load for Coldest Winter Month """
    this_year_beg = pd.Timestamp(year=year-1, month=10, day=31)
    this_year_end = pd.Timestamp(year=year, month=4, day=1)

    df = df.reset_index()
    winter_months = df[(df.end_date > this_year_beg) & (df.end_date < this_year_end)]
    filtered = winter_months[(winter_months.CycleDays >15)]
    if len(filtered) > 0:
        if poultry:
            row = filtered.mean().round(0)
            row['Pool'] = df.delivery_group[0]
        elif not poultry:
            row = filtered.loc[filtered.hdd_per_day.idxmax()]
        return (row, 0)
    else:
        last_year_beg = pd.Timestamp(year=year-2, month=10, day=31)
        last_year_end = pd.Timestamp(year=year-1, month=4, day=1)
        winter_months = df[(df.end_date > last_year_beg) & (df.end_date < last_year_end)]
        filtered = winter_months[(winter_months.CycleDays >15)]
        if len(filtered) > 0:
            if poultry:
                row = filtered.mean().round(0)
            elif not poultry:
                row = filtered.loc[filtered.hdd_per_day.idxmax()]
            row['Pool'] = df.delivery_group[0]
            return (row, 1)
        else:
            return (None, -1)

def designated_day_load(prem, summer, winter, poultry=False):
    global factors
    summer_base_load = summer
    coldest_winter_month = winter
    pool = None
    if (summer_base_load[1] == -1) or (coldest_winter_month[1] == -1):
        designated_day_load = None
        #designated_day_load = (df.DDmcf.max() / factors[str(year-1)][list(df.Pool)[0]]).round(3)
    else:
        winter_base_load = coldest_winter_month[0]['CycleDays'] * summer_base_load[0]
        '''step3'''
        heat_sensitivity = coldest_winter_month[0]['ccf'] - winter_base_load
        '''step4'''
        heat_sensitivity_factor = round(heat_sensitivity / coldest_winter_month[0]['HDD'], 3)
        '''step5'''
        if heat_sensitivity_factor < 0:
            load_response = 0
        else:
            pool = coldest_winter_month[0]['delivery_group']
            designday = factors[factors.pool==pool].designday.tolist()[0]
            load_response = heat_sensitivity_factor * designday
        '''step6+7'''
        designated_day_load = round((summer_base_load[0] + load_response)/10, 3)
    '''step8'''
    #true up w/o bucketing
    '''step9'''
    #bucket
    return (designated_day_load, summer_base_load[1], coldest_winter_month[1], prem, pool)

@timer
def assign_hdd_to_reads(df):
    """this should be re-written to an apply function or vectorized"""
    df['HDD'] = list(hdd['HDD65'][(hdd.index >= df.begin_date.iloc[x]) & (hdd.index <= df.end_date.iloc[x])].sum() for x in range(len(df)))
    df['hdd_per_day'] = df.HDD / df.CycleDays
    return df

@timer
def calculate_summer_base(gb, year):
    summer_base_load = gb.apply(lambda x: step1_sf(x, year))
    return summer_base_load

@timer
def find_coldest_month(gb, year):
    coldest_winter_month = gb.apply(lambda x: step2_sf(x, year))
    return coldest_winter_month

@timer
def calculate_designated_day_load(summer, winter):
    ddl = []
    for i in range(len(summer)):
        ddl.extend([designated_day_load(winter.index[i],
                                        summer.iloc[i],
                                        winter.iloc[i],
                                        None)
                   ])
    return ddl

@timer
def save(df, year):
    df.to_csv(os.path.join(YEARS_DIR,'%s.csv'%year), index=False)
    by_pool = df.groupby('pool').ddl.sum()
    by_pool.to_csv(os.path.join(YEARS_BY_POOL_DIR, '%s.csv'%year))

@timer
def estimate_factor(year):
    trail_three = factors[(factors.year<year)&(factors.year>=year-3)]
    estimate = trail_three.groupby('pool').factor.mean()    
    return estimate

@timer
def trueup(df, estimate=False):
    factor = factors[(factors.year==df.year[0])]
    if estimate:
        factor = factor[['pool', 'estimate']]
        df = df.merge(factor, how='left', on='pool')
        df['trued'] = df.ddl*df.estimate
    else:
        factor = factor[['pool', 'factor']]
        df = df.merge(factor, how='left', on='pool')
        df['trued'] = df.ddl*df.factor
    return df

def bucket(x):
    from bisect import bisect
    upper_bound = [0.099, 0.199, 0.299, 0.399, 0.499, 0.599, 0.699, 0.799, 0.899, 0.999, 1.099, 1.199, 1.299, 1.399, 1.499, 1.599, 1.699, 1.799, 1.899, 1.999, 2.099, 2.199, 2.299, 2.399, 2.499, 2.599, 2.699, 2.799, 2.899, 2.999, 3.499, 3.999, 4.499, 4.999, 5.499, 5.999, 6.499, 6.999, 7.499, 7.999, 8.499, 8.999, 9.499, 9.999, 11.999, 13.999, 17.999, 21.999, 27.999, 37.999, 59.999]
    buckets = [0.023, 0.15, 0.249, 0.349, 0.450, 0.549, 0.649, 0.749, 0.848, 0.949, 1.049, 1.149, 1.248, 1.348, 1.448, 1.549, 1.648, 1.749, 1.849, 1.950, 2.049, 2.147, 2.25, 2.349, 2.448, 2.55, 2.651, 2.749, 2.849, 2.95, 3.235, 3.738, 4.243, 4.744, 5.243, 5.743, 6.240, 6.738, 7.244, 7.745, 8.247, 8.742, 9.248, 9.735, 10.951, 12.964, 15.844, 19.882, 24.743, 32.399, 47.112]
    
    index = bisect(upper_bound, x)
    if index == len(buckets):
        return round(x*1.025, 3)
    else:
        return round(buckets[index]*1.025, 3)

@timer
def apply_buckets(df):
    df['bucketed'] = df.trued.apply(bucket)
    return df

def run_with_ddl(year, estimate=True):
    df = pd.read_csv(os.path.join(YEARS_DIR, '%s.csv'%year))
    #df = df.dropna()
    df = trueup(df, estimate=estimate)
    df = apply_buckets(df)
    return df

### start here for bucketed
result = run_with_ddl(2019)
oldyear = run_with_ddl(2018, estimate=False)
oldoldyear = run_with_ddl(2017, estimate=False)

oldyear = oldyear.merge(oldoldyear, how='left', on='prem', suffixes=('_18', '_17'))
del oldoldyear
result = result.merge(oldyear, how='left', on='prem')
del oldyear

result['variation'] = result[['ddl','ddl_18','ddl_17']].var(axis=1)
#result[result.variation >=  result.variation.quantile(.997)].to_csv('t.csv')

def read_years_by_pool():
    df = pd.DataFrame(columns=['pool', 'ddl', 'year'])
    for file in os.listdir(YEARS_BY_POOL_DIR):
        filepath = os.path.join(YEARS_BY_POOL_DIR, file)
        temp = pd.read_csv(filepath, header=None)
        temp = temp.rename(columns={0:'pool', 1:'ddl'})
        temp['year'] = file.split('.')[0]
        df = df.append(temp)
    df = df.reset_index(drop=True)
    df = df.astype({'year': int,})
    return df

# =============================================================================
# Start of the DDL calculation
# =============================================================================
assert 0
hdd = read_weather_csv()
columns=['ddl','summerflag','winterflag','prem','pool']
#year = 2019
for year in range(2019, 2013, -1):
    """
    read_weather_csv time: 0.05sec
    current_cd time: 1066.78sec (as high as 5922.04sec via VPN)
    clean time: 338.66sec
    assign_hdd_to_reads time: 15755.53sec
    calculate_summer_base time: 2717.62sec
    find_coldest_month time: 5853.55sec
    calculate_designated_day_load time: 1029.51sec
    save time: 9.19sec
    ### 26,767sec <=> 7hrs 26mins
    """
    df = get_cd(year) #df = get_cd_by_premise(year, 12245327)
    df = assign_hdd_to_reads(df)
    gb = df.groupby('agl_premise_number')
    del df
    summer_base_load = calculate_summer_base(gb, year)
    coldest_winter_month = find_coldest_month(gb, year)
    del gb
    data = calculate_designated_day_load(summer_base_load, coldest_winter_month)
    del summer_base_load, coldest_winter_month
    ddl = pd.DataFrame(data=data, columns=columns)
    ddl['year'] = year
    #save(ddl, year)
    del ddl
