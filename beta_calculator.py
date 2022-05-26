# -*- coding: utf-8 -*-
"""
@author: Shruti Mulage

Beta calculation with respect to S&P 500 index (SPY US Equity) 

"""
import pandas as pd
import numpy as np
import datetime as dt
from scipy.stats.mstats import winsorize

def calc_beta_spy(sfile, asofdate, win, freq):
    print('\n**** Processing (asofdate, win, freq): %s , %s, %d'%(asofdate,win,freq))
    #---------------------------------------------------------
    # determine interval dates and extract prices
    sdata=pd.read_excel(sfile)
    sdata.index=sdata.date
    sdata=sdata.drop('date',axis=1)
    startdate=asofdate-dt.timedelta(days=win)
    interval=dt.timedelta(days=freq)
    fdates=[]
    while startdate<=asofdate:
        while not (sdata.index==str(startdate)).any():
            if np.is_busday(startdate): # ignore weekends
                print('%s is unavailable in stock data. Shift forward 1d.'%str(startdate))
            startdate+=dt.timedelta(days=1)
        fdates.append(str(startdate))
        startdate += interval
    sdata=sdata.loc[sdata.index.isin(fdates)]
    print('No. of dates included: %d'%sdata.shape[0])

    #---------------------------------------------------------
    # impute outliers (stats stored for further analysis)
    bef_win=sdata.describe()
    for tk in sdata.columns:
        sdata[tk]=winsorize(sdata[tk], limits=[0.05, 0.05])
    aft_win=sdata.describe()
    
    bef_win.to_csv('./before_winsorize.csv',sep=',')
    aft_win.to_csv('./after_winsorize.csv',sep=',')

    #---------------------------------------------------------
    # calculate beta of each stock against the S&P 500 index
    sdata_log=np.log(sdata/sdata.shift())
    sdata_cov=sdata_log.cov()
    spy_var=sdata_cov.loc['SPY US Equity','SPY US Equity']
    sdata_cov['beta']=sdata_cov['SPY US Equity']/spy_var

    return sdata_cov['beta']


