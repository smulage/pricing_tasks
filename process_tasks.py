# -*- coding: utf-8 -*-
"""
@author: Shruti Mulage

# ---------------------------------------------------
#  TASK 1: Extract HYCDX option quotes

Assumptions: 
    - any date after the header section is assumed to be an expiry date
    - Bid/Ask is understood to be reported in cents 
      if any of the put's BidPrice>=StrikePrice
"""
print('\n ==================================================')
print(' TASK 1 - Extract HYCDX option quotes')

import pandas as pd
from hycdx_parser import run_parser  # pre-built function

nfiles=4
ftag='hycdx_option_quotes'
outfile='./parsed_'+ftag+'.csv'
outdf=pd.DataFrame()
for i in range(1,nfiles+1):
    outdf=outdf.append(run_parser('./'+ftag+'_'+str(i)+'.txt'))

print('TASK 1 - Output file:%s'%(outfile))
print('Total records:%d'%outdf.shape[0])
outdf.to_csv(outfile,sep=',',index=False)


"""
# ---------------------------------------------------
#  TASK 2: calculate beta against SPY US Equity

Assumptions/Risks/Open items:
    - assume time intervals are (year:365d,quarter:90d,month:30d,bi-weekly:15d)
    - every date captured in the 365d interval from the asofdate should exist in the stock data
      So, if 01-May does not exist (weekend/holiday), then move forward to the next
      available date(03-May), and then jump forward 7 days (if weekly interval) from that date(10-May).
      Hence, for a (win=1yr,freq=weekly) scenario, 52 datapoints should be captured.
"""
print('\n ==================================================')
print(' TASK 2: Calculate beta against SPY US Equity')

import datetime as dt
import pandas as pd
from beta_calculator import calc_beta_spy

sfile='./task2_stock_data.xlsx'
beta_outfile='./beta_spy.csv'
fdict={ 'daily': 1,'weekly': 7,'biweekly': 15
       ,'monthly': 30,'quarterly': 90}

# initialize variables
beta_args=[ [dt.date(2021, 10, 31),365,'daily']
           ,[dt.date(2021, 10, 31),365,'weekly']]

beta_df=pd.DataFrame()
for asofdate,win,freq in beta_args:
    tdf=calc_beta_spy(sfile, asofdate, win, fdict[freq])
    tdf=tdf.rename(str(asofdate)+'|'+str(win)+'|'+freq)
    beta_df=beta_df.append(tdf)

print('\nTASK 2 - Output file:%s'%(beta_outfile))
beta_df.to_csv(beta_outfile)


"""
# ---------------------------------------------------
"""
