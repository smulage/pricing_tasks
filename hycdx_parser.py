# -*- coding: utf-8 -*-
"""
@author: Shruti Mulage

Parse varied text files to extract HYCDX option prices

"""

import regex as re
from datetime import datetime
import dateutil.parser as parser
import pandas as pd
import numpy as np
from collections import Counter

# dictionary listing any column names that have spaces
cspaces={'vol chg':'volchg'
        ,'vol bpd':'volbpd'
        ,'prc vol':'prcvol'}

# for each of the final table's numeric columns 
# list the possible column names in the incoming files
cdict={'Put': ['pay','puts']
     , 'Call': ['rcv','rec','calls']
     , 'StrikePx': ['stk','k']
     , 'StrikeSpd': ['sprd']
     , 'Delta': ['delta','dlt','del']
     , 'ImpliedVolSpd': ['vol','sprdvol']
     , 'ImpliedVolBps': ['volbpd','b/e']
     , 'ImpliedVolPx': ['prcvol']}

# final table column order
fin_cols=['Date','Time','Firm','Expiration','OptionTyp','StrikePx','StrikeSpd'
         ,'BidPrice','AskPrice','Delta','ImpliedVolSpd','ImpliedVolBps'
         ,'ImpliedVolPx','RefPx']

#-----------------------------------------------------
# function to process any duplicate header names
#   apply rcv/pay tags to any duplicate names
#   if the call section is first in the file, then the first duplicate is rcv_xxx
def dup_headers(hline):
    hcols=hline.split('|')
    hctr=Counter(hcols)
    if len(hctr)<len(hcols):  # duplicate header names found
        cidx=[hcols.index(c) for c in cdict['Call'] if c in hcols][0]
        pidx=[hcols.index(p) for p in cdict['Put'] if p in hcols][0]
        for (k,v) in [(k,v) for k,v in hctr.items() if v==2]:
            hidx=[i for i,h in enumerate(hcols) if h==k]
            hcols[hidx[0]]='rcv_'+hcols[hidx[0]] \
                if cidx<pidx else 'pay_'+hcols[hidx[0]]
            hcols[hidx[1]]='rcv_'+hcols[hidx[1]] \
                if cidx>pidx else 'pay_'+hcols[hidx[1]]
    hline='|'.join(hcols)
    return hline

def run_parser(ifname):
    #-----------------------------------------------------
    # initialize variables
    pfname='./price_table.txt'
    prcfile=open(pfname,'w')  # pricing tables for each file w all expiry dates
    
    print('\n**** Parsing: %s'%(ifname))
    fdict={}     # file specifics (Date, Time, Firm, Ref)
    exp_date=''  # expiry date
    ncols=0      # no. of columns
    cp_delim=[]  # holds delimiters from file headers that need Bid/Ask formatting
    
    for fl in open(ifname,'r',encoding='utf-8'):
        #-----------------------------------------------------
        # extract details from header of file
        fl_list=re.split(' ', fl)
        if re.search('^ *From: ',fl):
            fdict['from']=fl_list[fl_list.index('From:')+1]
            fdict['date']=fl_list[fl_list.index('At:')+1]
            fdict['time']=fl_list[fl_list.index('At:')+2]
            continue # skip further processing on this line
        if re.search('^ *To: ',fl): continue
        if re.search(' *ref:* *',fl.lower()):
            fdict['ref']=[fl_list[i+1] for i,fll in enumerate(fl_list) if 'ref' in fll.lower()][0]
            fdict['ref']=re.sub('\D+$','',fdict['ref'])
        if re.search('^ *Subject: ',fl): continue
    
        # format untouched header prior to stripping
        for k,v in cspaces.items(): fl=re.sub(k,v,fl.lower()).strip()  # remove spaces in column names
        fl=re.sub('\s?\|\s?', '|', re.sub('\s+',' ',fl)) # shrink spacing
        if fl=='': continue  # skip blank lines
        
        #-----------------------------------------------------
        # extract and structure price tables by expiry date
        exp=re.search('[0-9][0-9]-?[a-z][a-z][a-z]-?[0-9][0-9][0-9]?[0-9]?',fl)
        if exp: 
            exp_date=fl[exp.start():exp.end()]
            exp_date=datetime.strftime(parser.parse(exp_date), '%d-%b-%y')
            continue  # skip further processing on the expiry date line
        if exp_date=='': continue # skip processing until an expiry date is found
        if ncols==0 and re.search('>',fl):  # if header has a put/call '>' delim
            cp_delim=re.findall(' +|>',fl)  # catch all delimiter locations
        if cp_delim:   # find location of > delim and substitute with / on every line
            ctr=-1
            for d in cp_delim:
                if d=='>':
                    ctr=re.search('>| ',fl[ctr+1:]).start()+ctr+1
                    fl=fl[:ctr]+'/'+fl[ctr+1:]
                else:
                    ctr=re.search(d,fl[ctr+1:]).start()+ctr+1
        fl=re.sub('[a-z]{3}[0-9]{2}/','',fl)  # remove any date specifics (found in header eg: DEC21>PAY)
        fl=re.sub('\s+/\s+','/',re.sub('[\[\]~%]','',fl))  # remove unwanted characters
        fl=re.sub('\s+','|',fl)  # apply the delimiter
        fl=re.sub('-*/-*','/',re.sub('-+\|','|',fl)) # standardize null values
        if ncols==0: 
            ncols=len(re.findall('\|',fl))  # no. of columns in header
            hline='exp_date|'+fl
            hline_prc=dup_headers(hline)   # process any duplicates
            prcfile.write(hline_prc+'\n')  # write the header once
        if ncols!=len(re.findall('\|',fl)): # catch any subsequent lines that do not match header length
            print('  *** skipping unformatted line:\n%s\n'%fl)
            continue  # skip unformatted lines
        if hline!='exp_date|'+fl:    # skip subsequent header lines
            prcfile.write(exp_date+'|'+fl+'\n')
    prcfile.close()
    print(fdict)
    
    #-----------------------------------------------------
    # load price table into call/put dataframes for further processing
    prcdf=pd.read_csv(pfname, sep='|')
    calldf=pd.DataFrame(data=['C']*len(prcdf),columns=['OptionTyp'],dtype='str')
    putdf= pd.DataFrame(data=['P']*len(prcdf),columns=['OptionTyp'],dtype='str')
    
    calldf['Expiration']=prcdf.exp_date
    putdf['Expiration'] =prcdf.exp_date
    # for each final table column, find the file specific column name
    # and load into respective option table
    for k,v in [(k,v) for k,v in cdict.items() if k not in ['Put','Call']]:
        cname=[x for x in prcdf.columns 
                  if x in cdict[k]
                  or x in ['rcv_'+y for y in cdict[k]]]
        if len(cname): calldf[k]=prcdf[cname[0]].replace('',np.NaN).astype('float32')
        else: calldf[k]=np.NaN
        cname=[x for x in prcdf.columns 
                  if x in cdict[k]
                  or x in ['pay_'+y for y in cdict[k]]]
        if len(cname): putdf[k]=prcdf[cname[0]].replace('',np.NaN).astype('float32')
        else: putdf[k]=np.NaN
    
    # extract the Bid/Ask prices for each option
    cname=[x for x in prcdf.columns if x in cdict['Call']]
    calldf['BidPrice']=prcdf[cname[0]].str.extract('(.*)/.*').replace('',np.NaN).astype('float32')
    calldf['AskPrice']=prcdf[cname[0]].str.extract('.*/(.*)').replace('',np.NaN).astype('float32')
    cname=[x for x in prcdf.columns if x in cdict['Put']]
    putdf['BidPrice']=prcdf[cname[0]].str.extract('(.*)/.*').replace('',np.NaN).astype('float32')
    putdf['AskPrice']=prcdf[cname[0]].str.extract('.*/(.*)').replace('',np.NaN).astype('float32')
    if ((putdf.BidPrice>=putdf.StrikePx).any()): # Bid/Ask reported in cents
        calldf[['BidPrice','AskPrice']]=calldf[['BidPrice','AskPrice']]/100  # convert to $
        putdf[['BidPrice','AskPrice']]=putdf[['BidPrice','AskPrice']]/100
        
    
    #-----------------------------------------------------
    # merge call/put records and add file specific details
    calldf['idx']=calldf.index
    putdf['idx']=putdf.index
    final_df=calldf.append(putdf)
    final_df['Date']=fdict['date']
    final_df['Time']=fdict['time']
    final_df['Firm']=fdict['from']
    final_df['RefPx']=fdict['ref']
    final_df=final_df.sort_values(['idx','OptionTyp'],ascending=[True,False])
    final_df=final_df[fin_cols]
    
    print('Records extracted: %d\n'%final_df.shape[0])
    return final_df   
    #-----------------------------------------------------
