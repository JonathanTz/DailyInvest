from bs4 import BeautifulSoup
import concurrent.futures
import requests
import time
import pandas as pd
import json
from random import randint
from os import listdir
from datetime import datetime


df_stkInfo = pd.read_csv('df_stkInfo.csv')
dtNow = datetime.now()
dtNow = int(dtNow.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
dtStart = datetime(2016, 1, 1)
dtStart = int(dtStart.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())

for item in df_stkInfo.ticker.values:
    df_stkInfo = pd.DataFrame(columns=['dtT', 'pxOpen', 'pxHigh', 'pxLow', 'pxClose'])
    urls = f"https://ws.api.cnyes.com/ws/api/v1/charting/history?symbol=TWS:{item}:STOCK&resolution=D&quote=1&from={dtNow}&to={dtStart}"
    
    res = requests.get(urls)
    data = res.text
    jFile = json.loads(data)
    df_stkInfo=pd.DataFrame({'dtT':[datetime.fromtimestamp(item) for item in jFile['data']['t']], 'pxOpen':jFile['data']['o'], 'pxHigh':jFile['data']['h'], 'pxLow':jFile['data']['l'], 'pxClose':jFile['data']['c']})
    df_stkInfo['dtT'] = pd.to_datetime(df_stkInfo['dtT'].dt.date)
    df_sub = df_stkInfo[(df_stkInfo.dtT<'2016-01-11') & (df_stkInfo.dtT > '2016-01-01')]
    profit = (df_sub['pxClose'].iloc[0]/df_sub['pxClose'].iloc[-1])-1
    print(df_stkInfo)