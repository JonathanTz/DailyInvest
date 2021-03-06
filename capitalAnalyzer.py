import sqlite3
import pandas as pd
import math
import requests
from datetime import datetime, timedelta
import json
import numpy as np
import matplotlib.pyplot as plt

dir = "C:\\Users\\Dell\\Documents\\Visual Studio 2015\\Projects\\FinPortfolio\\FinPortfolio\\bin\\Debug\\"
conn = sqlite3.connect(dir+"FP.db")
def getStkPrice(item,dtStart,dtEnd):
   item = (f"{item}:STOCK") if item != 'TWSE' else (f"{item}:INDEX")
   urls = f"https://ws.api.cnyes.com/ws/api/v1/charting/history?symbol=TWS:{item}&resolution=D&quote=1&from={dtEnd}&to={dtStart}" 
   res = requests.get(urls)
   data = res.text
   jFile = json.loads(data)
   df_input=pd.DataFrame({'idTicker': [item for i in jFile['data']['t']], 'dtT':[datetime.fromtimestamp(item) for item in jFile['data']['t']], 'pxOpen':jFile['data']['o'], 'pxHigh':jFile['data']['h'], 'pxLow':jFile['data']['l'], 'pxClose':jFile['data']['c']})
   df_input['dtT'] = pd.to_datetime(df_input['dtT'].dt.date)
   return df_input.sort_values(by=['dtT'])
def calPortfolioRisk(df_input, n = 10):
   dtEnd = datetime.now()+ timedelta(days=1)
   dtStart = dtEnd - timedelta(days=n)
   dtEnd = int(dtEnd.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
   dtStart = int(dtStart.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
   df_idx = getStkPrice("TSE01",dtStart,dtEnd)
   df_latPortfolio = pd.DataFrame()
   for item in df_input.idTicker.values:
      #df_input = pd.DataFrame(columns=['dtT','idTicker', 'pxOpen', 'pxHigh', 'pxLow', 'pxClose'])
      df_px = getStkPrice(item,dtStart,dtEnd)
      qtyHld = df_input[df_input['idTicker']==item].qtyHld.values[0]
      valBeta = np.corrcoef(df_px.pxClose.values,df_idx.pxClose.values)
      if len(df_latPortfolio)== 0:
         df_latPortfolio=pd.DataFrame({'dtT':df_px['dtT'].values,'amtHld':df_px.pxClose*qtyHld})
      else:
         df_latPortfolio.amtHld = df_latPortfolio.amtHld + df_px.pxClose*qtyHld
   df_latPortfolio = df_latPortfolio.reset_index(drop=True) 
   df_idx = df_idx.reset_index(drop=True) 
   df_latPortfolio['ret'] = ((df_latPortfolio.amtHld-df_latPortfolio.shift(1).amtHld)/df_latPortfolio.shift(1).amtHld)
   df_idx['ret'] = ((df_idx.pxClose-df_idx.shift(1).pxClose)/df_idx.shift(1).pxClose)
   #??????beta --- cor/(p.std/m.std)
   valBeta = np.corrcoef(df_latPortfolio.ret[1:],df_idx.ret[1:])[0][1] /(df_latPortfolio.ret[1:].std()/df_idx.ret[1:].std())
   #?????????????????????
   valVol = df_latPortfolio.ret[1:].values.std()
   #???????????????
   valRet = (df_latPortfolio.amtHld.values[-1]/df_latPortfolio.amtHld.values[0])-1
   #?????????????????????
   valIdxRet = (df_idx.pxClose.values[-1]/df_idx.pxClose.values[0])-1
   print(f"??????beta{valBeta} \n -?????????????????????{valVol}\n -???????????????{valRet}\n -?????????????????????{valIdxRet}")
   rtp = df_latPortfolio.amtHld/df_latPortfolio.amtHld[0]
   rtm = df_idx.pxClose/df_idx.pxClose[0]
   p1, = plt.plot(df_latPortfolio.dtT, rtp)
   p2, = plt.plot(df_latPortfolio.dtT, rtm)
   lg = plt.legend([p1,p2],[u'change of Portfolio',u'change of TWSE'], loc ='upper left')
   plt.show()
   return df_latPortfolio


c = conn.cursor()
cursor = c.execute("SELECT * FROM hldList WHERE dtHld=(SELECT MAX(dtHld) FROM hldList)")
df_portfolio = pd.DataFrame(columns=['dtHld', 'idTicker', 'nameCH','amtCost','qtyHld','amtMkt','urcg','percentage'])
for row in cursor:
   #print("?????????{0} ??????{1} {2}, ??????{3}/??????{4} ".format(row[0],row[1],row[2],row[3],row[5]))
   df_portfolio = df_portfolio.append({'dtHld': row[0],'idTicker': row[1], 'nameCH': row[2], 'amtCost': row[3], 'qtyHld': row[4], 'amtMkt': row[5], 'urcg': row[6]}, ignore_index=True)

##????????????
# df_portfolio.loc[df_portfolio['idTicker']=='6274','qtyHld']=3000
# df_portfolio.loc[df_portfolio['idTicker']=='6274','amtMkt']=100
##
calPortfolioRisk(df_portfolio,220)
df_portfolio['percentage'] = (df_portfolio['amtMkt'].values*df_portfolio['qtyHld'].values)
df_portfolio['percentage'] = df_portfolio['percentage']/df_portfolio['percentage'].values.sum()*100
df_portfolio = df_portfolio.round({"percentage":3})

rtnGain = round(((df_portfolio['amtMkt']*df_portfolio['qtyHld']).values.sum()/(df_portfolio['amtCost']*df_portfolio['qtyHld']).values.sum()-1),4)
amtGain = round(((df_portfolio['amtMkt']*df_portfolio['qtyHld']).values.sum()-(df_portfolio['amtCost']*df_portfolio['qtyHld']).values.sum()),2)
print("??????????????????{0}, ?????????????????????{1}, ?????????{2}".format(rtnGain,amtGain,(df_portfolio['amtMkt']*df_portfolio['qtyHld']).values.sum()))
print(df_portfolio)
