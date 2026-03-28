import sqlite3
import pandas as pd
import math
import requests
from datetime import datetime, timedelta
import json
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as dates
import os
import crawler_secInfo

dir = "C:\\Users\\Dell\\Documents\\Visual Studio 2015\\Projects\\FinPortfolio\\FinPortfolio\\bin\\Debug\\"
dir = '/Users/jonathantz/Documents/Project/Database/'
conn = sqlite3.connect(dir +'FP.db')
conn.execute('ATTACH "'+ dir +'MDEngine.db" AS MDEngine')

def getStkPrice(item,dtStart,dtEnd):
   item = (f"{item}:STOCK") if item != 'TWSE' else (f"{item}:INDEX")
   urls = f"https://ws.api.cnyes.com/ws/api/v1/charting/history?symbol=TWS:{item}&resolution=D&quote=1&from={dtEnd}&to={dtStart}" 
   res = requests.get(urls)
   data = res.text
   jFile = json.loads(data)
   df_input=pd.DataFrame({'idTicker': [item for i in jFile['data']['t']], 'dtT':[datetime.fromtimestamp(item) for item in jFile['data']['t']], 'pxOpen':jFile['data']['o'], 'pxHigh':jFile['data']['h'], 'pxLow':jFile['data']['l'], 'pxClose':jFile['data']['c']})
   df_input['dtT'] = pd.to_datetime(df_input['dtT'].dt.date)
   return df_input.sort_values(by=['dtT'])
def covtMktToPF(con):
   print("--1--Start MktPrice Update Local")
   sql = "Insert into equityDetail \
          select distinct M.idTicker,M.nameTicker,P.pxClose,P.dtMkt from MDEngine.securityInfo M \
          inner join MDEngine.securityPrice P on M.seqsec=P.seqsec \
          inner join (select idTicker,Max(dtMkt) dtMax from equitydetail group by idTicker) G on M.idticker=G.idticker \
          where M.tpData='Y' and G.dtMax<P.dtMkt order by P.dtMkt desc"

   cursor = con.execute(sql)
   con.commit()
   print(cursor.lastrowid)
   print("--1--End MktPrice Update Local")

   print("--2--Start Porfolio Update Price")
   c = con.cursor()
   cursor = c.execute("SELECT MAX(dtHld) dtHldMax FROM hldList")
   dtHldMax= ""
   for row in cursor:
      dtHldMax=row[0]
   
   cursor = c.execute("SELECT MAX(dtMkt) dtMktMax FROM equityDetail")
   dtMktMax= ""
   for row in cursor:
      dtMktMax=row[0]
   
   
   if dtHldMax<dtMktMax :
      cursor = c.execute("SELECT E.dtMkt,H.idTicker,H.nameTicker,H.avgPxCost,H.qtyHld,E.pxClose,(E.pxClose-H.avgPxCost)*H.qtyHld as urcg,'1' \
                          FROM hldList H inner join equityDetail E on H.idticker=E.idTicker WHERE E.dtMkt > '" + dtHldMax +"' and H.dtHld='" + dtHldMax+"'\
                          order by E.idticker,E.dtMkt")
      df_Hld = pd.DataFrame(columns=['dtHld', 'idTicker', 'nameTicker','avgPxCost','qtyHld','pxMkt','urcg','seqPF'])
      for row in cursor:
         #print("庫存日{0} 股票{1} {2}, 成本{3}/市價{4} ".format(row[0],row[1],row[2],row[3],row[5]))
         dict = {'dtHld': row[0],'idTicker': row[1], 'nameTicker': row[2], 'avgPxCost': row[3], 'qtyHld': row[4], 'pxMkt': row[5], 'urcg': row[6], 'seqPF': row[7]}
         df_Hld = pd.concat([df_Hld,pd.DataFrame(dict,index=[0])])
         #df_Hld = df_Hld._append({'dtHld': row[0],'idTicker': row[1], 'nameTicker': row[2], 'avgPxCost': row[3], 'qtyHld': row[4], 'pxMkt': row[5], 'urcg': row[6], 'seqPF': row[7]}, ignore_index=True)
      df_Hld.to_sql('hldList', con=con, if_exists='append',index=False)

      print(cursor.lastrowid)
      print("--2--End Porfolio Update Price")

      print("--3--Start MktPrice Update Local")
      c = con.cursor()
      cursor = c.execute("select sum(qtyHld) qtyOutstanding from appuserInfo")
      qtyOutstanding= ""
      for row in cursor:
         qtyOutstanding=row[0]
      
      sql = "Insert into PFList_bak select P.seqPF,P.namePF,P.totalAmt,M.amtMkt,P.amtRcg,(P.totalAmt+M.amtMkt)/"+ str(qtyOutstanding) +",strftime('%Y-%m-%d %H:%M:%S' ,M.dtHld) \
             from pfList_bak P inner join (select dtHld,sum(pxMkt*qtyHld) amtMkt \
             from hldList group by dtHld ) M on strftime('%Y-%m-%d',P.dtUpdate)='"+ dtHldMax +"' and M.dtHld>'"+ dtHldMax +"'"
      cursor = conn.execute(sql)
      con.commit()
      print(cursor.lastrowid)
      print("--3--End MktPrice Update Local")

   #cursor = con.execute(sql)





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
      # valBeta = np.corrcoef(df_px.pxClose.values,df_idx.pxClose.values)
      #20220420 modify in market value update realtime
      df_input.loc[df_input.idTicker.values==item,'amtMkt'] = df_px.iloc[-1,:].pxClose
      df_input.loc[df_input.idTicker.values==item,'urcg'] = (df_input.loc[df_input.idTicker.values==item,'amtMkt']-df_input.loc[df_input.idTicker.values==item,'amtCost'])*df_input.loc[df_input.idTicker.values==item,'qtyHld']
      df_input.loc[df_input.idTicker.values==item,'dtHld'] =df_px.dtT[0].strftime('%Y-%m-%d')
      if len(df_latPortfolio)== 0:
         df_latPortfolio=pd.DataFrame({'dtT':df_px['dtT'].values,'amtHld':df_px.pxClose*qtyHld})
      else:
         df_latPortfolio.amtHld = df_latPortfolio.amtHld + df_px.pxClose*qtyHld
   df_latPortfolio = df_latPortfolio.reset_index(drop=True) 
   df_idx = df_idx.reset_index(drop=True) 
   df_latPortfolio['ret'] = ((df_latPortfolio.amtHld-df_latPortfolio.shift(1).amtHld)/df_latPortfolio.shift(1).amtHld)
   df_idx['ret'] = ((df_idx.pxClose-df_idx.shift(1).pxClose)/df_idx.shift(1).pxClose)
   #投組beta --- cor/(p.std/m.std)
   # valBeta = np.corrcoef(df_latPortfolio.ret[1:],df_idx.ret[1:])[0][1] /(df_latPortfolio.ret[1:].std()/df_idx.ret[1:].std())
   df_latPortfolio_ret = pd.concat([df_latPortfolio.set_index(['dtT'])['ret'][1:],df_idx.set_index(['dtT'])['ret'][1:]],axis=1).fillna(method='ffill')
   df_latPortfolio_amtHld = pd.concat([df_latPortfolio.set_index(['dtT'])['amtHld'],df_idx.set_index(['dtT'])['ret']],axis=1).fillna(method='ffill')
   valBeta = df_latPortfolio_ret.corr().iloc[0,1]/(df_latPortfolio.ret[1:].std()/df_idx.ret[1:].std())
   #投組報酬波動度
   valVol = df_latPortfolio_ret.values.std()
   #投組報酬率
   valRet = (df_latPortfolio_amtHld.amtHld.values[-1]/df_latPortfolio_amtHld.amtHld.values[0])-1
   #期間指數報酬率
   valIdxRet = (df_idx.pxClose.values[-1]/df_idx.pxClose.values[0])-1
   print(f"投組beta{valBeta} \n -投組報酬波動度{valVol}\n -投組報酬率{valRet}\n -期間指數報酬率{valIdxRet}")

   #plot
   (1+df_latPortfolio_ret).cumprod().plot()
   plt.legend(["Ret_Portfolio","Ret_Index"])
   
   #rtp = df_latPortfolio_amtHld.amtHld/df_latPortfolio_amtHld.amtHld[0]
   #rtm = df_idx.pxClose/df_idx.pxClose[0]
   #p1, = plt.plot(df_latPortfolio_amtHld.index, rtp)
   #p2, = plt.plot(df_latPortfolio_amtHld.index, rtm)
   #lg = plt.legend([p1,p2],[u'change of Portfolio',u'change of TWSE'], loc ='upper left')
   ##plt.show()
   # return df_latPortfolio
   return df_input
#update 市場資料庫股價至投組資料庫
covtMktToPF(conn)
c = conn.cursor()
cursor = c.execute("SELECT * FROM hldList WHERE dtHld=(SELECT MAX(dtHld) FROM hldList)")
df_portfolio = pd.DataFrame(columns=['dtHld', 'idTicker', 'nameCH','amtCost','qtyHld','amtMkt','urcg','profitRatio','percentage'])
for row in cursor:
   #print("庫存日{0} 股票{1} {2}, 成本{3}/市價{4} ".format(row[0],row[1],row[2],row[3],row[5]))
   dict = {'dtHld': row[0],'idTicker': row[1], 'nameCH': row[2], 'amtCost': row[3], 'qtyHld': row[4], 'amtMkt': row[5], 'urcg': row[6],'profitRatio':round(row[6]/(row[3]*row[4])*100,2),'percentage' :0}
   df_portfolio = pd.concat([df_portfolio,pd.DataFrame(dict,index=[0])])
   #df_portfolio = df_portfolio._append({'dtHld': row[0],'idTicker': row[1], 'nameCH': row[2], 'amtCost': row[3], 'qtyHld': row[4], 'amtMkt': row[5], 'urcg': row[6]}, ignore_index=True)

##模擬加碼
# df_portfolio.loc[df_portfolio['idTicker']=='6274','qtyHld']=3000
# df_portfolio.loc[df_portfolio['idTicker']=='6274','amtMkt']=100
##
a = calPortfolioRisk(df_portfolio,620)
df_portfolio['percentage'] = (df_portfolio['amtMkt'].values*df_portfolio['qtyHld'].values)
df_portfolio['percentage'] = df_portfolio['percentage']/df_portfolio['percentage'].values.sum()*100
df_portfolio = df_portfolio.round({"percentage":3})

rtnGain = round(((df_portfolio['amtMkt']*df_portfolio['qtyHld']).values.sum()/(df_portfolio['amtCost']*df_portfolio['qtyHld']).values.sum()-1),4)
amtGain = round(((df_portfolio['amtMkt']*df_portfolio['qtyHld']).values.sum()-(df_portfolio['amtCost']*df_portfolio['qtyHld']).values.sum()),2)
# 持股市值
totalMkt = (df_portfolio['amtMkt']*df_portfolio['qtyHld']).values.sum()
print("未實現損益率{0}, 未實驗損益金額{1}, 總市值{2}".format(rtnGain,amtGain,totalMkt))

cursor = c.execute("select totalAmt from pflist_bak where Date(dtUpdate)=(SELECT MAX(dtHld) FROM hldList);")
totalCash= 0
qtyHld= 0
for row in cursor:
   totalCash = row[0]
cursor = c.execute("select SUM(qtyHld) as qtyHld from appUserInfo;")
for row in cursor:
   qtyHld = row[0]

pfNav =(totalMkt+totalCash)/qtyHld

sql = "UPDATE appUserInfo SET mktValue = qtyHld * ?"
cursor.execute(sql, (pfNav,))
conn.commit()

print("投組淨值為{0}  ".format(round(pfNav,3)))
print(df_portfolio.sort_values(by=['urcg'],ascending=False))


###畫出歷史投組淨值變化
##pflist = pd.read_sql("select * from pflist_bak;",conn)
##pflist.index=pd.to_datetime(pflist.dtUpdate, format = '%Y-%m-%d %H:%M:%S')
##pflist.index = pflist.index.strftime('%Y-%m-%d')
##axes = plt.gca()
##axes.xaxis.set_major_locator(dates.DayLocator(interval=10))
##plt.gcf().autofmt_xdate()
##plt.plot(pflist.index,pflist.valNet)
##plt.show()
#print(pflist)

