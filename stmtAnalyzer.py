import pandas as pd
import json

import requests
from datetime import datetime,timedelta
from random import randint
import time

#a= pd.read_csv('20210711_清單_30dayRtn')
#淨利連續n季成長,連續n季年成長
def ni_increased(stmt,n,t=0):
    ni = [0 if item[1]=='無' else int(item[1]) for item in stmt]
    ni = ni[:(len(ni)+t+1)]
    return all([( (ni[i] > ni[i-n]))   for i in range(n,len(ni))][-n:])
#自由現金流量連續n季n季流量總和大於0    
def fc_increased(stmt,n,t=0):
    fc = [0 if item[1]=='無' else int(item[1]) for item in stmt]
    fc = fc[:(len(fc)+t+1)]
    return all([sum(fc[(i-n):i])>0   for i in range(n,len(fc))][-n:])
#應收帳款週轉率連續n季n季流量總和大於0
def act_increased(stmt,n,t=0):
    act = [0 if item[1]=='無' else float(item[1]) for item in stmt]
    act = act[:(len(act)+t+1)]
    return all([((act[i] > act[i-1]))   for i in range(n,len(act))][-n:])
#取得財報日期,以推算價格
def getFiscalAnnounceDate(nq,tpDate ='dM'):
    dQ = int(nq[-1])
    dY = int(nq[:-1])
    monPx=""

    if dQ == 1:
        monPx = (str(dY)+ str("06")) if tpDate =='dM' else str(dY)+ str("0515")
    elif dQ == 2:
        monPx = str(dY)+ str("09") if tpDate =='dM' else str(dY)+ str("0814")
    elif dQ == 3:
        monPx = str(dY)+ str("12") if tpDate =='dM' else str(dY)+ str("1114")
    elif dQ == 4:
        monPx = str(dY+1)+ str("04") if tpDate =='dM' else str(dY)+ str("0331")
    return monPx

def getPxAvg(stmt,nq):
    monPx=getFiscalAnnounceDate(nq)
    
    idxPX = stmt['monthly']['Price']['data']['month']['axis']
    idxPX = sum([x[0] if x[1]==monPx else 0 for x in idxPX])
    pxList = [0 if item[1]=='無' else float(item[1]) for item in stmt['monthly']['Price']['data']['month']['data']]
    try:
        if idxPX != 0 :
            pxChg = float(pxList[idxPX])/float(pxList[idxPX-1])
            return pxChg-1
        else: 
            return 0
    except ZeroDivisionError:
        return 0
def getPxReturn(id, nQuarter, nDay=60):
    ##取價      
    dtNowOri = datetime.strptime(getFiscalAnnounceDate(nQuarter,tpDate ='dY'),'%Y%m%d')+timedelta(days = nDay)
    dtNow = int(dtNowOri.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    dtStartOri = datetime.strptime(getFiscalAnnounceDate(nQuarter,tpDate ='dY'),'%Y%m%d')
    dtStart = int(dtStartOri.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    df_stkInfo = pd.DataFrame(columns=['dtT', 'pxOpen', 'pxHigh', 'pxLow', 'pxClose'])
    urls = f"https://ws.api.cnyes.com/ws/api/v1/charting/history?symbol=TWS:{id}:STOCK&resolution=D&quote=1&from={dtNow}&to={dtStart}"
    

    res = requests.get(urls)
    data = res.text
    jFile = json.loads(data)
    if jFile['statusCode']==200:
        df_stkInfo=pd.DataFrame({'dtT':[datetime.fromtimestamp(item) for item in jFile['data']['t']], 'pxOpen':jFile['data']['o'], 'pxHigh':jFile['data']['h'], 'pxLow':jFile['data']['l'], 'pxClose':jFile['data']['c']})
        if len(df_stkInfo)>0:
            df_stkInfo['dtT'] = pd.to_datetime(df_stkInfo['dtT'].dt.date)
            df_sub = df_stkInfo[(df_stkInfo.dtT<dtNowOri.strftime('%Y%m%d')) & (df_stkInfo.dtT > dtStartOri.strftime('%Y%m%d'))]
            if len(df_sub)>0:
                profit = (df_sub['pxClose'].iloc[0]/df_sub['pxClose'].iloc[-1])-1
            else:
                profit = 0
        else:
                profit = 0
        ##取價end
        time.sleep(randint(2,6))
    else:
        getPxReturn(id, nQuarter)
        time.sleep(randint(2,6))
    return profit
mypath='raw\\'


df_stkInfo = pd.read_csv("df_stkInfo.csv",encoding='utf-8')
df_stkInfo = df_stkInfo[df_stkInfo['tpEx']!='tpex_rotc'].astype(str)
df_stkInfo['filename'] = df_stkInfo['ticker']+ " " +df_stkInfo['name'] +".txt"
#df_stkInfo = df_stkInfo[df_stkInfo.ticker=='5351']

df_stkBoard_p = pd.DataFrame(columns=['ticker', 'dQuarter', 'rtn','tpCat'])
df_stkBoard_m = pd.DataFrame(columns=['ticker', 'dQuarter', 'rtn','tpCat'])
df_stkBoard = pd.DataFrame(columns=['ticker', 'dQuarter', 'rtn','tpCat'])
for index, row in df_stkInfo.iterrows():
    with open((mypath+row['filename']).replace("*",""),"r",encoding="utf8") as f:
        #print(row['filename'])
        result = json.load(f)
        if len(result['common']['TimeFiscalQ']['data'])>15 :
            for tlag in range(-1,0):
                nQuarter = result['common']['TimeFiscalQ']['data'][tlag][1]
                #print(nQuarter)
                #財報狗均價,以用鉅亨網股價取代
                #pxLat = getPxAvg(result,nQuarter)
                flagNI = ni_increased(result['quarterly']['NetIncome'] ['data'],4,tlag)
                flagFreeCash = fc_increased(result['quarterly']['FreeCashFlow'] ['data'],4,tlag)
                flagAct = act_increased(result['quarterly']['AccountsAndNotesReceivableTurnoverRatio'] ['data'],2,tlag)
                tpCat = result['common']['StockInfo']['data']['category']
                
                if (flagNI == True) & (flagFreeCash == True) & (flagAct==True):
                    profit = getPxReturn(row['filename'].split('.')[0].split(' ')[0], nQuarter)
                    df_stkBoard = df_stkBoard.append({'ticker': row['filename'].split('.')[0],'dQuarter': nQuarter, 'rtn': profit, 'tpCat': tpCat}, ignore_index=True)
                    print(nQuarter+"_"+row['filename']+"__"+str(round(profit,4)))

#df_stkBoard.to_csv('20210704_清單')
df_stkBoard.to_csv('20220118_清單_60dayRtn')
print(df_stkBoard)



