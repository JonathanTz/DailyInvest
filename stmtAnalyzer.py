from cmath import nan
import pandas as pd
import json
import requests
from datetime import datetime,timedelta
from random import randint
import time
import sqlite3
import crawler_secInfo
import math
import numpy

#a= pd.read_csv('20210711_清單_30dayRtn')
#負債比連續n季降低,且低於50
def debt_increased(stmt,n,t=0):
    dratio = [0 if item[1]=='無' else float(item[1]) for item in stmt]
    dratio = dratio[:(len(dratio)+t+1)]
    return all([( (dratio[i] < dratio[i-n]))   for i in range(n,len(dratio))][-2:])
#淨利連續n季成長,連續n季年成長
def ni_increased(stmt,n,t=0):
    ni = [0 if item[1]=='無' else float(item[1]) for item in stmt]
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
        monPx = str(dY+1)+ str("04") if tpDate =='dM' else str(dY+1)+ str("0331")
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
def getPxReturn(id, nQuarter, nDay=30):
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
        getPxReturn(id, nQuarter,nDay)
        time.sleep(randint(2,6))
    return profit

##擷取DB股票股價計算報酬率
def getPxReturnlDB(id, nQuarter, nDay=30):
    ##取價      
    dtNowOri = (datetime.strptime(getFiscalAnnounceDate(nQuarter,tpDate ='dY'),'%Y%m%d')+timedelta(days = nDay)).strftime('%Y-%m-%d')
    
    dtStartOri = datetime.strptime(getFiscalAnnounceDate(nQuarter,tpDate ='dY'),'%Y%m%d').strftime('%Y-%m-%d')
    dir = '/Users/jonathantz/Documents/Project/Database/'
    conn = sqlite3.connect(dir +'MDEngine.db')
    seqSec= getSecID(conn,id)

    tickerInfo = pd.read_sql("select * from securityPrice where seqSec= '"+ str(seqSec)+"' and dtMkt between '"+ dtStartOri+"' and '"+ dtNowOri+"'",conn)
    if len(tickerInfo)!=0:
        ret = tickerInfo.pxClose.values[0]/tickerInfo.pxClose.values[-1]
    else:
        ret = None

    return ret
    #df_stkInfo = pd.DataFrame(columns=['dtT', 'pxOpen', 'pxHigh', 'pxLow', 'pxClose'])


##爬蟲擷取股票報酬率匯入DB    
def getPxReturnInterval(id, nQuarterA, nQuarterB, nDay=30):
    ##取價      
    dtNowOri = datetime.strptime(getFiscalAnnounceDate(nQuarterB,tpDate ='dY'),'%Y%m%d')+timedelta(days = nDay)
    dtNow = int(dtNowOri.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    dtStartOri = datetime.strptime(getFiscalAnnounceDate(nQuarterA,tpDate ='dY'),'%Y%m%d')
    dtStart = int(dtStartOri.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    df_stkInfo = pd.DataFrame(columns=['dtT', 'pxOpen', 'pxHigh', 'pxLow', 'pxClose'])
    urls = f"https://ws.api.cnyes.com/ws/api/v1/charting/history?symbol=TWS:{id}:STOCK&resolution=D&quote=1&from={dtNow}&to={dtStart}"
    
    dir = '/Users/jonathantz/Documents/Project/Database/'
    conn = sqlite3.connect(dir +'MDEngine.db')
    seqSec= getSecID(conn,id)

    if seqSec=='':
        return None

    
    res = requests.get(urls)
    data = res.text
    jFile = json.loads(data)
    if jFile['statusCode']==200:
        df_stkInfo=pd.DataFrame({'dtT':[datetime.fromtimestamp(item) for item in jFile['data']['t']], 'pxOpen':jFile['data']['o'], 'pxHigh':jFile['data']['h'], 'pxLow':jFile['data']['l'], 'pxClose':jFile['data']['c']})
        if len(df_stkInfo)>0:
            df_stkInfo['dtT'] = pd.to_datetime(df_stkInfo['dtT'].dt.date)
            df_sub = df_stkInfo[(df_stkInfo.dtT<dtNowOri.strftime('%Y%m%d')) & (df_stkInfo.dtT > dtStartOri.strftime('%Y%m%d'))]
            df_sub['tpSrc']='cnyes'
            df_sub['seqSec']=seqSec
            df_sub.sort_values(by=['dtT'])
            df_sub.rename(columns = {'dtT':'dtMkt'}, inplace = True)

            ##delete data
            sql = ''' delete from securityPrice where seqSec= ? and tpSrc = 'cnyes' and dtMkt > cast(? as date) '''
            cur = conn.cursor()
            cur.execute(sql, (seqSec,getFiscalAnnounceDate(nQuarterA,tpDate ='dY')))
            conn.commit()

            df_sub = df_sub[['seqSec','dtMkt','tpSrc','pxOpen','pxHigh','pxLow','pxClose']]
            df_sub.to_sql('securityPrice', con=conn, if_exists='append',index=False)

            if len(df_sub)>0:
                profit = (df_sub['pxClose'].iloc[0]/df_sub['pxClose'].iloc[-1])-1
            else:
                profit = 0
        else:
                profit = 0
        ##取價end
        time.sleep(randint(2,6))
    else:
        getPxReturnInterval(id, nQuarterA,nQuarterB)
        time.sleep(randint(2,6))
    return profit

##指定因子回測報酬率
def run_stmtBackTesting(qStart=-1,qEnd=0,qDateInterval=30):
    # 財報路徑
    mypath='raw/'
    # 主程式
    dtFile = datetime.now().strftime("%Y%m%d")
    df_stkInfo = pd.read_csv("df_stkInfo.csv",encoding='utf-8')
    df_stkInfo = df_stkInfo[df_stkInfo['tpEx']!='tpex_rotc'].astype(str)
    df_stkInfo['filename'] = df_stkInfo['ticker']+ " " +df_stkInfo['name'] +".txt"
    #df_stkInfo = df_stkInfo[df_stkInfo.ticker=='5351']
    df_stkBoard = pd.DataFrame(columns=['ticker', 'dQuarter', 'rtn','tpCat'])
    for index, row in df_stkInfo.iterrows():
        with open((mypath+row['filename']).replace("*",""),"r",encoding="utf8") as f:
            #print(row['filename'])
            result = json.load(f)

            if len(result['common']['TimeFiscalQ']['data'])>15 :
                for tlag in range(qStart,qEnd):
                    nQuarter = result['common']['TimeFiscalQ']['data'][tlag][1]
                    #print(nQuarter)
                    #財報狗均價,以用鉅亨網股價取代
                    flagDebt = debt_increased(result['quarterly']['DebtRatio'] ['data'],4,tlag)
                    flagNI = ni_increased(result['quarterly']['NetIncome'] ['data'],4,tlag)
                    flagFreeCash = fc_increased(result['quarterly']['FreeCashFlow'] ['data'],4,tlag)
                    flagAct = act_increased(result['quarterly']['AccountsAndNotesReceivableTurnoverRatio'] ['data'],2,tlag)
                    tpCat = result['common']['StockInfo']['data']['category']
                    
                    if (flagNI == True) & (flagFreeCash == True) & (flagAct==True):
                        profit = getPxReturn(row['filename'].split('.')[0].split(' ')[0], nQuarter,qDateInterval)
                        
                        dict ={'ticker': row['filename'].split('.')[0],'dQuarter': nQuarter, 'rtn': profit, 'tpCat': tpCat}
                        df_stkBoard = pd.concat([df_stkBoard,pd.DataFrame(dict,index=[0])])
                        #df_stkBoard = df_stkBoard.append({'ticker': row['filename'].split('.')[0],'dQuarter': nQuarter, 'rtn': profit, 'tpCat': tpCat}, ignore_index=True)
                        print(nQuarter+"_"+row['filename']+"__"+str(round(profit,4)))

    #df_stkBoard.to_csv('20210704_清單')
    df_stkBoard.to_csv(dtFile+'_清單_'+str(qDateInterval)+' dayRtn'+'.csv')
    print(df_stkBoard)
    return df_stkBoard
def run_stmtBackTestingViaDBGroup(qStart=-1,qEnd=0,qDateInterval=30):
    # 財報路徑
    mypath='raw/'
    # 主程式
    dtFile = datetime.now().strftime("%Y%m%d")
    df_stkInfo = pd.read_csv("df_stkInfo.csv",encoding='utf-8')
    df_stkInfo = df_stkInfo[df_stkInfo['tpEx']!='tpex_rotc'].astype(str)
    df_stkInfo['filename'] = df_stkInfo['ticker']+ " " +df_stkInfo['name'] +".txt"
    df_stkRatioSum = pd.DataFrame(columns=['idTicker', 'colName', 'dtFiscal','ratio','tpGroup'])

    dir = '/Users/jonathantz/Documents/Project/Database/'
    conn = sqlite3.connect(dir +'MDEngine.db')
    colName = getColName(conn)
    dtFiscal = getDtFiscal(conn)
    nLag=3
    
    #tickerInfoSQL=pd.read_sql("select idTicker,dtFiscal,colName,colValue from securityStatement where tpFreq='Q'  order by idTicker,colName asc,dtFiscal asc",conn)
    for col in colName:
        for dt in dtFiscal[nLag:]:
            print(dt)
            df_stkRatio = pd.DataFrame(columns=['idTicker', 'colName', 'dtFiscal','ratio'])

            tickerInfo = pd.read_sql("select b.idTicker idTicker,b.dtFiscal,b.colName,(b.colValue/a.colValue) ratio from securityStatement a \
                                      join securityStatement b on a.colName=b.colName and a.idTicker=b.idTicker and a.dtFiscal="+dtFiscal[dtFiscal.index(dt)-(nLag+1)]+" \
                                      and b.dtfiscal="+ dt +" \
                                      where b.dtFiscal="+ dt +" and a.colName='"+ col +"' and a.colValue<>'無' and b.colValue<>'無' order by (b.colValue/a.colValue) ",conn)
            tickerInfo=tickerInfo[tickerInfo.ratio.notnull()]

            df_tail=tickerInfo.sort_values(by=['ratio']).tail(10)
            df_tail['tpGroup']='A'
            df_head=tickerInfo.sort_values(by=['ratio']).head(10)
            df_head['tpGroup']='B'
            df_stkRatioSum = pd.concat([df_stkRatioSum,df_tail,df_head], ignore_index=True)
            print(dt)
            #for index, row in df_stkInfo.iterrows():
            #    tickerInfo=tickerInfoSQL[(tickerInfoSQL.idticker==row.ticker) & (tickerInfoSQL.colName==col)]
            #    subInfo = tickerInfo[(tickerInfo.dtFiscal<=dt) & (tickerInfo.colName==col)]
            #    if len(subInfo)<(nLag+1):
            #        continue
            #
            #    if (subInfo.iloc[-1-(nLag),3]!='0.00') & (subInfo.iloc[-1-(nLag),3]!='-0.00') & (subInfo.iloc[-1-(nLag),3]!='0') & \
            #        (subInfo.iloc[-1-(nLag),3]!='無') & (subInfo.iloc[-1,3]!='無'):
            #        subInfo = subInfo.iloc[:,:]
            #        list = [row.ticker, col, dt, float(subInfo.iloc[-1,3])/float(subInfo.iloc[-1-nLag,3])]
            #        df_stkRatio.loc[len(df_stkRatio)] = list
            #
            #df_stkRatioSum = pd.concat([df_stkRatioSum,df_stkRatio.sort_values(by=['ratio']).tail(8),df_stkRatio.sort_values(by=['ratio']).head(8)], ignore_index=True)
            df_stkRatioSum.to_csv('sum.csv')  
    return df_stkRatio
def run_stmtRtnViaDB(qStart=-1,qEnd=0,qDateInterval=30):
    # 財報路徑
    mypath='raw/'
    # 主程式
    dtFile = datetime.now().strftime("%Y%m%d")
    df_stkInfo = pd.read_csv("df_stkInfo.csv",encoding='utf-8')
    df_stkInfo = df_stkInfo[df_stkInfo['tpEx']!='tpex_rotc'].astype(str)
    df_stkInfo['filename'] = df_stkInfo['ticker']+ " " +df_stkInfo['name'] +".txt"
    #df_stkInfo = df_stkInfo[df_stkInfo.ticker=='5351']
    df_stkBoard = pd.DataFrame(columns=['ticker', 'dQuarter', 'rtn','tpCat'])
    for index, row in df_stkInfo.iterrows():
        with open((mypath+row['filename']).replace("*",""),"r",encoding="utf8") as f:
            #print(row['filename'])
            result = json.load(f)

            if len(result['common']['TimeFiscalQ']['data'])>15 :
                for tlag in range(qStart,qEnd):
                    nQuarter = result['common']['TimeFiscalQ']['data'][tlag][1]
                    #print(nQuarter)
                    #財報狗均價,以用鉅亨網股價取代
                    #flagNI = ni_increased(result['quarterly']['Equity'] ['data'],4,tlag)
                    #flagFreeCash = ni_increased(result['quarterly']['NAV'] ['data'],4,tlag)
                    #flagAct = ni_increased(result['quarterly']['ROA'] ['data'],4,tlag)

                    flagDebt = debt_increased(result['quarterly']['DebtRatio'] ['data'],4,tlag)
                    flagNI = ni_increased(result['quarterly']['NetIncome'] ['data'],4,tlag)
                    flagFreeCash = fc_increased(result['quarterly']['FreeCashFlow'] ['data'],4,tlag)
                    flagAct = act_increased(result['quarterly']['AccountsAndNotesReceivableTurnoverRatio'] ['data'],2,tlag)

                    tpCat = result['common']['StockInfo']['data']['category']
                    
                    if (flagNI == True) & (flagFreeCash == True) & (flagAct==True) & (flagDebt==True) :
                        profit = getPxReturn(row['filename'].split('.')[0].split(' ')[0], nQuarter,qDateInterval)

                        dict ={'ticker': row['filename'].split('.')[0],'dQuarter': nQuarter, 'rtn': profit, 'tpCat': tpCat}
                        df_stkBoard = pd.concat([df_stkBoard,pd.DataFrame(dict,index=[0])])                        
                        #df_stkBoard = df_stkBoard.append({'ticker': row['filename'].split('.')[0],'dQuarter': nQuarter, 'rtn': profit, 'tpCat': tpCat}, ignore_index=True)
                        print(nQuarter+"_"+row['filename']+"__"+str(round(profit,4)))

    #df_stkBoard.to_csv('20210704_清單')
    df_stkBoard.to_csv(dtFile+'_清單_'+str(qDateInterval)+' dayRtn'+'.csv')
    print(df_stkBoard)
    return df_stkBoard

def covtRaw2DB():
    dir = '/Users/jonathantz/Documents/Project/Database/'
    conn = sqlite3.connect(dir +'MDEngine.db')
    ##更新colmap
    #pd.DataFrame([['Q',x,strJs['quarterly'][x]['label']] for x in strJs['quarterly'].keys()], \
    #                columns =['tpFreq','colNameEng', 'colNameCh']).to_sql('colMap', con=conn, if_exists='append',index=False)
    colName = getColName(conn)

    # 財報路徑
    mypath='raw/'
    # 主程式
    dtFile = datetime.now().strftime("%Y%m%d")
    df_stkInfo = pd.read_csv("df_stkInfo.csv",encoding='utf-8')
    df_stkInfo = df_stkInfo[df_stkInfo['tpEx']!='tpex_rotc'].astype(str)
    df_stkInfo['filename'] = df_stkInfo['ticker']+ " " +df_stkInfo['name'] +".txt"
    #df_stkInfo = df_stkInfo[df_stkInfo.ticker=='5351']
    for index, row in df_stkInfo.iterrows():
        with open((mypath+row['filename']).replace("*",""),"r",encoding="utf8") as f:
            #print(row['filename'])
            strJs = json.load(f)
            noTicker = strJs['common']['StockInfo']['data']['ticker']

            for strCol in colName:
                ##delete data
                sql = ''' delete from securityStatement where idTicker= ? and colName= ? '''
                cur = conn.cursor()
                cur.execute(sql, (noTicker,strCol))
                conn.commit()


                #insert data 
                df_data = pd.DataFrame([['Q',strCol,x[1]] for x in strJs['quarterly'][strCol]['data']], \
                            columns =['tpFreq','colName','colValue'])#.to_sql('colMap', con=conn, if_exists='append',index=False)
                df_data['dtFiscal'] = [y[1] for y in strJs['common']['TimeFiscalQ']['data']]
                df_data['idTicker'] = strJs['common']['StockInfo']['data']['ticker']


                df_data = df_data[['idTicker','tpFreq','dtFiscal','colName','colValue']]
                df_data.to_sql('securityStatement', con=conn, if_exists='append',index=False)



    return "Done"
def getColName(con):
    cur = con.cursor()
    cur.execute("select colNameEng from colMap where flagUpdate='Y' ")
    rows = cur.fetchall()
    colName = [row[0] for row in rows]
    return colName
def getDtFiscal(con):
    cur = con.cursor()
    cur.execute("select distinct dtFiscal from securityStatement order by dtFiscal ")
    rows = cur.fetchall()
    dtFiscal = [row[0] for row in rows]
    return dtFiscal
# 取得券次編號
def getSecID(con,sec):
    cur = con.cursor()
    cur.execute("select seqSec from securityInfo where idTicker= ?",[str(sec)])
    #cur.execute("select seqSec from securityInfo where idTicker= ? ",['2330'])
    rows = cur.fetchone()
    ret = ''
    if rows == None:
        ret = ''
    else:    
        ret = rows[0]
    return ret

def getHLFactor():
    df_stkRatioSumPx = pd.read_csv('sum2.csv')
    df_sort= pd.DataFrame(columns=['colName','diff','std'])
    for e in df_stkRatioSumPx.colName.unique():
        df_stkRatioSumPxS =df_stkRatioSumPx[df_stkRatioSumPx.colName==e]
        nq = 0
        sumq=0
        arDiff=[]
        for h in df_stkRatioSumPxS.dtFiscal.unique():
            
            a= df_stkRatioSumPxS[(df_stkRatioSumPxS.dtFiscal==h) & (df_stkRatioSumPxS.tpGroup=='A')].retPx.mean()
            b= df_stkRatioSumPxS[(df_stkRatioSumPxS.dtFiscal==h) & (df_stkRatioSumPxS.tpGroup=='B')].retPx.mean()
            diff = a-b
            if abs(diff)>0: 
                nq+=1
                sumq +=diff
                arDiff.append(diff)
            #if (abs(diff)>0.05):
            #    print( e+"---"+str(h)+"---"+ str(diff))
        if abs(sumq)>0: 
            #print(str(e)+ "---" +'因子報酬差異'+str(sumq/nq))
            df_sort.loc[len(df_sort)] = {'colName':e,'diff':sumq/nq, 'std':numpy.std(arDiff)}

    df_sort = df_sort.sort_values(by=['diff'])
    df_sort['std2'] = pd.to_numeric(df_sort['std'])
    df_sort['diff2'] = pd.to_numeric(df_sort['diff'])
    df_sort = df_sort[(df_sort.diff2>0.015) | (df_sort.diff2<-0.015)] 
    
    return df_sort


def getRetPxQ(nDay):
    df_stkRatioSum = pd.read_csv('sum.csv')  
    df_t = df_stkRatioSum
    df_t["retPx"] = None
    nTotal = len(df_stkRatioSum.idTicker.unique())
    nCursor = 0
    for j in df_stkRatioSum.idTicker.unique():
        arDt = sorted(df_stkRatioSum[df_stkRatioSum.idTicker==j].dtFiscal.unique())
        for k in arDt:
            if str(k)!='20231':
                ret = (j,str(k),nDay)
                df_t.loc[(df_t.idTicker==j) & (df_t.dtFiscal==k),'retPx']=ret
                ##print(ret)
        nCursor+=1
        print(str(nCursor)+ "/"+str(nTotal))
    df_t.to_csv('sum2.csv') 
    return df_t
def craRetPxQ(): 
    df_stkRatioSum = pd.read_csv('sum.csv') 
    for j in df_stkRatioSum.idTicker.unique():
        arDt = sorted(df_stkRatioSum[df_stkRatioSum.idTicker==j].dtFiscal.unique())
        dtStart = str(arDt[0])
        dtEnd = str(arDt[-1])
        df_px = getPxReturnInterval(j,dtStart,dtEnd)     

        print(str(j)+'--Done')               



##Step 1
#covtRaw2DB()
#run_stmtBackTesting(-1,0,30)

#Step 2 篩選各期因子篩選標的
#run_stmtBackTestingViaDBGroup(-1,0,30)

dir = '/Users/jonathantz/Documents/Project/Database/'
#conn = sqlite3.connect(dir +'MDEngine.db')
#colName = getColName(conn)
#dtFiscal = getDtFiscal(conn)
nLag=3

##Step 3 ???
#getRetPxQ(100)###??????

##篩選因子投組差異排名
getHLFactor()

run_stmtRtnViaDB(-1,0,30)

