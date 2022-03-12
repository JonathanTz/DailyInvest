import requests
import time
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import json
# 證券主檔table
# # CREATE TABLE securityInfo(seqSec INTEGER PRIMARY KEY AUTOINCREMENT ,idTicker varchar(20) not null, nameTicker varchar(40) ,ISIN varchar(40) , tpMarket varchar(10), tpIndustry varchar(40), codeCountry varchar(10),tpData  varchar(10),dtPublic datetime,dtDelist datetime,dtUpdate timestamp DATE DEFAULT (datetime('now','localtime')));
# 證券價格table
#  CREATE TABLE securityPrice(seqSec integer, dtMkt date,tpSrc varchar(10),pxOpen decimal(18,6),pxHigh decimal(18,6),pxLow decimal(18,6),pxClose decimal(18,6),volume decimal(25,6), dtUpdate DATE DEFAULT (datetime('now','localtime')));
# 爬取上市/上櫃清單(2上市/4上櫃)
def cra_SecInfo(tpMkt):#2上市/4上櫃
    res = requests.get("https://isin.twse.com.tw/isin/C_public.jsp?strMode="+str(tpMkt))
    df = pd.read_html(res.text)[0]
    df.columns = df.iloc[0]
    df = df.iloc[2:]
    df = df.dropna(thresh=3,axis=0).dropna(thresh=3,axis=1)
    df = df.set_index("有價證券代號及名稱")
    df = df[df["CFICode"]=="ESVUFR"]
    df['idTicker'] = [id.split()[0] for id in df.index]
    df['nameTicker'] = [nm.split()[1] for nm in df.index]
    df['codeCountry'] = "TW"
    df['dtDelist'] = ""
    df['上市日'] = pd.to_datetime(df['上市日'], format='%Y/%m/%d')
    df_ret = df[['idTicker','nameTicker','國際證券辨識號碼(ISIN Code)','市場別','產業別','codeCountry','上市日','dtDelist']]
    df_ret.columns = ['idTicker','nameTicker','ISIN','tpMarket','tpIndustry','codeCountry','dtPublic','dtDelist']
    

    print("爬取價格--代碼"+str(tpMkt)+"(2上市/4上櫃)")
    return df_ret
# 取得最大資料日期
def getMaxMktDate(con):
    cur = con.cursor()
    cur.execute("select MAX(dtPublic) from securityInfo")
    rows = cur.fetchall()
    for row in rows:
        ret = row[0]
    return ret

# 爬取下市下櫃資訊至DB
def updateDelistData(con,pDate):
    # 爬取下櫃資料
    my_data = {'stk_code':'' ,
            'select_year': 'ALL',
            'DELIST_REASON': -1,
            'topage':1}
    res = requests.post("https://www.tpex.org.tw/web/regular_emerging/deListed/de-listed_companies.php?l=zh-tw", data = my_data)
    res.encoding = 'utf8'

    df_bak = pd.read_html(res.text)[0]
    df_bak.columns = df_bak.iloc[0]
    df_bak = df_bak.iloc[1:]
    df_bak = df_bak.reset_index(drop=True)[['股票代號','終止上櫃日期']]
    df_bak['終止上市日期'] = pd.to_datetime(df_bak['終止上櫃日期'], format='%Y-%m-%d')
    df_bak['市場別'] = '上櫃'
    # 爬取下市資料
    res = requests.get("https://www.twse.com.tw/company/suspendListingCsvAndHtml?type=html&selectYear=&lang=zh")
    df_bak1 = pd.read_html(res.text)[0]
    df_bak1.columns = df_bak1.iloc[0]
    df_bak1 = df_bak1.iloc[1:-1]
    df_bak1.columns =['終止上市日期','股票名稱','股票代號']
    df_bak1['終止上市日期'] = [str(int(str(dt).split("年")[0])+1911)+"年"+ str(dt).split("年")[1] for dt in df_bak1['終止上市日期'].values]
    df_bak1['終止上市日期'] = pd.to_datetime(df_bak1['終止上市日期'], format='%Y年%m月%d日')
    df_bak1['市場別'] = '上市'

    df_ret = pd.concat([df_bak[['股票代號','終止上市日期','市場別']],df_bak1[['股票代號','終止上市日期','市場別']]])
    df_ret = df_ret.reset_index(drop=True)
    if pDate != None:
        df_ret = df_ret[df_ret['終止上市日期']>pDate]

        # 更新資料庫下市櫃資訊
        for index,row in df_ret.iterrows():
            sql = ''' update securityInfo set dtDelist = ? where tpMarket = ? and idTicker = ?'''
            cur = conn.cursor()
            cur.execute(sql, (str(row['終止上市日期']),row['市場別'],str(row['股票代號'])))
            conn.commit()
        print("爬取/更新下市櫃資訊")

def covtSecPxToDB(conn,sec, dtStart, nDay=-60):
    idTicker = getSecID(conn,sec)
    # idTicker = '2330'
    ##取價      
    dtNowOri = datetime.strptime(dtStart,'%Y%m%d')+timedelta(days = 1)
    dtNow = int(dtNowOri.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    dtStartOri = datetime.strptime(dtStart,'%Y%m%d')+timedelta(days = nDay)
    dtStart = int(dtStartOri.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    df_stkInfo = pd.DataFrame(columns=['dtT', 'pxOpen', 'pxHigh', 'pxLow', 'pxClose'])
    urls = f"https://ws.api.cnyes.com/ws/api/v1/charting/history?symbol=TWS:{idTicker}:STOCK&resolution=D&quote=1&from={dtNow}&to={dtStart}"
    

    res = requests.get(urls)
    data = res.text
    jFile = json.loads(data)
    if jFile['statusCode']==200:
        df_stkInfo=pd.DataFrame({'dtMkt':[datetime.fromtimestamp(item) for item in jFile['data']['t']], 'pxOpen':jFile['data']['o'], 'pxHigh':jFile['data']['h'], 'pxLow':jFile['data']['l'], 'pxClose':jFile['data']['c'], 'volume':jFile['data']['v']})        
        df_stkInfo['seqSec'] = sec
        df_stkInfo['tpSrc'] = 'cnyes'
        df_stkInfo['dtMkt'] = pd.to_datetime(df_stkInfo['dtMkt']).dt.strftime('%Y-%m-%d')
        df_stkInfo.set_index(keys = ["seqSec","dtMkt","tpSrc"],inplace=True)
        df_stkInfo.to_sql('securityPrice', con=conn, if_exists='replace',index=True)

    
    return df_stkInfo
# 取得券次編號
def getSecID(con,sec):
    cur = con.cursor()
    cur.execute("select idTicker from securityInfo where seqSec= ?",[sec])
    rows = cur.fetchall()
    for row in rows:
        ret = row[0]
    return ret

# 主程式 ----Begin
start_time = time.time()  # 開始時間
conn = sqlite3.connect("D:/Data/Sqlite/MDEngine.db")  #建立資料庫連線

# Initiate
# 更新上市櫃清單(排除下市櫃)
dtMax = getMaxMktDate(conn)
if dtMax == None:
    df_bak = cra_SecInfo(2)
    df_SQL = pd.concat([df_bak,cra_SecInfo(4)])
    df_SQL.to_sql('securityInfo', con=conn, if_exists='append',index=False)
else:
    df_bak = cra_SecInfo(2)
    df_SQL = pd.concat([df_bak,cra_SecInfo(4)])
    df_SQL = df_SQL[df_SQL.dtPublic>dtMax]
    df_SQL.to_sql('securityInfo', con=conn, if_exists='append',index=False)
df_Delist = updateDelistData(conn,dtMax)

# 取價
a = covtSecPxToDB(conn,'12','20220218')


end_time = time.time()
print(f"{end_time - start_time} 秒")