import requests
import time
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import json
import os
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
def cra_DelistInfo(con,pDate):
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
    dtStartNew = int(dtStartOri.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    df_stkInfo = pd.DataFrame(columns=['dtT', 'pxOpen', 'pxHigh', 'pxLow', 'pxClose'])
    urls = f"https://ws.api.cnyes.com/ws/api/v1/charting/history?symbol=TWS:{idTicker}:STOCK&resolution=D&quote=1&from={dtNow}&to={dtStartNew}"
    

    res = requests.get(urls)
    data = res.text
    jFile = json.loads(data)
    if jFile['statusCode']==200:
        #delete data
        sql = ''' delete from securityPrice where seqsec = ? and dtMkt between ? and ? '''
        cur = conn.cursor()
        cur.execute(sql, (str(sec),str(datetime.strftime(datetime.strptime(dtStart,'%Y%m%d'),'%Y-%m-%d')),str(datetime.strftime(dtStartOri,'%Y-%m-%d'))))
        
        conn.commit()
        #insert data
        df_stkInfo=pd.DataFrame({'dtMkt':[datetime.fromtimestamp(item) for item in jFile['data']['t']], 'pxOpen':jFile['data']['o'], 'pxHigh':jFile['data']['h'], 'pxLow':jFile['data']['l'], 'pxClose':jFile['data']['c'], 'volume':jFile['data']['v']})        
        df_stkInfo['seqSec'] = sec
        df_stkInfo['tpSrc'] = 'cnyes'
        df_stkInfo['dtMkt'] = pd.to_datetime(df_stkInfo['dtMkt']).dt.strftime('%Y-%m-%d')
        df_stkInfo.set_index(keys = ["seqSec","dtMkt","tpSrc"],inplace=True)
        df_stkInfo.to_sql(
                                name="securityPrice", 
                                con=conn2,
                                if_exists='append', 
                                index=True
                            )

    
    return df_stkInfo
# 取得券次編號
def getSecID(con,sec):
    cur = con.cursor()
    cur.execute("select idTicker from MDEngine.securityInfo where seqSec= ?",[sec])
    rows = cur.fetchall()
    for row in rows:
        ret = row[0]
    return ret
# 更新欲取價證券註記
def updateSecTagOn(con, pStk):
    for idStk in pStk:
        sql = ''' update MDEngine.securityInfo set tpData = 'Y'  where idTicker = ?'''
        cur = con.cursor()
        cur.execute(sql, [idStk])
        con.commit()

# 取得股票陣列券次編號
def getStkSeq(con):
    cur = con.cursor()
    cur.execute("select seqSec from securityInfo where tpData='Y' ")
    rows = cur.fetchall()
    aSeq = [row[0] for row in rows]
    return aSeq

# 主程式 ----Begin
start_time = time.time()  # 開始時間
def check_is_cloud():
    # 檢查多個可能的 GCP 環境變數
    conditions = [
        os.getenv('GOOGLE_CLOUD_PROJECT'),        # 標準 GCP 環境變數
        os.getenv('DEVSHELL_PROJECT_ID'),         # Cloud Shell 專用
        os.getenv('GOOGLE_CLOUD_QUICKSTART_PROJECT') # 部分 Cloud Shell 快速啟動環境
    ]
    
    # 只要其中一個不是 None，就回傳 True
    return any(cond is not None for cond in conditions)

is_cloud = check_is_cloud()
#建立資料庫連線
def get_connection(idConn):
    if is_cloud:
        # 雲端環境：通常檔案會放在目前執行腳本的目錄下
        # 如果你把 db 放在特定資料夾，可以改為 f"/home/{getpass.getuser()}/your_folder/"
        base_dir = "./" 
        print("--- 偵測到環境：Google Cloud Shell ---")
    else:
        # Mac 本機環境
        base_dir = '/Users/jonathantz/Documents/Project/Database/'
        print("--- 偵測到環境：Mac Local ---")

    # 組合路徑
    fp_path = os.path.join(base_dir, 'FP.db')
    md_engine_path = os.path.join(base_dir, 'MDEngine.db')

    # 建立連線
    if idConn==1:
        conn = sqlite3.connect(fp_path)
        # 使用參數化或格式化字串來 ATTACH，避免路徑空格出錯
        conn.execute(f'ATTACH "{md_engine_path}" AS MDEngine')
        return conn
    else :
        conn2 = sqlite3.connect(md_engine_path)
        return conn2
    
    

# 使用範例
conn = get_connection(1)
conn2 = get_connection(2)
#主程式
def run_crawler():
    start_time = time.time()
    conn = get_connection(1)
    conn2 = get_connection(2)
    
    # 原本在 if __name__ == '__main__': 底下的內容
    dtMax = getMaxMktDate(conn2)
    if dtMax == None:
        df_bak = cra_SecInfo(2)
        # ... (中間省略) ...
    
    aStk = getStkSeq(conn2)
    dtNow = datetime.now().strftime('%Y%m%d')
    for ticker in aStk:
        covtSecPxToDB(conn, ticker, dtNow, -100)

    end_time = time.time()
    print(f"爬蟲任務完成，耗時：{end_time - start_time} 秒")

# 建議保留這段，這樣 b.py 依然可以單獨執行
if __name__ == '__main__':
    run_crawler()
# 更新註記
# aryStk = ['2070','8069','2441','5351','2454','6274','2616']
#updateSecTagOn(conn,aryStk)
aStk = getStkSeq(conn)
dtNow = datetime.now().strftime('%Y%m%d')
for ticker in aStk:
    covtSecPxToDB(conn,ticker,dtNow,-100)

    # EX: 取價
    # a = covtSecPxToDB(conn,'12','20220218')
end_time = time.time()
print(f"{end_time - start_time} 秒")