# capitalAnalyzer.py
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

# 💡 移除舊的 crawler_secInfo 連線依賴，直接引入統一的 db_manager
from db_manager import get_connection
# 依然保留 crawler_secInfo 的腳本觸發功能
import crawler_secInfo

# 使用範例：取得統一的 ATTACH 連線
conn = get_connection(1)

def getStkPrice(item, dtStart, dtEnd):
    item = (f"{item}:STOCK") if item != 'TWSE' else (f"{item}:INDEX")
    urls = f"https://ws.api.cnyes.com/ws/api/v1/charting/history?symbol=TWS:{item}&resolution=D&quote=1&from={dtEnd}&to={dtStart}" 
    res = requests.get(urls)
    data = res.text
    jFile = json.loads(data)
    
    df_input = pd.DataFrame({
        'idTicker': [item for i in jFile['data']['t']], 
        'dtT': [datetime.fromtimestamp(t) for t in jFile['data']['t']], 
        'pxOpen': jFile['data']['o'], 
        'pxHigh': jFile['data']['h'], 
        'pxLow': jFile['data']['l'], 
        'pxClose': jFile['data']['c']
    })
    df_input['dtT'] = pd.to_datetime(df_input['dtT'].dt.date)
    return df_input.sort_values(by=['dtT'])

def covtMktToPF(con):
    print("--1--Start MktPrice Update Local")
    sql = """
        Insert into equityDetail 
        select distinct M.idTicker, M.nameTicker, P.pxClose, P.dtMkt 
        from MDEngine.securityInfo M 
        inner join MDEngine.securityPrice P on M.seqsec = P.seqsec 
        inner join (select idTicker, Max(dtMkt) dtMax from equitydetail group by idTicker) G on M.idticker = G.idticker 
        where M.tpData = 'Y' and G.dtMax < P.dtMkt 
        order by P.dtMkt desc
    """
    cursor = con.execute(sql)
    con.commit()
    print(cursor.lastrowid)
    print("--1--End MktPrice Update Local")

    print("--2--Start Porfolio Update Price")
    c = con.cursor()
    cursor = c.execute("SELECT MAX(dtHld) dtHldMax FROM hldList")
    dtHldMax = cursor.fetchone()[0] or ""
    
    cursor = c.execute("SELECT MAX(dtMkt) dtMktMax FROM equityDetail")
    dtMktMax = cursor.fetchone()[0] or ""
    
    if dtHldMax < dtMktMax:
        sql_hld = f"""
            SELECT E.dtMkt, H.idTicker, H.nameTicker, H.avgPxCost, H.qtyHld, E.pxClose, (E.pxClose - H.avgPxCost) * H.qtyHld as urcg, '1' 
            FROM hldList H 
            inner join equityDetail E on H.idticker = E.idTicker 
            WHERE E.dtMkt > '{dtHldMax}' and H.dtHld = '{dtHldMax}'
            order by E.idticker, E.dtMkt
        """
        cursor = c.execute(sql_hld)
        
        # 💡 最佳化：改用 List 收集字典後一次轉換為 DataFrame，避免在迴圈中不斷使用 pd.concat 造成效能瓶頸
        hld_data = []
        for row in cursor:
            hld_data.append({
                'dtHld': row[0], 'idTicker': row[1], 'nameTicker': row[2], 
                'avgPxCost': row[3], 'qtyHld': row[4], 'pxMkt': row[5], 
                'urcg': row[6], 'seqPF': row[7]
            })
        
        if hld_data:
            df_Hld = pd.DataFrame(hld_data)
            df_Hld.to_sql('hldList', con=con, if_exists='append', index=False)

        print(cursor.lastrowid)
        print("--2--End Porfolio Update Price")

        print("--3--Start MktPrice Update Local")
        cursor = c.execute("select sum(qtyHld) qtyOutstanding from appuserInfo")
        qtyOutstanding = cursor.fetchone()[0] or 1 # 避免除以 0 的保護
        
        sql_bak = f"""
            Insert into PFList_bak 
            select P.seqPF, P.namePF, P.totalAmt, M.amtMkt, P.amtRcg, (P.totalAmt + M.amtMkt) / {qtyOutstanding}, strftime('%Y-%m-%d %H:%M:%S', M.dtHld) 
            from pfList_bak P 
            inner join (select dtHld, sum(pxMkt * qtyHld) amtMkt from hldList group by dtHld) M 
            on strftime('%Y-%m-%d', P.dtUpdate) = '{dtHldMax}' and M.dtHld > '{dtHldMax}'
        """
        # 💡 修正原本混用 conn 與 con 的變數錯誤，統一使用傳入的 con
        cursor = con.execute(sql_bak)
        con.commit()
        print(cursor.lastrowid)
        print("--3--End MktPrice Update Local")

def calPortfolioRisk(df_input, n=10):
    dtEnd = datetime.now() + timedelta(days=1)
    dtStart = dtEnd - timedelta(days=n)
    dtEnd = int(dtEnd.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    dtStart = int(dtStart.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    
    df_idx = getStkPrice("TSE01", dtStart, dtEnd)
    df_latPortfolio = pd.DataFrame()
    
    for item in df_input.idTicker.values:
        df_px = getStkPrice(item, dtStart, dtEnd).sort_values('dtT').reset_index(drop=True)
        qtyHld = df_input[df_input['idTicker'] == item].qtyHld.values[0]
        
        # 更新即時市值與未實現損益
        latest_px = df_px.iloc[-1]
        df_input.loc[df_input.idTicker == item, 'amtMkt'] = latest_px.pxClose
        df_input.loc[df_input.idTicker == item, 'urcg'] = ((latest_px.pxClose - df_input.loc[df_input.idTicker == item, 'amtCost'])* df_input.loc[df_input.idTicker == item, 'qtyHld'])
        df_input.loc[df_input.idTicker == item, 'dtHld'] = latest_px.dtT.strftime('%Y-%m-%d')
        
        if len(df_latPortfolio) == 0:
            df_latPortfolio = pd.DataFrame({'dtT': df_px['dtT'].values, 'amtHld': df_px.pxClose * qtyHld})
        else:
            df_latPortfolio.amtHld = df_latPortfolio.amtHld + df_px.pxClose * qtyHld
            
    df_latPortfolio = df_latPortfolio.reset_index(drop=True) 
    df_idx = df_idx.reset_index(drop=True) 
    
    df_latPortfolio['ret'] = ((df_latPortfolio.amtHld - df_latPortfolio.shift(1).amtHld) / df_latPortfolio.shift(1).amtHld)
    df_idx['ret'] = ((df_idx.pxClose - df_idx.shift(1).pxClose) / df_idx.shift(1).pxClose)
    
    df_latPortfolio_ret = pd.concat([df_latPortfolio.set_index(['dtT'])['ret'][1:], df_idx.set_index(['dtT'])['ret'][1:]], axis=1).ffill()
    df_latPortfolio_amtHld = pd.concat([df_latPortfolio.set_index(['dtT'])['amtHld'], df_idx.set_index(['dtT'])['ret']], axis=1).ffill()
    
    valBeta = df_latPortfolio_ret.corr().iloc[0, 1] / (df_latPortfolio.ret[1:].std() / df_idx.ret[1:].std())
    valVol = df_latPortfolio_ret.dropna().values.std()
    valRet = (df_latPortfolio_amtHld.amtHld.values[-1] / df_latPortfolio_amtHld.amtHld.dropna().values[0]) - 1
    valIdxRet = (df_idx.pxClose.values[-1] / df_idx.pxClose.values[0]) - 1
    
    print(f"投組beta: {valBeta:.4f} \n -投組報酬波動度: {valVol:.4f}\n -投組報酬率: {valRet:.4f}\n -期間指數報酬率: {valIdxRet:.4f}")

    # 繪圖
    (1 + df_latPortfolio_ret).cumprod().plot()
    plt.legend(["Ret_Portfolio", "Ret_Index"])
    return df_input

# 1. 執行爬蟲更新市場資料庫
crawler_secInfo.run_crawler()

# 2. 更新市場資料庫股價至投組資料庫
covtMktToPF(conn)

c = conn.cursor()
cursor = c.execute("SELECT * FROM hldList WHERE dtHld=(SELECT MAX(dtHld) FROM hldList)")

portfolio_data = []
for row in cursor:
    portfolio_data.append({
        'dtHld': row[0], 'idTicker': row[1], 'nameCH': row[2], 'amtCost': row[3], 
        'qtyHld': row[4], 'amtMkt': row[5], 'urcg': row[6],
        'profitRatio': round(row[6] / (row[3] * row[4]) * 100, 2) if row[3] * row[4] != 0 else 0,
        'percentage': 0
    })

df_portfolio = pd.DataFrame(portfolio_data)

# 3. 風險計算
df_portfolio = calPortfolioRisk(df_portfolio, 620)

# 4. 持股權重與損益計算
df_portfolio['percentage'] = (df_portfolio['amtMkt'] * df_portfolio['qtyHld'])
total_portfolio_mkt_val = df_portfolio['percentage'].sum()
if total_portfolio_mkt_val != 0:
    df_portfolio['percentage'] = (df_portfolio['percentage'] / total_portfolio_mkt_val) * 100
df_portfolio = df_portfolio.round({"percentage": 3})

rtnGain = round(((df_portfolio['amtMkt'] * df_portfolio['qtyHld']).sum() / (df_portfolio['amtCost'] * df_portfolio['qtyHld']).sum() - 1), 4)
amtGain = round(((df_portfolio['amtMkt'] * df_portfolio['qtyHld']).sum() - (df_portfolio['amtCost'] * df_portfolio['qtyHld']).sum()), 2)
totalMkt = (df_portfolio['amtMkt'] * df_portfolio['qtyHld']).sum()

print("未實現損益率{0}, 未實現損益金額{1}, 總市值{2}".format(rtnGain, amtGain, totalMkt))

# 5. 更新淨值至使用者表格
cursor = c.execute("select totalAmt from pflist_bak where Date(dtUpdate)=(SELECT MAX(dtHld) FROM hldList);")
row_cash = cursor.fetchone()
totalCash = row_cash[0] if row_cash else 0

cursor = c.execute("select SUM(qtyHld) as qtyHld from appUserInfo;")
row_qty = cursor.fetchone()
qtyHld = row_qty[0] if row_qty else 1

pfNav = (totalMkt + totalCash) / qtyHld

# 💡 修正：將原本的 mktValue 修正為符合你之前資料庫結構的 mktVal 欄位，避免欄位名稱錯誤
sql_update_user = "UPDATE appUserInfo SET mktValue = qtyHld * ?"
c.execute(sql_update_user, (pfNav,))
conn.commit()

print("投組淨值為{0}  ".format(round(pfNav, 3)))
print(df_portfolio.sort_values(by=['urcg'], ascending=False))