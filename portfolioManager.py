import sqlite3
from unicodedata import decimal
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

def manage_portfolio():
    # 連線到資料庫
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    while True:
        print("\n--- 持股管理工具 ---")
        print("1. 新增買入 (Add/Buy)")
        print("2. 紀錄賣出 (Sell)")
        print("3. 配息")
        print("4. 申購")
        print("5. 查看目前持股")
        print("6. 離開")
        
        choice = input("請選擇操作 (1-6): ")

        if choice == '1':
            stock_id = input("輸入欲買進的股票代碼: ")
            stock_name = input("輸入欲買進的股票名稱: ")
            buy_qty = float(input("買進數量: "))
            buy_amt = float(input("買進總價: "))
            dtTrade = input("交易日期: ")
            pxBuy=buy_amt/buy_qty
            
            # 先檢查剩餘股數
            cursor.execute("select * from hldlist where idticker= ? and dthld=(SELECT MAX(dtHld) dtHldMax FROM hldList)", (stock_id,))
            result = cursor.fetchone()
            if result:
                dtHldMax=result[0]
            else:
                dtHldMax=dtTrade

            # 新增買進紀錄
            cursor.execute("delete from equitydetail where idTicker= ? and dtMkt= ?", (stock_id,dtTrade))
            conn.commit()
            cursor.execute("insert into equitydetail values(?,?,?,?)", (stock_id,stock_name,pxBuy,dtTrade))
            conn.commit()
            cursor.execute("insert into tradelist values( ? , ? ,'股票','1','買進',?, ? ,0,?,'',1)", (dtHldMax,stock_id,buy_amt,buy_qty, buy_amt))
            conn.commit()

            if result :
                dtHldMax=result[0]
                new_qty = result[4]+buy_qty
                cursor.execute("update pflist_bak set totalamt=ROUND(totalamt - ?,2)  where dtupdate>= ? ", (buy_amt,dtHldMax))
                conn.commit()
                cursor.execute("update hldlist set qtyHld= ?,avgPxCost=(avgPxCost*qtyHld+?)/?,pxMkt=? where idticker= ?  and dthld>= ? ",(new_qty,buy_amt,new_qty,pxBuy,stock_id,dtHldMax))
                # update hldlist set avgpxcost=(avgpxcost+250.856)/2,qtyhld=2000 where idticker='3587' and dthld='2024-08-14';
                conn.commit()
            else:
                dtHldMax=dtTrade
                new_qty = buy_qty
                cursor.execute("update pflist_bak set totalamt=ROUND(totalamt - ?,2)  where dtupdate>= ? ", (buy_amt,dtHldMax))
                conn.commit()
                cursor.execute("insert into hldlist values (? ,?,?,?,?,?,0,1)",(dtHldMax,stock_id,stock_name,pxBuy,new_qty,pxBuy))
                conn.commit()
            print(f"買進成功！剩餘 {stock_id} 股數: {new_qty}")

            

        elif choice == '2':
            stock_id = input("輸入欲賣出的股票代碼: ")
            sell_qty = float(input("賣出數量: "))
            sell_amt = float(input("賣出總價: "))
            dtTrade = input("交易日期: ")
            
            # 先檢查剩餘股數
            cursor.execute("select * from hldlist where idticker= ? and dthld=(SELECT MAX(dtHld) dtHldMax FROM hldList)", (stock_id,))
            result = cursor.fetchone()
            
            if result and result[4] >= sell_qty:
                new_qty = result[4] - sell_qty
                dtHldMax=result[0]
                # 成本價＊單位
                amtCost=result[3] * result[4]  
                amtPL=sell_amt-amtCost
                
                # 更新股數，如果歸零可以考慮 DELETE
                cursor.execute("insert into tradelist values( ? , ? ,'股票','1','賣出',?, ? ,0,?,'',1)", (dtHldMax,stock_id,sell_amt,sell_qty, sell_amt))
                conn.commit()
                cursor.execute("update pflist_bak set totalAmt=ROUND(totalAmt+?,2),amtRcg=amtRcg+? where dtupdate>=?",(sell_amt,amtPL,dtHldMax))
                conn.commit()
                if new_qty>0:
                    cursor.execute("update hldlist set qtyHld= ? , urcg=? * (pxMkt-avgPxCost)  where idticker= ?  and dthld>= ? ",(new_qty,new_qty,stock_id,dtHldMax))
                else:    
                    cursor.execute("delete from hldlist where idticker= ?  and dthld>= ? ",(stock_id,dtHldMax))
                conn.commit()
                print(f"賣出成功！剩餘 {stock_id} 股數: {new_qty}")
            else:
                print("錯誤：持有股數不足或找不到該股票")

        elif choice == '3':
            exdvd_amt = float(input("每股配息金額: "))
            dtTrade = input("交易日期yyyy-mm-dd: ")
            cursor.execute("select totalamt,valnet from pfList_bak where dtupdate=(select max(dtupdate) from pflist_bak)")
            pflist = cursor.fetchone()

            # cursor.execute("insert into exdvdlist values( 1 , ? ,'現金股利','1','賣出',?, ? ,0,?,'',1)", (dtTrade,stock_id,sell_amt,sell_qty, sell_amt))

            cursor.execute("select * from appuserinfo")
            userInfo = cursor.fetchall()
            amtSum=0
            for row in userInfo:
                # row 是一個 tuple，例如 (1, 'UserA', 100.0, ...)
                print(f"正在處理用戶：{row['seqUser']}")
                amtSum+=row['qtyHld']*exdvd_amt
                cursor.execute("insert into appUsertrans values( 1 , ? ,'現金股利', ? ,'',?, '' , 1 , '',strftime('%Y-%m-%d', 'now'))", (dtTrade,row['qtyHld']*exdvd_amt  ,row['qtyHld']))
                conn.commit()
            cursor.execute("insert into exdvdlist values( 1 , ? ,'CASH', ? ,'',?, ? , strftime('%Y-%m-%d', 'now'))", (dtTrade, exdvd_amt , pflist['valnet']-exdvd_amt, pflist['valnet']))
            conn.commit()
            cursor.execute("update  pfList_bak set totalamt=ROUND(totalamt-?,2),valnet=? where dtupdate=(select max(dtupdate) from pflist_bak)", (amtSum,pflist['valnet']-exdvd_amt) )
            conn.commit()

        elif choice == '4':
            idUser = int(input("申購人: "))
            dtTrade = input("交易日期yyyy-mm-dd: ")
            amtTrade = float(input("申購金額: "))
            cursor.execute("select totalamt,valnet from pfList_bak where dtupdate=(select max(dtupdate) from pflist_bak)")
            pflist = cursor.fetchone()

            # cursor.execute("insert into exdvdlist values( 1 , ? ,'現金股利','1','賣出',?, ? ,0,?,'',1)", (dtTrade,stock_id,sell_amt,sell_qty, sell_amt))

            cursor.execute("select * from appuserinfo where sequser= ? ",(idUser,))
            userInfo = cursor.fetchone()
            qtyTrans = math.ceil(amtTrade/pflist['valnet'])
            amtTrade_ceil=qtyTrans*pflist['valnet']
            qtyHld=qtyTrans+userInfo['qtyHld']

            cursor.execute("Insert into appusertrans values(1, ? ,'申購', ? , ? , ? ,'',1,'',date())", (dtTrade,amtTrade_ceil ,qtyTrans,qtyHld))
            conn.commit()
            cursor.execute("update appuserinfo set totalCost=totalCost+?,qtyHld= ? ,dtUpdate= date() where sequser= ? ", (amtTrade_ceil,qtyHld ,idUser))
            conn.commit()

            cursor.execute("update  pfList_bak set totalamt=ROUND(totalamt+?,2) where dtupdate=(select max(dtupdate) from pflist_bak)", (amtTrade_ceil,))
            conn.commit()

        elif choice == '5':
            cursor.execute("SELECT stockId, qtyHld, mktVal FROM appUserInfo")
            rows = cursor.fetchall()
            print("\n現有持股報表：")
            for row in rows:
                print(f"代碼: {row[0]} | 數量: {row[1]} | 市值: {row[2]}")

        elif choice == '6':
            break
            
    conn.close()

if __name__ == "__main__":
    manage_portfolio()