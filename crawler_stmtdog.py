from bs4 import BeautifulSoup
import concurrent.futures
import requests
import time
import pandas as pd
import json
from random import randint
from os import listdir
from datetime import datetime
from pathlib import Path
import os
import time
from datetime import datetime

def scrape_stmt(urls):
    res = requests.get(urls)
    data = res.text
    try :
        jFile = json.loads(data)
        if 'error' not in jFile:
            idTicker = jFile['common']['StockInfo']['data']['ticker_name']
            fileS =open('raw\\'+idTicker.replace("*","")+'.txt','w',encoding="utf-8")
            fileS.write(data)
            fileS.close()
            print(idTicker)
            time.sleep(randint(10,20))
    except:
        scrape_stmt(urls)

 
 

start_time = time.time()  # 開始時間
#----------------- 抓取財報狗財報-同時建立及啟用10個執行緒
# base_url = "https://statementdog.com/api/v2/fundamentals"
# listStk = [f"{str(stkID).zfill(4)}" for stkID in range(6112, 9999)]#[3089,3609,4431,5321,5392,5432,5529,5859,6468,6721,7419,7421,8705]
# listStk = [1472,2340,2443,2736,4154,5604,6171,6517,6831,6840]
# urls = [f"{base_url}/{str(stkID).zfill(4)}/2016/2022/cf?qbu=true&qf=analysis" for stkID in listStk]  # 1~5頁的網址清單
# with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
#     executor.map(scrape_stmt, urls)
#-----------------

    
# 抓取財報狗財報-回補遺漏資料(上述多執行緒缺漏)
mypath='raw\\'  
files = listdir(mypath)
ii = [f"{str(stkID).zfill(4)}" for stkID in range(1100, 9999)]
new_urls = [str(i)[:4] for i in files]
# for j in ii:
#     if j not in new_urls:
#         scrape_stmt(base_url+"/"+str(j).zfill(4)+"/2016/2022/cf?qbu=true&qf=analysis")

# for item in files:
#     try:
#         with open(mypath+item,"r",encoding="cp950") as f:
#             contents = f.read()
#     except:
#         with open(mypath+item,"r",encoding="utf8") as f:
#             contents = f.read()
#     with open(mypath+item,"w",encoding="utf8") as f:
#         #  result = [dict(rs=rs.strip()) for rs in inp]
#         f.write(contents)
# for file in sorted([mypath+file for file in listdir(mypath)],key=os.path.getmtime):
#     if datetime.fromtimestamp(os.path.getmtime(file)).strftime('%Y%m%d')<'20220115':
#         print(file)
#         # scrape_stmt(urls)
 

#-----------------取得上市櫃分類資訊            
# df_stkInfo = pd.DataFrame(columns=['ticker', 'name', 'tpEx', 'tpCagy'])
# #Parse txt file json content
# for item in files:
#     try :
#         with open(mypath+item,"r",encoding="utf8") as inp:
#             #  result = [dict(rs=rs.strip()) for rs in inp]
#             result = json.load(inp)
#         ticker =result['common']['StockInfo']['data']['ticker']
#         name = result['common']['StockInfo']['data']['name']
#         tpEx = result['common']['StockInfo']['data']['stock_exchange']
#         tpCagy = result['common']['StockInfo']['data']['category']
#         dtLat = result['common']['StockInfo']['data']['latest_closing_price']  
        
#         if dtLat[:10] >'2021-06-01':
#             df_stkInfo = df_stkInfo.append({'ticker': ticker,'name': name, 'tpEx': tpEx, 'tpCagy': tpCagy}, ignore_index=True)
#     except:
#         print(item)

# df_stkInfo.to_csv("df_stkInfo_202201.csv",encoding='utf-8')
# df_stkInfo = pd.read_csv("df_stkInfo.csv",encoding='utf-8')

#-----------------取得上市櫃分類資訊
end_time = time.time()
print(f"{end_time - start_time} 秒")