# db_manager.py
import os
import sqlite3

def check_is_cloud():
    """檢查是否在 Google Cloud 環境 (相容 Cloud Run 與 Cloud Shell)"""
    conditions = [
        os.getenv('K_SERVICE'),                   # 💡 Cloud Run 專屬特徵
        os.getenv('GOOGLE_CLOUD_PROJECT'),        # Cloud Shell 專屬
        os.getenv('DEVSHELL_PROJECT_ID')
    ]
    return any(cond is not None for cond in conditions)

def get_connection(idConn):
    """
    取得資料庫連線
    idConn = 1: 回傳連線並 ATTACH MDEngine
    idConn = 2: 僅回傳 MDEngine 連線
    """
    is_cloud = check_is_cloud()
    
    if is_cloud:
        # 💡 在 Cloud Run 環境下，唯有 /tmp 目錄具備可讀寫權限
        base_dir = "/tmp/" 
        print("--- 偵測到環境：Google Cloud (Cloud Run / Shell) ---")
        
        # 💡 安全防護：如果雲端 /tmp 內還沒有這兩個 db 檔，自動觸發建立空的檔案
        # 避免後續 sqlite3.connect 或 ATTACH 時因為檔案不存在而噴錯
        for db_name in ['FP.db', 'MDEngine.db']:
            db_file_path = os.path.join(base_dir, db_name)
            if not os.path.exists(db_file_path):
                # 建立空檔案
                open(db_file_path, 'a').close()
                print(f"已在雲端臨時目錄初始化空白檔案: {db_name}")
    else:
        base_dir = '/Users/jonathantz/Documents/Project/Database/'
        print("--- 偵測到環境：Mac Local ---")

    fp_path = os.path.join(base_dir, 'FP.db')
    md_engine_path = os.path.join(base_dir, 'MDEngine.db')

    if idConn == 1:
        conn = sqlite3.connect(fp_path)
        # 加上對應，避免在多線程或跨檔案時因為庫名大小寫誤判
        conn.execute(f'ATTACH "{md_engine_path}" AS MDEngine')
        return conn
    else:
        conn2 = sqlite3.connect(md_engine_path)
        return conn2