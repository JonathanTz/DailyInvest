# db_manager.py
import os
import sqlite3

def check_is_cloud():
    """檢查是否在 Google Cloud 環境"""
    conditions = [
        os.getenv('GOOGLE_CLOUD_PROJECT'),
        os.getenv('DEVSHELL_PROJECT_ID'),
        os.getenv('GOOGLE_CLOUD_QUICKSTART_PROJECT')
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
        base_dir = "./" 
        print("--- 偵測到環境：Google Cloud Shell ---")
    else:
        base_dir = '/Users/jonathantz/Documents/Project/Database/'
        print("--- 偵測到環境：Mac Local ---")

    fp_path = os.path.join(base_dir, 'FP.db')
    md_engine_path = os.path.join(base_dir, 'MDEngine.db')

    if idConn == 1:
        conn = sqlite3.connect(fp_path)
        # 修正：加上對應，避免在多線程或跨檔案時因為庫名大小寫誤判
        conn.execute(f'ATTACH "{md_engine_path}" AS MDEngine')
        return conn
    else:
        conn2 = sqlite3.connect(md_engine_path)
        return conn2