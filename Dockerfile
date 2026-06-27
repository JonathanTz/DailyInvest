# 使用官方輕量級 Python 映像檔
FROM python:3.10-slim

# 設定貨櫃內的工作目錄
WORKDIR /app

# 複製本地的所有程式碼到貨櫃中的 /app
COPY . /app

# 安裝 Python 依賴庫
RUN pip install --no-cache-dir -r requirements.txt

# Streamlit 預設會開啟 8501 埠，但 Cloud Run 規定必須聽從環境變數 $PORT（通常是 8080）
EXPOSE 8080

# 啟動 Streamlit 的指令，強制將 port 對齊 Cloud Run 的規範，並關閉一些不必要的網頁防護
CMD ["streamlit", "run", "app.py", "--server.port=8080", "--server.address=0.0.0.0"]