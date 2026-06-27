import streamlit as st
import pandas as pd
import requests
import json
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go

# 引入您原本專案的資料庫連線與爬蟲模組
from db_manager import get_connection
import crawler_secInfo

st.set_page_config(page_title="DailyInvest 儀表板", page_icon="📈", layout="wide")
st.title("📈 DailyInvest 投資追蹤儀表板")

# =====================================================================
# 核心計算與邏輯函數（完全保留您的原本邏輯）
# =====================================================================

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
    sql = """
        Insert into equityDetail 
        select distinct M.idTicker, M.nameTicker, P.pxClose, P.dtMkt 
        from MDEngine.securityInfo M 
        inner join MDEngine.securityPrice P on M.seqsec = P.seqsec 
        inner join (select idTicker, Max(dtMkt) dtMax from equitydetail group by idTicker) G on M.idticker = G.idticker 
        where M.tpData = 'Y' and G.dtMax < P.dtMkt 
        order by P.dtMkt desc
    """
    con.execute(sql)
    con.commit()

    c = con.cursor()
    dtHldMax = c.execute("SELECT MAX(dtHld) FROM hldList").fetchone()[0] or ""
    dtMktMax = c.execute("SELECT MAX(dtMkt) FROM equityDetail").fetchone()[0] or ""
    
    if dtHldMax < dtMktMax:
        sql_hld = f"""
            SELECT E.dtMkt, H.idTicker, H.nameTicker, H.avgPxCost, H.qtyHld, E.pxClose, (E.pxClose - H.avgPxCost) * H.qtyHld as urcg, '1' 
            FROM hldList H 
            inner join equityDetail E on H.idticker = E.idTicker 
            WHERE E.dtMkt > '{dtHldMax}' and H.dtHld = '{dtHldMax}'
            order by E.idticker, E.dtMkt
        """
        cursor = c.execute(sql_hld)
        hld_data = [{
            'dtHld': r[0], 'idTicker': r[1], 'nameTicker': r[2], 'avgPxCost': r[3], 
            'qtyHld': r[4], 'pxMkt': r[5], 'urcg': r[6], 'seqPF': r[7]
        } for r in cursor]
        
        if hld_data:
            pd.DataFrame(hld_data).to_sql('hldList', con=con, if_exists='append', index=False)

        qtyOutstanding = c.execute("select sum(qtyHld) from appuserInfo").fetchone()[0] or 1
        sql_bak = f"""
            Insert into PFList_bak 
            select P.seqPF, P.namePF, P.totalAmt, M.amtMkt, P.amtRcg, (P.totalAmt + M.amtMkt) / {qtyOutstanding}, strftime('%Y-%m-%d %H:%M:%S', M.dtHld) 
            from pfList_bak P 
            inner join (select dtHld, sum(pxMkt * qtyHld) amtMkt from hldList group by dtHld) M 
            on strftime('%Y-%m-%d', P.dtUpdate) = '{dtHldMax}' and M.dtHld > '{dtHldMax}'
        """
        con.execute(sql_bak)
        con.commit()

def calPortfolioRisk(df_input, n=620):
    dtEnd = datetime.now() + timedelta(days=1)
    dtStart = dtEnd - timedelta(days=n)
    dtEnd = int(dtEnd.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    dtStart = int(dtStart.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    
    df_idx = getStkPrice("TSE01", dtStart, dtEnd)
    df_latPortfolio = pd.DataFrame()
    
    for item in df_input.idTicker.values:
        df_px = getStkPrice(item, dtStart, dtEnd)
        qtyHld = df_input[df_input['idTicker'] == item].qtyHld.values[0]
        
        df_input.loc[df_input.idTicker == item, 'amtMkt'] = df_px.iloc[-1, :].pxClose
        df_input.loc[df_input.idTicker == item, 'urcg'] = (df_input.loc[df_input.idTicker == item, 'amtMkt'] - df_input.loc[df_input.idTicker == item, 'amtCost']) * df_input.loc[df_input.idTicker == item, 'qtyHld']
        df_input.loc[df_input.idTicker == item, 'dtHld'] = df_px.dtT.max().strftime('%Y-%m-%d')
        
        if len(df_latPortfolio) == 0:
            df_latPortfolio = pd.DataFrame({'dtT': df_px['dtT'].values, 'amtHld': df_px.pxClose * qtyHld})
        else:
            df_latPortfolio.amtHld = df_latPortfolio.amtHld + df_px.pxClose * qtyHld
            
    df_latPortfolio = df_latPortfolio.reset_index(drop=True) 
    df_idx = df_idx.reset_index(drop=True) 
    
    df_latPortfolio['ret'] = ((df_latPortfolio.amtHld - df_latPortfolio.shift(1).amtHld) / df_latPortfolio.shift(1).amtHld)
    df_idx['ret'] = ((df_idx.pxClose - df_idx.shift(1).pxClose) / df_idx.shift(1).pxClose)
    
    df_latPortfolio_ret = pd.concat([df_latPortfolio.set_index(['dtT'])['ret'][1:], df_idx.set_index(['dtT'])['ret'][1:]], axis=1).ffill()
    
    valBeta = df_latPortfolio_ret.corr().iloc[0, 1] / (df_latPortfolio.ret[1:].std() / df_idx.ret[1:].std())
    cum_ret = (1 + df_latPortfolio_ret).cumprod()
    return df_input, valBeta, cum_ret

# 核心分析執行器
@st.cache_data(ttl=1800) # 快取 30 分鐘
def run_full_analysis(trigger_crawler=False):
    conn = get_connection(1)
    c = conn.cursor()

    if trigger_crawler:
        crawler_secInfo.run_crawler()
        covtMktToPF(conn)

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
    if df_portfolio.empty:
        return None, 0, None, 0, 0, 0, 0, "無資料"

    df_portfolio, valBeta, cum_ret_df = calPortfolioRisk(df_portfolio, 620)

    df_portfolio['percentage'] = (df_portfolio['amtMkt'] * df_portfolio['qtyHld'])
    totalMkt = df_portfolio['percentage'].sum()
    if totalMkt != 0:
        df_portfolio['percentage'] = (df_portfolio['percentage'] / totalMkt) * 100
    df_portfolio = df_portfolio.round({"percentage": 3})

    rtnGain = ((df_portfolio['amtMkt'] * df_portfolio['qtyHld']).sum() / (df_portfolio['amtCost'] * df_portfolio['qtyHld']).sum() - 1)
    amtGain = ((df_portfolio['amtMkt'] * df_portfolio['qtyHld']).sum() - (df_portfolio['amtCost'] * df_portfolio['qtyHld']).sum())

    row_cash = c.execute("select totalAmt from pflist_bak where Date(dtUpdate)=(SELECT MAX(dtHld) FROM hldList);").fetchone()
    totalCash = row_cash[0] if row_cash else 0

    row_qty = c.execute("select SUM(qtyHld) as qtyHld from appUserInfo;").fetchone()
    qtyHld = row_qty[0] if row_qty else 1

    pfNav = (totalMkt + totalCash) / qtyHld

    c.execute("UPDATE appUserInfo SET mktValue = qtyHld * ?", (pfNav,))
    conn.commit()
    
    # 順便抓出當前資料庫最新日期
    latest_db_date = df_portfolio['dtHld'].max()
    conn.close()

    return df_portfolio, valBeta, cum_ret_df, rtnGain, amtGain, totalMkt, pfNav, latest_db_date

# =====================================================================
# 💡 智慧檢查：判斷資料庫是否需要更新
# =====================================================================
def check_if_update_needed(db_date_str):
    if db_date_str == "無資料":
        return True
    
    try:
        db_date = datetime.strptime(db_date_str, "%Y-%m-%d").date()
    except:
        return True
        
    today = datetime.now().date()
    
    # 計算理論上應該要有的最新資料日期（排除週末）
    # 如果今天是週六，最新資料應該是週五；今天是週日或週一開盤前，最新也應該是週五
    if today.weekday() == 5:    # 週六
        expected_date = today - timedelta(days=1)
    elif today.weekday() == 6:  # 週日
        expected_date = today - timedelta(days=2)
    elif today.weekday() == 0 and datetime.now().hour < 14: # 週一開盤前/盤中
        expected_date = today - timedelta(days=3)
    else:
        # 平日 14:00 前可能還沒收盤更新，預設為昨天
        expected_date = today if datetime.now().hour >= 14 else today - timedelta(days=1)
        
    return db_date < expected_date

# =====================================================================
# 網頁控制中心 (UI Layout)
# =====================================================================

# 先行讀取現有資料庫數據（不跑爬蟲，1秒載入）
df, beta, cum_ret, rtnGain, amtGain, totalMkt, pfNav, db_date_str = run_full_analysis(trigger_crawler=False)

# 在側邊欄建立自動狀態檢查區
st.sidebar.header("⚙️ 系統狀態檢查")
st.sidebar.write(f"📂 資料庫最新日期: `{db_date_str}`")

need_update = check_if_update_needed(db_date_str)

if need_update:
    st.sidebar.warning("⚠️ 資料庫非最新狀態，建議更新。")
    # 只有需要更新時，按鈕會呈現醒目的紅色/主色調類型
    if st.sidebar.button("🚀 執行爬蟲更新最新數據", type="primary"):
        st.cache_data.clear() # 清除快取
        with st.spinner("正在執行完整爬蟲更新中..."):
            df, beta, cum_ret, rtnGain, amtGain, totalMkt, pfNav, db_date_str = run_full_analysis(trigger_crawler=True)
        st.sidebar.success("更新完畢！")
        st.rerun()
else:
    st.sidebar.success("✅ 資料庫已是最新狀態，無需更新。")
    # 雖然資料最新，還是留一個普通按鈕防萬一
    if st.sidebar.button("🔄 強制重新爬蟲"):
        st.cache_data.clear()
        with st.spinner("正在強制更新中..."):
            df, beta, cum_ret, rtnGain, amtGain, totalMkt, pfNav, db_date_str = run_full_analysis(trigger_crawler=True)
        st.sidebar.success("強制更新完畢！")
        st.rerun()

st.write("---")

# =====================================================================
# 數據圖表渲染
# =====================================================================
if df is not None:
    # 1. 頂部四大看板
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(label="投組淨值 (NAV)", value=f"${pfNav:.3f}")
    with col2:
        st.metric(label="總市值 (TWD)", value=f"${totalMkt:,.0f}")
    with col3:
        st.metric(label="未實現損益金額", value=f"${amtGain:,.0f}", delta=f"{rtnGain*100:.2f}%")
    with col4:
        st.metric(label="投組 Beta 值", value=f"{beta:.4f}")

    st.write("---")

    # 2. 雙欄圖表展示
    left_col, right_col = st.columns([1, 1.2])
    with left_col:
        st.subheader("📊 持股比例分佈")
        fig_pie = px.pie(df, values='percentage', names='nameCH', hole=0.4,
                         hover_data=['idTicker', 'qtyHld', 'amtMkt'])
        st.plotly_chart(fig_pie, use_container_width=True)

    with right_col:
        st.subheader("📈 投組 vs 大盤累積報酬 (近620日)")
        if cum_ret is not None:
            fig_line = go.Figure()
            fig_line.add_trace(go.Scatter(x=cum_ret.index, y=cum_ret.iloc[:, 0], mode='lines', name='我的投組'))
            fig_line.add_trace(go.Scatter(x=cum_ret.index, y=cum_ret.iloc[:, 1], mode='lines', name='大盤指數'))
            st.plotly_chart(fig_line, use_container_width=True)

    st.write("---")

    # 3. 數據表格
    st.subheader("📋 詳細持股明細")
    st.dataframe(df.sort_values(by=['urcg'], ascending=False).reset_index(drop=True), use_container_width=True)
else:
    st.error("暫無投組資料，請點擊左側按鈕執行爬蟲與計算。")