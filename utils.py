import streamlit as st
import pandas as pd
import yfinance as yf
import requests
import urllib3
from supabase import create_client, Client

# 忽略不安全的請求警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- 修改 1: 讓 Client 變數可以被更新 ---
# 我們不直接暴露一個固定的 client，而是提供一個函數來獲取或初始化
_supabase_client = None

def init_supabase():
    """初始化 Supabase 連線"""
    global _supabase_client
    if _supabase_client is not None:
        return _supabase_client
        
    try:
        url: str = st.secrets["SUPABASE_URL"]
        key: str = st.secrets["SUPABASE_KEY"]
        _supabase_client = create_client(url, key)
        return _supabase_client
    except Exception:
        st.error("❌ 無法連線至 Supabase，請檢查 .streamlit/secrets.toml 設定。")
        st.stop()

# 為了相容舊程式碼，我們還是先初始化一個，但它是全域的
supabase = init_supabase()

# --- 新增: 更新 Session 的 helper function ---
def update_supabase_session(access_token, refresh_token):
    """當使用者登入後，呼叫此函數更新 client 的權限"""
    global supabase
    if supabase:
        try:
            supabase.auth.set_session(access_token, refresh_token)
        except Exception as e:
            print(f"Session update failed: {e}")

@st.cache_resource(ttl=86400)
def get_official_tw_map():
    """從證交所抓取最新的台股代碼與名稱對照表"""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    url = "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2"
    master_map = {}
    try:
        res = requests.get(url, headers=headers, timeout=15, verify=False)
        res.encoding = 'cp950'
        df_list = pd.read_html(res.text, flavor='lxml')
        if df_list:
            df = df_list[0]
            for val in df[0].dropna():
                cleaned_val = val.replace('　', ' ').strip()
                if ' ' in cleaned_val:
                    p = cleaned_val.split(' ', 1)
                    if len(p) >= 2:
                        ticker, name = p[0].strip(), p[1].strip()
                        if ticker.isdigit():
                            master_map[ticker] = name
                            master_map[f"{ticker}.TW"] = name
    except Exception as e:
        # 在 utils 裡盡量不要直接呼叫 st.sidebar，避免在非 sidebar context 出錯
        print(f"台股清單抓取失敗: {str(e)}")
    return master_map

def get_display_name(ticker):
    """將代碼轉換為 '名稱 (代碼)' 格式"""
    t_str = str(ticker).upper().strip()
    tw_map = get_official_tw_map()
    base = t_str.replace(".TW", "")
    if base in tw_map: return f"{tw_map[base]} ({t_str})"
    return t_str

def get_market_data(tickers, period="5d"):
    """獲取即時/歷史現價與台幣匯率"""
    if not tickers: return {}, 32.5
    t_list = list(set([str(t).strip().upper() for t in tickers if pd.notna(t)]))
    query_list = [t if (".TW" in t or "-" in t) else f"{t}-USD" if t in ["BTC", "ETH", "SOL", "USDT"] else f"{t}.TW" if t.isdigit() else t for t in t_list]
    try:
        # 下載包含匯率的數據
        data = yf.download(list(set(query_list + ["TWD=X"])), period=period, progress=False)
        
        # 相容 yfinance 不同版本的資料結構
        if isinstance(data.columns, pd.MultiIndex):
            close_data = data['Close']
        else:
            close_data = data
            
        # 取得最新匯率
        ex_rate = 32.5
        if "TWD=X" in close_data.columns:
            val = close_data["TWD=X"].dropna().iloc[-1]
            if pd.notna(val): ex_rate = val
        
        prices = {}
        for orig in t_list:
            q_t = orig if (".TW" in orig or "-" in orig) else f"{orig}-USD" if orig in ["BTC", "ETH", "SOL"] else f"{orig}.TW" if orig.isdigit() else orig
            if q_t in close_data.columns: 
                val = close_data[q_t].dropna().iloc[-1]
                if pd.notna(val): prices[orig] = val
        return prices, ex_rate
    except: return {}, 32.5