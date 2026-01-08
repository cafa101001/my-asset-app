import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
import time

# --- 關鍵匯入 ---
# 引入 utils 中的 update_supabase_session 來同步權限
from utils import supabase as data_client, get_market_data, update_supabase_session

# 嘗試匯入 Supabase Client 設定，若版本過舊則提示
try:
    from supabase import create_client, ClientOptions
except ImportError:
    st.error("❌ 偵測到 Supabase 套件版本過舊。請在終端機執行: `pip install supabase --upgrade` 更新套件。")
    st.stop()

from logic import fetch_all_data, calculate_detailed_metrics, clean_df, save_daily_snapshot

# --- 1. 頁面基礎設定 ---
st.set_page_config(page_title="全球資產管理系統 V7.5", layout="wide")

# ==========================================
#      🔐 登入邏輯 (Session Storage + Sync)
# ==========================================

# 1. 初始化 Session State
if 'user' not in st.session_state:
    st.session_state.user = None
if 'user_id' not in st.session_state:
    st.session_state.user_id = None

# 定義自訂儲存類別 (確保 Verifier 不會在跳轉後遺失)
class StreamlitSessionStorage:
    def __init__(self):
        if "supabase_auth_storage" not in st.session_state:
            st.session_state.supabase_auth_storage = {}
    def get_item(self, key):
        return st.session_state.supabase_auth_storage.get(key)
    def set_item(self, key, value):
        st.session_state.supabase_auth_storage[key] = value
    def remove_item(self, key):
        if key in st.session_state.supabase_auth_storage:
            del st.session_state.supabase_auth_storage[key]

# 建立專用於登入驗證的 Client
# 使用 st.session_state 搭配 StreamlitSessionStorage 確保狀態持久化
if 'auth_client' not in st.session_state:
    try:
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_KEY"]
        # 關鍵：使用自訂 storage
        st.session_state.auth_client = create_client(url, key, options=ClientOptions(storage=StreamlitSessionStorage()))
    except Exception as e:
        st.error(f"❌ Auth Client 初始化失敗: {e}")
        st.stop()

def get_query_params():
    try: return st.query_params
    except: return st.experimental_get_query_params()

def clear_url():
    try: st.query_params.clear()
    except: st.experimental_set_query_params()

def handle_login():
    """處理登入流程與同步"""
    auth_client = st.session_state.auth_client

    # 1. 嘗試從 Storage 恢復 Session
    try:
        session = auth_client.auth.get_session()
        if session and session.user:
            st.session_state.user = session.user
            st.session_state.user_id = session.user.id
            # *** 關鍵修正：呼叫 utils.py 的函式更新全域權限 ***
            update_supabase_session(session.access_token, session.refresh_token)
            return
    except Exception:
        pass

    # 2. 處理網址回調 (Google 登入後帶回的 code)
    params = get_query_params()
    code = params.get("code")
    if isinstance(code, list): code = code[0]

    if code:
        try:
            # 交換 Session
            res = auth_client.auth.exchange_code_for_session(code)
            if res.user:
                st.session_state.user = res.user
                st.session_state.user_id = res.user.id
                
                # *** 關鍵修正：同步權限 ***
                update_supabase_session(res.session.access_token, res.session.refresh_token)
                
                st.success(f"✅ 歡迎回來，{res.user.email}！")
                time.sleep(0.5)
                clear_url()
                st.rerun()
        except Exception as e:
            # 靜默處理錯誤並重試 (通常是因為 code 重複使用)
            # 清除網址讓使用者回到乾淨狀態
            clear_url()
            st.rerun()

def show_login_UI():
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.title("🔐 全球資產管理系統 V7.5")
        st.markdown("### 請登入以存取您的個人資產數據")
        
        # 預設埠號設為 8501，但允許手動修改
        try:
            redirect_url = st.secrets["REDIRECT_URL"]
        except:
            redirect_url = "http://localhost:8501" 
            
        with st.expander("⚙️ 設定登入回調網址 (若無法登入請檢查)", expanded=False):
            redirect_url = st.text_input("Redirect URL", value=redirect_url)
        
        if st.button("🚀 使用 Google 帳號登入", type="primary", use_container_width=True):
            try:
                res = st.session_state.auth_client.auth.sign_in_with_oauth({
                    "provider": "google",
                    "options": {
                        "redirect_to": redirect_url,
                        "queryParams": {"access_type": "offline", "prompt": "consent select_account"}
                    }
                })
                if res.url:
                    st.markdown(f'<meta http-equiv="refresh" content="0;url={res.url}">', unsafe_allow_html=True)
            except Exception as e:
                st.error(f"❌ 初始化失敗: {e}")

# --- 執行登入檢查 ---
handle_login()

if not st.session_state.user:
    show_login_UI()
    st.stop()

# ==========================================
#      🚀 主程式邏輯 (登入成功後)
# ==========================================

# 再次確保 Session 同步 (雙重保險，針對頁面重新整理的情況)
if st.session_state.user:
    try:
        session = st.session_state.auth_client.auth.get_session()
        if session:
            update_supabase_session(session.access_token, session.refresh_token)
    except:
        pass

# 初始化資料
if 'transactions' not in st.session_state:
    with st.spinner('🔄 正在載入您的加密資料...'):
        try:
            fetch_all_data()
        except Exception as e:
            # 初始化空的資料結構以免報錯 (針對新帳號)
            st.session_state.transactions = pd.DataFrame(columns=['id', 'user_id', '類型', '類別', '代碼', '數量', '單價', '日期'])
            st.session_state.income_df = pd.DataFrame()
            st.session_state.liabilities_df = pd.DataFrame()
            st.session_state.liquidity_df = pd.DataFrame()
            st.session_state.snapshots_df = pd.DataFrame()
            st.session_state.settings = {"monthly_expense": 80000, "fire_mode": "依月開銷推算 (25倍法則)", "custom_target": 24000000}
            
            # 如果不是因為空資料而是權限錯誤，才顯示警告
            if "policy" in str(e).lower() or "permission" in str(e).lower():
                st.error(f"資料載入權限錯誤: {e}")

# 靜態資料
PR_DATA_113 = {10: 33.0, 20: 38.6, 30: 44.0, 40: 49.3, 50: 54.6, 60: 61.4, 70: 71.9, 80: 88.5, 90: 131.2}

# --- 2. 核心數據運算 ---
total_market_val, total_holding_cost, current_ex_rate = 0, 0, 32.5
holdings_df, detailed_tx_global = pd.DataFrame(), pd.DataFrame()
total_pnl, realized_all = 0, 0

# A. 即時投資數據計算
if not st.session_state.transactions.empty:
    t_list = st.session_state.transactions['代碼'].dropna().unique().tolist()
    prices, current_ex_rate = get_market_data(t_list)
    holdings_df, realized_all, detailed_tx_global = calculate_detailed_metrics(st.session_state.transactions, current_ex_rate)
    
    if not holdings_df.empty:
        holdings_df['現價'] = holdings_df['代碼'].map(prices).fillna(0)
        holdings_df['匯率'] = holdings_df['類別'].apply(lambda x: current_ex_rate if x != '台股' else 1.0)
        holdings_df['市值(TWD)'] = holdings_df['現價'] * holdings_df['持倉數量'] * holdings_df['匯率']
        holdings_df['成本(TWD)'] = holdings_df['平均成本'] * holdings_df['持倉數量'] * holdings_df['匯率']
        holdings_df['損益(TWD)'] = holdings_df['市值(TWD)'] - holdings_df['成本(TWD)']
        holdings_df['報酬率'] = (holdings_df['損益(TWD)'] / holdings_df['成本(TWD)'].replace(0, 1)) * 100
        
        total_market_val = holdings_df['市值(TWD)'].sum()
        total_holding_cost = holdings_df['成本(TWD)'].sum()
        total_pnl = (total_market_val - total_holding_cost) + realized_all

# B. 流動資金與負債計算
total_liquidity = st.session_state.liquidity_df['amount'].sum() if not st.session_state.liquidity_df.empty else 0
total_liabilities = st.session_state.liabilities_df['amount'].sum() if not st.session_state.liabilities_df.empty else 0
net_assets = total_market_val + total_liquidity - total_liabilities

# --- 自動存檔當日快照 ---
save_daily_snapshot(total_market_val, total_liquidity, total_liabilities, net_assets)

# --- 3. 精準時間段對比邏輯 ---
def get_historical_stats(days_back=None, start_date=None):
    if 'snapshots_df' not in st.session_state or st.session_state.snapshots_df.empty:
        return total_market_val, total_liquidity, total_liabilities, net_assets
    df_s = st.session_state.snapshots_df.copy()
    df_s['snapshot_date'] = pd.to_datetime(df_s['snapshot_date']).dt.date
    target_date = start_date if start_date else (datetime.now().date() - timedelta(days=days_back if days_back else 0))
    past_records = df_s[df_s['snapshot_date'] <= target_date]
    rec = past_records.iloc[0] if not past_records.empty else df_s.iloc[-1]
    return rec['market_value'], rec['liquidity_amount'], rec['liabilities_amount'], rec['net_assets']

# --- 4. 側邊欄：資產輸入 ---
with st.sidebar:
    st.title("🛡️ 雲端資產管理 V7.5")
    
    if st.session_state.user:
        user_email = st.session_state.user.email
        st.caption(f"👤 已登入: {user_email}")
        if st.button("登出系統", type="secondary"):
            # 登出
            st.session_state.auth_client.auth.sign_out()
            st.session_state.clear()
            st.rerun()
    st.divider()

    with st.form("trade_form", clear_on_submit=True):
        st.subheader("📝 新增投資交易")
        t_type = st.radio("交易類型", ["買入", "賣出"], horizontal=True)
        t_cat = st.selectbox("資產類別", ["台股", "美股", "加密貨幣"])
        t_ticker = st.text_input("標的代碼 (如 2330, TSLA)").upper().strip()
        t_qty = st.number_input("數量", min_value=0.0, format="%.4f")
        t_price = st.number_input("單價", min_value=0.0, format="%.4f")
        t_date = st.date_input("交易日期", datetime.now())
        if st.form_submit_button("✅ 存入雲端數據庫"):
            if t_ticker and t_qty > 0:
                data = {"user_id": st.session_state.user_id, "類型": t_type, "類別": t_cat, "代碼": t_ticker, "數量": t_qty, "單價": t_price, "日期": t_date.isoformat()}
                data_client.table("transactions").insert(data).execute()
                fetch_all_data(); st.rerun()

# --- 5. 主畫面內容 ---
tab1, tab_liab, tab2, tab3 = st.tabs(["📊 資產儀表板", "📉 負債管理", "💰 收入與流動資金", "🎯 FIRE 規劃"])

# --- Tab 1: 資產儀表板 ---
with tab1:
    title_col, filter_col = st.columns([3, 1])
    with title_col: st.subheader("📊 全球資產概況")
    with filter_col:
        time_range = st.selectbox("對比基準點", ["不對比", "日 (前一日)", "月 (前一月)", "年 (前一年)", "自定義"], label_visibility="collapsed")
    
    # 執行對比邏輯
    if time_range == "日 (前一日)": hist_m, hist_l, hist_liab, hist_net = get_historical_stats(days_back=1)
    elif time_range == "月 (前一月)": hist_m, hist_l, hist_liab, hist_net = get_historical_stats(days_back=30)
    elif time_range == "年 (前一年)": hist_m, hist_l, hist_liab, hist_net = get_historical_stats(days_back=365)
    elif time_range == "自定義":
        d_range = st.date_input("開始日期", value=(datetime.now() - timedelta(days=7)), label_visibility="collapsed")
        hist_m, hist_l, hist_liab, hist_net = get_historical_stats(start_date=d_range)
    else: hist_m, hist_l, hist_liab, hist_net = total_market_val, total_liquidity, total_liabilities, net_assets

    net_delta, liq_delta, mkt_delta = net_assets - hist_net, total_liquidity - hist_l, total_market_val - hist_m

    col_m1, col_m2, col_m3, col_m4, col_m5 = st.columns(5)
    delta_tag = f"({time_range})" if time_range != "不對比" else None
    
    with col_m1: st.metric("淨資產 (TWD)", f"NT$ {net_assets:,.0f}", delta=f"{net_delta:,.0f}" if delta_tag else None, help=delta_tag)
    with col_m2: st.metric("目前流動資金", f"NT$ {total_liquidity:,.0f}", delta=f"{liq_delta:,.0f}" if delta_tag else None)
    with col_m3:
        m_c = "#D62728" if mkt_delta >= 0 else "#2CA02C"
        delta_str = f"{mkt_delta:+,.0f}" if delta_tag else ""
        st.markdown(f"<p style='color:gray; font-size:16px;'>投資總市值</p><h2 style='margin-top:-15px;'>NT$ {total_market_val:,.0f}</h2><p style='color:{m_c}; font-size:14px; margin-top:-10px;'>{delta_str} <span style='color:gray;'>| 成本: {total_holding_cost:,.0f}</span></p>", unsafe_allow_html=True)
    with col_m4:
        p_c = "#D62728" if total_pnl >= 0 else "#2CA02C"
        st.markdown(f"<p style='color:gray; font-size:16px;'>累積總損益</p><h2 style='color:{p_c}; margin-top:-15px;'>NT$ {total_pnl:,.0f}</h2><p style='color:{p_c}; font-size:14px; margin-top:-10px;'>{(total_pnl/total_holding_cost*100 if total_holding_cost else 0):+.2f}% (ROI)</p>", unsafe_allow_html=True)
    with col_m5: st.metric("總負債額", f"NT$ {total_liabilities:,.0f}", delta=f"-{total_liabilities:,.0f}" if total_liabilities > 0 else None, delta_color="inverse")

    st.divider()

    c_l, c_r = st.columns([2, 1])
    with c_l:
        if not st.session_state.snapshots_df.empty:
            df_plot = st.session_state.snapshots_df.sort_values('snapshot_date')
            
            # 1. 定義中文名稱對照表
            name_map = {
                'net_assets': '淨資產',
                'market_value': '市場價值',
                'liquidity_amount': '流動金額'
            }
            
            # 2. 繪圖
            fig = px.line(
                df_plot, 
                x='snapshot_date', 
                y=list(name_map.keys()), 
                title="資產歷史趨勢"
            )
            
            # 3. 強制修改線條名稱
            fig.for_each_trace(lambda t: t.update(name = name_map.get(t.name, t.name)))
            
            # 4. 修改圖例標題
            fig.update_layout(legend_title_text='資產種類')
            
            st.plotly_chart(fig, use_container_width=True)
    with c_r:
        pie_df = pd.DataFrame({"項目": ["投資", "流動資金", "負債"], "金額": [total_market_val, total_liquidity, total_liabilities]})
        st.plotly_chart(px.pie(pie_df, values='金額', names='項目', hole=0.4, color_discrete_sequence=["#ff7f0e", "#2ca02c", "#d62728"]), use_container_width=True)

    st.divider()
    asset_tabs = st.tabs(["🇹🇼 台股", "🇺🇸 美股", "🪙 加密貨幣"])
    cat_map = {"台股": "🇹🇼 台股", "美股": "🇺🇸 美股", "加密貨幣": "🪙 加密貨幣"}
    for i, (internal_cat, display_cat) in enumerate(cat_map.items()):
        with asset_tabs[i]:
            df_sub = holdings_df[holdings_df['類別'] == internal_cat] if not holdings_df.empty else pd.DataFrame()
            if not df_sub.empty:
                st.plotly_chart(px.bar(df_sub.sort_values('市值(TWD)'), x='市值(TWD)', y='顯示名稱', orientation='h', text_auto='.2s', color='市值(TWD)', title=f"{internal_cat} 標的占比"), use_container_width=True)
                st.dataframe(df_sub[['顯示名稱', '持倉數量', '平均成本', '現價', '市值(TWD)', '損益(TWD)', '報酬率']].style.format({'市值(TWD)': '{:,.0f}', '損益(TWD)': '{:,.0f}', '報酬率': '{:+.2f}%', '現價': '{:,.2f}', '平均成本': '{:,.2f}'}), use_container_width=True)
                s_v, s_p, s_c = df_sub['市值(TWD)'].sum(), df_sub['損益(TWD)'].sum(), df_sub['成本(TWD)'].sum()
                sc1, sc2, sc3 = st.columns(3)
                sc1.metric("總市值", f"NT$ {s_v:,.0f}")
                if internal_cat != "台股": sc1.caption(f"📏 換算匯率: 1 USD = {current_ex_rate:.2f} TWD")
                sc2.metric("總損益", f"NT$ {s_p:,.0f}", delta=f"{s_p:,.0f}")
                sc3.metric("報酬率", f"{(s_p/s_c*100 if s_c != 0 else 0):.2f}%")

# --- Tab: 負債管理 ---
with tab_liab:
    st.header("📉 負債與貸款管理")
    l_col1, l_col2 = st.columns([1, 1.5])
    with l_col1:
        st.subheader("🖋️ 紀錄負債項目")
        with st.form("liab_form"):
            l_cat, l_name = st.selectbox("負債類別", ["台股融資", "美股融資", "信貸", "其他"]), st.text_input("項目名稱")
            l_amt = st.number_input("欠款金額 (TWD)", min_value=0.0)
            if st.form_submit_button("💾 儲存負債"):
                data_client.table("liabilities").upsert({"user_id": st.session_state.user_id, "category": l_cat, "name": l_name if l_name else l_cat, "amount": l_amt, "updated_at": datetime.now().isoformat()}, on_conflict='user_id, name').execute()
                fetch_all_data(); st.rerun()
    with l_col2:
        if not st.session_state.liabilities_df.empty: st.dataframe(st.session_state.liabilities_df[['category', 'name', 'amount', 'updated_at']], use_container_width=True)

# --- Tab 2: 收入與流動資金 (整合您的 PR 分析與我的帳戶管理) ---
with tab2:
    st.header("💰 收入與流動資金管理")
    
    # 上半部：流動資金帳戶管理
    st.subheader("💵 流動資金帳戶明細 (TWD)")
    liq_col1, liq_col2 = st.columns([1, 1.5])
    with liq_col1:
        with st.form("liquidity_form"):
            acc_name, acc_amt = st.text_input("帳戶名稱"), st.number_input("金額 (TWD)", min_value=0.0)
            if st.form_submit_button("💾 儲存帳戶"):
                if acc_name:
                    data_client.table("liquidity").upsert({"user_id": st.session_state.user_id, "account_name": acc_name, "amount": acc_amt, "updated_at": datetime.now().isoformat()}, on_conflict='user_id, account_name').execute()
                    fetch_all_data(); st.rerun()
    with liq_col2:
        if not st.session_state.liquidity_df.empty:
            st.dataframe(st.session_state.liquidity_df[['account_name', 'amount', 'updated_at']], use_container_width=True)
            st.metric("總流動資金加總", f"NT$ {total_liquidity:,.0f}")

    st.divider()

    # 下半部：恢復您的完整收入管理邏輯
    st.subheader("💰 收入與薪資 PR 分析")
    col_in1, col_in2 = st.columns(2)
    with col_in1:
        st.subheader("🖋️ 紀錄新收入")
        in_mode = st.radio("輸入方式", ["薪資+獎金", "直接年收"], horizontal=True)
        with st.form("income_form"):
            if in_mode == "薪資+獎金":
                m = st.number_input("月薪", min_value=0, step=1000)
                b = st.number_input("獎金/其他", min_value=0, step=1000)
                ann = (m * 12) + b
                st.info(f"計算出的總年收：NT$ {ann:,.0f}")
            else:
                ann = st.number_input("年收總計", min_value=0, step=10000)
            note = st.text_input("備註 (例如: 2026年薪)")
            if st.form_submit_button("💾 儲存收入紀錄"):
                data_client.table("income_history").insert({"user_id": st.session_state.user_id, "紀錄日期": datetime.now().isoformat(), "年收入": ann, "備註": note}).execute()
                fetch_all_data(); st.rerun()

    with col_in2:
        st.subheader("📈 歷史收入與 PR")
        if not st.session_state.income_df.empty:
            curr_ann = st.session_state.income_df['年收入'].iloc[-1]
            ann_wan = curr_ann / 10000
            user_pr = 0
            for pr, val in sorted(PR_DATA_113.items()):
                if ann_wan >= val: user_pr = pr
            st.metric("當前紀錄年收", f"NT$ {curr_ann:,.0f}", help="以最後一筆紀錄為準")
            st.markdown(f"您的年薪領先全台約 **{user_pr}%** 的受薪階級。")
            
            st.write("歷史紀錄 (可直接編輯)")
            edited_in = st.data_editor(st.session_state.income_df.copy(), num_rows="dynamic", disabled=['id'])
            if st.button("🚀 同步更新收入資料"):
                st.warning("同步功能開發中，建議目前以新增為主。")

# --- Tab 3: FIRE 規劃 (恢復您的完整介面與說明) ---
with tab3:
    st.header("🎯 FIRE 退休規劃")
    col_f1, col_f2 = st.columns([1, 1.5])
    
    with col_f1:
        st.subheader("⚙️ 設定退休目標")
        settings = st.session_state.settings
        f_mode = st.radio("目標設定方式", ["依月開銷推算 (25倍法則)", "自定義目標"], 
                          index=0 if settings.get('fire_mode') == "依月開銷推算 (25倍法則)" else 1)
        
        if f_mode == "依月開銷推算 (25倍法則)":
            m_exp = st.number_input("退休後預估每月生活費", value=int(settings.get('monthly_expense', 80000)), step=1000)
            ann_exp = m_exp * 12
            fire_target = ann_exp * 25
            st.info(f"🔹 預估年支出：NT$ {ann_exp:,.0f}")
            st.markdown(f"🚩 自動算出目標：**NT$ {fire_target:,.0f}**")
        else:
            fire_target = st.number_input("自定義目標金額", value=int(settings.get('custom_target', 15000000)), step=100000)
            m_exp = settings.get('monthly_expense', 80000)

        with st.expander("💡 為何是 25 倍？"):
            st.write("這源自『4% 法則』：當資產達到年支出的 25 倍，每年提取 4% 生活費，資金有極高機率永遠領不完。")
        
        if st.button("💾 儲存退休設定"):
            data_client.table("user_settings").upsert({"user_id": st.session_state.user_id, "monthly_expense": m_exp, "custom_target": fire_target, "fire_mode": f_mode}).execute()
            fetch_all_data(); st.rerun()

    with col_f2:
        st.subheader("📊 財富森林成長進度")
        if fire_target > 0:
            # 這裡改用淨資產 net_assets 作為達成率基準，更精確
            rate = (net_assets / fire_target * 100)
            missing = fire_target - net_assets
            
            st.write(f"### 目前淨資產：NT$ {net_assets:,.0f}")
            st.write(f"### 目標金額：NT$ {fire_target:,.0f}")
            
            # 成長階段與 Icon (恢復您的完整清單)
            stages = [("種子", "🫘"), ("萌芽", "🌱"), ("幼苗", "🌿"), ("茁壯", "🪴"), ("繁葉", "🍃"), 
                      ("枝繁葉茂", "🌿✨"), ("林木", "🌲"), ("深根", "🌳"), ("碩果", "🍎🌳"), ("圓滿", "🏆🌳"), ("森林", "🌳🌲🎊")]
            idx = min(int(max(0, rate) // 10), 10)
            name, icon = stages[idx]
            
            st.markdown(f"<div style='text-align: center; background-color: #f0f2f6; padding: 20px; border-radius: 10px;'>"
                        f"<h1 style='font-size: 80px; margin: 0;'>{icon}</h1>"
                        f"<h2 style='margin: 0;'>等級：{name}</h2>"
                        f"</div>", unsafe_allow_html=True)
            
            st.divider()
            st.progress(min(max(rate/100, 0.0), 1.0))
            
            c_r1, c_r2 = st.columns(2)
            c_r1.metric("FIRE 達成率", f"{rate:.2f}%")
            c_r2.metric("尚欠金額", f"NT$ {max(0, missing):,.0f}", delta=f"-{max(0, missing):,.0f}", delta_color="inverse")

# --- 底部流水帳 ---
st.divider()
st.subheader("📜 歷史交易編輯")
if not st.session_state.transactions.empty:
    disp_tx = detailed_tx_global.copy().sort_values('日期', ascending=False)
    disp_tx['日期'] = pd.to_datetime(disp_tx['日期']).dt.date
    st.data_editor(disp_tx[['id', '日期', '類型', '類別', '代碼', '數量', '單價']], use_container_width=True, disabled=['id'])