import pandas as pd
import streamlit as st
from utils import supabase, get_display_name

def clean_df(df):
    if df.empty: return df
    df = df.loc[:, ~df.columns.duplicated()] 
    df.columns = [str(c).strip() for c in df.columns]
    if '代碼' in df.columns: 
        df['代碼'] = df['代碼'].astype(str).str.strip().str.upper()
    if '日期' in df.columns: 
        df['日期'] = pd.to_datetime(df['日期'], errors='coerce')
    return df.drop_duplicates().reset_index(drop=True)

def fetch_all_data():
    u_id = st.session_state.user_id
    
    # 1. 交易紀錄
    tx_res = supabase.table("transactions").select("*").eq("user_id", u_id).order("日期", desc=True).execute()
    st.session_state.transactions = clean_df(pd.DataFrame(tx_res.data))
    
    # 2. 收入歷史
    in_res = supabase.table("income_history").select("*").eq("user_id", u_id).execute()
    st.session_state.income_df = pd.DataFrame(in_res.data)
    
    # 3. 負債資料
    liab_res = supabase.table("liabilities").select("*").eq("user_id", u_id).execute()
    st.session_state.liabilities_df = pd.DataFrame(liab_res.data)
    
    # 4. 流動資金 (多帳戶)
    liq_res = supabase.table("liquidity").select("*").eq("user_id", u_id).execute()
    st.session_state.liquidity_df = pd.DataFrame(liq_res.data)
    
    # 5. 現金歷史 (備用)
    cash_res = supabase.table("cash_history").select("*").eq("user_id", u_id).order("record_date", desc=True).execute()
    st.session_state.cash_df = pd.DataFrame(cash_res.data)

    # 6. 新增：資產快照紀錄
    snap_res = supabase.table("portfolio_snapshots").select("*").eq("user_id", u_id).order("snapshot_date", desc=True).execute()
    st.session_state.snapshots_df = pd.DataFrame(snap_res.data)
    
    # 7. 使用者設定
    set_res = supabase.table("user_settings").select("*").eq("user_id", u_id).execute()
    if set_res.data: 
        st.session_state.settings = set_res.data[0]
    else: 
        st.session_state.settings = {"monthly_expense": 80000, "fire_mode": "依月開銷推算 (25倍法則)", "custom_target": 24000000}

def save_daily_snapshot(m_val, l_val, liab_val, net_val):
    """每日自動存檔當前資產狀態"""
    u_id = st.session_state.user_id
    data = {
        "user_id": u_id,
        "market_value": m_val,
        "liquidity_amount": l_val,
        "liabilities_amount": liab_val,
        "net_assets": net_val,
        "snapshot_date": pd.Timestamp.now().date().isoformat()
    }
    # 使用 upsert，如果今天存過了就更新今天的紀錄
    try:
        supabase.table("portfolio_snapshots").upsert(data, on_conflict='user_id, snapshot_date').execute()
    except:
        pass

def calculate_detailed_metrics(df, ex_rate):
    if df.empty: return pd.DataFrame(), 0, df.assign(**{'每筆損益(原幣)': 0.0})
    holdings, realized_pnl_twd = [], 0 
    temp_df = df.sort_values('日期').copy()
    tracker, row_pnls_orig = {}, []
    
    for _, row in temp_df.iterrows():
        t, cat = str(row['代碼']).strip().upper(), row['類別']
        if t not in tracker: tracker[t] = {'qty': 0, 'cost_basis': 0}
        pnl = 0
        if row['類型'] == '買入':
            new_qty = tracker[t]['qty'] + row['數量']
            if new_qty > 0: 
                tracker[t]['cost_basis'] = ((tracker[t]['qty'] * tracker[t]['cost_basis']) + (row['數量'] * row['單價'])) / new_qty
            tracker[t]['qty'] = new_qty
        else:
            pnl = (row['單價'] - tracker[t]['cost_basis']) * row['數量']
            realized_pnl_twd += (pnl * (1.0 if cat == "台股" else ex_rate))
            tracker[t]['qty'] -= row['數量']
        row_pnls_orig.append(pnl)
        
    for t, info in tracker.items():
        if info['qty'] > 0.0001:
            match = df[df['代碼'] == t]
            cat = match['類別'].iloc[0] if not match.empty else "台股"
            holdings.append({'代碼': t, '顯示名稱': get_display_name(t), '持倉數量': info['qty'], '平均成本': info['cost_basis'], '類別': cat})
    
    temp_df['每筆損益(原幣)'] = row_pnls_orig
    return pd.DataFrame(holdings), realized_pnl_twd, temp_df