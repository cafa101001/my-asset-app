import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

import plotly.express as px
import plotly.graph_objects as go

from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import json
import streamlit.components.v1 as components
import requests
import re
from bs4 import BeautifulSoup

# --- 關鍵匯入 ---
# 引入 utils 中的 update_supabase_session 來同步權限
from utils import supabase as data_client, get_market_data, update_supabase_session

# 嘗試匯入 Supabase Client 設定，若版本過舊則提示
try:
    from supabase import create_client
    try:
        # 部分版本直接提供 ClientOptions
        from supabase import ClientOptions  # type: ignore
    except Exception:
        # 新版常見路徑
        from supabase.lib.client_options import ClientOptions  # type: ignore
except Exception:
    st.error("❌ 偵測到 Supabase 套件版本過舊或未安裝。請確認 requirements.txt 內有 `supabase`，並重新部署。")
    st.stop()

from logic import fetch_all_data, calculate_detailed_metrics, clean_df, save_daily_snapshot

# --- 1. 頁面基礎設定 ---
st.set_page_config(page_title="全球資產管理系統 V7.5", layout="wide")

# ==========================================
#      🇹🇼 台股代碼 -> 中文名稱（快取）
# ==========================================

def _norm_twse_text(s: str) -> str:
    s = str(s).replace("\u3000", " ").replace("　", " ").strip()
    s = re.sub(r"\s+", " ", s)
    return s

def _parse_isin_table(html: str) -> dict:
    """解析 TWSE ISIN 清單頁：取出『代號 -> 中文名稱』"""
    mp: dict = {}
    soup = BeautifulSoup(html, "html.parser")

    tables = soup.find_all("table")
    if not tables:
        return mp

    # 優先找含「有價證券代號及名稱」字樣的表格
    target = None
    for tbl in tables:
        if "有價證券代號及名稱" in tbl.get_text():
            target = tbl
            break

    # 找不到就取 tr 最多的那張表（保底）
    if target is None:
        target = max(tables, key=lambda t: len(t.find_all("tr")))

    for tr in target.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue

        cells = [_norm_twse_text(td.get_text(" ", strip=True)) for td in tds]
        if not cells:
            continue

        first = cells[0]
        if not first:
            continue
        if "有價證券代號及名稱" in first:
            continue

        code = None
        name = None

        # Case 1：第一欄就是「2330 台積電」這種格式
        m = re.match(r"^([0-9A-Za-z]{4,8})\s+(.+)$", first)
        if m:
            c = m.group(1).strip().upper()
            n = m.group(2).strip()
            if any(ch.isdigit() for ch in c) and n:
                code, name = c, n

        # Case 2：欄位分開（第一欄是代碼、第二欄是名稱）
        if code is None and re.fullmatch(r"[0-9A-Za-z]{4,8}", first) and len(cells) >= 2:
            c = first.strip().upper()
            n = cells[1].strip()
            if any(ch.isdigit() for ch in c) and n:
                code, name = c, n

        if not code or not name:
            continue

        mp[code] = name
        if code.isdigit():
            mp[f"{code}.TW"] = name

    return mp

@st.cache_data(ttl=86400, show_spinner=False)
def _load_twse_stock_map(_cache_bust: str = "v3") -> dict:
    """抓取上市/上櫃清單並合併（成功才會被 cache；失敗會丟例外避免 cache 空結果）"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    }

    mp: dict = {}

    # strMode=2：上市、ETF 等；strMode=4：上櫃
    for mode in ("2", "4"):
        url = f"https://isin.twse.com.tw/isin/C_public.jsp?strMode={mode}"
        r = requests.get(url, headers=headers, timeout=30)
        # ISIN 清單頁多為 Big5；避免 requests 誤判成 ISO-8859-1
        if (not r.encoding) or (r.encoding.lower() == "iso-8859-1"):
            r.encoding = "big5"
        mp.update(_parse_isin_table(r.text))

    # 防呆：如果太小，代表抓取/解析失敗，不要 cache
    if len(mp) < 500:
        raise RuntimeError(f"TWSE mapping too small: {len(mp)}")

    return mp

def get_twse_stock_map() -> dict:
    """回傳台股代碼->中文名稱對照表（快取 1 天）。"""
    try:
        # 透過 cache_bust 版本字串確保部署更新後會重新抓取
        return _load_twse_stock_map(_cache_bust="v3_2026-01-09")
    except Exception as e:
        # 不要 st.error 以免打斷流程，改用 log
        print(f"TWSE 清單抓取/解析失敗: {e}")
        return {}

@st.cache_data(ttl=86400, show_spinner=False)
def _twse_code_query(code: str) -> str:
    """若全量清單抓不到，用 TWSE codeQuery 以代碼查名稱（結果會 cache）。"""
    code = str(code).strip().upper().replace(".TW", "").replace(".TWO", "")
    if not code:
        return ""

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    }
    url = f"https://www.twse.com.tw/zh/api/codeQuery?query={code}"
    r = requests.get(url, headers=headers, timeout=10)
    if r.status_code != 200:
        return ""
    try:
        j = r.json()
    except Exception:
        return ""

    sugs = j.get("suggestions") or []
    for s in sugs:
        s = str(s)
        parts = s.split("\t")
        if parts and parts[0].strip() == code:
            if len(parts) > 1 and parts[1].strip():
                return parts[1].strip()

    # fallback：有些格式可能是「2330 台積電」
    for s in sugs:
        ss = _norm_twse_text(s)
        if ss.startswith(code + " "):
            return ss[len(code) + 1 :].strip()

    return ""

def get_tw_stock_name(code: str):
    """回傳台股中文名稱；查不到則回傳 None"""
    base = str(code).strip().upper().replace(".TW", "").replace(".TWO", "")
    if not base:
        return None

    mp = get_twse_stock_map()
    if mp:
        name = mp.get(base) or mp.get(f"{base}.TW")
        if name:
            return name

    # 全量清單抓不到時的保底查詢（單筆查詢也會 cache）
    qname = _twse_code_query(base)
    return qname if qname else None


def _format_dt_series(s: pd.Series) -> pd.Series:
    """把時間欄位格式化為 YYYY-MM-DD HH:MM（支援 timezone-aware / naive）"""
    dt = pd.to_datetime(s, errors="coerce")
    try:
        if getattr(dt.dt, "tz", None) is not None:
            dt = dt.dt.tz_convert("Asia/Taipei").dt.tz_localize(None)
    except Exception:
        pass
    return dt.dt.strftime("%Y-%m-%d %H:%M")

def _normalize_id(v):
    if v is None:
        return None
    try:
        if isinstance(v, float) and pd.isna(v):
            return None
    except Exception:
        pass
    try:
        return int(v)
    except Exception:
        return str(v)

def _safe_float(v, default=0.0):
    try:
        if v is None:
            return default
        if isinstance(v, float) and pd.isna(v):
            return default
        return float(v)
    except Exception:
        return default

def _delete_rows_by_ids(table_name: str, ids: list):
    """依 id 刪除多筆資料（Supabase PostgREST）"""
    ids = [i for i in ids if i is not None]
    if not ids:
        return
    try:
        data_client.table(table_name).delete().in_("id", ids).execute()
    except Exception:
        # fallback: 逐筆刪除
        for _id in ids:
            data_client.table(table_name).delete().eq("id", _id).execute()
# ==========================================
#      📝 Data Editor 同步（編輯/刪除 -> Supabase）
# ==========================================

def _sync_liabilities(original_df: pd.DataFrame, edited_df_zh: pd.DataFrame):
    """同步『負債管理』表格：支援編輯、刪除、（可選）新增"""
    if edited_df_zh is None:
        return

    inv = {"負債類別": "category", "項目名稱": "name", "金額(TWD)": "amount"}
    df = edited_df_zh.rename(columns=inv).copy()

    if "id" not in df.columns:
        st.error("❌ 負債表格缺少 id 欄位，無法同步")
        return

    # 1) 刪除：原本有、現在沒有的 id
    orig_ids = set()
    if original_df is not None and (not original_df.empty) and "id" in original_df.columns:
        orig_ids = set(_normalize_id(x) for x in original_df["id"].dropna())
    new_ids = set(_normalize_id(x) for x in df["id"].dropna())
    del_ids = [i for i in orig_ids if i not in new_ids]
    _delete_rows_by_ids("liabilities", del_ids)

    # 2) 更新 / 新增
    now_iso = datetime.now().isoformat()
    user_id = st.session_state.user_id

    for _, row in df.iterrows():
        rid = _normalize_id(row.get("id"))
        name = str(row.get("name") or "").strip()
        if not name:
            continue  # 忽略空白列

        cat = str(row.get("category") or "").strip() or "其他"
        amt = _safe_float(row.get("amount"), 0.0)

        if rid is None:
            # 新增：用 upsert（避免重複 name）
            data_client.table("liabilities").upsert(
                {"user_id": user_id, "category": cat, "name": name, "amount": amt, "updated_at": now_iso},
                on_conflict="user_id, name",
            ).execute()
        else:
            data_client.table("liabilities").update(
                {"category": cat, "name": name, "amount": amt, "updated_at": now_iso}
            ).eq("id", rid).execute()

def _sync_liquidity(original_df: pd.DataFrame, edited_df_zh: pd.DataFrame):
    """同步『流動資金』表格：支援編輯、刪除、（可選）新增"""
    if edited_df_zh is None:
        return

    inv = {"帳戶名稱": "account_name", "金額(TWD)": "amount"}
    df = edited_df_zh.rename(columns=inv).copy()

    if "id" not in df.columns:
        st.error("❌ 流動資金表格缺少 id 欄位，無法同步")
        return

    orig_ids = set()
    if original_df is not None and (not original_df.empty) and "id" in original_df.columns:
        orig_ids = set(_normalize_id(x) for x in original_df["id"].dropna())
    new_ids = set(_normalize_id(x) for x in df["id"].dropna())
    del_ids = [i for i in orig_ids if i not in new_ids]
    _delete_rows_by_ids("liquidity", del_ids)

    now_iso = datetime.now().isoformat()
    user_id = st.session_state.user_id

    for _, row in df.iterrows():
        rid = _normalize_id(row.get("id"))
        acc = str(row.get("account_name") or "").strip()
        if not acc:
            continue

        amt = _safe_float(row.get("amount"), 0.0)

        if rid is None:
            data_client.table("liquidity").upsert(
                {"user_id": user_id, "account_name": acc, "amount": amt, "updated_at": now_iso},
                on_conflict="user_id, account_name",
            ).execute()
        else:
            data_client.table("liquidity").update(
                {"account_name": acc, "amount": amt, "updated_at": now_iso}
            ).eq("id", rid).execute()

def _sync_income_history(original_df: pd.DataFrame, edited_df_zh: pd.DataFrame):
    """同步『收入』表格：支援編輯、刪除、（可選）新增"""
    if edited_df_zh is None:
        return

    df = edited_df_zh.copy()
    # 顯示用欄位，不回寫資料庫
    if "上傳時間" in df.columns:
        df = df.drop(columns=["上傳時間"])

    if "id" not in df.columns:
        st.error("❌ 收入表格缺少 id 欄位，無法同步")
        return

    orig_ids = set()
    if original_df is not None and (not original_df.empty) and "id" in original_df.columns:
        orig_ids = set(_normalize_id(x) for x in original_df["id"].dropna())
    new_ids = set(_normalize_id(x) for x in df["id"].dropna())
    del_ids = [i for i in orig_ids if i not in new_ids]
    _delete_rows_by_ids("income_history", del_ids)

    user_id = st.session_state.user_id

    for _, row in df.iterrows():
        rid = _normalize_id(row.get("id"))
        ann = row.get("年收入")
        note = str(row.get("備註") or "").strip()

        if ann is None or (isinstance(ann, float) and pd.isna(ann)):
            # 忽略空白列
            if rid is None:
                continue
            ann_val = None
        else:
            try:
                ann_val = int(float(ann))
            except Exception:
                ann_val = None

        if rid is None:
            if ann_val is None:
                continue
            data_client.table("income_history").insert(
                {"user_id": user_id, "紀錄日期": datetime.now().isoformat(), "年收入": ann_val, "備註": note}
            ).execute()
        else:
            payload = {}
            if ann_val is not None:
                payload["年收入"] = ann_val
            payload["備註"] = note
            if payload:
                data_client.table("income_history").update(payload).eq("id", rid).execute()

def _sync_transactions(original_df: pd.DataFrame, edited_df: pd.DataFrame):
    """同步『交易』表格：支援編輯、刪除、（可選）新增"""
    if edited_df is None:
        return

    df = edited_df.copy()
    # 顯示用欄位，不回寫資料庫
    if "台股名稱" in df.columns:
        df = df.drop(columns=["台股名稱"])

    if "id" not in df.columns:
        st.error("❌ 交易表格缺少 id 欄位，無法同步")
        return

    orig_ids = set()
    if original_df is not None and (not original_df.empty) and "id" in original_df.columns:
        orig_ids = set(_normalize_id(x) for x in original_df["id"].dropna())
    new_ids = set(_normalize_id(x) for x in df["id"].dropna())
    del_ids = [i for i in orig_ids if i not in new_ids]
    _delete_rows_by_ids("transactions", del_ids)

    user_id = st.session_state.user_id

    for _, row in df.iterrows():
        rid = _normalize_id(row.get("id"))
        t_type = str(row.get("類型") or "").strip()
        t_cat = str(row.get("類別") or "").strip()
        ticker = str(row.get("代碼") or "").upper().strip()
        qty = _safe_float(row.get("數量"), 0.0)
        price = _safe_float(row.get("單價"), 0.0)
        date_v = row.get("日期")

        # 忽略空白列
        if not ticker or qty <= 0:
            if rid is None:
                continue

        try:
            date_iso = pd.to_datetime(date_v, errors="coerce").date().isoformat() if date_v else None
        except Exception:
            date_iso = None

        payload = {
            "user_id": user_id,
            "類型": t_type,
            "類別": t_cat,
            "代碼": ticker,
            "數量": qty,
            "單價": price,
            "日期": date_iso,
        }

        # 移除 None，避免寫入失敗
        payload = {k: v for k, v in payload.items() if v is not None}

        if rid is None:
            data_client.table("transactions").insert(payload).execute()
        else:
            payload.pop("user_id", None)  # 更新時不必動到 user_id
            data_client.table("transactions").update(payload).eq("id", rid).execute()

# ==========================================
#      🔐 登入邏輯 (Session Storage + Sync)
# ==========================================

# 1. 初始化 Session State
if "user" not in st.session_state:
    st.session_state.user = None
if "user_id" not in st.session_state:
    st.session_state.user_id = None


class StreamlitSessionStorage:
    """讓 supabase/auth-py 能把 PKCE verifier 與 session token 存在 Streamlit 的 session_state 內。"""

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
if "auth_client" not in st.session_state:
    try:
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_KEY"]
    except Exception:
        st.error("❌ 找不到 SUPABASE_URL / SUPABASE_KEY。請在 Streamlit secrets 設定。")
        st.stop()

    try:
        st.session_state.auth_client = create_client(
            url,
            key,
            options=ClientOptions(
                storage=StreamlitSessionStorage(),
                flow_type="pkce",
            ),
        )
    except Exception as e:
        st.error(f"❌ Auth Client 初始化失敗: {e}")
        st.stop()


def get_query_params():
    try:
        return st.query_params
    except Exception:
        return st.experimental_get_query_params()


def clear_url():
    try:
        st.query_params.clear()
    except Exception:
        st.experimental_set_query_params()


def _first(v):
    """把 query param 的值統一成單一字串"""
    if v is None:
        return None
    if isinstance(v, list):
        return v[0] if v else None
    return v


def _find_code_verifier(storage: dict):
    """從 storage 找到 (verifier_key, verifier_value)"""
    if not isinstance(storage, dict):
        return None, None

    # 先嘗試常見 key
    common_keys = [
        "supabase.auth.token-code-verifier",
        "supabase.auth.token-code_verifier",
        "code_verifier",
        "code-verifier",
    ]
    for k in common_keys:
        v = storage.get(k)
        if isinstance(v, str) and v.strip():
            return k, v

    # 再嘗試所有包含 verifier 的 key
    for k, v in storage.items():
        if not v:
            continue
        lk = str(k).lower()
        if "code-verifier" in lk or "code_verifier" in lk or "verifier" in lk:
            vv = str(v)
            if vv.strip():
                return k, vv

    return None, None


def _inject_cv_into_redirect_to(oauth_url: str, cv_key: str, cv_value: str) -> str:
    """把 code_verifier 放到 oauth_url 的 redirect_to 裡（以 cv/cvk query 帶回）"""
    u = urlparse(oauth_url)
    qs = parse_qs(u.query)

    redirect_to = qs.get("redirect_to", [None])[0] or qs.get("redirectTo", [None])[0]
    if not redirect_to:
        return oauth_url

    ru = urlparse(redirect_to)
    rqs = parse_qs(ru.query)
    rqs["cv"] = [cv_value]
    rqs["cvk"] = [cv_key]

    new_redirect_to = urlunparse(ru._replace(query=urlencode(rqs, doseq=True)))

    if "redirect_to" in qs:
        qs["redirect_to"] = [new_redirect_to]
    elif "redirectTo" in qs:
        qs["redirectTo"] = [new_redirect_to]

    return urlunparse(u._replace(query=urlencode(qs, doseq=True)))


def handle_login():
    """處理登入流程與同步（Supabase OAuth code -> session）"""
    auth_client = st.session_state.get("auth_client")
    if auth_client is None:
        st.error("❌ auth_client 尚未初始化（st.session_state.auth_client 不存在）")
        st.stop()

    # 1) 嘗試從既有 session 恢復
    try:
        session = auth_client.auth.get_session()
        if session and getattr(session, "user", None):
            st.session_state.user = session.user
            st.session_state.user_id = session.user.id
            update_supabase_session(session.access_token, session.refresh_token)
            return
    except Exception:
        pass

    # 2) 處理 OAuth 回調：URL query 內的 code + (cv/cvk)
    params = get_query_params()
    code = _first(params.get("code"))
    cv = _first(params.get("cv"))
    cvk = _first(params.get("cvk"))

    if code:
        try:
            # ✅ 若有 cv，就先把 verifier 放回 storage，讓 exchange_code_for_session 找得到
            if cv:
                if "supabase_auth_storage" not in st.session_state:
                    st.session_state.supabase_auth_storage = {}

                if cvk:
                    st.session_state.supabase_auth_storage[cvk] = cv

                # 再保險：補一個常見 key（不同版本可能會用到）
                st.session_state.supabase_auth_storage["supabase.auth.token-code-verifier"] = cv

            # ✅ 重要：Python 版用 dict 參數，不要傳純字串
            res = auth_client.auth.exchange_code_for_session({"auth_code": code})

            session = getattr(res, "session", None)
            user = getattr(res, "user", None)

            if user and session:
                st.session_state.user = user
                st.session_state.user_id = user.id
                update_supabase_session(session.access_token, session.refresh_token)

                clear_url()
                st.rerun()
            else:
                st.error("❌ 交換 session 失敗：res.user 或 res.session 為空")
                st.write(res)
                st.stop()

        except Exception as e:
            st.error(f"❌ exchange_code_for_session 失敗：{e}")
            st.write("Query params:", params)
            st.stop()


def show_login_UI():
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.title("🔐 全球資產管理系統 V7.5")
        st.markdown("### 請登入以存取您的個人資產數據")

        # 預設使用 secrets 的雲端網址，否則退回 localhost（僅本機測試用）
        try:
            default_redirect_url = st.secrets["REDIRECT_URL"]
        except Exception:
            default_redirect_url = "http://localhost:8501"

        with st.expander("⚙️ 設定登入回調網址 (若無法登入請檢查)", expanded=False):
            redirect_url = st.text_input("Redirect URL", value=default_redirect_url).strip()

            if ("localhost" in redirect_url) or ("127.0.0.1" in redirect_url):
                st.warning(
                    "⚠️ 你目前的 Redirect URL 是 localhost。\n\n"
                    "如果你部署在 Streamlit Cloud，這裡必須填你的雲端網址，例如：\n"
                    "`https://my-wealth-v7.streamlit.app`"
                )

        if st.button("🚀 使用 Google 帳號登入", type="primary", use_container_width=True):
            try:
                res = st.session_state.auth_client.auth.sign_in_with_oauth(
                    {
                        "provider": "google",
                        "options": {
                            "redirect_to": redirect_url,
                            "query_params": {
                                "access_type": "offline",
                                "prompt": "consent select_account",
                            },
                        },
                    }
                )

                oauth_url = getattr(res, "url", None)
                if not oauth_url:
                    st.error("❌ 無法取得 OAuth URL（res.url 為空）")
                    st.stop()

                # ✅ 取得 code_verifier（此時通常已被 SDK 存進 storage）
                storage = st.session_state.get("supabase_auth_storage", {}) or {}
                cvk, cv = _find_code_verifier(storage)
                if not cvk or not cv:
                    st.error("❌ 找不到 PKCE code_verifier（supabase_auth_storage 內沒有 verifier）")
                    st.write("storage keys:", list(storage.keys()))
                    st.stop()

                # ✅ 把 verifier 注入 redirect_to query，讓回跳時帶回來
                oauth_url2 = _inject_cv_into_redirect_to(oauth_url, str(cvk), str(cv))

                # ✅ 同分頁自動跳轉（避免開新分頁造成 session 遺失）
                components.html(
                    f"""
                    <script>
                      window.location.href = {json.dumps(oauth_url2)};
                    </script>
                    """,
                    height=0,
                )

                # 保底：若瀏覽器擋 script，仍提供可點連結
                st.markdown(f"[👉 若未自動跳轉，請點此登入 Google]({oauth_url2})")
                st.stop()

            except Exception as e:
                st.error(f"❌ 初始化失敗: {e}")
                st.stop()


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
        # ✅ 台股代碼 -> 中文名稱（第一次會抓取全量清單並快取）
        if '顯示名稱' not in holdings_df.columns:
            holdings_df['顯示名稱'] = holdings_df['代碼']
        mask_tw = holdings_df['類別'] == '台股'
        if mask_tw.any():
            def _tw_disp(code):
                base = str(code).upper().replace('.TW', '').strip()
                name = get_tw_stock_name(base)
                return f"{name}({base})" if name else base
            holdings_df.loc[mask_tw, '顯示名稱'] = holdings_df.loc[mask_tw, '代碼'].apply(_tw_disp)

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

        # 台股代碼即時顯示中文名稱（第一次會抓取全量清單並快取）
        if t_cat == "台股" and t_ticker:
            tw_name = get_tw_stock_name(t_ticker)
            if tw_name:
                st.caption(f"📌 股票名稱：{tw_name}")
            else:
                st.caption("⚠️ 查無此台股代碼（仍可存入）")
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
        st.subheader("📋 負債明細（可編輯 / 刪除）")
        if st.session_state.liabilities_df.empty:
            st.info("目前尚無負債資料")
        else:
            liab_src = st.session_state.liabilities_df.copy()

            # 上傳時間 / 更新時間：只顯示到「年-月-日 時:分」
            if "updated_at" in liab_src.columns:
                liab_src["updated_at"] = _format_dt_series(liab_src["updated_at"])

            disp = liab_src.copy()
            show_cols = []
            if "id" in disp.columns:
                show_cols.append("id")
            for c in ["category", "name", "amount", "updated_at"]:
                if c in disp.columns:
                    show_cols.append(c)
            disp = disp[show_cols].rename(columns={
                "category": "負債類別",
                "name": "項目名稱",
                "amount": "金額(TWD)",
                "updated_at": "更新時間",
            })

            edited_liab = st.data_editor(
                disp,
                use_container_width=True,
                num_rows="dynamic",
                disabled=[c for c in ["id", "更新時間"] if c in disp.columns],
                key="liab_editor",
            )

            if st.button("💾 儲存負債表格修改", key="save_liab_btn"):
                try:
                    _sync_liabilities(liab_src, edited_liab)
                    fetch_all_data()
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ 儲存負債修改失敗：{e}")

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
        st.subheader("📋 帳戶明細（可編輯 / 刪除）")
        if st.session_state.liquidity_df.empty:
            st.info("目前尚無流動資金帳戶資料")
        else:
            liq_src = st.session_state.liquidity_df.copy()

            # 上傳時間 / 更新時間：只顯示到「年-月-日 時:分」
            if "updated_at" in liq_src.columns:
                liq_src["updated_at"] = _format_dt_series(liq_src["updated_at"])

            disp = liq_src.copy()
            show_cols = []
            if "id" in disp.columns:
                show_cols.append("id")
            for c in ["account_name", "amount", "updated_at"]:
                if c in disp.columns:
                    show_cols.append(c)
            disp = disp[show_cols].rename(columns={
                "account_name": "帳戶名稱",
                "amount": "金額(TWD)",
                "updated_at": "更新時間",
            })

            edited_liq = st.data_editor(
                disp,
                use_container_width=True,
                num_rows="dynamic",
                disabled=[c for c in ["id", "更新時間"] if c in disp.columns],
                key="liq_editor",
            )

            if st.button("💾 儲存流動資金表格修改", key="save_liq_btn"):
                try:
                    _sync_liquidity(liq_src, edited_liq)
                    fetch_all_data()
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ 儲存流動資金修改失敗：{e}")

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
            
            st.write("歷史紀錄（可編輯 / 刪除）")
            in_src = st.session_state.income_df.copy()

            # 上傳時間：只顯示到「年-月-日 時:分」
            if "紀錄日期" in in_src.columns:
                in_src["上傳時間"] = _format_dt_series(in_src["紀錄日期"])
            else:
                in_src["上傳時間"] = ""

            disp_in = in_src.copy()
            show_cols = []
            if "id" in disp_in.columns:
                show_cols.append("id")
            if "上傳時間" in disp_in.columns:
                show_cols.append("上傳時間")
            for c in ["年收入", "備註"]:
                if c in disp_in.columns:
                    show_cols.append(c)
            disp_in = disp_in[show_cols]

            edited_in = st.data_editor(
                disp_in,
                num_rows="dynamic",
                use_container_width=True,
                disabled=[c for c in ["id", "上傳時間"] if c in disp_in.columns],
                key="income_editor",
            )

            if st.button("💾 儲存收入表格修改", key="save_income_btn"):
                try:
                    _sync_income_history(in_src, edited_in)
                    fetch_all_data()
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ 儲存收入修改失敗：{e}")

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
if st.session_state.transactions.empty:
    st.info("尚無交易紀錄")
else:
    tx_src = st.session_state.transactions.copy()

    # 日期欄位統一成 date，方便直接編輯
    if "日期" in tx_src.columns:
        tx_src["日期"] = pd.to_datetime(tx_src["日期"], errors="coerce").dt.date

    # 台股代碼 -> 中文名稱（顯示用，不回寫）
    tx_src["台股名稱"] = ""
    try:
        if "類別" in tx_src.columns and "代碼" in tx_src.columns:
            mask = tx_src["類別"] == "台股"
            if mask.any():
                def _tw_name_only(code):
                    base = str(code).upper().replace(".TW", "").strip()
                    return get_tw_stock_name(base) or ""
                tx_src.loc[mask, "台股名稱"] = tx_src.loc[mask, "代碼"].apply(_tw_name_only)
    except Exception:
        pass

    show_cols = [c for c in ["id", "日期", "類型", "類別", "代碼", "台股名稱", "數量", "單價"] if c in tx_src.columns]
    disp_tx = tx_src[show_cols].sort_values("日期", ascending=False)

    edited_tx = st.data_editor(
        disp_tx,
        use_container_width=True,
        num_rows="dynamic",
        disabled=[c for c in ["id", "台股名稱"] if c in disp_tx.columns],
        key="tx_editor",
    )

    if st.button("💾 儲存交易表格修改", key="save_tx_btn"):
        try:
            _sync_transactions(tx_src, edited_tx)
            fetch_all_data()
            st.rerun()
        except Exception as e:
            st.error(f"❌ 儲存交易修改失敗：{e}")
