# filename: dmi_app.py
import streamlit as st
import pandas as pd
import yfinance as yf
import ta
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from dataclasses import dataclass
from typing import List, Tuple, Optional
import unicodedata
from pathlib import Path
import re, io, requests
from bs4 import BeautifulSoup

st.title("📈 日本株（東証全銘柄・自動取得）名前/コード検索")

# 足種選択
option = st.selectbox("足種を選んでください", ["日足", "週足", "月足"])
interval = {"日足": "1d", "週足": "1wk", "月足": "1mo"}[option]

# ---------- 文字正規化ヘルパ ----------
def _to_katakana(s: str) -> str:
    # ひらがな→カタカナ
    return "".join(chr(ord(ch)+0x60) if "ぁ" <= ch <= "ゖ" else ch for ch in s)

def _norm(s: str) -> str:
    if not isinstance(s, str): return ""
    s = unicodedata.normalize("NFKC", s).strip()
    s = _to_katakana(s)
    return s.casefold()

def _looks_like_ticker_jp(s: str) -> bool:
    # 4桁 or 4桁+.T をティッカーと見なす
    s2 = unicodedata.normalize("NFKC", s).strip()
    return (s2.isdigit() and len(s2) == 4) or s2.upper().endswith(".T")

def _ensure_dot_t(s: str) -> str:
    s2 = unicodedata.normalize("NFKC", s).strip().upper()
    if s2.endswith(".T"): return s2
    if s2.isdigit() and len(s2) == 4: return f"{s2}.T"
    return s2

# ---------- JPX（上場銘柄一覧）をWebから自動取得 ----------
JPX_LIST_JA = "https://www.jpx.co.jp/markets/statistics-equities/misc/01.html"
JPX_LIST_EN = "https://www.jpx.co.jp/english/markets/statistics-equities/misc/01.html"

def _find_excel_url(page_url: str) -> tuple[str | None, str | None]:
    """(.xlsx優先) 戻り値: (url, ext) extは'.xlsx'または'.xls'"""
    html = requests.get(page_url, timeout=20).text
    soup = BeautifulSoup(html, "html.parser")
    xlsx, xls = None, None
    for a in soup.find_all("a", href=True):
        href = a["href"]
        low = href.lower()
        if low.endswith(".xlsx") and xlsx is None:
            xlsx = requests.compat.urljoin(page_url, href)
        if low.endswith(".xls") and xls is None:
            xls = requests.compat.urljoin(page_url, href)
    if xlsx:
        return xlsx, ".xlsx"
    if xls:
        return xls, ".xls"
    return None, None

def _read_jpx_excel(xls_bytes: bytes, ext: str) -> pd.DataFrame:
    # 1) 拡張子に合わせて読む（.xlsx→openpyxl、.xls→xlrd）
    if ext == ".xlsx":
        df = pd.read_excel(io.BytesIO(xls_bytes), header=0, engine="openpyxl")
    else:  # ".xls"
        df = pd.read_excel(io.BytesIO(xls_bytes), header=0, engine="xlrd")

    # 2) 列名ゆれに対応（日本語/英語）
    cols = {c: str(c) for c in df.columns}

    def pick(*keys):
        for c in cols:
            s = str(c)
            if any(k in s for k in keys):
                return c
        return None

    col_code    = pick("コード", "Code")
    col_name    = pick("銘柄名", "Issue Name")
    col_kana    = pick("銘柄名（カナ）", "Kana")
    col_section = pick("市場・商品区分", "Section/Products", "Market Segment")

    if col_code is None or col_name is None:
        raise ValueError("JPX Excelにコード/銘柄名が見つかりません。")

    # 3) 4桁コードのみ抽出し、.Tを付与
    df = df[df[col_code].astype(str).str.fullmatch(r"\d{4}")].copy()
    df["ticker"]    = df[col_code].astype(str).str.zfill(4) + ".T"
    df["name_ja"]   = df[col_name].astype(str)
    df["name_kana"] = df[col_kana].astype(str) if (col_kana and col_kana in df.columns) else ""

    # 4) ETF/ETN/REIT/投信などを除外（株式のみ）
    if col_section and col_section in df.columns:
        df = df[~df[col_section].astype(str).str.contains(
            r"ETF|ETN|REIT|投資|ファンド|出資|受益|優先出資|外国ETF|外国投資", regex=True
        )].copy()

    return df[["ticker", "name_ja", "name_kana"]].reset_index(drop=True)

@st.cache_data(ttl=60*60*24)  # 1日キャッシュ
def fetch_tse_master() -> pd.DataFrame:
    # 日本語→英語ページの順でExcelを探索（.xlsx優先）
    url, ext = _find_excel_url(JPX_LIST_JA)
    if not url:
        url, ext = _find_excel_url(JPX_LIST_EN)
    if not url:
        st.warning("JPXのExcelが見つからないため、最小フォールバックの銘柄表を使用します。")
        return pd.DataFrame([
            ["7203.T", "トヨタ自動車", "トヨタ"],
            ["6758.T", "ソニーグループ", "ソニー"],
            ["9984.T", "ソフトバンクグループ", "ソフトバンクグループ"],
            ["7974.T", "任天堂", "ニンテンドー"],
        ], columns=["ticker","name_ja","name_kana"])

    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return _read_jpx_excel(r.content, ext)

# ---------- 銘柄マスター作成（自動取得） ----------
master = fetch_tse_master()
master["_key"] = (
    master["ticker"].map(_norm) + " " +
    master["name_ja"].map(_norm) + " " +
    master["name_kana"].map(_norm)
)

# ---------- 入力→候補→ティッカー決定 ----------
q = st.text_input(
    "銘柄名（漢字/カナ/ひらがな）または4桁コードで検索",
    value="",
    placeholder="例）トヨタ / とよた / 7203 など"
)

selected_ticker = None
cands = master
if q:
    nq = _norm(q)
    cands = master[
        master["_key"].str.contains(nq)
        | master["ticker"].map(_norm).str.fullmatch(nq, na=False)
    ].head(50)

# 4桁コード直入力を優先
if q and _looks_like_ticker_jp(q):
    selected_ticker = _ensure_dot_t(q)

if selected_ticker is None:
    if cands.empty:
        st.info("候補が見つかりません。別の表記で試してください。")
        st.stop()
    options = [f"{r['name_ja']}（{r['ticker']}）" for _, r in cands.iterrows()]
    choice = st.selectbox("候補から選択", options, index=0)
    selected_ticker = cands.iloc[options.index(choice)]["ticker"]

# 以降の処理で使う変数
ticker = selected_ticker

# ★ 追加：表示名（銘柄名）を作る。見つからなければ ticker を使う
try:
    display_name = master.loc[master["ticker"] == ticker, "name_ja"].iloc[0]
except Exception:
    display_name = ticker

# ★ ここで選択銘柄を明示（NameError の原因だった位置より後に移動）
st.success(f"選択中: {display_name}（{ticker}）")

# ---------- yfinance で価格データ取得 ----------
@st.cache_data(ttl=3600)
def get_stock_data_jp(ticker: str, interval: str) -> pd.DataFrame:
    df = yf.download(ticker, start="2018-12-19", interval=interval, progress=False, threads=True)
    if df is None or df.empty:
        return pd.DataFrame()
    # マルチカラム対策（yfinanceのバージョン差）
    if hasattr(df.columns, "__iter__") and len(df.columns) > 0 and isinstance(df.columns[0], tuple):
        df.columns = [c[0] for c in df.columns]
    # 日本語環境列名の揺れ対応
    df = df.rename(columns={"始値":"Open","高値":"High","安値":"Low","終値":"Close","出来高":"Volume"})
    for c in ["Open","High","Low","Close","Volume"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df.index = pd.to_datetime(df.index)
    return df.dropna(subset=["Open","High","Low","Close"])

# yfinanceで価格取得
df = get_stock_data_jp(ticker, interval)
if df is None or df.empty:
    st.error(f"{ticker}: 価格データを取得できませんでした。ネットワーク/ティッカー/期間を確認してください。")
    st.stop()

# 必須カラムチェック
required = {"High", "Low", "Close"}
missing = required - set(df.columns)
if missing:
    st.error(f"必要列が不足しています: {missing}（列名のリネーム処理を確認してください）")
    st.stop()

# ===============================
# DMI計算（ta）
# ===============================
adx = ta.trend.ADXIndicator(high=df["High"], low=df["Low"], close=df["Close"], window=14)
df["+DI"] = adx.adx_pos()
(df["-DI"]) = adx.adx_neg()
(df["ADX"])  = adx.adx()
df[["+DI","-DI","ADX"]] = df[["+DI","-DI","ADX"]].fillna(method="ffill").fillna(method="bfill")

# ===============================
# （従来）角度＆最適角度（チャートのタイトル用。グラフ自体は残す）
# ===============================
df["diff"] = df["+DI"] - df["-DI"]
df["angle"] = np.degrees(np.arctan(df["diff"].diff()))

def simulate_angle(threshold):
    trades = []
    buy_price = None
    # チャートには使わないが、既存ロジックに合わせてシグナル列は付与
    sig = pd.Series("保持", index=df.index)
    cross_buy = (df["+DI"] > df["-DI"]) & (df["+DI"].shift(1) <= df["-DI"].shift(1)) & (abs(df["angle"]) > threshold)
    cross_sell = (df["+DI"] < df["-DI"]) & (df["+DI"].shift(1) >= df["-DI"].shift(1)) & (abs(df["angle"]) > threshold)
    sig[cross_buy] = "買い"
    sig[cross_sell] = "売り"

    for idx, row in df.iterrows():
        if sig.loc[idx] == "買い":
            buy_price = row["Close"]
        elif sig.loc[idx] == "売り" and buy_price is not None:
            ret = (row["Close"] - buy_price) / buy_price * 100
            trades.append(ret)
            buy_price = None
    return np.mean(trades) if len(trades) else 0

best_angle = 45
for ang in range(10, 90, 5):
    if simulate_angle(ang) >= 20:
        best_angle = ang
        break

# ===============================
# 価格＋DMIチャート（残す）
# ===============================
fig = make_subplots(
    rows=2, cols=1, shared_xaxes=True,
    vertical_spacing=0.12,
    row_heights=[0.65, 0.35],
    subplot_titles=(f"{display_name}（{ticker}）株価（{option}）", f"DMI（従来式の最適角度: {best_angle:.0f}°）")
)
fig.add_trace(go.Candlestick(
    x=df.index, open=df["Open"], high=df["High"], low=df["Low"], close=df["Close"], name="株価"
), row=1, col=1)
fig.add_trace(go.Scatter(x=df.index, y=df["+DI"], mode="lines", name="+DI", line=dict(color="green", width=2)), row=2, col=1)
fig.add_trace(go.Scatter(x=df.index, y=df["-DI"], mode="lines", name="-DI", line=dict(color="red", width=2)), row=2, col=1)
fig.add_trace(go.Scatter(x=df.index, y=df["ADX"], mode="lines", name="ADX", line=dict(color="blue", width=2)), row=2, col=1)
fig.update_yaxes(range=[0, 100], row=2, col=1)
end_dt = df.index.max()
start_dt = end_dt - pd.DateOffset(years=1)
fig.update_xaxes(range=[start_dt, end_dt], tickformat="%m/%d")
fig.update_layout(
    hovermode="x unified",
    dragmode=False,
    height=700,
    margin=dict(l=10, r=10, t=60, b=20),
    legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="right", x=1)
)
st.plotly_chart(fig, use_container_width=True, theme="streamlit")


# =======================================================================================
# ここから：dmi_angle式の「新・リターン表」（買値/売値 追加、リターンは整数％）
# =======================================================================================

# dmi_angle 設定
ANGLE_SCALE = 30.0            # -DI 変化→角度の感度
MIN_SELL_ANGLE_DEG = 40.0     # 売り角度の下限（40°以下は売らない）
MIN_HOLD_DAYS = 5             # 保有5日以内（<=5）は除外
SEARCH_RANGE = range(40, 86)  # 40..85 を探索
LOOKBACK_YEARS = 5

# -DIの変化から角度（度）を算出（dmi_angle式）
df["angle_minus_deg"] = np.degrees(np.arctan(df["-DI"].diff() * ANGLE_SCALE))

def _cross_series(dfe: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    cross_up = (dfe["+DI"] > dfe["-DI"]) & (dfe["+DI"].shift(1) <= dfe["-DI"].shift(1))
    cross_down = (dfe["-DI"] > dfe["+DI"]) & (dfe["-DI"].shift(1) <= dfe["+DI"].shift(1))
    return cross_up, cross_down

@dataclass
class Trade:
    buy_dt: pd.Timestamp
    sell_dt: pd.Timestamp
    days: int
    ret_pct: int
    buy_px: int
    sell_px: int

def backtest_with_angle(dfall: pd.DataFrame, theta_deg: float, years: int = LOOKBACK_YEARS) -> Tuple[pd.DataFrame, Tuple[pd.Timestamp, pd.Timestamp]]:
    """+DI上抜けで買い、-DI下抜け & angle_minus_deg >= max(theta,40°) で売り。
       保有<=5日は除外。未決済は期末評価。"""
    last_date = dfall.index.max()
    start_dt = last_date - pd.DateOffset(years=years)
    dfe = dfall[dfall.index >= start_dt].copy()

    cross_up, cross_down = _cross_series(dfe)
    in_pos = False
    buy_px: Optional[float] = None
    buy_dt: Optional[pd.Timestamp] = None
    trades: List[Trade] = []

    sell_angle = max(theta_deg, MIN_SELL_ANGLE_DEG)

    for idx, row in dfe.iterrows():
        # 買い
        if (not in_pos) and cross_up.loc[idx]:
            in_pos = True
            buy_px = float(row["Close"])
            buy_dt = idx
            continue

        # 売り
        if in_pos and cross_down.loc[idx] and (row["angle_minus_deg"] >= sell_angle):
            sell_px = float(row["Close"])
            sell_dt = idx
            ret_pct = (sell_px - buy_px) / buy_px * 100.0
            days = (sell_dt - buy_dt).days
            trades.append(
                Trade(
                    buy_dt=buy_dt,
                    sell_dt=sell_dt,
                    days=days,
                    ret_pct=int(np.rint(ret_pct)),  # 整数％
                    buy_px=int(np.rint(buy_px)),    # 買値（整数）
                    sell_px=int(np.rint(sell_px))   # 売値（整数）
                )
            )
            in_pos = False
            buy_px = None
            buy_dt = None

    # 期末評価
    if in_pos and buy_px is not None:
        sell_px = float(dfe["Close"].iloc[-1])
        sell_dt = dfe.index[-1]
        ret_pct = (sell_px - buy_px) / buy_px * 100.0
        days = (sell_dt - buy_dt).days
        trades.append(
            Trade(
                buy_dt=buy_dt,
                sell_dt=sell_dt,
                days=days,
                ret_pct=int(np.rint(ret_pct)),
                buy_px=int(np.rint(buy_px)),
                sell_px=int(np.rint(sell_px))
            )
        )

    # DataFrame化 & フィルタ
    tdf = pd.DataFrame([
        {"買い日": t.buy_dt, "売り日": t.sell_dt, "日数": t.days, "買値": t.buy_px, "売値": t.sell_px, "リターン(%)": t.ret_pct}
        for t in trades
    ])
    tdf = tdf[tdf["日数"] > MIN_HOLD_DAYS].reset_index(drop=True)
    return tdf, (dfe.index[0], dfe.index[-1])

def compute_cagr(trades_df: pd.DataFrame, start_date: pd.Timestamp, end_date: pd.Timestamp) -> float:
    if trades_df.empty:
        return 0.0
    growth = (1.0 + trades_df["リターン(%)"].astype(float) / 100.0).prod()
    years = max((end_date - start_date).days / 365.25, 1e-9)
    cagr = growth ** (1.0 / years) - 1.0
    return float(cagr * 100.0)

def search_best_angle_by_cagr(dfall: pd.DataFrame, angle_range=SEARCH_RANGE, years: int = LOOKBACK_YEARS) -> Tuple[int, float, pd.DataFrame]:
    best_theta = None
    best_cagr = -1e18
    best_trades = pd.DataFrame()
    for theta in angle_range:
        tdf, (sdt, edt) = backtest_with_angle(dfall, theta, years=years)
        cagr = compute_cagr(tdf, sdt, edt)
        if cagr > best_cagr:
            best_cagr = cagr
            best_theta = theta
            best_trades = tdf

    if not best_trades.empty:
        best_trades = best_trades.sort_values("買い日", ascending=False).copy()
        best_trades["買い日"] = best_trades["買い日"].dt.strftime("%Y/%m/%d")
        best_trades["売り日"] = best_trades["売り日"].dt.strftime("%Y/%m/%d")

    return int(best_theta if best_theta is not None else 40), float(np.round(best_cagr, 2)), best_trades

# 角度最適化 → 新リターン表
best_theta2, best_cagr2, returns_df2 = search_best_angle_by_cagr(df)

st.subheader(f"📊 dmi_angle式 リターン一覧（採用角度: {best_theta2}°｜角度>=40°, 保有>5日｜直近{LOOKBACK_YEARS}年）")
c1, c2, c3 = st.columns(3)
c1.metric("採用角度（°）", f"{best_theta2}")
c2.metric("年平均リターン（CAGR）", f"{best_cagr2:.2f}%")
c3.metric("取引回数", f"{len(returns_df2)}")

if returns_df2.empty:
    st.info("条件を満たすトレードが直近期間にありませんでした。")
else:
    def color_ret(val):
        return "color: green;" if val > 0 else "color: red;"
    styled_df2 = returns_df2.style.map(color_ret, subset=["リターン(%)"])
    st.dataframe(styled_df2, use_container_width=True)

    # 概要
    win_rate = (returns_df2["リターン(%)"] > 0).mean() * 100
    max_ret = returns_df2["リターン(%)"].max()
    min_ret = returns_df2["リターン(%)"].min()
    avg_ret = returns_df2["リターン(%)"].mean()
    st.markdown(
        f"""
- 平均リターン：**{avg_ret:.0f}%**
- 勝率：**{win_rate:.1f}%**
- 最大リターン：**{max_ret:.0f}%**
- 最小リターン：**{min_ret:.0f}%**
        """
    )

# ===============================
# ===== NEW: 全銘柄を自動スクリーニング → 最適角度付近だけ → 年平均リターンTop10 =====
# ===============================
st.subheader("🏆 DMIゴールデンクロス × 最適角度付近 → 年平均リターン Top10（自動）")

TOPK    = 10
TOL_DEG = 3.0  # “最適角度”θとの許容差（±度）

@st.cache_data(ttl=3600, show_spinner=False)
def _screen_top10_all(interval: str, tol_deg: float) -> pd.DataFrame:
    rows = []
    tickers = master["ticker"].tolist()  # 東証全銘柄マスター想定

    for tk in tickers:
        d0 = get_stock_data_jp(tk, interval)
        if d0 is None or d0.empty or len(d0) < 20:
            continue

        # DMI算出
        adx0 = ta.trend.ADXIndicator(high=d0["High"], low=d0["Low"], close=d0["Close"], window=14)
        pdi  = adx0.adx_pos().ffill().bfill()
        ndi  = adx0.adx_neg().ffill().bfill()

        # 直近で +DI が -DI を上抜け（ゴールデンクロス）
        if not (pdi.iloc[-1] > ndi.iloc[-1] and pdi.iloc[-2] <= ndi.iloc[-2]):
            continue

        # 現在角度と“最適角度”θの算出
        d0["+DI"], d0["-DI"] = pdi, ndi
        d0["angle_minus_deg"] = np.degrees(np.arctan(d0["-DI"].diff() * ANGLE_SCALE))
        cur_angle = float(d0["angle_minus_deg"].iloc[-1])

        theta, cagr, _ = search_best_angle_by_cagr(d0)  # 既存ロジックを利用

        # “最適角度付近”のみ採用
        if abs(cur_angle - theta) > tol_deg:
            continue

        name = master.loc[master["ticker"] == tk, "name_ja"].iloc[0] if (master["ticker"] == tk).any() else tk
        rows.append({
            "ticker": tk,
            "銘柄名": name,
            "年平均リターン(%)": cagr,
            "最適角度θ(°)": theta,
            "現在角度(°)": round(cur_angle, 2),
        })

    if not rows:
        return pd.DataFrame()

    df = (pd.DataFrame(rows)
          .sort_values("年平均リターン(%)", ascending=False)
          .head(TOPK)
          .reset_index(drop=True))
    return df

with st.spinner("全銘柄スクリーニング中…"):
    top10_df = _screen_top10_all(interval, TOL_DEG)

if top10_df.empty:
    st.info("該当なし（ゴールデンクロスかつ最適角度付近の銘柄がありませんでした）。")
else:
    st.dataframe(top10_df, use_container_width=True)