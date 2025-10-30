import streamlit as st
import pandas as pd
import yfinance as yf
import ta

st.title("📈 DMI株価シグナルアプリ")

# ソフトバンク株価を取得
df = yf.download("9434.T", period="60d", interval="1d")

# Series に変換してから渡す
high = df["High"].squeeze()
low = df["Low"].squeeze()
close = df["Close"].squeeze()

# ADXIndicatorを使ってDMIを計算
adx = ta.trend.ADXIndicator(high=high, low=low, close=close)
df["+DI"] = adx.adx_pos()
df["-DI"] = adx.adx_neg()
df["ADX"] = adx.adx()

# シグナル判定
def check_signal(row, prev_row):
    plus_di = row["+DI"].item() if hasattr(row["+DI"], "item") else row["+DI"]
    minus_di = row["-DI"].item() if hasattr(row["-DI"], "item") else row["-DI"]
    prev_plus_di = prev_row["+DI"].item() if hasattr(prev_row["+DI"], "item") else prev_row["+DI"]
    prev_minus_di = prev_row["-DI"].item() if hasattr(prev_row["-DI"], "item") else prev_row["-DI"]

    if plus_di > minus_di and prev_plus_di < prev_minus_di:
        return "📈 買い"
    elif plus_di < minus_di and prev_plus_di > prev_minus_di:
        return "📉 売り"
    else:
        return "⏸ 保持"

signals = ["⏸ 初期"]
for i in range(1, len(df)):
    signals.append(check_signal(df.iloc[i], df.iloc[i-1]))
df["シグナル"] = signals

st.subheader("最新シグナル")
st.write(df[["Close", "+DI", "-DI", "ADX", "シグナル"]].tail(1))

with st.expander("過去データを見る"):
    st.dataframe(df[["Close", "+DI", "-DI", "ADX", "シグナル"]].tail(20))
