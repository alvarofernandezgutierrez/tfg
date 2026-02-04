import pandas as pd

df = pd.read_csv("data/raw/market/sp500_historical_components.csv")
df["date"] = pd.to_datetime(df["date"])

def sp500_tickers_on(date):
    d = pd.to_datetime(date)
    row = df[df["date"] <= d].iloc[-1]
    return row["tickers"].split(",")
