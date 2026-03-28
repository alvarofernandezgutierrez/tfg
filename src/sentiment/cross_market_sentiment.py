import pandas as pd
import numpy as np
from pathlib import Path

SENTIMENT_PATH = Path("data/processed/news/fnspid_sentiment_aggregated.parquet")
MARKET_PATH = Path("data/raw/market/sp500_ohlc.csv")
OUT_PATH = Path("data/processed/market_sentiment.parquet")

OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

def main():
    # ---- 1. Cargar sentimiento agregado ----
    print("Cargando sentimiento agregado...")
    sent = pd.read_parquet(SENTIMENT_PATH)
    sent["date"] = pd.to_datetime(sent["date"]).dt.tz_localize(None).dt.normalize()

    # ---- 2. Cargar OHLC ----
    print("Cargando OHLC...")
    mkt = pd.read_csv(MARKET_PATH)
    mkt["date"] = pd.to_datetime(mkt["date"]).dt.tz_localize(None).dt.normalize()
    mkt.columns = [c.lower() for c in mkt.columns]
    mkt["date"] = pd.to_datetime(mkt["date"]).dt.normalize()
    mkt = mkt.sort_values("date").reset_index(drop=True)

    # Calcular retorno diario y volatilidad rolling
    mkt["ret"] = np.log(mkt["close"] / mkt["close"].shift(1))
    mkt["vol_30d"] = mkt["ret"].rolling(30).std() * np.sqrt(252)

    mkt = mkt[["date", "close", "ret", "vol_30d"]]
    print(f"Filas mercado: {len(mkt):,}")

    # ---- 3. Cruce por fecha ----
    print("Cruzando por fecha...")
    df = sent.merge(mkt, on="date", how="inner")
    print(f"Filas tras cruce: {len(df):,}")
    print(f"Rango de fechas: {df['date'].min()} -> {df['date'].max()}")

    # ---- 4. Guardar ----
    df.to_parquet(OUT_PATH, index=False)
    print(f"\n✅ Guardado en: {OUT_PATH}")
    print("\nPrimeras filas:")
    print(df.head())
    print("\nColumnas:")
    print(df.dtypes)

if __name__ == "__main__":
    main()