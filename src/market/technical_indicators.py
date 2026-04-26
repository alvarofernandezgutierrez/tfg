import pandas as pd
import numpy as np
from pathlib import Path

MARKET_PATH = Path("data/raw/market/sp500_ohlc.csv")
OUT_PATH = Path("data/processed/market/technical_indicators.parquet")
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

def compute_rsi(series, window=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=window - 1, min_periods=window).mean()
    avg_loss = loss.ewm(com=window - 1, min_periods=window).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def main():
    # ---- 1. Cargar OHLC ----
    print("Cargando OHLC...")
    df = pd.read_csv(MARKET_PATH)
    df.columns = [c.lower() for c in df.columns]
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df = df.sort_values("date").reset_index(drop=True)
    print(f"Filas OHLC: {len(df):,}")
    print(f"Rango: {df['date'].min()} -> {df['date'].max()}")

    # ---- 2. Retornos diarios ----
    df["ret"] = np.log(df["close"] / df["close"].shift(1))

    # ---- 3. RSI ----
    print("Calculando RSI...")
    df["RSI"] = compute_rsi(df["close"], window=14)

    # ---- 4. Medias móviles (ratio precio/media) ----
    print("Calculando medias móviles...")
    df["MA50"] = df["close"].rolling(50).mean()
    df["MA200"] = df["close"].rolling(200).mean()
    df["MA50_ratio"] = df["close"] / df["MA50"]
    df["MA200_ratio"] = df["close"] / df["MA200"]

    # ---- 5. Momentum ----
    print("Calculando momentum...")
    df["mom21"] = df["close"] / df["close"].shift(21) - 1
    df["mom126"] = df["close"] / df["close"].shift(126) - 1

    # ---- 6. Volatilidad rolling ----
    print("Calculando volatilidad...")
    df["vol_30d"] = df["ret"].rolling(30).std() * np.sqrt(252)
    df["vol_252d"] = df["ret"].rolling(252).std() * np.sqrt(252)

    # ---- 7. Skewness rolling ----
    print("Calculando skewness...")
    df["skew_30d"] = df["ret"].rolling(30).skew()
    df["skew_252d"] = df["ret"].rolling(252).skew()

    # ---- 8. Seleccionar columnas finales ----
    cols = [
        "date", "close", "ret",
        "RSI",
        "MA50_ratio", "MA200_ratio",
        "mom21", "mom126",
        "vol_30d", "vol_252d",
        "skew_30d", "skew_252d"
    ]
    df = df[cols].dropna()

    print(f"\nTabla final: {len(df):,} filas x {len(df.columns)} columnas")
    print(f"Rango tras dropna: {df['date'].min()} -> {df['date'].max()}")
    print("\nPrimeras filas:")
    print(df.head(5))
    print("\nEstadísticas:")
    stats = df.describe().round(4)
    print(stats)
    stats.to_csv("data/processed/market/technical_indicators.csv", index=False)

    # ---- 9. Guardar ----
    df.to_parquet(OUT_PATH, index=False)
    print(f"\n✅ Guardado en: {OUT_PATH}")

if __name__ == "__main__":
    main()