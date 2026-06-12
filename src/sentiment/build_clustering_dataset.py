import pandas as pd
import numpy as np
from pathlib import Path

DEBUG_MODE = False
SENTIMENT_PATH = Path("data/processed/news/fnspid_sentiment_by_sector_debug.parquet") if DEBUG_MODE else Path("data/processed/news/fnspid_sentiment_by_sector.parquet")
TECHNICAL_PATH = Path("data/processed/market/technical_indicators.parquet")
OUT_PATH = Path("data/processed/clustering_dataset_debug.parquet") if DEBUG_MODE else Path("data/processed/clustering_dataset.parquet")
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)


DATE_CUTOFF = "2024-12-31"

def main():
    print(f"{'[DEBUG MODE]' if DEBUG_MODE else '[FULL MODE]'}")

    # ---- 1. Cargar sentimiento sectorial ----
    print("Cargando sentimiento sectorial...")
    sent = pd.read_parquet(SENTIMENT_PATH)
    sent["date"] = pd.to_datetime(sent["date"]).dt.normalize()
    print(f"Filas sentimiento: {len(sent):,}")

    # ---- 2. Cargar indicadores técnicos ----
    print("\nCargando indicadores técnicos...")
    tech = pd.read_parquet(TECHNICAL_PATH)
    tech["date"] = pd.to_datetime(tech["date"]).dt.normalize()

    rename_map = {
        "RSI": "RSI_sp500",
        "MA50_ratio": "MA50_ratio_sp500",
        "MA200_ratio": "MA200_ratio_sp500",
        "mom21": "mom21_sp500",
        "mom126": "mom126_sp500",
        "vol_30d": "vol30d_sp500",
        "vol_252d": "vol252d_sp500",
        "skew_30d": "skew30d_sp500",
        "skew_252d": "skew252d_sp500"
    }
    tech = tech.rename(columns=rename_map)
    tech = tech.drop(columns=["ret", "close"], errors="ignore")
    print(f"Filas técnicos: {len(tech):,}")

    # ---- 3. Cruce por fecha ----
    print("\nCruzando por fecha...")
    df = sent.merge(tech, on="date", how="inner")
    print(f"Filas tras cruce: {len(df):,}")

    # ---- 4. Filtro temporal hasta 2019 ----
    df = df[df["date"] <= DATE_CUTOFF].copy()
    print(f"Filas tras filtro hasta {DATE_CUTOFF}: {len(df):,}")
    print(f"Rango fechas: {df['date'].min()} -> {df['date'].max()}")
    print(f"Total columnas: {len(df.columns)}")

    # ---- 5. Verificar NaN ----
    nans = df.isnull().sum()
    if nans.sum() > 0:
        print(f"\nColumnas con NaN:")
        print(nans[nans > 0])
    else:
        print("\nSin NaN — dataset limpio")

    # ---- 6. Guardar ----
    df.to_parquet(OUT_PATH, index=False)
    print(f"\n✅ Guardado en: {OUT_PATH}")
    print(f"   Período: {df['date'].min().date()} → {df['date'].max().date()}")
    print(f"   Días de trading: {len(df):,}")

if __name__ == "__main__":
    main()
