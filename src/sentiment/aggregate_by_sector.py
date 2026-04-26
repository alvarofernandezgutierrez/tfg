import pandas as pd
import numpy as np
from pathlib import Path

DEBUG_MODE = False
SENTIMENT_PATH = Path("data/processed/news/fnspid_sentiment_debug.parquet") if DEBUG_MODE else Path("data/processed/news/fnspid_sentiment.parquet")
GICS_PATH = Path("data/raw/market/ticker_gics_mapping.csv")
OUT_PATH = Path("data/processed/news/fnspid_sentiment_by_sector_debug.parquet") if DEBUG_MODE else Path("data/processed/news/fnspid_sentiment_by_sector.parquet")
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

def main():
    print(f"{'[DEBUG MODE]' if DEBUG_MODE else '[FULL MODE]'}")

    # ---- 1. Cargar sentimiento ----
    print("Cargando sentimiento...")
    df = pd.read_parquet(SENTIMENT_PATH)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    df = df.dropna(subset=["date", "ticker", "sentiment_score"])
    print(f"Filas cargadas: {len(df):,}")

    # ---- 2. Añadir sector GICS ----
    print("Añadiendo sector GICS...")
    gics = pd.read_csv(GICS_PATH)
    gics["ticker"] = gics["ticker"].astype(str).str.strip().str.upper()
    df = df.merge(gics[["ticker", "sector"]], on="ticker", how="inner")
    print(f"Filas tras merge con GICS: {len(df):,}")
    print(f"Sectores únicos: {df['sector'].nunique()}")
    print(df["sector"].value_counts())

    # ---- 3. Agregar por (date, sector) ----
    print("\nAgregando por (date, sector)...")
    agg = (
        df.groupby(["date", "sector"])["sentiment_score"]
        .agg(sentiment_avg="mean", sentiment_std="std", n_noticias="count")
        .reset_index()
    )
    agg["sentiment_std"] = agg["sentiment_std"].fillna(0)

    # ---- 4. Pivotar ----
    print("Pivotando tabla...")
    pivot_avg = agg.pivot(index="date", columns="sector", values="sentiment_avg")
    pivot_avg.columns = [f"sentiment_avg_{c.replace(' ', '_')}" for c in pivot_avg.columns]

    pivot_std = agg.pivot(index="date", columns="sector", values="sentiment_std")
    pivot_std.columns = [f"sentiment_std_{c.replace(' ', '_')}" for c in pivot_std.columns]

    final = pd.concat([pivot_avg, pivot_std], axis=1).reset_index()
    sentiment_cols = [c for c in final.columns if c != "date"]
    final[sentiment_cols] = final[sentiment_cols].fillna(0)

    print(f"\nTabla final: {len(final):,} filas x {len(final.columns)} columnas")
    print(f"Rango fechas: {final['date'].min()} -> {final['date'].max()}")

    # ---- 5. Guardar ----
    final.to_parquet(OUT_PATH, index=False)
    print(f"\n✅ Guardado en: {OUT_PATH}")

if __name__ == "__main__":
    main()