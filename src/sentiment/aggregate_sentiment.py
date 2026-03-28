import pandas as pd
from pathlib import Path

IN_PATH = Path("data/processed/news/fnspid_sentiment.parquet")
OUT_PATH = Path("data/processed/news/fnspid_sentiment_aggregated.parquet")

def main():
    print("Cargando sentimiento...")
    df = pd.read_parquet(IN_PATH)
    print(f"Filas cargadas: {len(df):,}")

    # ---- Agregar por (date, ticker) ----
    agg = (
        df.groupby(["date", "ticker"])
        .agg(
            sentiment_score_mean=("sentiment_score", "mean"),
            sentiment_score_std=("sentiment_score", "std"),
            n_noticias=("sentiment_score", "count"),
            n_positive=("sentiment_label", lambda x: (x == "positive").sum()),
            n_negative=("sentiment_label", lambda x: (x == "negative").sum()),
            n_neutral=("sentiment_label", lambda x: (x == "neutral").sum()),
        )
        .reset_index()
    )

    # Label dominante del día
    agg["dominant_label"] = agg.apply(
        lambda r: "positive" if r["n_positive"] >= r["n_negative"] and r["n_positive"] >= r["n_neutral"]
        else ("negative" if r["n_negative"] >= r["n_neutral"] else "neutral"),
        axis=1
    )

    print(f"Combinaciones (date, ticker) únicas: {len(agg):,}")
    print("\nPrimeras filas:")
    print(agg.head(10))
    print("\nEstadísticas del score medio:")
    print(agg["sentiment_score_mean"].describe())
    print("\nDistribución de label dominante:")
    print(agg["dominant_label"].value_counts())

    agg.to_parquet(OUT_PATH, index=False)
    print(f"\n✅ Guardado en: {OUT_PATH}")

if __name__ == "__main__":
    main()