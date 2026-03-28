import glob
import pandas as pd
from pathlib import Path
from transformers import pipeline
import torch

PARQUET_DIR = Path("outputs/eda/ticker_quality_raw")
OUT_PATH = Path("data/processed/news/fnspid_sentiment.parquet")
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

BATCH_SIZE = 64
SUBSET = 50_000

def main():
    # ---- 1. Cargar noticias ----
    print("Cargando parquets...")
    files = sorted(glob.glob(str(PARQUET_DIR / "*.parquet")))
    news = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

    # ---- Limpiar columnas innecesarias ----
    cols_to_drop = [
        "Unnamed: 0",
        "Lsa_summary", "Luhn_summary",
        "Textrank_summary", "Lexrank_summary"
    ]
    news = news.drop(columns=cols_to_drop, errors="ignore")

    # Normalizar columnas
    col_mapping = {}
    for col in news.columns:
        c = col.lower()
        if 'date' in c: col_mapping[col] = 'date'
        elif 'symbol' in c or 'ticker' in c: col_mapping[col] = 'ticker'
        elif 'title' in c: col_mapping[col] = 'title'
    news = news.rename(columns=col_mapping)

    news["date"] = pd.to_datetime(news["date"], errors="coerce").dt.normalize()
    news["ticker"] = news["ticker"].astype(str).str.strip().str.upper()
    news = news.dropna(subset=["date", "ticker", "title"])

    # ---- 2. Subset ----
    news = news.sample(n=SUBSET, random_state=42)
    print(f"Trabajando con subset de {SUBSET:,} noticias")

    # ---- 3. Cargar FinBERT ----
    print("\nCargando FinBERT en GPU...")
    device = 0 if torch.cuda.is_available() else -1
    finbert = pipeline(
        "text-classification",
        model="ProsusAI/finbert",
        tokenizer="ProsusAI/finbert",
        device=device,
        top_k=None
    )

    # ---- 4. Inferencia por batches ----
    print(f"Ejecutando inferencia (batch_size={BATCH_SIZE})...")
    texts = news["title"].tolist()
    texts_truncated = [t[:512] for t in texts]

    results = []
    total = len(texts_truncated)
    for i in range(0, total, BATCH_SIZE):
        batch = texts_truncated[i:i + BATCH_SIZE]
        outputs = finbert(batch, truncation=True, max_length=512)
        for out in outputs:
            scores = {item["label"]: item["score"] for item in out}
            p_pos = scores.get("positive", 0)
            p_neg = scores.get("negative", 0)
            p_neu = scores.get("neutral", 0)
            results.append({
                "p_positive": p_pos,
                "p_negative": p_neg,
                "p_neutral": p_neu,
                "sentiment_score": p_pos - p_neg,
                "sentiment_label": max(scores, key=scores.get)
            })

        if (i // BATCH_SIZE) % 10 == 0:
            print(f"  {i:,}/{total:,} ({i/total:.1%})")

    # ---- 5. Montar dataframe final ----
    results_df = pd.DataFrame(results)
    final = pd.concat([news.reset_index(drop=True), results_df], axis=1)

    # ---- 6. Guardar ----
    final.to_parquet(OUT_PATH, index=False)
    print(f"\n✅ Guardado en: {OUT_PATH}")
    print(f"Filas: {len(final):,}")
    print("\nDistribución de sentimiento:")
    print(final["sentiment_label"].value_counts())
    print("\nEstadísticas del score continuo:")
    print(final["sentiment_score"].describe())

if __name__ == "__main__":
    main()