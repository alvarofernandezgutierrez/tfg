import re
import glob
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

NEWS_PATH = Path("data/processed/news/fnspid_sp500.parquet")
OUT_DIR = Path("outputs/eda/news")
OUT_DIR.mkdir(parents=True, exist_ok=True)

BIG_TICKERS = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA"]
STOPWORDS = set("""
a an and are as at be by for from has have he her his i in is it its of on or she that the to was were will with you your
this these those than then there here not but we our they them their
""".split())

def normalize_text(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def top_words(series: pd.Series) -> pd.Series:
    counts = {}
    for text in series.dropna().astype(str):
        text = normalize_text(text)
        if not text:
            continue
        for w in text.split():
            if len(w) < 3:
                continue
            if w in STOPWORDS:
                continue
            counts[w] = counts.get(w, 0) + 1
    return pd.Series(counts).sort_values(ascending=False).head(30)

def main():
    # ---- CARGA DESDE PARQUET ----
    print("Cargando parquets...")
    df = pd.read_parquet(NEWS_PATH)
    print(f"Filas cargadas: {len(df):,}")

    # Normalizar nombres de columnas
    col_mapping = {}
    for col in df.columns:
        c = col.lower()
        if 'url' in c: col_mapping[col] = 'url'
        elif 'symbol' in c or 'ticker' in c: col_mapping[col] = 'ticker'
        elif 'title' in c: col_mapping[col] = 'title'
        elif 'date' in c: col_mapping[col] = 'date'
    df = df.rename(columns=col_mapping)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "title", "url"])
    df["ticker"] = df["ticker"].astype(str).str.strip()
    df.loc[df["ticker"].isin(["", "nan", "NaN", "None"]), "ticker"] = pd.NA

    # Distribución temporal
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.to_period("M").astype(str)
    df["day"] = df["date"].dt.date

    news_by_year = df.groupby("year").size().sort_index()
    news_by_month = df.groupby("month").size()
    news_by_day = df.groupby("day").size()

    # Guardar stats básicos
    stats_path = OUT_DIR / "summary.txt"
    with open(stats_path, "w", encoding="utf-8") as f:
        f.write(f"Filas (tras dropna date/title/url): {len(df)}\n")
        f.write(f"Rango fechas: {df['date'].min()} -> {df['date'].max()}\n")
        f.write(f"Tickers únicos (no nulos): {df['ticker'].nunique(dropna=True)}\n")
        f.write("\nNoticias por año:\n")
        f.write(news_by_year.to_string())
        f.write("\n\nNoticias por día (describe):\n")
        f.write(news_by_day.describe().to_string())
        f.write("\n")

    # Noticias por año
    plt.figure()
    news_by_year.plot(kind="bar")
    plt.title("Noticias por año")
    plt.xlabel("Año")
    plt.ylabel("Número de noticias")
    plt.tight_layout()
    plt.savefig(OUT_DIR / "news_by_year.png", dpi=200)
    plt.close()

    # Noticias por mes
    month_idx = pd.to_datetime(news_by_month.index + "-01", errors="coerce")
    news_by_month_series = pd.Series(news_by_month.values, index=month_idx).sort_index()

    plt.figure()
    news_by_month_series.plot()
    plt.title("Noticias por mes")
    plt.xlabel("Mes")
    plt.ylabel("Número de noticias")
    plt.tight_layout()
    plt.savefig(OUT_DIR / "news_by_month.png", dpi=200)
    plt.close()

    # Empresas grandes del SP500
    ticker_counts = df["ticker"].value_counts(dropna=True)
    big_stats = [(t, int(ticker_counts.get(t, 0))) for t in BIG_TICKERS]
    big_df = pd.DataFrame(big_stats, columns=["ticker", "news_count"]).sort_values("news_count", ascending=False)
    big_df.to_csv(OUT_DIR / "big_tickers_counts.csv", index=False)

    plt.figure()
    big_df.set_index("ticker")["news_count"].plot(kind="bar")
    plt.title("Noticias: empresas grandes S&P500 (por ticker)")
    plt.xlabel("Ticker")
    plt.ylabel("Número de noticias")
    plt.tight_layout()
    plt.savefig(OUT_DIR / "big_tickers_bar.png", dpi=200)
    plt.close()

    # Distribución temporal top 3 tickers grandes
    top_big = big_df.sort_values("news_count", ascending=False).head(3)["ticker"].tolist()
    for t in top_big:
        tmp = df[df["ticker"] == t].copy()
        if tmp.empty:
            continue
        tmp_month = tmp.groupby(tmp["date"].dt.to_period("M")).size()
        idx = pd.to_datetime(tmp_month.index.astype(str) + "-01", errors="coerce")
        s = pd.Series(tmp_month.values, index=idx).sort_index()

        plt.figure()
        s.plot()
        plt.title(f"Noticias por mes para {t}")
        plt.xlabel("Mes")
        plt.ylabel("Número de noticias")
        plt.tight_layout()
        plt.savefig(OUT_DIR / f"news_by_month_{t}.png", dpi=200)
        plt.close()

    # Top palabras
    top_global = top_words(df["title"])
    top_global.to_csv(OUT_DIR / "top_words_global.csv", header=["count"])

    split_date = df["date"].median()
    top_words(df[df["date"] <= split_date]["title"]).to_csv(OUT_DIR / "top_words_early.csv", header=["count"])
    top_words(df[df["date"] > split_date]["title"]).to_csv(OUT_DIR / "top_words_late.csv", header=["count"])

    plt.figure()
    top_global.head(15).sort_values().plot(kind="barh")
    plt.title("Top 15 palabras (titulares) - global")
    plt.xlabel("Frecuencia")
    plt.tight_layout()
    plt.savefig(OUT_DIR / "top_words_global_top15.png", dpi=200)
    plt.close()

    print("✅ Completado. Outputs en:", OUT_DIR)
    print("Resumen:", stats_path)

if __name__ == "__main__":
    main()