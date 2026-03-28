import pandas as pd
import glob
from pathlib import Path

# -------- PATHS --------
NEWS_DIR = Path("outputs/eda/ticker_quality_raw")
OUT_DIR = Path("outputs/eda/ticker_quality")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def main():
    print("Loading news dataset...")

    # ---- CAMBIO PRINCIPAL: leer parquets en lugar de CSV ----
    files = sorted(glob.glob(str(NEWS_DIR / "*.parquet")))
    news = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

    print(f"Total rows: {len(news)}")

    col_mapping = {}
    for col in news.columns:
        c = col.lower()
        if 'url' in c: col_mapping[col] = 'url'
        elif 'symbol' in c or 'ticker' in c: col_mapping[col] = 'ticker'
        elif 'title' in c: col_mapping[col] = 'title'

    news = news.rename(columns=col_mapping)

    # ================================
    # 1. URL duplicated with tickers
    # ================================
    print("\n--- URL duplication analysis ---")
    url_ticker_counts = (
        news.groupby("url")["ticker"]
        .nunique()
        .sort_values(ascending=False)
    )
    print(url_ticker_counts.head(20))
    print(f"Share URLs with multiple tickers: {(url_ticker_counts > 1).mean():.4f}")
    url_ticker_counts.to_csv(OUT_DIR / "url_multi_ticker_counts.csv")

    # ================================
    # 2. Ticker in title
    # ================================
    print("\n--- Ticker in title analysis ---")
    titulos = news['title'].astype(str).str.lower()
    tickers = news['ticker'].astype(str).str.lower()
    news['ticker_in_title'] = [tck in tit for tit, tck in zip(titulos, tickers)]
    print(f"Ticker mentioned in title ratio: {news['ticker_in_title'].mean():.4f}")

    # ================================
    # 3. Top tickers
    # ================================
    print("\n--- Top tickers ---")
    top_tickers = news["ticker"].value_counts()
    print(top_tickers.head(30))
    top_tickers.to_csv(OUT_DIR / "top_tickers.csv")

    # ================================
    # 4. Manual audit sample
    # ================================
    news.sample(200, random_state=42).to_csv(OUT_DIR / "manual_sample.csv", index=False)
    print("\nManual audit sample exported.")

if __name__ == "__main__":
    main()